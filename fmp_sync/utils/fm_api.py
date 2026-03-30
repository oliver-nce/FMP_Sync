# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""
FileMaker data-access abstraction layer.

All FileMaker interaction (metadata + record data) funnels through
**get_fm_metadata()** and **get_fm_data()**.  Callers never touch OData
(or any future transport) directly — swap the backend here and
schema_mirror / data_sync / fm_tables keep working unchanged.

Current backend: OData v4 (Basic Auth over HTTPS).
Planned:         FileMaker Data API (token-based REST).

Public API
----------
get_fm_session(fm_conn_doc=None)
    → (requests.Session, base_url)   — authenticated transport handle

get_fm_metadata(fm_conn_doc=None, table_name=None)
    → metadata dict  (tables list  **or**  single-table field schema)

get_fm_data(table_name, *, fm_conn_doc=None, select=None, filter_expr=None,
            top=None, timeout=None)
    → list[dict]  — record rows (all pages collected)

Low-level (re-exported for callers that still need them during migration)
------------------------------------------------------------------------
odata_get, odata_get_all, build_odata_filter, build_odata_select,
count_fm_records
"""

import json
import time

import frappe
import requests as _requests
from frappe import _


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Default HTTP timeouts: (connect_seconds, read_seconds)
DEFAULT_TIMEOUT = (30, 180)

# Retry policy for transient connection failures (ChunkedEncodingError,
# IncompleteRead).  FileMaker's OData endpoint frequently drops the TCP
# connection mid-body when Python's requests sends Accept-Encoding: gzip
# (chunked transfer).  Curl works because it defaults to identity encoding.
ODATA_GET_RETRIES = 3
ODATA_RETRY_BASE_DELAY = 1.0  # seconds; multiplied by attempt number


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------


def get_fm_session(fm_conn_doc=None):
	"""Get an authenticated OData session and base URL.

	Args:
		fm_conn_doc: FileMaker Connection document (loads singleton if None)

	Returns:
		tuple: (requests.Session, base_url str)
	"""
	if fm_conn_doc is None:
		fm_conn_doc = frappe.get_single("FileMaker Connection")
	return fm_conn_doc.get_odata_session()


# Legacy alias — remove once all callers are migrated.
get_fm_connection = get_fm_session


def _resolve_session(fm_conn_doc=None, _session_tuple=None):
	"""Internal: return (session, base_url, fm_conn_doc) from flexible input.

	Callers may pass an explicit fm_conn_doc, a pre-built (session, base_url)
	tuple, or nothing (singleton loaded).
	"""
	if _session_tuple is not None:
		session, base_url = _session_tuple
		if fm_conn_doc is None:
			fm_conn_doc = frappe.get_single("FileMaker Connection")
		return session, base_url, fm_conn_doc

	if fm_conn_doc is None:
		fm_conn_doc = frappe.get_single("FileMaker Connection")
	session, base_url = fm_conn_doc.get_odata_session()
	return session, base_url, fm_conn_doc


def http_timeout(fm_conn_doc):
	"""Return ``(connect, read)`` timeout tuple from FileMaker Connection settings.

	Read timeout applies per OData page; slow servers / large payloads may need more.
	"""
	read = getattr(fm_conn_doc, "odata_read_timeout", None)
	if read in (None, ""):
		read = DEFAULT_TIMEOUT[1]
	try:
		read = int(read)
	except (TypeError, ValueError):
		read = DEFAULT_TIMEOUT[1]
	read = max(30, min(read, 900))
	connect = min(30, max(5, read // 6))
	return (connect, read)


# ---------------------------------------------------------------------------
# Low-level OData transport  (single source of truth)
# ---------------------------------------------------------------------------


def odata_get(session, url, params=None, timeout=None):
	"""Single OData GET with retry logic, error handling, and JSON parsing.

	FileMaker's OData endpoint frequently drops the TCP connection mid-body
	(``IncompleteRead`` / ``ChunkedEncodingError``) when the client advertises
	``Accept-Encoding: gzip``.  This function:

	1. Forces ``Accept-Encoding: identity`` so FM sends an uncompressed,
	   non-chunked response (matching curl's default behaviour).
	2. Retries up to ``ODATA_GET_RETRIES`` times on transient connection
	   errors with exponential back-off.

	Args:
		session: requests.Session with Basic Auth
		url: Full OData URL
		params: Optional dict of query parameters ($filter, $select, …)
		timeout: ``requests`` timeout — int/float or ``(connect, read)`` tuple

	Returns:
		Parsed JSON response dict

	Raises:
		Exception with descriptive message on HTTP errors
	"""
	if timeout is None:
		timeout = DEFAULT_TIMEOUT
	elif isinstance(timeout, (int, float)):
		t = int(timeout)
		timeout = (min(30, max(5, t // 4)), t)

	from fmp_sync.fmp_sync.doctype.filemaker_connection.filemaker_connection import (
		_fm_odata_url,
	)

	url = _fm_odata_url(url, params)

	# Force identity encoding to avoid FM's broken chunked-transfer responses.
	headers = {"Accept-Encoding": "identity"}

	resp = _odata_get_with_retries(session, url, timeout=timeout, headers=headers)

	if resp.status_code == 401:
		frappe.throw(_("OData authentication failed (401). Check credentials."))
	if resp.status_code == 403:
		frappe.throw(_("OData access forbidden (403). Check fmodata privilege."))
	if resp.status_code == 404:
		frappe.throw(_("OData resource not found (404): {0}").format(resp.url))

	resp.raise_for_status()
	return resp.json()


def _odata_get_with_retries(session, url, timeout=None, headers=None):
	"""HTTP GET with retries on transient connection errors.

	Handles ``ChunkedEncodingError`` and ``ConnectionError`` which FileMaker
	OData commonly raises when the response body is truncated.

	Returns:
		requests.Response
	"""
	last_exc = None
	for attempt in range(ODATA_GET_RETRIES):
		try:
			return session.get(url, timeout=timeout, headers=headers)
		except (
			_requests.exceptions.ChunkedEncodingError,
			_requests.exceptions.ConnectionError,
		) as exc:
			last_exc = exc
			if attempt + 1 >= ODATA_GET_RETRIES:
				raise
			delay = ODATA_RETRY_BASE_DELAY * (attempt + 1)
			frappe.logger("fmp_sync").warning(
				"OData GET attempt %d/%d failed (%s), retrying in %.1fs: %s",
				attempt + 1,
				ODATA_GET_RETRIES,
				type(exc).__name__,
				delay,
				url,
			)
			time.sleep(delay)


def odata_get_all(session, url, params=None, timeout=None, page_size=None):
	"""Paginated OData GET — collects all records across multiple requests.

	Two pagination strategies:

	1. **Explicit $top/$skip** (when ``page_size`` is set) — each request asks for
	   exactly ``page_size`` rows.  This keeps every HTTP response body small and
	   avoids the ``IncompleteRead`` / ``ChunkedEncodingError`` that FileMaker
	   triggers on large payloads.  Pagination stops when a page returns fewer
	   than ``page_size`` rows.

	2. **Server-driven** (when ``page_size`` is None/0) — follows
	   ``@odata.nextLink`` until FM says there are no more pages.  This is the
	   original behaviour and relies on FM's default page size (~1 000 rows).

	Args:
		session: requests.Session with Basic Auth
		url: Initial OData entity-set URL
		params: Initial query params ($filter, $select, $orderby, …)
		timeout: Per-request timeout (int or ``(connect, read)`` tuple)
		page_size: Rows per request.  None or 0 = server-driven paging.

	Returns:
		list of record dicts (all pages combined)
	"""
	if timeout is None:
		timeout = DEFAULT_TIMEOUT

	page_size = int(page_size) if page_size else 0

	if page_size > 0:
		return _odata_get_all_batched(session, url, params, timeout, page_size)
	else:
		return _odata_get_all_nextlink(session, url, params, timeout)


def _odata_get_all_nextlink(session, url, params, timeout):
	"""Server-driven pagination via @odata.nextLink."""
	all_records = []
	next_url = url
	next_params = params

	while next_url:
		data = odata_get(session, next_url, params=next_params, timeout=timeout)
		all_records.extend(data.get("value", []))
		next_url = data.get("@odata.nextLink")
		next_params = None

	return all_records


def _odata_get_all_batched(session, url, params, timeout, page_size):
	"""Client-driven pagination via explicit $top/$skip."""
	all_records = []
	skip = 0
	base_params = dict(params or {})

	while True:
		page_params = dict(base_params)
		page_params["$top"] = str(page_size)
		if skip > 0:
			page_params["$skip"] = str(skip)

		data = odata_get(session, url, params=page_params, timeout=timeout)
		batch = data.get("value", [])
		all_records.extend(batch)

		if len(batch) < page_size:
			# Last page — fewer rows than requested means we're done.
			break

		skip += len(batch)

	return all_records


# ---------------------------------------------------------------------------
# OData query builders
# ---------------------------------------------------------------------------


def build_odata_filter(ts_field, cutoff, create_ts_field=None):
	"""Build an OData ``$filter`` for changed-row detection.

	Args:
		ts_field: FM modification-timestamp field name
		cutoff: datetime — rows newer than this are returned
		create_ts_field: Optional FM creation-timestamp field

	Returns:
		str: OData $filter expression
	"""
	if cutoff.tzinfo is None:
		iso = cutoff.isoformat() + "Z"
	else:
		iso = cutoff.isoformat()

	expr = f"{ts_field} gt {iso}"
	if create_ts_field and create_ts_field != ts_field:
		expr = f"({ts_field} gt {iso} or {create_ts_field} gt {iso})"
	return expr


def build_odata_select(column_mapping):
	"""Build an OData ``$select`` from a column_mapping dict.

	Only requests fields present in the mapping (avoids containers, skipped
	fields, etc.).  Non-simple FM names are double-quoted per FM OData rules.

	Returns:
		str or None
	"""
	if not column_mapping:
		return None
	from fmp_sync.fmp_sync.doctype.filemaker_connection.filemaker_connection import (
		_fm_join_select_clause,
	)

	return _fm_join_select_clause(list(column_mapping.keys()))


def count_fm_records(session, base_url, table_name, filter_expr=None, timeout=None):
	"""Record count via OData ``$count``.

	Returns:
		int
	"""
	url = f"{base_url}/{table_name}/$count"
	params = {}
	if filter_expr:
		params["$filter"] = filter_expr

	t = timeout if timeout is not None else DEFAULT_TIMEOUT
	from fmp_sync.fmp_sync.doctype.filemaker_connection.filemaker_connection import (
		_fm_odata_url,
	)

	url = _fm_odata_url(url, params if params else None)
	resp = _odata_get_with_retries(
		session, url, timeout=t, headers={"Accept-Encoding": "identity"}
	)
	resp.raise_for_status()

	try:
		return int(resp.text.strip())
	except (ValueError, TypeError):
		return 0


# ═══════════════════════════════════════════════════════════════════════════
# HIGH-LEVEL PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════


def get_fm_metadata(fm_conn_doc=None, table_name=None):
	"""Retrieve FileMaker schema metadata.

	This is the **single entry point** for all schema/metadata retrieval.
	Swap the implementation here when migrating to a different FM API.

	Usage:
		# Discover all tables
		tables = get_fm_metadata()

		# Get field schema for one table
		schema = get_fm_metadata(table_name="Contacts")

	Args:
		fm_conn_doc: FileMaker Connection doc (loads singleton if None)
		table_name: If provided, return field-level schema for that table.
		            If None, return the list of available tables.

	Returns:
		If table_name is None:
			list of dicts: [{"table_name": "...", "table_type": "BASE TABLE"}, ...]

		If table_name is given:
			dict with keys: columns, primary_key, unique_keys, indexes, skipped
	"""
	if fm_conn_doc is None:
		fm_conn_doc = frappe.get_single("FileMaker Connection")

	if table_name is None:
		# ── Discover tables ──────────────────────────────────────────
		return _metadata_discover_tables(fm_conn_doc)
	else:
		# ── Field schema for one table ───────────────────────────────
		return _metadata_table_schema(fm_conn_doc, table_name)


def get_fm_data(
	table_name,
	*,
	fm_conn_doc=None,
	session_tuple=None,
	select=None,
	filter_expr=None,
	top=None,
	page_size=None,
	timeout=None,
):
	"""Retrieve record data from a FileMaker table.

	This is the **single entry point** for all record-level data retrieval.
	Swap the implementation here when migrating to a different FM API.

	Usage:
		# All records
		rows = get_fm_data("Contacts")

		# With OData $filter + $select
		rows = get_fm_data(
		    "Contacts",
		    select="FirstName,LastName,Email",
		    filter_expr="ModifiedTS gt 2026-03-01T00:00:00Z",
		)

		# First N rows
		rows = get_fm_data("Contacts", top=1)

		# Batched (100 rows per request)
		rows = get_fm_data("Contacts", page_size=100)

	Args:
		table_name: FileMaker table / table-occurrence name
		fm_conn_doc: FileMaker Connection doc (loads singleton if None)
		session_tuple: Optional pre-built (session, base_url) — avoids
		               creating a new session when the caller already has one.
		select: OData ``$select`` string (comma-separated FM field names)
		filter_expr: OData ``$filter`` expression string
		top: Limit to first N records (``$top``)
		page_size: Rows per OData request ($top/$skip batching).
		           None or 0 = server-driven paging.
		timeout: Per-request HTTP timeout (int or (connect, read) tuple)

	Returns:
		list of record dicts (raw FM field names as keys)
	"""
	session, base_url, fm_conn_doc = _resolve_session(fm_conn_doc, session_tuple)
	if timeout is None:
		timeout = http_timeout(fm_conn_doc)

	try:
		return _data_fetch_records(
			session,
			base_url,
			table_name,
			select=select,
			filter_expr=filter_expr,
			top=top,
			page_size=page_size,
			timeout=timeout,
		)
	finally:
		# Only close if we created the session ourselves
		if session_tuple is None:
			session.close()


# ---------------------------------------------------------------------------
# Backend implementations  (private — swap these for a new transport)
# ---------------------------------------------------------------------------


def _metadata_discover_tables(fm_conn_doc):
	"""OData backend: discover tables via service document + FileMaker_Tables."""
	if hasattr(fm_conn_doc, "discover_tables"):
		return fm_conn_doc.discover_tables()

	session, base_url = fm_conn_doc.get_odata_session()
	try:
		return _odata_discover_base_tables(session, base_url)
	finally:
		session.close()


def _odata_discover_base_tables(session, base_url):
	"""Query OData service document + FileMaker_Tables to list base tables."""
	# Step 1: entity sets from service document
	data = odata_get(session, base_url)
	entity_sets = data.get("value", [])
	all_tables = {es.get("name") or es.get("url"): "TABLE" for es in entity_sets}

	# Step 2: distinguish base tables from table occurrences
	base_tables = set()
	try:
		fm_data = odata_get(session, f"{base_url}/FileMaker_Tables")
		for row in fm_data.get("value", []):
			table_name = row.get("TableName") or row.get("tableName")
			base_name = row.get("BaseTableName") or row.get("baseTableName")
			if table_name and base_name and table_name == base_name:
				base_tables.add(table_name)
	except Exception:
		pass  # FileMaker_Tables may not be accessible

	result = []
	for tbl in sorted(all_tables.keys()):
		if tbl.startswith("FileMaker_"):
			continue
		result.append({
			"table_name": tbl,
			"table_type": "BASE TABLE" if (tbl in base_tables or not base_tables) else "TABLE OCCURRENCE",
		})
	return result


def _metadata_table_schema(fm_conn_doc, table_name):
	"""OData backend: field schema from fm_schema cache on FileMaker Connection.

	Uses the cached OData system-table data (FileMaker_Fields etc.) stored
	in fm_conn_doc.fm_schema.  Does NOT hit $metadata.
	"""
	# Delegate to schema_mirror which owns the cache-parsing logic.
	# Import here to avoid circular import (schema_mirror imports from us).
	from fmp_sync.utils.schema_mirror import get_table_schema as _sm_get_table_schema

	session, base_url = fm_conn_doc.get_odata_session()
	try:
		return _sm_get_table_schema((session, base_url), table_name, fm_conn_doc)
	finally:
		session.close()


def _data_fetch_records(session, base_url, table_name, *, select=None,
                        filter_expr=None, top=None, page_size=None, timeout=None):
	"""OData backend: fetch records with optional $filter / $select / $top / batching."""
	url = f"{base_url}/{table_name}"
	params = {}

	if filter_expr:
		params["$filter"] = filter_expr
	if select:
		params["$select"] = select
	if top is not None:
		params["$top"] = str(int(top))

	if top is not None:
		# When $top is set, a single page is enough — no pagination needed
		data = odata_get(session, url, params=params if params else None, timeout=timeout)
		return data.get("value", [])
	else:
		return odata_get_all(
			session, url, params=params if params else None,
			timeout=timeout, page_size=page_size,
		)
