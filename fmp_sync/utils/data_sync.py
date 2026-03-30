# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""
Data synchronization utilities for FMP_Sync.
Handles sync from FileMaker Server → Frappe via OData v4 API.

Primary direction: FileMaker → Frappe (TS Compare / Truncate & Replace).
Reverse direction: Frappe → FileMaker (placeholder — see reverse_sync.py).

All FileMaker access goes through the OData v4 REST API (no SQL).
Each request carries Basic Auth; there is no persistent connection to close.
"""

import json
from datetime import datetime, timedelta

import frappe
import pytz
from frappe import _
from frappe.utils import now_datetime

from fmp_sync.utils.fm_api import (
	get_fm_session,
	http_timeout as odata_http_timeout,
	odata_get as _odata_get,
	odata_get_all as _odata_get_all,
	build_odata_filter as _build_odata_filter,
	build_odata_select as _build_odata_select,
	count_fm_records as _count_fm_records,
	DEFAULT_TIMEOUT as ODATA_DEFAULT_TIMEOUT,
	_quote_fm_filter_name,
)

# Batch size for upserts to avoid long DB locks
BATCH_SIZE = 500

# OData page size — FM Server default is 1000; explicit $top keeps things predictable
ODATA_PAGE_SIZE = 1000


# =============================================================================
# Helper Functions  (KEEP — data-source agnostic unless noted)
# =============================================================================


def get_frappe_fieldname(fm_col, column_mapping):
	"""
	Get the Frappe fieldname for a FileMaker column using the column mapping.

	Handles both old format (string) and new format (dict with fieldname key).
	Falls back to lowercase FM field name if not in mapping.

	Args:
		fm_col: FileMaker column name
		column_mapping: Dict mapping FM field names to Frappe fieldnames

	Returns:
		Frappe fieldname (string)
	"""
	if column_mapping and fm_col in column_mapping:
		mapping_info = column_mapping[fm_col]
		if isinstance(mapping_info, dict):
			return mapping_info["fieldname"]
		else:
			return mapping_info
	return fm_col.lower()


def build_reverse_mapping(column_mapping):
	"""
	Build a reverse mapping from Frappe fieldnames to FM field names.

	Args:
		column_mapping: Dict mapping FM field names to Frappe fieldnames

	Returns:
		Dict mapping Frappe fieldnames to FM field names
	"""
	reverse = {}
	for fm_col, mapping_info in (column_mapping or {}).items():
		if isinstance(mapping_info, dict):
			reverse[mapping_info["fieldname"]] = fm_col
		else:
			reverse[mapping_info] = fm_col
	return reverse


def _normalize_key_value(value):
	"""
	Normalize a key value for consistent comparison between FM and Frappe.
	Converts to string to handle int/string type mismatches.

	Args:
		value: The value to normalize

	Returns:
		Normalized string value, or None if value is None
	"""
	if value is None:
		return None
	# Convert to string for consistent comparison
	return str(value)


def get_timezone(tz_name):
	"""
	Get a pytz timezone object, falling back to UTC if invalid.

	Args:
		tz_name: Timezone name (e.g., 'America/New_York')

	Returns:
		pytz timezone object
	"""
	if not tz_name:
		return pytz.UTC
	try:
		return pytz.timezone(tz_name)
	except pytz.UnknownTimeZoneError:
		return pytz.UTC


# =============================================================================
# OData helpers — now imported from fm_api.py (single source of truth).
# The names _odata_get, _odata_get_all, _build_odata_filter, _build_odata_select,
# and _count_fm_records are re-imported at the top of this file so existing
# callers (including fm_tables.py) continue to work without changes.
# =============================================================================


def _fetch_changed_records(
	session,
	base_url,
	table_name,
	ts_field,
	create_ts_field,
	cutoff,
	select_fields=None,
	http_timeout=None,
	page_size=None,
):
	"""Fetch records changed since cutoff via OData $filter.

	Args:
		session: requests.Session
		base_url: OData base URL
		table_name: FM table name
		ts_field: FM modification timestamp field name
		create_ts_field: FM creation timestamp field name (may be None)
		cutoff: datetime cutoff (None = fetch all records)
		select_fields: Optional $select string
		http_timeout: Per-request timeout for OData (tuple or int)
		page_size: Rows per OData request (0/None = server paging)

	Returns:
		list of record dicts from OData
	"""
	url = f"{base_url}/{table_name}"
	params = {}

	if cutoff:
		params["$filter"] = _build_odata_filter(ts_field, create_ts_field=create_ts_field, cutoff=cutoff)

	if select_fields:
		params["$select"] = select_fields

	return _odata_get_all(session, url, params=params if params else None, timeout=http_timeout, page_size=page_size)


def _fetch_all_records(session, base_url, table_name, select_fields=None, http_timeout=None, page_size=None):
	"""Fetch all records from a FM table (for Truncate & Replace).

	Args:
		session: requests.Session
		base_url: OData base URL
		table_name: FM table name
		select_fields: Optional $select string
		http_timeout: Per-request timeout for OData (tuple or int)
		page_size: Rows per OData request (0/None = server paging)

	Returns:
		list of record dicts from OData
	"""
	url = f"{base_url}/{table_name}"
	params = {}
	if select_fields:
		params["$select"] = select_fields
	return _odata_get_all(session, url, params=params if params else None, timeout=http_timeout, page_size=page_size)


def _fetch_fm_key_set(
	session,
	base_url,
	table_name,
	matching_keys,
	reverse_mapping,
	column_mapping,
	http_timeout=None,
	page_size=None,
):
	"""Fetch all matching key values from FileMaker via OData.

	Only requests the key fields ($select) to minimise data transfer.

	Args:
		session: requests.Session
		base_url: OData base URL
		table_name: FM table name
		matching_keys: List of Frappe fieldnames used as matching keys
		reverse_mapping: Dict mapping Frappe fieldnames → FM field names
		column_mapping: Dict mapping FM field names → Frappe fieldname info
		http_timeout: Per-request timeout for OData (tuple or int)
		page_size: Rows per OData request (0/None = server paging)

	Returns:
		set of key tuples (normalised to strings)
	"""
	# Build $select with just the key columns
	fm_key_columns = []
	for frappe_key in matching_keys:
		fm_col = reverse_mapping.get(frappe_key, frappe_key)
		fm_key_columns.append(fm_col)

	from fmp_sync.fmp_sync.doctype.filemaker_connection.filemaker_connection import (
		_fm_join_select_clause,
	)
	select_str = _fm_join_select_clause(fm_key_columns)
	url = f"{base_url}/{table_name}"
	rows = _odata_get_all(
		session, url, params={"$select": select_str}, timeout=http_timeout, page_size=page_size
	)

	# Build set of normalised key tuples
	fm_key_set = set()
	for row in rows:
		converted = _convert_row(row, column_mapping)
		key_tuple = tuple(_normalize_key_value(converted.get(k)) for k in matching_keys)
		fm_key_set.add(key_tuple)

	return fm_key_set


def _fetch_records_by_keys(
	session,
	base_url,
	table_name,
	matching_keys,
	reverse_mapping,
	column_mapping,
	key_set,
	select_fields=None,
	http_timeout=None,
	page_size=None,
):
	"""Fetch full rows from FM for a specific set of matching-key tuples.

	For single-column keys, builds batched OData $filter with 'or' chains.
	For composite keys, falls back to fetching all rows and filtering in Python.

	Args:
		session: requests.Session
		base_url: OData base URL
		table_name: FM table name
		matching_keys: List of Frappe fieldnames used as matching keys
		reverse_mapping: Dict mapping Frappe fieldnames → FM field names
		column_mapping: Dict mapping FM field names → Frappe fieldname info
		key_set: Set of key tuples to fetch
		select_fields: Optional $select string
		http_timeout: Per-request timeout for OData (tuple or int)

	Returns:
		list of record dicts from OData
	"""
	if not key_set:
		return []

	url = f"{base_url}/{table_name}"

	if len(matching_keys) == 1:
		fm_col = reverse_mapping.get(matching_keys[0], matching_keys[0])
		values = [k[0] for k in key_set if k[0] is not None]
		if not values:
			return []

		# Batch into groups of 50 to keep $filter URL length manageable
		# (FM OData has URL length limits)
		rows = []
		batch_size = 50
		for i in range(0, len(values), batch_size):
			batch = values[i:i + batch_size]
			# Build $filter: "fm_col" eq 'val1' or "fm_col" eq 'val2' or ...
			# FM OData requires double-quoted field names for names with
			# special chars (including underscores).
			qcol = _quote_fm_filter_name(fm_col)
			clauses = []
			for v in batch:
				# Quote string values, leave numeric values unquoted
				try:
					# If it parses as a number, use unquoted
					float(v)
					clauses.append(f"{qcol} eq {v}")
				except (ValueError, TypeError):
					escaped = str(v).replace("'", "''")
					clauses.append(f"{qcol} eq '{escaped}'")

			filter_expr = " or ".join(clauses)
			params = {"$filter": filter_expr}
			if select_fields:
				params["$select"] = select_fields
			rows.extend(_odata_get_all(session, url, params=params, timeout=http_timeout, page_size=page_size))

		return rows

	# Composite key — fetch all and filter in Python
	params = {}
	if select_fields:
		params["$select"] = select_fields
	all_rows = _odata_get_all(
		session, url, params=params if params else None, timeout=http_timeout, page_size=page_size
	)

	result = []
	for row in all_rows:
		converted = _convert_row(row, column_mapping)
		key_tuple = tuple(_normalize_key_value(converted.get(k)) for k in matching_keys)
		if key_tuple in key_set:
			result.append(row)
	return result


# =============================================================================
# Timestamp / timezone helpers
# =============================================================================


def convert_frappe_ts_to_odata(ts):
	"""Convert a Frappe (server-tz) timestamp to an OData-compatible ISO 8601 string.

	OData v4 DateTimeOffset literals use ISO 8601 format.
	Frappe stores naive datetimes in the server timezone.

	Args:
		ts: datetime (naive, in Frappe server TZ) or None

	Returns:
		str: ISO 8601 with UTC offset, or None
	"""
	if ts is None:
		return None

	frappe_tz = get_timezone(frappe.utils.get_system_timezone())

	if ts.tzinfo is None:
		ts = frappe_tz.localize(ts)

	# Convert to UTC for clean OData filter expressions
	return ts.astimezone(pytz.UTC).isoformat()


def _convert_odata_ts_to_frappe(ts_value):
	"""Convert an OData DateTimeOffset value to a Frappe-native naive datetime.

	OData v4 returns timestamps as ISO 8601 strings with UTC offset baked in
	(e.g. '2026-03-15T14:30:00+00:00'). We parse the offset and convert to
	the Frappe server timezone, then strip tzinfo for Frappe DB storage.

	Args:
		ts_value: str (ISO 8601) or datetime from OData response

	Returns:
		datetime (naive, in Frappe server TZ) or None
	"""
	if ts_value is None:
		return None

	if isinstance(ts_value, str):
		# Python 3.7+ fromisoformat handles most ISO 8601 variants
		# but not the 'Z' suffix — normalise it
		ts_value = ts_value.replace("Z", "+00:00")
		try:
			ts_value = datetime.fromisoformat(ts_value)
		except ValueError:
			return None

	if ts_value.tzinfo is None:
		# Shouldn't happen with OData DateTimeOffset, but guard anyway
		return ts_value

	frappe_tz = get_timezone(frappe.utils.get_system_timezone())
	return ts_value.astimezone(frappe_tz).replace(tzinfo=None)


# =============================================================================
# Main sync entry point
# =============================================================================


def sync_table(fm_table_doc):
	"""
	Main entry point for syncing a FileMaker table to Frappe.

	Reads sync settings from the FM Tables document and dispatches
	to the appropriate sync method (TS Compare or Truncate & Replace).

	All FileMaker data is fetched via OData v4 (no SQL connection).

	Args:
		fm_table_doc: FM Tables document

	Returns:
		dict with sync results (rows_synced, rows_deleted, etc.)
	"""
	if fm_table_doc.mirror_status not in ("Mirrored", "Linked"):
		frappe.throw(_("Table must be in Mirrored or Linked status before syncing"))

	if not fm_table_doc.frappe_doctype:
		frappe.throw(_("No Frappe DocType associated with this table"))

	# Get FileMaker OData session
	fm_conn_doc = frappe.get_single("FileMaker Connection")
	if not fm_conn_doc:
		frappe.throw(_("FileMaker Connection not configured"))

	session, base_url = get_fm_session(fm_conn_doc)
	http_timeout = odata_http_timeout(fm_conn_doc)

	# Determine sync method
	sync_method = fm_table_doc.sync_method or "TS Compare"

	# Get effective timestamp field
	ts_field = _get_effective_ts_field(fm_table_doc)

	try:
		if sync_method == "Truncate & Replace":
			result = _sync_truncate_replace(
				session,
				base_url,
				fm_table_doc,
				fm_conn_doc,
				fm_table_doc.frappe_doctype,
				http_timeout=http_timeout,
			)
		else:
			# Default to TS Compare
			result = _sync_ts_compare(
				session,
				base_url,
				fm_table_doc,
				fm_conn_doc,
				fm_table_doc.frappe_doctype,
				ts_field,
				http_timeout=http_timeout,
			)

	finally:
		session.close()

	# Reverse sync: push Frappe-created records back to FileMaker
	# Only runs when direction is explicitly "Both" and the FM PK maps to Frappe name
	sync_direction = getattr(fm_table_doc, "sync_direction", "FM to Frappe") or "FM to Frappe"
	if sync_direction != "FM to Frappe" and getattr(fm_table_doc, "name_field_column", None):
		from fmp_sync.utils.reverse_sync import sync_frappe_to_fm

		reverse_result = sync_frappe_to_fm(fm_table_doc)
		result["reverse_inserted"] = reverse_result.get("inserted", 0)
		result["reverse_updated"] = reverse_result.get("updated", 0)
		result["reverse_errors"] = reverse_result.get("errors", 0)

	return result


def _get_effective_ts_field(fm_table_doc):
	"""
	Returns the timestamp field to use for sync.

	Uses modified_timestamp_field if available, falls back to created_timestamp_field.
	For Truncate & Replace, returns None (not needed).

	Args:
		fm_table_doc: FM Tables document

	Returns:
		str: Field name or None
	"""
	if fm_table_doc.sync_method == "Truncate & Replace":
		return None

	ts_field = fm_table_doc.modified_timestamp_field or fm_table_doc.created_timestamp_field

	if not ts_field:
		frappe.throw(
			_(
				"No timestamp field configured for table '{0}'. "
				"Either set a timestamp field or use 'Truncate & Replace' sync method."
			).format(fm_table_doc.table_name)
		)

	return ts_field


def _get_matching_keys(fm_table_doc):
	"""
	Parse matching_fields from the FM Tables document.
	Converts FM field names to Frappe fieldnames using column_mapping.
	When name_field_column is set, returns ["name"] for fast direct lookup.

	Args:
		fm_table_doc: FM Tables document

	Returns:
		list of Frappe fieldnames to use as matching keys
	"""
	# When name_field_column is set, use direct name lookup (faster)
	name_field_column = getattr(fm_table_doc, "name_field_column", None)
	if name_field_column:
		return ["name"]

	if not fm_table_doc.matching_fields:
		frappe.throw(
			_(
				"No matching fields configured for table '{0}'. Please re-mirror and select matching fields."
			).format(fm_table_doc.table_name)
		)

	# Get FM field names from matching_fields
	fm_columns = [f.strip() for f in fm_table_doc.matching_fields.split(",") if f.strip()]

	# Convert to Frappe fieldnames using column_mapping
	column_mapping = {}
	if fm_table_doc.column_mapping:
		column_mapping = json.loads(fm_table_doc.column_mapping)

	return [get_frappe_fieldname(fm_col, column_mapping) for fm_col in fm_columns]


def _get_cutoff_timestamp(frappe_doctype, frappe_ts_field, fallback_ts_field=None):
	"""
	Get the cutoff timestamp for incremental sync by finding the latest
	effective timestamp already stored in Frappe.

	Uses GREATEST(COALESCE(mod_ts, create_ts), create_ts) so that rows
	with NULL modified_ts are handled via their created_ts — mirroring
	the same logic applied on the FM query side.

	When no modified_ts field exists at all, uses created_ts directly.

	The returned datetime is in Frappe server timezone (naive).
	The caller converts it to an OData filter expression via convert_frappe_ts_to_odata().

	Args:
		frappe_doctype: Name of the Frappe DocType
		frappe_ts_field: Frappe fieldname for the modified timestamp (may be None)
		fallback_ts_field: Frappe fieldname for the created timestamp (may be None)

	Returns:
		datetime (naive, Frappe server TZ) or None
	"""
	if not frappe_ts_field and not fallback_ts_field:
		return None

	if frappe_ts_field and fallback_ts_field and frappe_ts_field != fallback_ts_field:
		ts_expr = f"GREATEST(COALESCE(`{frappe_ts_field}`, `{fallback_ts_field}`), `{fallback_ts_field}`)"
	elif frappe_ts_field:
		ts_expr = f"`{frappe_ts_field}`"
	else:
		ts_expr = f"`{fallback_ts_field}`"

	max_ts_result = frappe.db.sql(f"SELECT MAX({ts_expr}) FROM `tab{frappe_doctype}`")
	max_ts = max_ts_result[0][0] if max_ts_result else None

	if not max_ts:
		return None

	if isinstance(max_ts, str):
		max_ts = datetime.fromisoformat(max_ts)

	return max_ts


def _publish_sync_progress(table_name, rows_processed, total_rows, user=None):
	"""
	Publish sync progress as toast notifications via realtime.
	Targets the user who triggered the sync so toasts reach their browser.

	Args:
		table_name: FM Tables document name
		rows_processed: Number of rows processed so far
		total_rows: Total rows expected
		user: Username to target (required for background jobs; worker has no session)
	"""
	message = f"{table_name}: {rows_processed} of {total_rows} rows uploaded"
	frappe.publish_realtime(
		"msgprint",
		{"message": message, "indicator": "blue", "alert": 1},
		user=user,
	)
	# Also update the doc so progress is visible on page refresh
	frappe.db.set_value(
		"FM Tables",
		table_name,
		"last_sync_log",
		f"Syncing: {rows_processed} of {total_rows} rows uploaded",
		update_modified=False,
	)
	frappe.db.commit()


# =============================================================================
# Sync methods
# =============================================================================


def _sync_ts_compare(
	session,
	base_url,
	fm_table_doc,
	fm_conn_doc,
	frappe_doctype,
	ts_field,
	http_timeout=None,
):
	"""
	Sync using timestamp comparison method.

	Steps:
	1. Pull matching keys from FM (via OData), diff against Frappe, delete orphans
	2. Pull changed rows ($filter on timestamp field), convert timestamps
	3. Upsert into Frappe DocType by matching key

	Args:
		session: requests.Session with Basic Auth
		base_url: OData base URL
		fm_table_doc: FM Tables document
		fm_conn_doc: FileMaker Connection document
		frappe_doctype: Name of the Frappe DocType
		ts_field: FM timestamp field name for comparison
		http_timeout: ``(connect, read)`` timeout for each OData request

	Returns:
		dict with sync results
	"""
	if http_timeout is None:
		http_timeout = odata_http_timeout(fm_conn_doc)
	table_name = fm_table_doc.table_name
	matching_keys = _get_matching_keys(fm_table_doc)
	sync_user = getattr(fm_table_doc, "_sync_user", None)

	# OData batch size from table config (0 = server-driven paging)
	page_size = getattr(fm_table_doc, "odata_batch_size", None) or 0

	# Load column mapping (FM field name -> {fieldname, ...})
	column_mapping = None
	if fm_table_doc.column_mapping:
		column_mapping = json.loads(fm_table_doc.column_mapping)

	# Build reverse mapping for looking up FM field names from Frappe fieldnames
	reverse_mapping = build_reverse_mapping(column_mapping)

	# Build $select to only fetch mirrored fields
	select_fields = _build_odata_select(column_mapping)

	# Step 1: Delete detection — get all matching keys from FM and delete orphans.
	fm_key_set = _fetch_fm_key_set(
		session,
		base_url,
		table_name,
		matching_keys,
		reverse_mapping,
		column_mapping,
		http_timeout=http_timeout,
		page_size=page_size,
	)
	rows_deleted, frappe_key_set = _delete_orphans(frappe_doctype, matching_keys, fm_key_set)

	# Step 2a: Identify FM rows that are completely missing from Frappe
	missing_keys = fm_key_set - frappe_key_set

	# Step 2b: Get rows changed since last sync (timestamp-based via OData $filter)
	frappe_ts_field = get_frappe_fieldname(ts_field, column_mapping) if ts_field else None
	frappe_create_ts_field = (
		get_frappe_fieldname(fm_table_doc.created_timestamp_field, column_mapping)
		if fm_table_doc.created_timestamp_field
		else None
	)
	cutoff_frappe = _get_cutoff_timestamp(
		frappe_doctype, frappe_ts_field, fallback_ts_field=frappe_create_ts_field
	)

	# Convert cutoff from Frappe server TZ to OData-compatible ISO 8601
	cutoff_odata = None
	if cutoff_frappe:
		frappe_tz = get_timezone(frappe.utils.get_system_timezone())
		cutoff_aware = frappe_tz.localize(cutoff_frappe) if cutoff_frappe.tzinfo is None else cutoff_frappe
		cutoff_odata = cutoff_aware

	changed_rows = _fetch_changed_records(
		session,
		base_url,
		table_name,
		ts_field,
		fm_table_doc.created_timestamp_field,
		cutoff_odata,
		select_fields=select_fields,
		http_timeout=http_timeout,
		page_size=page_size,
	)

	# Build set of keys already covered by the TS-changed fetch so we
	# don't double-count or double-process them in the missing pass.
	changed_keys = set()
	for row in changed_rows:
		converted = _convert_row(row, column_mapping)
		key_tuple = tuple(_normalize_key_value(converted.get(k)) for k in matching_keys)
		changed_keys.add(key_tuple)

	# Step 2c: Fetch missing rows that were NOT already in the TS-changed set
	# (these have old timestamps but were never synced into Frappe)
	missing_keys_only = missing_keys - changed_keys
	missing_rows = _fetch_records_by_keys(
		session,
		base_url,
		table_name,
		matching_keys,
		reverse_mapping,
		column_mapping,
		missing_keys_only,
		select_fields=select_fields,
		http_timeout=http_timeout,
		page_size=page_size,
	)

	total_to_sync = len(changed_rows) + len(missing_rows)

	# Step 3: Upsert — first the TS-changed rows, then the missing rows
	rows_upserted = 0
	rows_inserted = 0
	rows_skipped = 0
	skip_errors = []

	def _upsert_batch(rows):
		nonlocal rows_upserted, rows_inserted, rows_skipped, skip_errors
		for i in range(0, len(rows), BATCH_SIZE):
			batch = rows[i : i + BATCH_SIZE]
			for row in batch:
				try:
					converted_row = _convert_row(row, column_mapping)
					was_new = _upsert_record(frappe_doctype, matching_keys, converted_row)
					rows_upserted += 1
					if was_new:
						rows_inserted += 1
				except Exception as e:
					rows_skipped += 1
					if len(skip_errors) < 10:
						skip_errors.append(str(e)[:200])
					frappe.db.rollback()
				if (rows_upserted + rows_skipped) % 500 == 0:
					_publish_sync_progress(
						fm_table_doc.name,
						rows_upserted,
						total_to_sync,
						user=sync_user,
					)
			frappe.db.commit()

	_upsert_batch(changed_rows)
	if missing_rows:
		_upsert_batch(missing_rows)

	if rows_skipped:
		frappe.log_error(
			title=f"Sync skipped rows: {fm_table_doc.table_name}",
			message=f"Skipped {rows_skipped} rows.\n" + "\n".join(skip_errors),
		)

	# Final progress update (always fires so small tables get at least one toast)
	_publish_sync_progress(
		fm_table_doc.name,
		rows_upserted,
		total_to_sync,
		user=sync_user,
	)

	return {
		"method": "TS Compare",
		"rows_upserted": rows_upserted,
		"rows_inserted": rows_inserted,
		"rows_deleted": rows_deleted,
		"rows_skipped": rows_skipped,
		"total_fm_rows": len(fm_key_set),
		"missing_rows_found": len(missing_rows),
	}


def _sync_truncate_replace(
	session, base_url, fm_table_doc, fm_conn_doc, frappe_doctype, http_timeout=None
):
	"""
	Sync using truncate and replace method.

	Deletes all Frappe records and re-inserts from FileMaker via OData.

	Args:
		session: requests.Session with Basic Auth
		base_url: OData base URL
		fm_table_doc: FM Tables document
		fm_conn_doc: FileMaker Connection document
		frappe_doctype: Name of the Frappe DocType
		http_timeout: ``(connect, read)`` timeout for each OData request

	Returns:
		dict with sync results
	"""
	if http_timeout is None:
		http_timeout = odata_http_timeout(fm_conn_doc)
	table_name = fm_table_doc.table_name

	# OData batch size from table config (0 = server-driven paging)
	page_size = getattr(fm_table_doc, "odata_batch_size", None) or 0

	# Load column mapping (FM field name -> {fieldname, ...})
	column_mapping = None
	if fm_table_doc.column_mapping:
		column_mapping = json.loads(fm_table_doc.column_mapping)

	# Build $select to only fetch mirrored fields
	select_fields = _build_odata_select(column_mapping)

	# Step 1: Delete all existing Frappe records
	frappe.db.delete(frappe_doctype)
	frappe.db.commit()

	# Step 2: Fetch all records from FM via OData (paginated)
	all_rows = _fetch_all_records(
		session,
		base_url,
		table_name,
		select_fields=select_fields,
		http_timeout=http_timeout,
		page_size=page_size,
	)
	total_to_sync = len(all_rows)

	# Step 3: Insert all rows in batches (with in_sync flag to prevent live push-back)
	rows_inserted = 0
	frappe.flags.in_sync = True
	try:
		for i in range(0, len(all_rows), BATCH_SIZE):
			batch = all_rows[i : i + BATCH_SIZE]

			for row in batch:
				converted_row = _convert_row(row, column_mapping)
				_insert_record(frappe_doctype, converted_row)
				rows_inserted += 1

				if rows_inserted % 500 == 0:
					_publish_sync_progress(
						fm_table_doc.name,
						rows_inserted,
						total_to_sync,
						user=getattr(fm_table_doc, "_sync_user", None),
					)

			frappe.db.commit()
	finally:
		frappe.flags.in_sync = False

	# Final progress update (always fires)
	_publish_sync_progress(
		fm_table_doc.name,
		rows_inserted,
		total_to_sync,
		user=getattr(fm_table_doc, "_sync_user", None),
	)

	return {
		"method": "Truncate & Replace",
		"rows_inserted": rows_inserted,
		"rows_deleted": "all",
	}


# =============================================================================
# Row conversion
# =============================================================================


def _convert_row(row, column_mapping=None):
	"""
	Convert an OData record dict for insertion into Frappe:
	- Maps FM field names to Frappe fieldnames using the stored mapping
	- Converts DateTimeOffset strings to naive Frappe-TZ datetimes
	- Casts name field to str (Frappe name is always varchar)

	OData returns JSON with native types (numbers, booleans, strings).
	DateTimeOffset comes as ISO 8601 strings with UTC offset.
	No timezone parameter is needed — the offset is embedded in the value.

	Args:
		row: dict from OData JSON response (FM field names as keys)
		column_mapping: dict mapping FM field names to Frappe fieldnames

	Returns:
		dict with Frappe fieldnames as keys
	"""
	converted = {}
	for fm_key, value in row.items():
		# Skip OData metadata properties
		if fm_key.startswith("@odata") or fm_key.startswith("@"):
			continue

		frappe_key = get_frappe_fieldname(fm_key, column_mapping)

		if isinstance(value, str) and _looks_like_datetime(value):
			converted[frappe_key] = _convert_odata_ts_to_frappe(value)
		elif isinstance(value, datetime):
			converted[frappe_key] = _convert_odata_ts_to_frappe(value)
		elif frappe_key == "name" and value is not None:
			# Frappe name is always varchar — cast to str so integer PKs
			# (e.g. FM auto-enter serial id) match correctly on subsequent syncs
			converted[frappe_key] = str(value)
		else:
			converted[frappe_key] = value
	return converted


def _looks_like_datetime(s):
	"""Quick check if a string looks like an ISO 8601 datetime.

	OData DateTimeOffset format: 2026-03-15T14:30:00+00:00 or 2026-03-15T14:30:00Z
	OData Date format: 2026-03-15
	OData TimeOfDay: 14:30:00

	We only want to convert DateTimeOffset (has 'T' and offset).
	Plain dates and times should pass through as strings for Frappe Date/Time fields.
	"""
	if not s or len(s) < 19:
		return False
	# Must have the 'T' separator and look like a full datetime
	return "T" in s and s[4:5] == "-" and s[7:8] == "-"


# =============================================================================
# Frappe record operations  (KEEP — pure Frappe logic)
# =============================================================================


def _upsert_record(frappe_doctype, matching_keys, row_data):
	"""
	Insert or update a single Frappe document by matching key lookup.
	When matching_keys is ["name"], uses direct frappe.db.exists for faster lookup.

	Sets frappe.flags.in_sync = True while saving so the live_sync hook
	does not push the inbound FM data back out.

	Args:
		frappe_doctype: Name of the Frappe DocType
		matching_keys: List of field names to match on
		row_data: Dict of field values from FileMaker

	Returns:
		bool: True if a new record was inserted, False if an existing one was updated
	"""
	# Check if record exists - use direct name lookup when matching on name (faster)
	if matching_keys == ["name"]:
		name_value = row_data.get("name")
		# Always coerce to str — Frappe name is varchar, FM PK may be an integer
		if name_value is not None:
			name_value = str(name_value)
			row_data["name"] = name_value
		existing = frappe.db.exists(frappe_doctype, name_value) if name_value is not None else None
	else:
		filters = {key: row_data.get(key) for key in matching_keys}
		existing = frappe.db.get_value(frappe_doctype, filters, "name")

	# Get valid field names from DocType meta
	valid_fields = {df.fieldname for df in frappe.get_meta(frappe_doctype).fields}

	frappe.flags.in_sync = True
	try:
		if existing:
			doc = frappe.get_doc(frappe_doctype, existing)
			for key, value in row_data.items():
				if key in valid_fields and key != "name":
					doc.set(key, value)
			doc.flags.ignore_permissions = True
			doc.flags.ignore_mandatory = True
			doc.flags.ignore_links = True
			doc.save()
			return False
		else:
			_insert_record(frappe_doctype, row_data)
			return True
	finally:
		frappe.flags.in_sync = False


def _insert_record(frappe_doctype, row_data):
	"""
	Insert a new Frappe document.

	Args:
		frappe_doctype: Name of the Frappe DocType
		row_data: Dict of field values from FileMaker
	"""
	doc = frappe.new_doc(frappe_doctype)

	# Get valid field names from DocType meta
	valid_fields = {df.fieldname for df in frappe.get_meta(frappe_doctype).fields}
	valid_fields.add("name")  # name is always valid

	for key, value in row_data.items():
		if key in valid_fields:
			# Frappe name is varchar — guard against integer values from FM PKs
			if key == "name" and value is not None:
				value = str(value)
			doc.set(key, value)

	doc.flags.ignore_permissions = True
	doc.flags.ignore_mandatory = True
	doc.flags.ignore_links = True
	doc.insert()


def _delete_orphans(frappe_doctype, matching_keys, fm_key_set):
	"""
	Delete Frappe records whose matching keys are not in the FileMaker key set.

	Skips records with negative integer names — those are new records created
	locally in Frappe (temp IDs) that have not yet been pushed to FileMaker.
	Only deletes records with real positive FM IDs that no longer exist in the source.

	Args:
		frappe_doctype: Name of the Frappe DocType
		matching_keys: List of field names to match on
		fm_key_set: Set of key tuples from FileMaker

	Returns:
		tuple: (deleted_count, frappe_key_set) where frappe_key_set contains
		       all non-temp key tuples currently in Frappe
	"""
	frappe_records = frappe.get_all(frappe_doctype, fields=["name", *matching_keys], limit_page_length=0)

	deleted_count = 0
	frappe_key_set = set()

	for record in frappe_records:
		# Skip temp records (negative integer names) — not yet pushed to FM
		try:
			if int(record.name) < 0:
				continue
		except (ValueError, TypeError):
			pass

		frappe_key = tuple(_normalize_key_value(record.get(k)) for k in matching_keys)

		if frappe_key not in fm_key_set:
			frappe.delete_doc(frappe_doctype, record.name, force=True, ignore_permissions=True)
			deleted_count += 1
		else:
			frappe_key_set.add(frappe_key)

	if deleted_count > 0:
		frappe.db.commit()

	return deleted_count, frappe_key_set


# =============================================================================
# Scheduler / orchestration  (KEEP — pure Frappe logic)
# =============================================================================


def _get_sync_frequency_minutes():
	"""
	Get the global sync frequency from Sync Manager in minutes.

	Returns:
		int: Frequency in minutes (default 60)
	"""
	frequency_map = {
		"Every 5 Minutes": 5,
		"Every 15 Minutes": 15,
		"Every 30 Minutes": 30,
		"Hourly": 60,
		"Every 6 Hours": 360,
		"Daily": 1440,
		"Weekly": 10080,
	}

	try:
		sync_manager = frappe.get_single("Sync Manager")
		return frequency_map.get(sync_manager.sync_frequency, 60)
	except Exception:
		return 60  # Default to hourly


def run_scheduled_syncs():
	"""
	Scheduler entry point: sync all tables that are due.

	Called by Frappe scheduler based on hooks.py configuration.
	Checks each FM Table with auto_sync_active=1 and syncs
	if enough time has passed since last_synced.

	Uses global sync frequency from Sync Manager.
	Also updates Sync Manager status and cleans up old Sync Log records.
	"""
	# Check if syncing is globally enabled
	try:
		sync_manager = frappe.get_single("Sync Manager")
		if sync_manager.syncing_active != "Yes":
			return  # Global sync is disabled
	except Exception:
		return  # Sync Manager not configured

	# Get global sync frequency
	sync_frequency = _get_sync_frequency_minutes()

	# Get all tables eligible for auto-sync
	tables = frappe.get_all(
		"FM Tables",
		filters={
			"auto_sync_active": 1,
			"mirror_status": ["in", ["Mirrored", "Linked"]],
		},
		fields=["name", "table_name", "last_synced"],
	)

	now = now_datetime()
	tables_synced = 0
	tables_failed = 0
	log_messages = []

	for table_info in tables:
		try:
			# Check if sync is due
			last_synced = table_info.last_synced

			if last_synced:
				if isinstance(last_synced, str):
					last_synced = datetime.fromisoformat(last_synced)
				time_since_sync = (now - last_synced).total_seconds() / 60
				if time_since_sync < sync_frequency:
					continue  # Not due yet

			# Sync is due - run it
			fm_table_doc = frappe.get_doc("FM Tables", table_info.name)
			_run_sync_with_status(fm_table_doc)
			tables_synced += 1
			log_messages.append(f"✓ {table_info.table_name}")

		except Exception as e:
			# Log error but continue with other tables
			tables_failed += 1
			log_messages.append(f"✗ {table_info.table_name}: {str(e)[:100]}")
			frappe.log_error(title=f"Scheduled Sync Error: {table_info.table_name}", message=str(e))

	# Update Sync Manager status
	_update_sync_manager_status(sync_manager, sync_frequency, tables_synced, tables_failed, log_messages)

	# Cleanup old Sync Log records (keep only 20 most recent)
	_cleanup_old_sync_logs(keep_count=20)


def _update_sync_manager_status(sync_manager, sync_frequency, tables_synced, tables_failed, log_messages):
	"""
	Update Sync Manager with run status.

	Args:
		sync_manager: Sync Manager document
		sync_frequency: Frequency in minutes
		tables_synced: Count of successfully synced tables
		tables_failed: Count of failed syncs
		log_messages: List of log message strings
	"""
	now = now_datetime()

	sync_manager.last_run = now
	sync_manager.next_scheduled_run = now + timedelta(minutes=sync_frequency)

	if tables_failed == 0 and tables_synced > 0:
		sync_manager.last_run_status = "Success"
	elif tables_failed > 0 and tables_synced > 0:
		sync_manager.last_run_status = "Partial"
	elif tables_failed > 0:
		sync_manager.last_run_status = "Failed"
	else:
		sync_manager.last_run_status = "Success"  # No tables due

	# Build log summary
	if log_messages:
		sync_manager.last_run_log = f"Synced: {tables_synced}, Failed: {tables_failed}\n" + "\n".join(
			log_messages
		)
	else:
		sync_manager.last_run_log = "No tables due for sync"

	sync_manager.save(ignore_permissions=True)
	frappe.db.commit()


def _cleanup_old_sync_logs(keep_count=20):
	"""
	Delete old Sync Log records, keeping only the most recent ones.

	Args:
		keep_count: Number of recent records to keep (default 20)
	"""
	# Get all Sync Log names ordered by creation (newest first)
	all_logs = frappe.get_all(
		"Sync Log",
		fields=["name"],
		order_by="creation desc",
		limit_page_length=0,
	)

	# Delete records beyond keep_count
	if len(all_logs) > keep_count:
		logs_to_delete = all_logs[keep_count:]
		for log in logs_to_delete:
			frappe.delete_doc("Sync Log", log.name, force=True, ignore_permissions=True)

		frappe.db.commit()


def run_sync_for_table(fm_table_name, user=None):
	"""
	Background-job entry point: load the FM Tables doc by name and sync it.
	Sends toast notifications on completion or error to the user who triggered it.

	Args:
		fm_table_name: Name (primary key) of the FM Tables document
		user: Username to receive progress toasts (from frappe.session.user when enqueued)
	"""
	fm_table_doc = frappe.get_doc("FM Tables", fm_table_name)
	fm_table_doc._sync_user = user or frappe.session.user
	label = fm_table_doc.fmp_name or fm_table_doc.table_name

	try:
		_run_sync_with_status(fm_table_doc, suppress_notifications=True)

		frappe.db.commit()
		frappe.publish_realtime(
			"msgprint",
			{"message": f"{label}: Sync complete ✓", "indicator": "green", "alert": True},
			user=fm_table_doc._sync_user,
		)
	except Exception as e:
		frappe.db.commit()
		frappe.publish_realtime(
			"msgprint",
			{"message": f"{label}: Sync failed — {str(e)[:120]}", "indicator": "red", "alert": True},
			user=fm_table_doc._sync_user,
		)
		raise
	finally:
		# Tell the open form to reload so the status badge updates
		frappe.publish_realtime(
			"doc_update",
			{"doctype": "FM Tables", "name": fm_table_name},
			doctype="FM Tables",
			docname=fm_table_name,
		)


def _run_sync_with_status(fm_table_doc, suppress_notifications=False):
	"""
	Run sync and update status fields on the FM Tables document.
	Also creates a Sync Log record for audit trail.

	Args:
		fm_table_doc: FM Tables document
	"""
	import traceback

	# Set status to Running
	fm_table_doc.last_sync_status = "Running"
	fm_table_doc.last_sync_log = "Sync started..."
	fm_table_doc.save()
	frappe.db.commit()

	sync_started = now_datetime()
	sync_method = fm_table_doc.sync_method or "TS Compare"

	if suppress_notifications:
		frappe.flags.in_import = True
		frappe.flags.mute_emails = True

	try:
		result = sync_table(fm_table_doc)

		# Check for anomaly: FM has rows but Frappe table is empty after sync
		total_fm_rows = result.get("total_fm_rows", 0) or result.get("rows_inserted", 0)
		frappe_count = frappe.db.count(fm_table_doc.frappe_doctype)

		if total_fm_rows > 0 and frappe_count == 0:
			fm_table_doc.last_sync_status = "Warning"
			fm_table_doc.last_sync_log = (
				f"ANOMALY: FM has {total_fm_rows} rows but Frappe table is empty. "
				f"last_synced NOT updated - next sync will do full pull. "
				f"Check matching keys and column mapping."
			)
			fm_table_doc.save()

			_create_sync_log(
				fm_table_doc.name,
				sync_method,
				sync_started,
				status="Partial",
				error_message=fm_table_doc.last_sync_log,
			)
			return

		# Calculate record counts
		rows_upserted = result.get("rows_upserted", 0)
		rows_inserted = result.get("rows_inserted", 0)
		rows_deleted = result.get("rows_deleted", 0)
		if rows_deleted == "all":
			rows_deleted = 0
		reverse_inserted = result.get("reverse_inserted", 0)
		reverse_updated = result.get("reverse_updated", 0)

		has_changes = (rows_upserted + rows_deleted + reverse_inserted + reverse_updated) > 0

		# Update status on success
		fm_table_doc.last_synced = now_datetime()
		fm_table_doc.last_sync_status = "Success"

		# Build summary log
		if result.get("method") == "Truncate & Replace":
			fm_table_doc.last_sync_log = f"Truncate & Replace: {rows_inserted} rows inserted"
			records_synced = rows_inserted
			records_created = rows_inserted
			records_updated = 0
		else:
			missing_found = result.get("missing_rows_found", 0)
			ts_rows_inserted = result.get("rows_inserted", 0)
			ts_rows_updated = rows_upserted - ts_rows_inserted

			parts = [f"TS Compare: {rows_upserted} upserted"]
			if ts_rows_inserted:
				parts.append(f"{ts_rows_inserted} new")
			if ts_rows_updated:
				parts.append(f"{ts_rows_updated} updated")
			if missing_found:
				parts.append(f"{missing_found} missing rows recovered")
			parts.append(f"{rows_deleted} deleted")
			parts.append(f"{result.get('total_fm_rows', 0)} total FM rows")
			parts.append(f"{frappe_count} in Frappe")

			fm_table_doc.last_sync_log = ", ".join(parts)
			records_synced = rows_upserted
			records_created = ts_rows_inserted
			records_updated = ts_rows_updated

		fm_table_doc.save()
		frappe.db.commit()

		# Only create a Sync Log record when something actually changed
		if has_changes:
			_create_sync_log(
				fm_table_doc.name,
				sync_method,
				sync_started,
				status="Success",
				records_synced=records_synced,
				records_created=records_created,
				records_updated=records_updated,
				records_deleted=rows_deleted if isinstance(rows_deleted, int) else 0,
			)

	except Exception as e:
		fm_table_doc.last_sync_status = "Error"
		fm_table_doc.last_sync_log = str(e)[:500]
		fm_table_doc.save()

		_create_sync_log(
			fm_table_doc.name,
			sync_method,
			sync_started,
			status="Failed",
			error_message=str(e)[:500],
			error_traceback=traceback.format_exc(),
		)

		frappe.log_error(title=f"Sync Error: {fm_table_doc.table_name}", message=str(e))
		raise

	finally:
		if suppress_notifications:
			frappe.flags.in_import = False
			frappe.flags.mute_emails = False


def _create_sync_log(
	fm_table_name,
	sync_method,
	sync_started,
	status="Success",
	records_synced=0,
	records_created=0,
	records_updated=0,
	records_deleted=0,
	error_message=None,
	error_traceback=None,
):
	"""Create a Sync Log record. Called only when there are actual changes or errors."""
	sync_log = frappe.new_doc("Sync Log")
	sync_log.fm_table = fm_table_name
	sync_log.sync_method = sync_method
	sync_log.status = status
	sync_log.sync_started = sync_started
	sync_log.sync_completed = now_datetime()
	sync_log.duration_seconds = (sync_log.sync_completed - sync_started).total_seconds()
	sync_log.records_synced = records_synced
	sync_log.records_created = records_created
	sync_log.records_updated = records_updated
	sync_log.records_deleted = records_deleted
	if error_message:
		sync_log.error_message = error_message
	if error_traceback:
		sync_log.error_traceback = error_traceback
	sync_log.insert(ignore_permissions=True)
	frappe.db.commit()
