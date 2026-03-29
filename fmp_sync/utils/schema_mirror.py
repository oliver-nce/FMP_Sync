# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""
Schema mirroring utilities for FMP_Sync.
Handles FileMaker schema from the connection cache and Frappe DocType generation.

Data flow:
  1. get_fm_session()          → requests.Session with Basic Auth
  2. discover_base_tables()    → GET service doc + FileMaker_Tables
  3. get_table_schema()        → FileMaker Connection.fm_schema cache only (no $metadata)
  4. classify_field()          → include / skip (unstored calc, container, repeating)
  5. map_edm_to_frappe_type()  → Edm.* → Frappe fieldtype
  6. preview_table_schema()    → UI contract: return field list for user review
  7. mirror_table_schema()     → create/update Frappe Custom DocType
"""

import json
import re

import frappe
from frappe import _
from frappe.utils import cstr

from fmp_sync.utils.workspace_utils import add_to_workspace

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Frappe reserved fieldnames - cannot be used as custom field names
RESTRICTED_FIELDNAMES = (
	"name",
	"parent",
	"creation",
	"owner",
	"modified",
	"modified_by",
	"parentfield",
	"parenttype",
	"file_list",
	"flags",
	"docstatus",
)


# ---------------------------------------------------------------------------
# Field name helpers  (KEEP — data-source agnostic)
# ---------------------------------------------------------------------------


def sanitize_fieldname(fieldname):
	"""Sanitize a fieldname to avoid Frappe restricted names.

	Appends '_field' if the name collides with a Frappe system field.
	"""
	if fieldname.lower() in RESTRICTED_FIELDNAMES:
		return f"{fieldname}_field"
	return fieldname


def normalize_frappe_fieldname_fragment(name):
	"""Reduce arbitrary text to a valid Frappe fieldname fragment (lowercase a-z, 0-9, _)."""
	if not name:
		return ""
	base = frappe.scrub(cstr(name))
	base = re.sub(r"[^a-z0-9_]", "", base)
	base = re.sub(r"_+", "_", base).strip("_")
	return base


def resolve_fieldname(col_name, label_overrides=None, fieldname_overrides=None):
	"""Determine the Frappe fieldname for a FM field.

	FileMaker allows spaces and punctuation (e.g. ``active?``); Frappe allows only
	``[a-z0-9_]``. User ``fieldname_overrides`` (FM column → desired fieldname) win.

	For restricted source names (e.g. 'name'), if a label override is provided the
	fieldname can be derived from that label when no explicit override is given.
	"""
	if fieldname_overrides and col_name in fieldname_overrides:
		norm = normalize_frappe_fieldname_fragment(fieldname_overrides[col_name])
		if not norm:
			norm = normalize_frappe_fieldname_fragment(col_name) or "unnamed_field"
		return sanitize_fieldname(norm)

	col_lower = (col_name or "").lower()
	if col_lower in RESTRICTED_FIELDNAMES and label_overrides and col_name in label_overrides:
		derived = normalize_frappe_fieldname_fragment(label_overrides[col_name])
		if derived and derived.lower() not in RESTRICTED_FIELDNAMES:
			return derived
	scrubbed = normalize_frappe_fieldname_fragment(col_name) or "unnamed_field"
	return sanitize_fieldname(scrubbed)


def get_matching_fields_list(fm_table_doc):
	"""Parse matching_fields from FM Tables document into a list."""
	if not fm_table_doc.matching_fields:
		return []
	return [f.strip() for f in fm_table_doc.matching_fields.split(",") if f.strip()]


# ---------------------------------------------------------------------------
# OData session  (NEW — replaces get_fm_connection / PyMySQL)
# ---------------------------------------------------------------------------


def get_fm_session(fm_conn_doc=None):
	"""Get an OData session and base URL from the FileMaker Connection single doc.

	Args:
		fm_conn_doc: FileMaker Connection document (optional — loads singleton if None)

	Returns:
		tuple: (requests.Session, base_url string)
	"""
	if fm_conn_doc is None:
		fm_conn_doc = frappe.get_single("FileMaker Connection")
	return fm_conn_doc.get_odata_session()


# Keep the old name as an alias so callers that haven't been updated yet still work.
# TODO: remove once all callers are migrated.
get_fm_connection = get_fm_session


# ---------------------------------------------------------------------------
# OData helpers
# ---------------------------------------------------------------------------


def _odata_get(session, url, params=None, timeout=30):
	"""Perform an OData GET request with error handling.

	Args:
		session: requests.Session with auth configured
		url: Full URL to GET
		params: Optional query string params dict
		timeout: Request timeout in seconds

	Returns:
		Parsed JSON response dict

	Raises:
		Exception with descriptive error on failure
	"""
	from fmp_sync.fmp_sync.doctype.filemaker_connection.filemaker_connection import _fm_odata_url

	url = _fm_odata_url(url, params)
	resp = session.get(url, timeout=timeout)

	if resp.status_code == 401:
		frappe.throw(_("OData authentication failed (401). Check credentials."))
	if resp.status_code == 403:
		frappe.throw(_("OData access forbidden (403). Check fmodata privilege."))
	if resp.status_code == 404:
		frappe.throw(_("OData resource not found (404): {0}").format(url))

	resp.raise_for_status()
	return resp.json()


# ---------------------------------------------------------------------------
# Schema discovery  (NEW — replaces information_schema queries)
# ---------------------------------------------------------------------------


def discover_tables_and_views(session_or_conn_doc):
	"""Discover all tables from FileMaker via OData.

	Delegates to the FileMaker Connection doctype's discover_tables() if
	passed a document, or queries OData directly if passed a session tuple.

	This function name is kept for backward compatibility with callers
	(e.g. filemaker_connection.py).

	Args:
		session_or_conn_doc: Either a FileMaker Connection doc or (session, base_url) tuple

	Returns:
		List of dicts: [{"table_name": "...", "table_type": "BASE TABLE"}, ...]
	"""
	if hasattr(session_or_conn_doc, "get_odata_session"):
		# It's a FileMaker Connection doc — delegate
		return session_or_conn_doc.discover_tables()

	# It's a (session, base_url) tuple
	session, base_url = session_or_conn_doc
	return _discover_base_tables(session, base_url)


def _discover_base_tables(session, base_url):
	"""Query OData service document + FileMaker_Tables to find base tables.

	Returns:
		List of dicts: [{"table_name": "...", "table_type": "BASE TABLE"|"TABLE OCCURRENCE"}, ...]
	"""
	# Step 1: Get all entity sets from service document
	data = _odata_get(session, base_url)
	entity_sets = data.get("value", [])
	all_tables = {es.get("name") or es.get("url"): "TABLE" for es in entity_sets}

	# Step 2: Try FileMaker_Tables to distinguish base tables from TOs
	base_tables = set()
	try:
		fm_data = _odata_get(session, f"{base_url}/FileMaker_Tables")
		for row in fm_data.get("value", []):
			table_name = row.get("TableName") or row.get("tableName")
			base_name = row.get("BaseTableName") or row.get("baseTableName")
			if table_name and base_name and table_name == base_name:
				base_tables.add(table_name)
	except Exception:
		pass  # FileMaker_Tables may not be accessible — fall back to all

	result = []
	for table_name in sorted(all_tables.keys()):
		if table_name.startswith("FileMaker_"):
			continue
		result.append({
			"table_name": table_name,
			"table_type": "BASE TABLE" if (table_name in base_tables or not base_tables) else "TABLE OCCURRENCE",
		})
	return result


def get_table_schema(session_or_tuple, table_name, fm_conn_doc):
	"""Get field schema from FileMaker Connection.fm_schema only — never OData $metadata.

	Args:
		session_or_tuple: (session, base_url) tuple (unused; kept for caller compatibility)
		table_name: FileMaker table / table occurrence name
		fm_conn_doc: FileMaker Connection singleton (required)

	Returns:
		Dict with columns, primary_key, unique_keys, indexes, skipped
	"""
	if fm_conn_doc is None:
		frappe.throw(
			_("FileMaker Connection document is required for schema lookup (there is no $metadata fallback).")
		)
	data = _load_fm_schema_payload(fm_conn_doc)
	if not data or not isinstance(data.get("tables"), list) or len(data["tables"]) == 0:
		frappe.throw(
			_(
				"FM schema cache is empty. Open **FileMaker Connection**, click "
				"**Refresh Schema Cache** or **Discover Tables**, then try again."
			)
		)
	cached = _get_table_schema_from_cache_data(data, table_name)
	if cached is None:
		frappe.throw(
			_(
				"Table '{0}' is not in the FM schema cache. Open **FileMaker Connection** and click "
				"**Refresh Schema Cache**, then try again."
			).format(table_name)
		)
	if not cached["columns"]:
		frappe.throw(
			_(
				"No importable fields for table '{0}' in the schema cache (all skipped or empty). "
				"Try **Refresh Schema Cache** on FileMaker Connection."
			).format(table_name)
		)
	return cached


def _load_fm_schema_payload(fm_conn_doc):
	"""Return parsed schema dict with 'tables' from FileMaker Connection.fm_schema, or None."""
	raw = getattr(fm_conn_doc, "fm_schema", None)
	if raw is None:
		return None
	if isinstance(raw, dict):
		return raw if raw.get("tables") is not None else None
	s = str(raw).strip()
	if not s:
		return None
	try:
		data = json.loads(s)
	except (TypeError, ValueError):
		return None
	# JSON field was sometimes assigned json.dumps(str) → one more decode
	if isinstance(data, str):
		try:
			data = json.loads(data)
		except (TypeError, ValueError):
			return None
	if not isinstance(data, dict) or data.get("tables") is None:
		return None
	return data


def _find_cache_table_entry(tables, table_name):
	"""Match FM Tables table_name to a cache row (table_name / base_table_name, case-insensitive)."""
	if not table_name or not tables:
		return None
	want = table_name.strip()
	want_lower = want.lower()
	for t in tables:
		tn = (t.get("table_name") or "").strip()
		if tn == want:
			return t
	for t in tables:
		tn = (t.get("table_name") or "").strip()
		if tn.lower() == want_lower:
			return t
	for t in tables:
		bn = (t.get("base_table_name") or "").strip()
		if bn == want:
			return t
	for t in tables:
		bn = (t.get("base_table_name") or "").strip()
		if bn.lower() == want_lower:
			return t
	return None


def _get_table_schema_from_cache_data(data, table_name):
	"""Build get_table_schema-shaped dict from parsed cache *data*, or None if table missing."""
	tables = data.get("tables") or []
	entry = _find_cache_table_entry(tables, table_name)
	if entry is None:
		return None
	columns = []
	skipped = []
	for f in entry.get("fields") or []:
		field_dict = _cache_row_to_field_dict(f)
		if not field_dict.get("COLUMN_NAME"):
			continue
		classification = classify_field(field_dict)
		if classification == "include":
			columns.append(field_dict)
		else:
			skipped.append({**field_dict, "skip_reason": classification})
	return {
		"columns": columns,
		"primary_key": ["ROWID"],
		"unique_keys": {},
		"indexes": {},
		"skipped": skipped,
	}


def _fm_fieldtype_to_edm(field_type_str):
	"""Map FileMaker_Fields.FieldType SQL keyword to OData EDM type string."""
	if not field_type_str:
		return "Edm.String"
	raw = str(field_type_str).strip().upper()
	base = raw.split("(")[0].strip()
	mapping = {
		"VARCHAR": "Edm.String",
		"TEXT": "Edm.String",
		"INT": "Edm.Int64",
		"NUMERIC": "Edm.Decimal",
		"DECIMAL": "Edm.Decimal",
		"DATE": "Edm.Date",
		"TIME": "Edm.TimeOfDay",
		"TIMESTAMP": "Edm.DateTimeOffset",
		"BLOB": "Edm.Binary",
		"VARBINARY": "Edm.Binary",
	}
	return mapping.get(base, "Edm.String")


def _fm_field_class_computed(field_class_str):
	"""True for Calculated/Summary (unstored calcs); Normal → False."""
	if not field_class_str:
		return False
	fc = str(field_class_str).strip()
	return fc in ("Calculated", "Summary")


def _cache_row_to_field_dict(f):
	"""Normalize a cached FM schema field entry to the dict shape used by classify_field."""
	name = f.get("COLUMN_NAME")
	edm = f.get("EDM_TYPE") or "Edm.String"
	fc = f.get("FIELD_CLASS") or "Normal"
	computed = f.get("COMPUTED")
	if computed is None:
		computed = _fm_field_class_computed(fc)
	max_reps = f.get("MAX_REPETITIONS", 1)
	try:
		max_reps = int(max_reps) if max_reps is not None else 1
	except (TypeError, ValueError):
		max_reps = 1
	ml = f.get("MAX_LENGTH")
	if ml is not None and ml != "":
		try:
			ml = int(ml)
		except (TypeError, ValueError):
			ml = None
	else:
		ml = None
	col_type = f.get("COLUMN_TYPE")
	if not col_type:
		col_type = edm.replace("Edm.", "") if edm else "String"
	return {
		"COLUMN_NAME": name,
		"EDM_TYPE": edm,
		"IS_NULLABLE": f.get("IS_NULLABLE") or "YES",
		"MAX_LENGTH": ml,
		"COMPUTED": bool(computed),
		"AUTO_GENERATED": bool(f.get("AUTO_GENERATED", False)),
		"VERSION_ID": bool(f.get("VERSION_ID", False)),
		"MAX_REPETITIONS": max_reps,
		"COLUMN_TYPE": col_type,
	}


# ---------------------------------------------------------------------------
# Field classification  (NEW)
# ---------------------------------------------------------------------------


def classify_field(field_dict):
	"""Classify a field as include or skip.

	Args:
		field_dict: Dict with COLUMN_NAME, EDM_TYPE, COMPUTED, MAX_REPETITIONS, etc.

	Returns:
		str: "include", "skip_unstored_calc", "skip_container", "skip_repeating"
	"""
	edm_type = field_dict.get("EDM_TYPE", "")

	# Skip container fields (Edm.Binary / Edm.Stream)
	if edm_type in ("Edm.Binary", "Edm.Stream"):
		return "skip_container"

	# Skip repeating fields (MaxRepetitions > 1)
	if field_dict.get("MAX_REPETITIONS", 1) > 1:
		return "skip_repeating"

	# Skip unstored calculations (COMPUTED / FieldClass in cache)
	if field_dict.get("COMPUTED"):
		return "skip_unstored_calc"

	return "include"


# ---------------------------------------------------------------------------
# EDM → Frappe type mapping  (NEW — replaces map_mariadb_to_frappe_type)
# ---------------------------------------------------------------------------


def map_edm_to_frappe_type(field_dict):
	"""Map an OData EDM type to a Frappe field type.

	Args:
		field_dict: Dict with EDM_TYPE, MAX_LENGTH keys

	Returns:
		Dict with fieldtype and optional length, precision, options
	"""
	edm_type = field_dict.get("EDM_TYPE", "Edm.String")
	max_length = field_dict.get("MAX_LENGTH")

	# String types
	if edm_type == "Edm.String":
		if max_length and max_length <= 255:
			return {"fieldtype": "Data", "length": max_length}
		elif max_length and max_length <= 1000:
			return {"fieldtype": "Small Text"}
		else:
			# No MaxLength or very long — use Long Text
			return {"fieldtype": "Long Text"}

	# Numeric types
	if edm_type == "Edm.Decimal":
		return {"fieldtype": "Float"}
	if edm_type in ("Edm.Int64", "Edm.Int32", "Edm.Int16"):
		return {"fieldtype": "Int"}
	if edm_type == "Edm.Double":
		return {"fieldtype": "Float"}
	if edm_type == "Edm.Single":
		return {"fieldtype": "Float"}

	# Boolean
	if edm_type == "Edm.Boolean":
		return {"fieldtype": "Check"}

	# Date/Time types
	if edm_type == "Edm.DateTimeOffset":
		return {"fieldtype": "Datetime"}
	if edm_type == "Edm.Date":
		return {"fieldtype": "Date"}
	if edm_type == "Edm.TimeOfDay":
		return {"fieldtype": "Time"}

	# Binary / Stream (containers) — shouldn't reach here if classify_field skips them,
	# but handle as fallback
	if edm_type in ("Edm.Binary", "Edm.Stream"):
		return {"fieldtype": "Long Text"}

	# Default fallback
	return {"fieldtype": "Data"}


# Keep old name as alias for backward compatibility
map_mariadb_to_frappe_type = map_edm_to_frappe_type


# ---------------------------------------------------------------------------
# Timestamp detection  (reads from cached schema columns)
# ---------------------------------------------------------------------------


def detect_timestamp_fields(session_or_tuple, table_name, fm_conn_doc=None):
	"""Auto-detect created and modified timestamp fields from cached schema.

	Looks for Edm.DateTimeOffset columns. Uses VersionID and common name patterns.

	Args:
		session_or_tuple: (session, base_url) tuple
		table_name: FileMaker table name
		fm_conn_doc: FileMaker Connection doc; loads singleton if omitted

	Returns:
		Dict with 'created' and 'modified' field names (or None)
	"""
	if fm_conn_doc is None:
		fm_conn_doc = frappe.get_single("FileMaker Connection")
	try:
		schema = get_table_schema(session_or_tuple, table_name, fm_conn_doc)

		created_field = None
		modified_field = None

		created_patterns = [
			"creationtimestamp", "created_at", "created", "created_date",
			"date_created", "createtimestamp",
		]
		modified_patterns = [
			"modificationtimestamp", "modified_at", "updated_at", "modified",
			"updated", "last_modified", "last_updated", "modifytimestamp",
		]

		for col in schema["columns"]:
			edm_type = col.get("EDM_TYPE", "")
			if edm_type != "Edm.DateTimeOffset":
				continue

			col_name = col["COLUMN_NAME"]
			col_lower = col_name.lower().replace(" ", "").replace("_", "")

			# VersionID annotation → this IS the modification timestamp
			if col.get("VERSION_ID"):
				modified_field = col_name
				continue

			# Pattern matching
			if not created_field:
				for pattern in created_patterns:
					if pattern in col_lower:
						created_field = col_name
						break

			if not modified_field:
				for pattern in modified_patterns:
					if pattern in col_lower:
						modified_field = col_name
						break

		return {"created": created_field, "modified": modified_field}

	except Exception as e:
		frappe.log_error(title="Timestamp Detection Error", message=str(e))
		return {"created": None, "modified": None}


# ---------------------------------------------------------------------------
# Build Frappe field  (from cached FM field dict)
# ---------------------------------------------------------------------------


def build_frappe_field(
	col,
	schema,
	fm_table_doc,
	field_overrides=None,
	label_overrides=None,
	fieldname_overrides=None,
	idx=1,
):
	"""Build a Frappe field dict from a FileMaker/OData field definition.

	Args:
		col: Field dict from schema['columns'] (has COLUMN_NAME, EDM_TYPE, etc.)
		schema: Full schema dict (for primary_key)
		fm_table_doc: FM Tables document (for timestamp and matching fields)
		field_overrides: Optional dict of {column_name: fieldtype}
		label_overrides: Optional dict of {column_name: label}
		fieldname_overrides: Optional dict of {column_name: frappe_fieldname}
		idx: Field index

	Returns:
		Dict suitable for DocType field definition
	"""
	col_name = col["COLUMN_NAME"]
	safe_fieldname = resolve_fieldname(col_name, label_overrides, fieldname_overrides)
	field_mapping = map_edm_to_frappe_type(col)

	# Apply user override if provided
	if field_overrides and col_name in field_overrides:
		field_mapping["fieldtype"] = field_overrides[col_name]

	# Default label from Frappe fieldname; user label overrides win
	label = safe_fieldname.replace("_", " ").title()
	if label_overrides and col_name in label_overrides:
		label = label_overrides[col_name]

	field = {
		"fieldname": safe_fieldname,
		"fieldtype": field_mapping["fieldtype"],
		"label": label,
		"reqd": 1 if col.get("IS_NULLABLE") == "NO" else 0,
		"idx": idx,
	}

	# Add type-specific properties
	if "length" in field_mapping:
		field["length"] = field_mapping["length"]
	if "precision" in field_mapping:
		field["precision"] = field_mapping["precision"]
	if "options" in field_mapping:
		field["options"] = field_mapping["options"]

	# Mark primary key columns
	if col_name in schema.get("primary_key", []):
		field["unique"] = 1

	# Mark matching/timestamp fields as indexed for query performance
	matching_fields = get_matching_fields_list(fm_table_doc)
	is_matching = col_name in matching_fields
	is_timestamp = col_name in (
		getattr(fm_table_doc, "modified_timestamp_field", None),
		getattr(fm_table_doc, "created_timestamp_field", None),
	)
	if is_matching or is_timestamp:
		field["search_index"] = 1

	return field


# ---------------------------------------------------------------------------
# User-skipped columns (mapping dialog)
# ---------------------------------------------------------------------------


def _parse_user_skipped_columns(raw):
	if not raw:
		return []
	if isinstance(raw, str):
		return [x.strip() for x in raw.split(",") if x.strip()]
	return [str(x).strip() for x in raw if str(x).strip()]


def _filter_schema_columns_for_user_skips(
	schema,
	user_skipped_columns,
	*,
	name_field_column=None,
	modified_ts_field=None,
	created_ts_field=None,
	matching_fields_list=None,
):
	"""Return a copy of schema with listed FM columns removed; validate required roles."""
	us = _parse_user_skipped_columns(user_skipped_columns)
	if not us:
		return schema
	skip_lower = {x.lower() for x in us}
	mf_list = matching_fields_list or []

	for label, val in (
		(_("Frappe ID / name field"), name_field_column),
		(_("Modified timestamp field"), modified_ts_field),
		(_("Created timestamp field"), created_ts_field),
	):
		if val and val.lower() in skip_lower:
			frappe.throw(_("Cannot skip column '{0}': it is selected as {1}.").format(val, label))
	for mf in mf_list:
		if mf.lower() in skip_lower:
			frappe.throw(_("Cannot skip column '{0}': it is used as a matching field.").format(mf))

	out = dict(schema)
	out["columns"] = [c for c in schema["columns"] if c["COLUMN_NAME"].lower() not in skip_lower]
	if out.get("primary_key"):
		out["primary_key"] = [pk for pk in out["primary_key"] if pk.lower() not in skip_lower]
	return out


# ---------------------------------------------------------------------------
# Preview  (uses fm_schema cache)
# ---------------------------------------------------------------------------


def preview_table_schema(fm_conn_doc, fm_table_doc):
	"""Introspect a FileMaker table via OData and return proposed field mappings
	for user review before creating the DocType.

	Args:
		fm_conn_doc: FileMaker Connection document
		fm_table_doc: FM Tables document

	Returns:
		Dict with fields, timestamps, skipped_fields, doctype_name, etc.
	"""
	session_tuple = get_fm_session(fm_conn_doc)
	table_name = fm_table_doc.table_name

	schema = get_table_schema(session_tuple, table_name, fm_conn_doc=fm_conn_doc)
	timestamps = detect_timestamp_fields(session_tuple, table_name, fm_conn_doc=fm_conn_doc)
	session_tuple[0].close()

	# Get previously selected matching fields
	previous_matching = []
	if fm_table_doc.matching_fields:
		previous_matching = [f.strip() for f in fm_table_doc.matching_fields.split(",") if f.strip()]

	previous_name_column = getattr(fm_table_doc, "name_field_column", None) or None

	previous_auto_gen = []
	auto_gen_raw = getattr(fm_table_doc, "auto_generated_columns", None) or ""
	if auto_gen_raw:
		previous_auto_gen = [c.strip().lower() for c in auto_gen_raw.split(",") if c.strip()]

	# Build lookup of existing Frappe field labels (for remap: show saved labels)
	existing_field_labels = {}
	existing_fieldnames = {}
	existing_columns = set()
	if fm_table_doc.frappe_doctype and frappe.db.exists("DocType", fm_table_doc.frappe_doctype):
		col_map_raw = getattr(fm_table_doc, "column_mapping", None)
		col_map = {}
		if col_map_raw:
			col_map = json.loads(col_map_raw)
			existing_columns = set(col_map.keys())

		meta = frappe.get_meta(fm_table_doc.frappe_doctype)
		fieldname_to_label = {df.fieldname: df.label for df in meta.fields}

		for fm_col in existing_columns:
			mapping_info = col_map.get(fm_col, {})
			fn = mapping_info.get("fieldname", fm_col.lower()) if isinstance(mapping_info, dict) else mapping_info
			if isinstance(mapping_info, dict) and mapping_info.get("fieldname"):
				existing_fieldnames[fm_col] = mapping_info["fieldname"]
			if fn in fieldname_to_label:
				existing_field_labels[fm_col] = fieldname_to_label[fn]

	preview = []
	for col in schema["columns"]:
		col_name = col["COLUMN_NAME"]
		field_mapping = map_edm_to_frappe_type(col)

		is_pk = col_name in schema.get("primary_key", [])

		proposed_fieldname = existing_fieldnames.get(col_name) or resolve_fieldname(col_name, None, None)
		# Label from DocType if mirrored; else title-case from proposed Frappe fieldname
		label = existing_field_labels.get(
			col_name,
			proposed_fieldname.replace("_", " ").title(),
		)

		preview.append({
			"column_name": col_name,
			"db_type": col.get("COLUMN_TYPE", col.get("EDM_TYPE", "")),
			"proposed_fieldname": proposed_fieldname,
			"proposed_fieldtype": field_mapping["fieldtype"],
			"label": label,
			"is_nullable": col.get("IS_NULLABLE", "YES"),
			"is_primary_key": is_pk,
			"is_unique": is_pk,  # FM OData: only PK is known to be unique
			"is_indexed": False,  # FM OData: no index metadata
			"is_virtual": col.get("COMPUTED", False),
			"is_auto_increment": col.get("AUTO_GENERATED", False),
			"is_existing": col_name in existing_columns,
			"length": field_mapping.get("length", 0),
			"precision": field_mapping.get("precision", 0),
			"options": field_mapping.get("options", ""),
		})

	preview.sort(key=lambda r: (r["column_name"] or "").lower())

	prev_skip_raw = getattr(fm_table_doc, "user_skipped_columns", None) or ""
	previous_user_skipped = [x.strip().lower() for x in prev_skip_raw.split(",") if x.strip()]

	return {
		"fields": preview,
		"timestamps": timestamps,
		"doctype_name": fm_table_doc.fmp_name or table_name,
		"previous_matching_fields": previous_matching,
		"previous_name_field_column": previous_name_column,
		"previous_auto_generated_columns": previous_auto_gen,
		"previous_modified_ts": getattr(fm_table_doc, "modified_timestamp_field", None) or "",
		"previous_created_ts": getattr(fm_table_doc, "created_timestamp_field", None) or "",
		"previous_user_skipped_columns": previous_user_skipped,
		"skipped_fields": schema.get("skipped", []),
	}


# ---------------------------------------------------------------------------
# Mirror  (MODIFIED — uses fm_schema cache, stores skipped/stored-calc info)
# ---------------------------------------------------------------------------


def _assert_unique_mirror_fieldnames(schema, name_field_column, label_overrides, fieldname_overrides):
	names = []
	for col in schema["columns"]:
		cn = col["COLUMN_NAME"]
		if name_field_column and cn == name_field_column:
			continue
		names.append(resolve_fieldname(cn, label_overrides, fieldname_overrides))
	seen = set()
	dups = set()
	for n in names:
		if n in seen:
			dups.add(n)
		seen.add(n)
	if dups:
		frappe.throw(
			_(
				"Duplicate Frappe field names: {0}. Edit field names in the preview so each is unique."
			).format(", ".join(sorted(dups)))
		)


def mirror_table_schema(
	fm_conn_doc,
	fm_table_doc,
	field_overrides=None,
	label_overrides=None,
	fieldname_overrides=None,
	name_field_column=None,
	auto_generated_columns=None,
	modified_ts_field=None,
	created_ts_field=None,
	user_skipped_columns=None,
):
	"""Mirror a FileMaker table schema to a Frappe Custom DocType.

	Args:
		fm_conn_doc: FileMaker Connection document
		fm_table_doc: FM Tables document
		field_overrides: Optional dict of {column_name: fieldtype}
		label_overrides: Optional dict of {column_name: label}
		fieldname_overrides: Optional dict of {column_name: frappe_fieldname}
		name_field_column: Optional FM field that maps directly to Frappe name
		auto_generated_columns: List of FM field names that are auto-generated
		modified_ts_field: FM field name for modification timestamp
		created_ts_field: FM field name for creation timestamp
		user_skipped_columns: Comma-separated FM column names (or list) to exclude from mirror/sync
	"""
	try:
		session_tuple = get_fm_session(fm_conn_doc)
		table_name = fm_table_doc.table_name

		schema = get_table_schema(session_tuple, table_name, fm_conn_doc=fm_conn_doc)

		matching_list = get_matching_fields_list(fm_table_doc)
		schema = _filter_schema_columns_for_user_skips(
			schema,
			user_skipped_columns,
			name_field_column=name_field_column,
			modified_ts_field=modified_ts_field,
			created_ts_field=created_ts_field,
			matching_fields_list=matching_list,
		)

		# Auto-detect timestamp fields if not already set by user
		if not fm_table_doc.created_timestamp_field or not fm_table_doc.modified_timestamp_field:
			timestamps = detect_timestamp_fields(session_tuple, table_name, fm_conn_doc=fm_conn_doc)
			if not fm_table_doc.created_timestamp_field and timestamps["created"]:
				fm_table_doc.created_timestamp_field = timestamps["created"]
			if not fm_table_doc.modified_timestamp_field and timestamps["modified"]:
				fm_table_doc.modified_timestamp_field = timestamps["modified"]

		session_tuple[0].close()

		# Determine DocType name
		doctype_name = fm_table_doc.fmp_name or table_name

		_assert_unique_mirror_fieldnames(schema, name_field_column, label_overrides, fieldname_overrides)

		# Check if DocType already exists
		if frappe.db.exists("DocType", doctype_name):
			frappe.msgprint(
				_("DocType {0} already exists. Updating fields...").format(doctype_name),
				indicator="orange",
			)
			try:
				update_existing_doctype(
					doctype_name,
					schema,
					fm_table_doc,
					field_overrides,
					label_overrides,
					name_field_column,
					fieldname_overrides=fieldname_overrides,
				)
			except Exception as update_error:
				if "appears multiple times" in str(update_error):
					frappe.log_error(
						title=f"Recreating Broken DocType: {doctype_name}",
						message=f"Update failed with duplicate field error. Deleting and recreating.\n\nError: {update_error!s}",
					)
					frappe.msgprint(
						_("DocType {0} appears broken. Deleting and recreating...").format(doctype_name),
						indicator="orange",
					)
					frappe.delete_doc("DocType", doctype_name, force=True, ignore_permissions=True)
					frappe.db.commit()
					create_custom_doctype(
						doctype_name,
						schema,
						fm_table_doc,
						field_overrides,
						label_overrides,
						name_field_column,
						fieldname_overrides=fieldname_overrides,
					)
				else:
					raise
		else:
			create_custom_doctype(
				doctype_name,
				schema,
				fm_table_doc,
				field_overrides,
				label_overrides,
				name_field_column,
				fieldname_overrides=fieldname_overrides,
			)

		# Build column mapping
		auto_gen_set = set()
		if auto_generated_columns:
			auto_gen_set = {c.strip().lower() for c in auto_generated_columns if c.strip()}

		column_mapping = {}
		for col in schema["columns"]:
			fm_col_name = col["COLUMN_NAME"]
			is_computed = col.get("COMPUTED", False)
			is_auto_gen = col.get("AUTO_GENERATED", False) or fm_col_name.lower() in auto_gen_set

			if name_field_column and fm_col_name == name_field_column:
				column_mapping[fm_col_name] = {
					"fieldname": "name",
					"is_virtual": is_computed,
					"is_auto_generated": is_auto_gen,
					"is_name": True,
				}
			else:
				frappe_fieldname = resolve_fieldname(fm_col_name, label_overrides, fieldname_overrides)
				column_mapping[fm_col_name] = {
					"fieldname": frappe_fieldname,
					"is_virtual": is_computed,
					"is_auto_generated": is_auto_gen,
				}

		# Derive auto_generated_columns string from mapping for storage
		stored_auto_gen = ",".join(
			fm_col for fm_col, info in column_mapping.items() if info.get("is_auto_generated")
		)

		# Store skipped fields info on the FM Tables doc
		skipped_fields = schema.get("skipped", [])

		# Update FM Tables record
		fm_table_doc.frappe_doctype = doctype_name
		fm_table_doc.mirror_status = "Mirrored"
		fm_table_doc.error_log = None
		fm_table_doc.column_mapping = json.dumps(column_mapping)
		fm_table_doc.name_field_column = name_field_column or None
		fm_table_doc.auto_generated_columns = stored_auto_gen or None

		us_list = _parse_user_skipped_columns(user_skipped_columns)
		if hasattr(fm_table_doc, "user_skipped_columns"):
			fm_table_doc.user_skipped_columns = ",".join(sorted(us_list, key=str.lower)) if us_list else None

		# Store skipped fields as JSON (new field on FM Tables doctype)
		if hasattr(fm_table_doc, "skipped_fields"):
			fm_table_doc.skipped_fields = json.dumps(skipped_fields) if skipped_fields else None

		# Timestamp fields: user selection from the mirror dialog takes precedence
		if modified_ts_field:
			fm_table_doc.modified_timestamp_field = modified_ts_field
		if created_ts_field:
			fm_table_doc.created_timestamp_field = created_ts_field

		fm_table_doc.save()

		# Add to workspace
		add_to_workspace(doctype_name, label=fm_table_doc.fmp_name or doctype_name)

		frappe.db.commit()

	except Exception as e:
		fm_table_doc.mirror_status = "Error"
		fm_table_doc.error_log = str(e)
		fm_table_doc.save()
		frappe.db.commit()
		raise


# ---------------------------------------------------------------------------
# DocType creation / update  (KEEP — data-source agnostic)
# ---------------------------------------------------------------------------


def create_custom_doctype(
	doctype_name,
	schema,
	fm_table_doc,
	field_overrides=None,
	label_overrides=None,
	name_field_column=None,
	fieldname_overrides=None,
):
	"""Create a new Custom DocType programmatically.

	Args:
		doctype_name: Name for the new DocType
		schema: Schema dict from get_table_schema
		fm_table_doc: FM Tables document
		field_overrides: Optional dict of {column_name: fieldtype}
		label_overrides: Optional dict of {column_name: label}
		fieldname_overrides: Optional dict of {column_name: frappe_fieldname}
		name_field_column: Optional FM field that maps directly to Frappe name
	"""
	autoname = "hash"
	if name_field_column:
		autoname = "prompt"
	else:
		matching_fields = get_matching_fields_list(fm_table_doc)
		if matching_fields:
			first_match_field = matching_fields[0]
			safe_fieldname = resolve_fieldname(
				first_match_field, label_overrides, fieldname_overrides
			)
			autoname = f"field:{safe_fieldname}"

	fields = []
	idx = 1
	for col in schema["columns"]:
		col_name = col["COLUMN_NAME"]
		if name_field_column and col_name == name_field_column:
			continue
		field = build_frappe_field(
			col,
			schema,
			fm_table_doc,
			field_overrides,
			label_overrides,
			fieldname_overrides,
			idx,
		)
		fields.append(field)
		idx += 1

	doctype_doc = frappe.get_doc({
		"doctype": "DocType",
		"name": doctype_name,
		"module": "FMP Sync",
		"custom": 1,
		"autoname": autoname,
		"fields": fields,
		"permissions": [
			{
				"role": "System Manager",
				"read": 1,
				"write": 1,
				"create": 1,
				"delete": 1,
				"submit": 0,
				"cancel": 0,
				"amend": 0,
			}
		],
		"track_changes": 1,
	})

	doctype_doc.insert(ignore_permissions=True)
	frappe.db.commit()


def update_existing_doctype(
	doctype_name,
	schema,
	fm_table_doc,
	field_overrides=None,
	label_overrides=None,
	name_field_column=None,
	fieldname_overrides=None,
):
	"""Update an existing DocType with new fields from schema.

	Adds missing fields without removing existing ones.
	"""
	doctype_doc = frappe.get_doc("DocType", doctype_name)

	# Update autoname
	if name_field_column:
		new_autoname = "prompt"
	else:
		matching_fields = get_matching_fields_list(fm_table_doc)
		if matching_fields:
			first_match_field = matching_fields[0]
			safe_fieldname = resolve_fieldname(
				first_match_field, label_overrides, fieldname_overrides
			)
			new_autoname = f"field:{safe_fieldname}"
		else:
			new_autoname = doctype_doc.autoname or "hash"

	if doctype_doc.autoname != new_autoname:
		doctype_doc.autoname = new_autoname
		frappe.msgprint(
			_("Updated naming rule. Note: Existing records will keep their old names."),
			indicator="orange",
		)

	existing_fields = {f.fieldname for f in doctype_doc.fields}

	new_fields_added = False
	idx = len(doctype_doc.fields) + 1

	for col in schema["columns"]:
		col_name = col["COLUMN_NAME"]
		if name_field_column and col_name == name_field_column:
			continue
		safe_fieldname = resolve_fieldname(col_name, label_overrides, fieldname_overrides)

		if safe_fieldname not in existing_fields:
			field = build_frappe_field(
				col,
				schema,
				fm_table_doc,
				field_overrides,
				label_overrides,
				fieldname_overrides,
				idx,
			)
			doctype_doc.append("fields", field)
			new_fields_added = True
			idx += 1

	if new_fields_added:
		doctype_doc.save(ignore_permissions=True)
		frappe.db.commit()
	else:
		frappe.msgprint(_("No new fields to add to {0}").format(doctype_name), indicator="blue")
