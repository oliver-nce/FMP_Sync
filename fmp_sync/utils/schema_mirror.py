# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""
Schema mirroring utilities for FMP_Sync.
Handles FileMaker OData schema introspection and Frappe DocType generation.

Data flow:
  1. get_fm_session()          → requests.Session with Basic Auth
  2. discover_base_tables()    → GET service doc + FileMaker_Tables
  3. get_table_schema()        → GET $metadata, parse CSDL XML
  4. classify_field()          → include / skip (unstored calc, container, repeating)
  5. map_edm_to_frappe_type()  → Edm.* → Frappe fieldtype
  6. preview_table_schema()    → UI contract: return field list for user review
  7. mirror_table_schema()     → create/update Frappe Custom DocType
"""

import json
import xml.etree.ElementTree as ET

import frappe
from frappe import _

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

# OData CSDL XML namespaces
_NS = {
	"edmx": "http://docs.oasis-open.org/odata/ns/edmx",
	"edm": "http://docs.oasis-open.org/odata/ns/edm",
}


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


def resolve_fieldname(col_name, label_overrides=None):
	"""Determine the Frappe fieldname for a FM field.

	For restricted source names (e.g. 'name'), if a label override is provided
	the fieldname is derived from the custom label (e.g. 'Event Name' -> 'event_name').
	Otherwise falls back to sanitize_fieldname (which appends '_field').
	"""
	lower = col_name.lower()
	if lower in RESTRICTED_FIELDNAMES and label_overrides and col_name in label_overrides:
		derived = frappe.scrub(label_overrides[col_name])
		if derived and derived not in RESTRICTED_FIELDNAMES:
			return derived
	return sanitize_fieldname(lower)


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
	resp = session.get(url, params=params, timeout=timeout)

	if resp.status_code == 401:
		frappe.throw(_("OData authentication failed (401). Check credentials."))
	if resp.status_code == 403:
		frappe.throw(_("OData access forbidden (403). Check fmodata privilege."))
	if resp.status_code == 404:
		frappe.throw(_("OData resource not found (404): {0}").format(url))

	resp.raise_for_status()
	return resp.json()


def _odata_get_xml(session, url, timeout=30):
	"""Perform an OData GET request expecting XML (e.g. $metadata).

	Returns:
		xml.etree.ElementTree.Element (root)
	"""
	resp = session.get(url, headers={"Accept": "application/xml"}, timeout=timeout)
	resp.raise_for_status()
	return ET.fromstring(resp.content)


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


def get_table_schema(session_or_tuple, table_name):
	"""Get field schema for a FileMaker table via OData $metadata.

	Parses the CSDL XML to extract Property definitions for the named EntityType,
	then classifies each field (include / skip).

	Args:
		session_or_tuple: (session, base_url) tuple
		table_name: FileMaker table name

	Returns:
		Dict with:
		  columns     — list of field dicts (see _parse_entity_type_properties)
		  primary_key — list of key property names (usually ["ROWID"])
		  unique_keys — dict (empty for FM — no unique constraint metadata in OData)
		  indexes     — dict (empty for FM — no index metadata in OData)
		  skipped     — list of dicts for fields that were skipped (unstored calcs, containers, repeating)
	"""
	session, base_url = session_or_tuple
	root = _odata_get_xml(session, f"{base_url}/$metadata")

	# Find the EntityType matching table_name
	columns = []
	primary_key = []
	skipped = []

	for schema_el in root.iter(f"{{{_NS['edm']}}}Schema"):
		for entity_type in schema_el.findall(f"{{{_NS['edm']}}}EntityType"):
			et_name = entity_type.get("Name")
			if et_name != table_name:
				continue

			# Extract primary key
			key_el = entity_type.find(f"{{{_NS['edm']}}}Key")
			if key_el is not None:
				for prop_ref in key_el.findall(f"{{{_NS['edm']}}}PropertyRef"):
					primary_key.append(prop_ref.get("Name"))

			# Extract properties
			for prop in entity_type.findall(f"{{{_NS['edm']}}}Property"):
				field_dict = _property_to_field_dict(prop)
				classification = classify_field(field_dict)

				if classification == "include":
					columns.append(field_dict)
				else:
					skipped.append({**field_dict, "skip_reason": classification})

			break  # Found our EntityType

	if not columns:
		# Maybe the table name in metadata uses a different casing or encoding
		# List available entity types for the error message
		available = []
		for schema_el in root.iter(f"{{{_NS['edm']}}}Schema"):
			for et in schema_el.findall(f"{{{_NS['edm']}}}EntityType"):
				available.append(et.get("Name"))
		frappe.throw(
			_("No fields found for table '{0}' in $metadata. Available EntityTypes: {1}").format(
				table_name, ", ".join(available[:20])
			)
		)

	return {
		"columns": columns,
		"primary_key": primary_key,
		"unique_keys": {},  # FM OData doesn't expose unique constraints
		"indexes": {},  # FM OData doesn't expose index metadata
		"skipped": skipped,
	}


def _property_to_field_dict(prop_element):
	"""Convert an OData CSDL <Property> XML element to a field dict.

	Returns dict with keys:
	  COLUMN_NAME, EDM_TYPE, IS_NULLABLE, MAX_LENGTH,
	  COMPUTED, AUTO_GENERATED, VERSION_ID, MAX_REPETITIONS,
	  COLUMN_TYPE (human-readable type string for preview UI)
	"""
	name = prop_element.get("Name")
	edm_type = prop_element.get("Type", "Edm.String")
	nullable = prop_element.get("Nullable", "true")
	max_length = prop_element.get("MaxLength")

	# Parse annotations from child elements or attributes
	computed = prop_element.get("Computed", "").lower() == "true"
	auto_generated = False
	version_id = False
	max_reps = 1

	for ann in prop_element.findall(f"{{{_NS['edm']}}}Annotation"):
		term = ann.get("Term", "")
		if "AutoGenerated" in term:
			auto_generated = ann.get("Bool", "true").lower() == "true"
		elif "VersionID" in term:
			version_id = ann.get("Bool", "true").lower() == "true"
		elif "MaxRepetitions" in term:
			try:
				max_reps = int(ann.get("Int", "1"))
			except ValueError:
				max_reps = 1

	# Also check for Computed in annotations (some FM versions use annotation instead of attribute)
	if not computed:
		for ann in prop_element.findall(f"{{{_NS['edm']}}}Annotation"):
			term = ann.get("Term", "")
			if "Computed" in term:
				computed = ann.get("Bool", "true").lower() == "true"

	return {
		"COLUMN_NAME": name,
		"EDM_TYPE": edm_type,
		"IS_NULLABLE": "YES" if nullable.lower() == "true" else "NO",
		"MAX_LENGTH": int(max_length) if max_length and max_length.isdigit() else None,
		"COMPUTED": computed,
		"AUTO_GENERATED": auto_generated,
		"VERSION_ID": version_id,
		"MAX_REPETITIONS": max_reps,
		# Human-readable type for the preview UI (replaces MariaDB's COLUMN_TYPE)
		"COLUMN_TYPE": edm_type.replace("Edm.", ""),
	}


# ---------------------------------------------------------------------------
# Field classification  (NEW)
# ---------------------------------------------------------------------------


def classify_field(field_dict):
	"""Classify a field as include or skip.

	Args:
		field_dict: Dict from _property_to_field_dict

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

	# Skip unstored calculations (Computed=true in $metadata)
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
# Timestamp detection  (MODIFIED — reads from parsed $metadata instead of SQL)
# ---------------------------------------------------------------------------


def detect_timestamp_fields(session_or_tuple, table_name):
	"""Auto-detect created and modified timestamp fields from OData schema.

	Looks for Edm.DateTimeOffset properties. Uses:
	  - VersionID annotation → modified timestamp
	  - Common name patterns → created/modified

	Args:
		session_or_tuple: (session, base_url) tuple
		table_name: FileMaker table name

	Returns:
		Dict with 'created' and 'modified' field names (or None)
	"""
	try:
		schema = get_table_schema(session_or_tuple, table_name)

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
# Build Frappe field  (MODIFIED — reads from OData field dict instead of MariaDB column)
# ---------------------------------------------------------------------------


def build_frappe_field(col, schema, fm_table_doc, field_overrides=None, label_overrides=None, idx=1):
	"""Build a Frappe field dict from a FileMaker/OData field definition.

	Args:
		col: Field dict from schema['columns'] (has COLUMN_NAME, EDM_TYPE, etc.)
		schema: Full schema dict (for primary_key)
		fm_table_doc: FM Tables document (for timestamp and matching fields)
		field_overrides: Optional dict of {column_name: fieldtype}
		label_overrides: Optional dict of {column_name: label}
		idx: Field index

	Returns:
		Dict suitable for DocType field definition
	"""
	col_name = col["COLUMN_NAME"]
	safe_fieldname = resolve_fieldname(col_name, label_overrides)
	field_mapping = map_edm_to_frappe_type(col)

	# Apply user override if provided
	if field_overrides and col_name in field_overrides:
		field_mapping["fieldtype"] = field_overrides[col_name]

	# Use label override if provided, otherwise generate from column name
	label = col_name.replace("_", " ").title()
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
# Preview  (MODIFIED — uses OData-sourced schema)
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

	schema = get_table_schema(session_tuple, table_name)
	timestamps = detect_timestamp_fields(session_tuple, table_name)
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
	existing_columns = set()
	if fm_table_doc.frappe_doctype and frappe.db.exists("DocType", fm_table_doc.frappe_doctype):
		col_map_raw = getattr(fm_table_doc, "column_mapping", None)
		if col_map_raw:
			col_map = json.loads(col_map_raw)
			existing_columns = set(col_map.keys())

		meta = frappe.get_meta(fm_table_doc.frappe_doctype)
		fieldname_to_label = {df.fieldname: df.label for df in meta.fields}

		for fm_col in existing_columns:
			mapping_info = col_map.get(fm_col, {})
			fn = mapping_info.get("fieldname", fm_col.lower()) if isinstance(mapping_info, dict) else mapping_info
			if fn in fieldname_to_label:
				existing_field_labels[fm_col] = fieldname_to_label[fn]

	preview = []
	for col in schema["columns"]:
		col_name = col["COLUMN_NAME"]
		field_mapping = map_edm_to_frappe_type(col)

		is_pk = col_name in schema.get("primary_key", [])

		# Use existing Frappe label if available, otherwise auto-generate
		label = existing_field_labels.get(col_name, col_name.replace("_", " ").title())

		preview.append({
			"column_name": col_name,
			"db_type": col.get("COLUMN_TYPE", col.get("EDM_TYPE", "")),
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

	return {
		"fields": preview,
		"timestamps": timestamps,
		"doctype_name": fm_table_doc.fmp_name or table_name,
		"previous_matching_fields": previous_matching,
		"previous_name_field_column": previous_name_column,
		"previous_auto_generated_columns": previous_auto_gen,
		"previous_modified_ts": getattr(fm_table_doc, "modified_timestamp_field", None) or "",
		"previous_created_ts": getattr(fm_table_doc, "created_timestamp_field", None) or "",
		"skipped_fields": schema.get("skipped", []),
	}


# ---------------------------------------------------------------------------
# Mirror  (MODIFIED — uses OData-sourced schema, stores skipped/stored-calc info)
# ---------------------------------------------------------------------------


def mirror_table_schema(
	fm_conn_doc, fm_table_doc, field_overrides=None, label_overrides=None,
	name_field_column=None, auto_generated_columns=None,
	modified_ts_field=None, created_ts_field=None
):
	"""Mirror a FileMaker table schema to a Frappe Custom DocType.

	Args:
		fm_conn_doc: FileMaker Connection document
		fm_table_doc: FM Tables document
		field_overrides: Optional dict of {column_name: fieldtype}
		label_overrides: Optional dict of {column_name: label}
		name_field_column: Optional FM field that maps directly to Frappe name
		auto_generated_columns: List of FM field names that are auto-generated
		modified_ts_field: FM field name for modification timestamp
		created_ts_field: FM field name for creation timestamp
	"""
	try:
		session_tuple = get_fm_session(fm_conn_doc)
		table_name = fm_table_doc.table_name

		schema = get_table_schema(session_tuple, table_name)

		# Auto-detect timestamp fields if not already set by user
		if not fm_table_doc.created_timestamp_field or not fm_table_doc.modified_timestamp_field:
			timestamps = detect_timestamp_fields(session_tuple, table_name)
			if not fm_table_doc.created_timestamp_field and timestamps["created"]:
				fm_table_doc.created_timestamp_field = timestamps["created"]
			if not fm_table_doc.modified_timestamp_field and timestamps["modified"]:
				fm_table_doc.modified_timestamp_field = timestamps["modified"]

		session_tuple[0].close()

		# Determine DocType name
		doctype_name = fm_table_doc.fmp_name or table_name

		# Check if DocType already exists
		if frappe.db.exists("DocType", doctype_name):
			frappe.msgprint(
				_("DocType {0} already exists. Updating fields...").format(doctype_name),
				indicator="orange",
			)
			try:
				update_existing_doctype(
					doctype_name, schema, fm_table_doc, field_overrides, label_overrides, name_field_column
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
						doctype_name, schema, fm_table_doc, field_overrides, label_overrides, name_field_column
					)
				else:
					raise
		else:
			create_custom_doctype(
				doctype_name, schema, fm_table_doc, field_overrides, label_overrides, name_field_column
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
				frappe_fieldname = resolve_fieldname(fm_col_name, label_overrides)
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
	doctype_name, schema, fm_table_doc, field_overrides=None, label_overrides=None, name_field_column=None
):
	"""Create a new Custom DocType programmatically.

	Args:
		doctype_name: Name for the new DocType
		schema: Schema dict from get_table_schema
		fm_table_doc: FM Tables document
		field_overrides: Optional dict of {column_name: fieldtype}
		label_overrides: Optional dict of {column_name: label}
		name_field_column: Optional FM field that maps directly to Frappe name
	"""
	autoname = "hash"
	if name_field_column:
		autoname = "prompt"
	else:
		matching_fields = get_matching_fields_list(fm_table_doc)
		if matching_fields:
			first_match_field = matching_fields[0]
			safe_fieldname = resolve_fieldname(first_match_field, label_overrides)
			autoname = f"field:{safe_fieldname}"

	fields = []
	idx = 1
	for col in schema["columns"]:
		col_name = col["COLUMN_NAME"]
		if name_field_column and col_name == name_field_column:
			continue
		field = build_frappe_field(col, schema, fm_table_doc, field_overrides, label_overrides, idx)
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
	doctype_name, schema, fm_table_doc, field_overrides=None, label_overrides=None, name_field_column=None
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
			safe_fieldname = resolve_fieldname(first_match_field, label_overrides)
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
		safe_fieldname = resolve_fieldname(col_name, label_overrides)

		if safe_fieldname not in existing_fields:
			field = build_frappe_field(col, schema, fm_table_doc, field_overrides, label_overrides, idx)
			doctype_doc.append("fields", field)
			new_fields_added = True
			idx += 1

	if new_fields_added:
		doctype_doc.save(ignore_permissions=True)
		frappe.db.commit()
	else:
		frappe.msgprint(_("No new fields to add to {0}").format(doctype_name), indicator="blue")
