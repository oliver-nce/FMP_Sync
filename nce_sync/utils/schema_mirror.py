# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""
Schema mirroring utilities for NCE_Sync.
Handles WordPress database introspection and Frappe DocType generation.
"""

import frappe
import pymysql
from frappe import _

from nce_sync.utils.workspace_utils import add_to_workspace

# Frappe reserved fieldnames - cannot be used as custom field names
# These are system fields used internally by Frappe
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


def sanitize_fieldname(fieldname):
	"""
	Sanitize a fieldname to avoid Frappe restricted names.

	Args:
		fieldname: Original fieldname (lowercase)

	Returns:
		Safe fieldname (appends '_field' if restricted)
	"""
	if fieldname.lower() in RESTRICTED_FIELDNAMES:
		return f"{fieldname}_field"
	return fieldname


def get_matching_fields_list(wp_table_doc):
	"""
	Parse matching_fields from WP Tables document into a list.

	Args:
		wp_table_doc: WP Tables document

	Returns:
		List of matching field column names
	"""
	if not wp_table_doc.matching_fields:
		return []
	return [f.strip() for f in wp_table_doc.matching_fields.split(",") if f.strip()]


def build_frappe_field(col, schema, wp_table_doc, field_overrides=None, label_overrides=None, idx=1):
	"""
	Build a Frappe field dict from a WordPress column definition.

	Args:
		col: Column dict from schema['columns']
		schema: Full schema dict (for unique_keys, indexes)
		wp_table_doc: WP Tables document (for timestamp fields)
		field_overrides: Optional dict of {column_name: fieldtype}
		label_overrides: Optional dict of {column_name: label}
		idx: Field index

	Returns:
		Dict suitable for DocType field definition
	"""
	col_name = col["COLUMN_NAME"]
	safe_fieldname = sanitize_fieldname(col_name.lower())
	field_mapping = map_mariadb_to_frappe_type(col)

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
		"reqd": 1 if col["IS_NULLABLE"] == "NO" else 0,
		"idx": idx,
	}

	# Add type-specific properties
	if "length" in field_mapping:
		field["length"] = field_mapping["length"]
	if "precision" in field_mapping:
		field["precision"] = field_mapping["precision"]
	if "options" in field_mapping:
		field["options"] = field_mapping["options"]

	# Mark unique columns (only from actual DB unique constraints)
	if any(col_name in uk for uk in schema["unique_keys"].values()):
		field["unique"] = 1

	# Mark indexed columns (from DB indexes OR matching fields OR timestamp fields)
	matching_fields = get_matching_fields_list(wp_table_doc)
	is_indexed = any(col_name in idx_cols for idx_cols in schema["indexes"].values())
	is_matching = col_name in matching_fields
	is_timestamp = col_name in (wp_table_doc.modified_timestamp_field, wp_table_doc.created_timestamp_field)

	if is_indexed or is_matching or is_timestamp:
		field["search_index"] = 1

	return field


def get_wp_connection(wp_conn_doc):
	"""
	Establish PyMySQL connection to WordPress database.

	Args:
		wp_conn_doc: WordPress Connection document

	Returns:
		pymysql.Connection or None
	"""
	try:
		conn = pymysql.connect(
			host=wp_conn_doc.host,
			port=wp_conn_doc.port or 3306,
			user=wp_conn_doc.username,
			password=wp_conn_doc.get_password("password"),
			database=wp_conn_doc.database,
			charset="utf8mb4",
			cursorclass=pymysql.cursors.DictCursor,
		)
		return conn
	except Exception as e:
		frappe.log_error(title="WordPress Connection Error", message=str(e))
		raise


def discover_tables_and_views(conn):
	"""
	Discover all tables and views from WordPress database.

	Args:
		conn: PyMySQL connection

	Returns:
		List of dicts with table_name and table_type
	"""
	try:
		cursor = conn.cursor()
		query = """
			SELECT TABLE_NAME, TABLE_TYPE
			FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = DATABASE()
			ORDER BY TABLE_NAME
		"""
		cursor.execute(query)
		results = cursor.fetchall()
		cursor.close()

		# Transform to simpler format
		tables_and_views = []
		for row in results:
			table_type = "View" if row["TABLE_TYPE"] == "VIEW" else "Table"
			tables_and_views.append({"table_name": row["TABLE_NAME"], "table_type": table_type})

		return tables_and_views

	except Exception as e:
		frappe.log_error(title="Table Discovery Error", message=str(e))
		raise


def detect_timestamp_fields(conn, table_name):
	"""
	Auto-detect created and modified timestamp fields.

	Args:
		conn: PyMySQL connection
		table_name: Name of the table

	Returns:
		Dict with 'created' and 'modified' field names (or None)
	"""
	try:
		cursor = conn.cursor()
		query = """
			SELECT COLUMN_NAME, DATA_TYPE, COLUMN_DEFAULT, EXTRA
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE()
			AND TABLE_NAME = %s
			AND DATA_TYPE IN ('datetime', 'timestamp')
			ORDER BY ORDINAL_POSITION
		"""
		cursor.execute(query, (table_name,))
		timestamp_columns = cursor.fetchall()
		cursor.close()

		created_field = None
		modified_field = None

		# Common patterns for created timestamp
		created_patterns = [
			"created_at",
			"created",
			"created_date",
			"create_time",
			"date_created",
		]
		# Common patterns for modified timestamp
		modified_patterns = [
			"modified_at",
			"updated_at",
			"modified",
			"updated",
			"last_modified",
			"last_updated",
			"update_time",
		]

		for col in timestamp_columns:
			col_name_lower = col["COLUMN_NAME"].lower()

			# Check for created field
			if not created_field:
				if col_name_lower in created_patterns:
					created_field = col["COLUMN_NAME"]
				# Also check for CURRENT_TIMESTAMP default without ON UPDATE
				elif (
					"CURRENT_TIMESTAMP" in (col["COLUMN_DEFAULT"] or "").upper()
					and "on update" not in (col["EXTRA"] or "").lower()
				):
					created_field = col["COLUMN_NAME"]

			# Check for modified field
			if not modified_field:
				if col_name_lower in modified_patterns:
					modified_field = col["COLUMN_NAME"]
				# Also check for ON UPDATE CURRENT_TIMESTAMP
				elif "on update" in (col["EXTRA"] or "").lower():
					modified_field = col["COLUMN_NAME"]

		return {"created": created_field, "modified": modified_field}

	except Exception as e:
		frappe.log_error(title="Timestamp Detection Error", message=str(e))
		return {"created": None, "modified": None}


def get_table_schema(conn, table_name):
	"""
	Get full schema information for a table.

	Args:
		conn: PyMySQL connection
		table_name: Name of the table

	Returns:
		Dict with columns, primary_key, unique_keys, indexes
	"""
	try:
		cursor = conn.cursor()

		# Get columns (including GENERATION_EXPRESSION for virtual/computed columns)
		query = """
			SELECT
				COLUMN_NAME,
				DATA_TYPE,
				CHARACTER_MAXIMUM_LENGTH,
				NUMERIC_PRECISION,
				NUMERIC_SCALE,
				IS_NULLABLE,
				COLUMN_DEFAULT,
				EXTRA,
				COLUMN_TYPE,
				GENERATION_EXPRESSION
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE()
			AND TABLE_NAME = %s
			ORDER BY ORDINAL_POSITION
		"""
		cursor.execute(query, (table_name,))
		columns = cursor.fetchall()

		# Get primary key
		query = """
			SELECT COLUMN_NAME
			FROM information_schema.KEY_COLUMN_USAGE
			WHERE TABLE_SCHEMA = DATABASE()
			AND TABLE_NAME = %s
			AND CONSTRAINT_NAME = 'PRIMARY'
			ORDER BY ORDINAL_POSITION
		"""
		cursor.execute(query, (table_name,))
		primary_key = [row["COLUMN_NAME"] for row in cursor.fetchall()]

		# Get unique keys
		query = """
			SELECT CONSTRAINT_NAME, COLUMN_NAME
			FROM information_schema.KEY_COLUMN_USAGE
			WHERE TABLE_SCHEMA = DATABASE()
			AND TABLE_NAME = %s
			AND CONSTRAINT_NAME != 'PRIMARY'
			ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION
		"""
		cursor.execute(query, (table_name,))
		unique_key_rows = cursor.fetchall()

		# Group by constraint name
		unique_keys = {}
		for row in unique_key_rows:
			constraint = row["CONSTRAINT_NAME"]
			if constraint not in unique_keys:
				unique_keys[constraint] = []
			unique_keys[constraint].append(row["COLUMN_NAME"])

		# Get indexes
		query = """
			SELECT DISTINCT INDEX_NAME, COLUMN_NAME
			FROM information_schema.STATISTICS
			WHERE TABLE_SCHEMA = DATABASE()
			AND TABLE_NAME = %s
			AND INDEX_NAME != 'PRIMARY'
			AND NON_UNIQUE = 1
			ORDER BY INDEX_NAME, SEQ_IN_INDEX
		"""
		cursor.execute(query, (table_name,))
		index_rows = cursor.fetchall()

		# Group by index name
		indexes = {}
		for row in index_rows:
			index = row["INDEX_NAME"]
			if index not in indexes:
				indexes[index] = []
			indexes[index].append(row["COLUMN_NAME"])

		cursor.close()

		return {
			"columns": columns,
			"primary_key": primary_key,
			"unique_keys": unique_keys,
			"indexes": indexes,
		}

	except Exception as e:
		frappe.log_error(title="Schema Introspection Error", message=str(e))
		raise


def map_mariadb_to_frappe_type(column):
	"""
	Map MariaDB column type to Frappe field type.

	Args:
		column: Column dict from information_schema.COLUMNS

	Returns:
		Dict with fieldtype and additional properties
	"""
	data_type = column["DATA_TYPE"].lower()
	column_type = column["COLUMN_TYPE"].lower()
	max_length = column["CHARACTER_MAXIMUM_LENGTH"]

	# VARCHAR
	# Frappe Data fields default to 140 chars, but can be longer
	# Use Data for varchar up to 255 (single-line input), Small Text only for longer
	if data_type == "varchar":
		if max_length and max_length <= 255:
			return {"fieldtype": "Data", "length": max_length}
		else:
			return {"fieldtype": "Small Text"}

	# CHAR
	elif data_type == "char":
		return {"fieldtype": "Data", "length": max_length}

	# TINYINT(1) -> Check
	elif data_type == "tinyint" and "tinyint(1)" in column_type:
		return {"fieldtype": "Check"}

	# Integer types
	elif data_type in ("tinyint", "smallint", "mediumint", "int", "bigint", "integer"):
		return {"fieldtype": "Int"}

	# Float types
	elif data_type in ("float", "double"):
		return {"fieldtype": "Float"}

	# Decimal
	elif data_type == "decimal":
		scale = column["NUMERIC_SCALE"] or 2
		return {"fieldtype": "Float", "precision": scale}

	# Date/Time types
	elif data_type == "date":
		return {"fieldtype": "Date"}
	elif data_type in ("datetime", "timestamp"):
		return {"fieldtype": "Datetime"}
	elif data_type == "time":
		return {"fieldtype": "Time"}

	# Text types
	elif data_type == "text":
		return {"fieldtype": "Text"}
	elif data_type == "mediumtext":
		return {"fieldtype": "Long Text"}
	elif data_type == "longtext":
		return {"fieldtype": "Long Text"}

	# ENUM -> Select
	elif data_type == "enum":
		# Parse enum values from COLUMN_TYPE
		# Format: enum('value1','value2','value3')
		if "enum(" in column_type:
			values_str = column_type[column_type.find("(") + 1 : column_type.rfind(")")]
			# Remove quotes and split
			values = [v.strip("'\"") for v in values_str.split(",")]
			options = "\n".join(values)
			return {"fieldtype": "Select", "options": options}
		return {"fieldtype": "Data"}

	# SET -> Data (no direct equivalent)
	elif data_type == "set":
		return {"fieldtype": "Data"}

	# JSON
	elif data_type == "json":
		return {"fieldtype": "JSON"}

	# BLOB types
	elif data_type in ("blob", "mediumblob", "longblob"):
		return {"fieldtype": "Long Text"}

	# Default fallback
	else:
		return {"fieldtype": "Data"}


def preview_table_schema(wp_conn_doc, wp_table_doc):
	"""
	Introspect a WordPress table and return proposed field mappings
	for user review before creating the DocType.

	Always fetches fresh schema from WordPress to detect any column changes.
	Restores previous user selections (matching fields) if available.

	Args:
		wp_conn_doc: WordPress Connection document
		wp_table_doc: WP Tables document

	Returns:
		Dict with fields, timestamps, doctype_name, and previous_matching_fields
	"""
	import json

	conn = get_wp_connection(wp_conn_doc)
	table_name = wp_table_doc.table_name

	schema = get_table_schema(conn, table_name)

	# Auto-detect timestamp fields
	timestamps = detect_timestamp_fields(conn, table_name)
	conn.close()

	# Get previously selected matching fields
	previous_matching = []
	if wp_table_doc.matching_fields:
		previous_matching = [f.strip() for f in wp_table_doc.matching_fields.split(",") if f.strip()]

	preview = []
	for col in schema["columns"]:
		col_name = col["COLUMN_NAME"]
		field_mapping = map_mariadb_to_frappe_type(col)

		is_unique = any(col_name in uk for uk in schema["unique_keys"].values())
		is_indexed = any(col_name in idx_cols for idx_cols in schema["indexes"].values())
		is_pk = col_name in schema.get("primary_key", [])

		# Check if this is a virtual/generated column
		extra = col.get("EXTRA", "") or ""
		is_virtual = "VIRTUAL" in extra.upper() or "GENERATED" in extra.upper()

		preview.append(
			{
				"column_name": col_name,
				"db_type": col["COLUMN_TYPE"],
				"proposed_fieldtype": field_mapping["fieldtype"],
				"label": col_name.replace("_", " ").title(),
				"is_nullable": col["IS_NULLABLE"],
				"is_primary_key": is_pk,
				"is_unique": is_unique,
				"is_indexed": is_indexed,
				"is_virtual": is_virtual,
				"length": field_mapping.get("length", 0),
				"precision": field_mapping.get("precision", 0),
				"options": field_mapping.get("options", ""),
			}
		)

	return {
		"fields": preview,
		"timestamps": timestamps,
		"doctype_name": wp_table_doc.nce_name or table_name,
		"previous_matching_fields": previous_matching,
	}


def mirror_table_schema(wp_conn_doc, wp_table_doc, field_overrides=None, label_overrides=None):
	"""
	Mirror a WordPress table schema to a Frappe Custom DocType.

	Args:
		wp_conn_doc: WordPress Connection document
		wp_table_doc: WP Tables document
		field_overrides: Optional dict of {column_name: fieldtype} from user review
		label_overrides: Optional dict of {column_name: label} from user review
	"""
	try:
		# Connect to WordPress DB
		conn = get_wp_connection(wp_conn_doc)
		table_name = wp_table_doc.table_name

		# Get schema
		schema = get_table_schema(conn, table_name)

		# Auto-detect timestamp fields if not already set by user
		if not wp_table_doc.created_timestamp_field or not wp_table_doc.modified_timestamp_field:
			timestamps = detect_timestamp_fields(conn, table_name)
			# Only set if user hasn't provided values (source-of-truth enforcement)
			if not wp_table_doc.created_timestamp_field and timestamps["created"]:
				wp_table_doc.created_timestamp_field = timestamps["created"]
			if not wp_table_doc.modified_timestamp_field and timestamps["modified"]:
				wp_table_doc.modified_timestamp_field = timestamps["modified"]

		conn.close()

		# Determine DocType name
		doctype_name = wp_table_doc.nce_name or table_name

		# Check if DocType already exists
		if frappe.db.exists("DocType", doctype_name):
			# Update existing DocType
			frappe.msgprint(
				_("DocType {0} already exists. Updating fields...").format(doctype_name),
				indicator="orange",
			)
			try:
				update_existing_doctype(doctype_name, schema, wp_table_doc, field_overrides, label_overrides)
			except Exception as update_error:
				# If update fails (e.g., duplicate field error), try deleting and recreating
				if "appears multiple times" in str(update_error):
					frappe.log_error(
						title=f"Recreating Broken DocType: {doctype_name}",
						message=f"Update failed with duplicate field error. Deleting and recreating.\n\nError: {update_error!s}",
					)
					frappe.msgprint(
						_("DocType {0} appears to be in a broken state. Deleting and recreating...").format(
							doctype_name
						),
						indicator="orange",
					)
					# Delete the broken DocType
					frappe.delete_doc("DocType", doctype_name, force=True, ignore_permissions=True)
					frappe.db.commit()
					# Recreate it
					create_custom_doctype(
						doctype_name, schema, wp_table_doc, field_overrides, label_overrides
					)
				else:
					raise  # Re-raise if it's a different error
		else:
			# Create new Custom DocType
			create_custom_doctype(doctype_name, schema, wp_table_doc, field_overrides, label_overrides)

		# Build column mapping: WP column name -> mapping info
		# Frappe lowercases all fieldnames, so we need to track the original WP names
		# Also track if column is virtual/generated (for reverse sync)
		# Sanitize restricted fieldnames (e.g., 'name' -> 'name_field')
		import json

		column_mapping = {}
		for col in schema["columns"]:
			wp_col_name = col["COLUMN_NAME"]
			frappe_fieldname = sanitize_fieldname(wp_col_name.lower())
			extra = col.get("EXTRA", "") or ""
			is_virtual = "VIRTUAL" in extra.upper() or "GENERATED" in extra.upper()
			column_mapping[wp_col_name] = {
				"fieldname": frappe_fieldname,
				"is_virtual": is_virtual,
			}

		# Update WP Tables record
		wp_table_doc.frappe_doctype = doctype_name
		wp_table_doc.mirror_status = "Mirrored"
		wp_table_doc.error_log = None
		wp_table_doc.column_mapping = json.dumps(column_mapping)
		wp_table_doc.save()

		# Add to workspace
		add_to_workspace(doctype_name, label=wp_table_doc.nce_name or doctype_name)

		frappe.db.commit()

	except Exception as e:
		wp_table_doc.mirror_status = "Error"
		wp_table_doc.error_log = str(e)
		wp_table_doc.save()
		frappe.db.commit()
		raise


def create_custom_doctype(doctype_name, schema, wp_table_doc, field_overrides=None, label_overrides=None):
	"""
	Create a new Custom DocType programmatically.

	Args:
		doctype_name: Name for the new DocType
		schema: Schema dict from get_table_schema
		wp_table_doc: WP Tables document
		field_overrides: Optional dict of {column_name: fieldtype} from user review
		label_overrides: Optional dict of {column_name: label} from user review
	"""
	# Determine naming rule
	autoname = "hash"  # Default
	primary_key = schema.get("primary_key", [])
	if len(primary_key) == 1:
		pk_col = next(
			(col for col in schema["columns"] if col["COLUMN_NAME"] == primary_key[0]),
			None,
		)
		if pk_col and "auto_increment" in (pk_col.get("EXTRA") or "").lower():
			autoname = "autoincrement"

	# Build fields using shared helper
	fields = []
	for idx, col in enumerate(schema["columns"], start=1):
		field = build_frappe_field(col, schema, wp_table_doc, field_overrides, label_overrides, idx)
		fields.append(field)

	# Create DocType document
	doctype_doc = frappe.get_doc(
		{
			"doctype": "DocType",
			"name": doctype_name,
			"module": "NCE Sync",
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
		}
	)

	doctype_doc.insert(ignore_permissions=True)
	frappe.db.commit()


def update_existing_doctype(doctype_name, schema, wp_table_doc, field_overrides=None, label_overrides=None):
	"""
	Update an existing DocType with new fields from schema.
	Adds missing fields without removing existing ones.

	Args:
		doctype_name: Name of the existing DocType
		schema: Schema dict from get_table_schema
		wp_table_doc: WP Tables document
		field_overrides: Optional dict of {column_name: fieldtype} from user review
		label_overrides: Optional dict of {column_name: label} from user review
	"""
	doctype_doc = frappe.get_doc("DocType", doctype_name)

	# Get existing field names
	existing_fields = {f.fieldname for f in doctype_doc.fields}

	# Find new fields to add
	new_fields_added = False
	idx = len(doctype_doc.fields) + 1

	for col in schema["columns"]:
		col_name = col["COLUMN_NAME"]
		safe_fieldname = sanitize_fieldname(col_name.lower())

		if safe_fieldname not in existing_fields:
			field = build_frappe_field(col, schema, wp_table_doc, field_overrides, label_overrides, idx)
			doctype_doc.append("fields", field)
			new_fields_added = True
			idx += 1

	if new_fields_added:
		doctype_doc.save(ignore_permissions=True)
		frappe.db.commit()
	else:
		frappe.msgprint(_("No new fields to add to {0}").format(doctype_name), indicator="blue")
