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

		# Get columns
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
				COLUMN_TYPE
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
	if data_type == "varchar":
		if max_length and max_length <= 140:
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


def mirror_table_schema(wp_conn_doc, wp_table_doc):
	"""
	Mirror a WordPress table schema to a Frappe Custom DocType.

	Args:
		wp_conn_doc: WordPress Connection document
		wp_table_doc: WP Tables document
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
			update_existing_doctype(doctype_name, schema, wp_table_doc)
		else:
			# Create new Custom DocType
			create_custom_doctype(doctype_name, schema, wp_table_doc)

		# Update WP Tables record
		wp_table_doc.frappe_doctype = doctype_name
		wp_table_doc.mirror_status = "Mirrored"
		wp_table_doc.error_log = None
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


def create_custom_doctype(doctype_name, schema, wp_table_doc):
	"""
	Create a new Custom DocType programmatically.

	Args:
		doctype_name: Name for the new DocType
		schema: Schema dict from get_table_schema
		wp_table_doc: WP Tables document
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

	# Build fields
	fields = []
	idx = 1

	for col in schema["columns"]:
		col_name = col["COLUMN_NAME"]
		field_mapping = map_mariadb_to_frappe_type(col)

		field = {
			"fieldname": col_name,
			"fieldtype": field_mapping["fieldtype"],
			"label": col_name.replace("_", " ").title(),
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

		# Mark unique columns
		if any(col_name in uk for uk in schema["unique_keys"].values()):
			field["unique"] = 1

		# Mark indexed columns
		if any(col_name in idx_cols for idx_cols in schema["indexes"].values()):
			field["search_index"] = 1

		fields.append(field)
		idx += 1

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


def update_existing_doctype(doctype_name, schema, wp_table_doc):
	"""
	Update an existing DocType with new fields from schema.
	Adds missing fields without removing existing ones.

	Args:
		doctype_name: Name of the existing DocType
		schema: Schema dict from get_table_schema
		wp_table_doc: WP Tables document
	"""
	doctype_doc = frappe.get_doc("DocType", doctype_name)

	# Get existing field names
	existing_fields = {f.fieldname for f in doctype_doc.fields}

	# Find new fields to add
	new_fields_added = False
	idx = len(doctype_doc.fields) + 1

	for col in schema["columns"]:
		col_name = col["COLUMN_NAME"]

		if col_name not in existing_fields:
			field_mapping = map_mariadb_to_frappe_type(col)

			field = {
				"fieldname": col_name,
				"fieldtype": field_mapping["fieldtype"],
				"label": col_name.replace("_", " ").title(),
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

			# Mark unique columns
			if any(col_name in uk for uk in schema["unique_keys"].values()):
				field["unique"] = 1

			# Mark indexed columns
			if any(col_name in idx_cols for idx_cols in schema["indexes"].values()):
				field["search_index"] = 1

			doctype_doc.append("fields", field)
			new_fields_added = True
			idx += 1

	if new_fields_added:
		doctype_doc.save(ignore_permissions=True)
		frappe.db.commit()
	else:
		frappe.msgprint(_("No new fields to add to {0}").format(doctype_name), indicator="blue")
