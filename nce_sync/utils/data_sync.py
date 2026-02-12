# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""
Data synchronization utilities for NCE_Sync.
Handles one-way sync from WordPress tables to Frappe DocTypes.
"""

import json
from datetime import datetime, timedelta

import frappe
import pytz
from frappe import _
from frappe.utils import now_datetime

from nce_sync.utils.schema_mirror import get_wp_connection

# Batch size for upserts to avoid long DB locks
BATCH_SIZE = 500

# Buffer time (minutes) to catch records modified during previous sync
SYNC_BUFFER_MINUTES = 5


# =============================================================================
# Helper Functions
# =============================================================================


def get_frappe_fieldname(wp_col, column_mapping):
	"""
	Get the Frappe fieldname for a WordPress column using the column mapping.

	Handles both old format (string) and new format (dict with fieldname key).
	Falls back to lowercase WP column name if not in mapping.

	Args:
		wp_col: WordPress column name
		column_mapping: Dict mapping WP column names to Frappe fieldnames

	Returns:
		Frappe fieldname (string)
	"""
	if column_mapping and wp_col in column_mapping:
		mapping_info = column_mapping[wp_col]
		if isinstance(mapping_info, dict):
			return mapping_info["fieldname"]
		else:
			return mapping_info
	return wp_col.lower()


def build_reverse_mapping(column_mapping):
	"""
	Build a reverse mapping from Frappe fieldnames to WP column names.

	Args:
		column_mapping: Dict mapping WP column names to Frappe fieldnames

	Returns:
		Dict mapping Frappe fieldnames to WP column names
	"""
	reverse = {}
	for wp_col, mapping_info in (column_mapping or {}).items():
		if isinstance(mapping_info, dict):
			reverse[mapping_info["fieldname"]] = wp_col
		else:
			reverse[mapping_info] = wp_col
	return reverse


def _normalize_key_value(value):
	"""
	Normalize a key value for consistent comparison between WP and Frappe.
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


def convert_frappe_ts_to_wp_tz(ts, wp_tz_name):
	"""
	Convert a Frappe timestamp to WordPress timezone for queries.

	Args:
		ts: datetime object in Frappe timezone
		wp_tz_name: WordPress timezone name

	Returns:
		datetime in WordPress timezone (naive, for SQL queries)
	"""
	if ts is None:
		return None

	frappe_tz = get_timezone(frappe.utils.get_system_timezone())
	wp_tz = get_timezone(wp_tz_name)

	# Localize and convert
	if ts.tzinfo is None:
		ts = frappe_tz.localize(ts)

	return ts.astimezone(wp_tz).replace(tzinfo=None)


def sync_table(wp_table_doc):
	"""
	Main entry point for syncing a WordPress table to Frappe.

	Reads sync settings from the WP Tables document and dispatches
	to the appropriate sync method (TS Compare or Truncate & Replace).

	Args:
		wp_table_doc: WP Tables document

	Returns:
		dict with sync results (rows_synced, rows_deleted, etc.)
	"""
	if wp_table_doc.mirror_status != "Mirrored":
		frappe.throw(_("Table must be mirrored before syncing"))

	if not wp_table_doc.frappe_doctype:
		frappe.throw(_("No Frappe DocType associated with this table"))

	# Get WordPress connection
	wp_conn_doc = frappe.get_single("WordPress Connection")
	if not wp_conn_doc:
		frappe.throw(_("WordPress Connection not configured"))

	# Determine sync method
	sync_method = wp_table_doc.sync_method or "TS Compare"

	# Get effective timestamp field
	ts_field = _get_effective_ts_field(wp_table_doc)

	# Connect to WordPress
	conn = get_wp_connection(wp_conn_doc)

	try:
		if sync_method == "Truncate & Replace":
			result = _sync_truncate_replace(conn, wp_table_doc, wp_conn_doc, wp_table_doc.frappe_doctype)
		else:
			# Default to TS Compare
			result = _sync_ts_compare(conn, wp_table_doc, wp_conn_doc, wp_table_doc.frappe_doctype, ts_field)

		return result

	finally:
		conn.close()


def _get_effective_ts_field(wp_table_doc):
	"""
	Returns the timestamp field to use for sync.

	Uses modified_timestamp_field if available, falls back to created_timestamp_field.
	For Truncate & Replace, returns None (not needed).

	Args:
		wp_table_doc: WP Tables document

	Returns:
		str: Field name or None
	"""
	if wp_table_doc.sync_method == "Truncate & Replace":
		return None

	ts_field = wp_table_doc.modified_timestamp_field or wp_table_doc.created_timestamp_field

	if not ts_field:
		frappe.throw(
			_(
				"No timestamp field configured for table '{0}'. "
				"Either set a timestamp field or use 'Truncate & Replace' sync method."
			).format(wp_table_doc.table_name)
		)

	return ts_field


def _convert_wp_ts_to_frappe_tz(ts, wp_tz_name):
	"""
	Convert a WordPress timestamp to Frappe server timezone.

	Args:
		ts: datetime object or string from WordPress
		wp_tz_name: WordPress timezone name (e.g., 'America/New_York')

	Returns:
		datetime in Frappe server timezone
	"""
	if ts is None:
		return None

	if isinstance(ts, str):
		ts = datetime.fromisoformat(ts)

	wp_tz = get_timezone(wp_tz_name)
	frappe_tz = get_timezone(frappe.utils.get_system_timezone())

	# Localize WordPress timestamp and convert to Frappe timezone
	if ts.tzinfo is None:
		ts = wp_tz.localize(ts)

	return ts.astimezone(frappe_tz).replace(tzinfo=None)


def _get_matching_keys(wp_table_doc):
	"""
	Parse matching_fields from the WP Tables document.
	Converts WP column names to Frappe fieldnames using column_mapping.

	Args:
		wp_table_doc: WP Tables document

	Returns:
		list of Frappe fieldnames to use as matching keys
	"""
	if not wp_table_doc.matching_fields:
		frappe.throw(
			_(
				"No matching fields configured for table '{0}'. Please re-mirror and select matching fields."
			).format(wp_table_doc.table_name)
		)

	# Get WP column names from matching_fields
	wp_columns = [f.strip() for f in wp_table_doc.matching_fields.split(",") if f.strip()]

	# Convert to Frappe fieldnames using column_mapping
	column_mapping = {}
	if wp_table_doc.column_mapping:
		column_mapping = json.loads(wp_table_doc.column_mapping)

	return [get_frappe_fieldname(wp_col, column_mapping) for wp_col in wp_columns]


def _get_wp_key_set(conn, table_name, matching_keys, reverse_mapping, column_mapping):
	"""
	Fetch all matching key values from WordPress and build a set for comparison.

	Args:
		conn: PyMySQL connection
		table_name: WordPress table name
		matching_keys: List of Frappe fieldnames to match on
		reverse_mapping: Dict mapping Frappe fieldnames to WP column names
		column_mapping: Dict mapping WP column names to Frappe fieldnames

	Returns:
		Set of key tuples (normalized to strings)
	"""
	# Build WP column list for query
	wp_key_columns = []
	for frappe_key in matching_keys:
		wp_col = reverse_mapping.get(frappe_key, frappe_key)
		wp_key_columns.append(f"`{wp_col}`")

	cursor = conn.cursor()
	cursor.execute(f"SELECT {', '.join(wp_key_columns)} FROM `{table_name}`")
	wp_rows = cursor.fetchall()
	cursor.close()

	# Build set of normalized key tuples
	wp_key_set = set()
	for row in wp_rows:
		converted_row = _convert_row(row, None, column_mapping)
		key_tuple = tuple(_normalize_key_value(converted_row.get(k)) for k in matching_keys)
		wp_key_set.add(key_tuple)

	return wp_key_set


def _get_cutoff_timestamp(frappe_doctype, frappe_ts_field, wp_tz):
	"""
	Get the cutoff timestamp for incremental sync by finding max timestamp in Frappe.

	Args:
		frappe_doctype: Name of the Frappe DocType
		frappe_ts_field: Frappe fieldname for the timestamp field
		wp_tz: WordPress timezone name

	Returns:
		Cutoff datetime in WP timezone, or None if no data exists
	"""
	if not frappe_ts_field:
		return None

	max_ts_result = frappe.db.sql(f"SELECT MAX(`{frappe_ts_field}`) FROM `tab{frappe_doctype}`")

	if not max_ts_result or not max_ts_result[0][0]:
		return None

	max_ts = max_ts_result[0][0]
	if isinstance(max_ts, str):
		max_ts = datetime.fromisoformat(max_ts)

	# Apply buffer to catch records modified during previous sync
	cutoff = max_ts - timedelta(minutes=SYNC_BUFFER_MINUTES)

	# Convert from Frappe TZ to WP TZ for the query
	return convert_frappe_ts_to_wp_tz(cutoff, wp_tz)


def _fetch_changed_rows(conn, table_name, ts_field, create_ts_field, cutoff):
	"""
	Fetch rows from WordPress that have changed since the cutoff.

	Args:
		conn: PyMySQL connection
		table_name: WordPress table name
		ts_field: Modified timestamp field name
		create_ts_field: Created timestamp field name (may be None)
		cutoff: Cutoff datetime in WP timezone (None = fetch all)

	Returns:
		List of row dicts from WordPress
	"""
	cursor = conn.cursor()

	if cutoff:
		if create_ts_field and create_ts_field != ts_field:
			# Check both: modified OR created since cutoff
			cursor.execute(
				f"SELECT * FROM `{table_name}` WHERE `{ts_field}` >= %s OR `{create_ts_field}` >= %s",
				(cutoff, cutoff),
			)
		else:
			# Only mod_ts available
			cursor.execute(f"SELECT * FROM `{table_name}` WHERE `{ts_field}` >= %s", (cutoff,))
	else:
		# No cutoff - get all rows
		cursor.execute(f"SELECT * FROM `{table_name}`")

	rows = cursor.fetchall()
	cursor.close()
	return rows


def _sync_ts_compare(conn, wp_table_doc, wp_conn_doc, frappe_doctype, ts_field):
	"""
	Sync using timestamp comparison method.

	Steps:
	1. Pull matching keys from WP, diff against Frappe, delete orphans
	2. Pull changed rows (ts_field >= last_synced - buffer), convert TZ
	3. Upsert into Frappe DocType by matching key

	Args:
		conn: PyMySQL connection
		wp_table_doc: WP Tables document
		wp_conn_doc: WordPress Connection document
		frappe_doctype: Name of the Frappe DocType
		ts_field: Timestamp field name for comparison

	Returns:
		dict with sync results
	"""
	table_name = wp_table_doc.table_name
	matching_keys = _get_matching_keys(wp_table_doc)
	wp_tz = wp_conn_doc.wp_timezone

	# Load column mapping (WP column name -> {fieldname, is_virtual})
	column_mapping = None
	if wp_table_doc.column_mapping:
		column_mapping = json.loads(wp_table_doc.column_mapping)

	# Build reverse mapping for looking up WP column names from Frappe fieldnames
	reverse_mapping = build_reverse_mapping(column_mapping)

	# Step 1: Delete detection - get all matching keys from WP and delete orphans
	wp_key_set = _get_wp_key_set(conn, table_name, matching_keys, reverse_mapping, column_mapping)
	rows_deleted = _delete_orphans(frappe_doctype, matching_keys, wp_key_set)

	# Step 2: Get changed rows from WP
	frappe_ts_field = get_frappe_fieldname(ts_field, column_mapping) if ts_field else None
	cutoff = _get_cutoff_timestamp(frappe_doctype, frappe_ts_field, wp_tz)
	changed_rows = _fetch_changed_rows(
		conn, table_name, ts_field, wp_table_doc.created_timestamp_field, cutoff
	)

	# Step 3: Upsert changed rows in batches
	rows_upserted = 0
	for i in range(0, len(changed_rows), BATCH_SIZE):
		batch = changed_rows[i : i + BATCH_SIZE]

		for row in batch:
			# Convert row: map WP columns to Frappe fieldnames, convert timestamps
			converted_row = _convert_row(row, wp_tz, column_mapping)
			_upsert_record(frappe_doctype, matching_keys, converted_row)
			rows_upserted += 1

		# Commit after each batch
		frappe.db.commit()

	return {
		"method": "TS Compare",
		"rows_upserted": rows_upserted,
		"rows_deleted": rows_deleted,
		"total_wp_rows": len(wp_key_set),
	}


def _sync_truncate_replace(conn, wp_table_doc, wp_conn_doc, frappe_doctype):
	"""
	Sync using truncate and replace method.

	Deletes all Frappe records and re-inserts from WordPress.

	Args:
		conn: PyMySQL connection
		wp_table_doc: WP Tables document
		wp_conn_doc: WordPress Connection document
		frappe_doctype: Name of the Frappe DocType

	Returns:
		dict with sync results
	"""
	table_name = wp_table_doc.table_name
	wp_tz = wp_conn_doc.wp_timezone

	# Load column mapping (WP column name -> {fieldname, is_virtual})
	column_mapping = None
	if wp_table_doc.column_mapping:
		column_mapping = json.loads(wp_table_doc.column_mapping)

	# Step 1: Delete all existing Frappe records
	frappe.db.delete(frappe_doctype)
	frappe.db.commit()

	# Step 2: Get all rows from WordPress
	cursor = conn.cursor()
	cursor.execute(f"SELECT * FROM `{table_name}`")
	all_rows = cursor.fetchall()
	cursor.close()

	# Step 3: Insert all rows in batches
	rows_inserted = 0
	for i in range(0, len(all_rows), BATCH_SIZE):
		batch = all_rows[i : i + BATCH_SIZE]

		for row in batch:
			# Convert row: map WP columns to Frappe fieldnames, convert timestamps
			converted_row = _convert_row(row, wp_tz, column_mapping)
			_insert_record(frappe_doctype, converted_row)
			rows_inserted += 1

		# Commit after each batch
		frappe.db.commit()

	return {
		"method": "Truncate & Replace",
		"rows_inserted": rows_inserted,
		"rows_deleted": "all",
	}


def _convert_row(row, wp_tz, column_mapping=None):
	"""
	Convert a WordPress row for insertion into Frappe:
	- Maps WP column names to Frappe fieldnames using the stored mapping
	- Converts datetime fields from WP timezone to Frappe timezone

	Args:
		row: dict of column values from WordPress
		wp_tz: WordPress timezone name
		column_mapping: dict mapping WP column names to Frappe fieldnames
		                (can be old format: {wp_col: fieldname} or
		                 new format: {wp_col: {fieldname: ..., is_virtual: ...}})

	Returns:
		dict with Frappe fieldnames as keys
	"""
	converted = {}
	for wp_key, value in row.items():
		frappe_key = get_frappe_fieldname(wp_key, column_mapping)

		if isinstance(value, datetime):
			converted[frappe_key] = _convert_wp_ts_to_frappe_tz(value, wp_tz)
		else:
			converted[frappe_key] = value
	return converted


def _upsert_record(frappe_doctype, matching_keys, row_data):
	"""
	Insert or update a single Frappe document by matching key lookup.

	Args:
		frappe_doctype: Name of the Frappe DocType
		matching_keys: List of field names to match on
		row_data: Dict of field values from WordPress
	"""
	# Build filter for matching keys
	filters = {key: row_data.get(key) for key in matching_keys}

	# Check if record exists
	existing = frappe.db.get_value(frappe_doctype, filters, "name")

	# Get valid field names from DocType meta
	valid_fields = {df.fieldname for df in frappe.get_meta(frappe_doctype).fields}

	if existing:
		# Update existing record
		doc = frappe.get_doc(frappe_doctype, existing)
		for key, value in row_data.items():
			if key in valid_fields and key != "name":
				doc.set(key, value)
		doc.flags.ignore_permissions = True
		doc.flags.ignore_mandatory = True
		doc.save()
	else:
		# Insert new record
		_insert_record(frappe_doctype, row_data)


def _insert_record(frappe_doctype, row_data):
	"""
	Insert a new Frappe document.

	Args:
		frappe_doctype: Name of the Frappe DocType
		row_data: Dict of field values from WordPress
	"""
	doc = frappe.new_doc(frappe_doctype)

	# Get valid field names from DocType meta
	valid_fields = {df.fieldname for df in frappe.get_meta(frappe_doctype).fields}
	valid_fields.add("name")  # name is always valid

	for key, value in row_data.items():
		if key in valid_fields:
			doc.set(key, value)

	doc.flags.ignore_permissions = True
	doc.flags.ignore_mandatory = True
	doc.insert()


def _delete_orphans(frappe_doctype, matching_keys, wp_key_set):
	"""
	Delete Frappe records whose matching keys are not in the WordPress key set.

	Args:
		frappe_doctype: Name of the Frappe DocType
		matching_keys: List of field names to match on
		wp_key_set: Set of key tuples from WordPress

	Returns:
		int: Number of records deleted
	"""
	# Get all Frappe records' matching keys
	frappe_records = frappe.get_all(frappe_doctype, fields=["name", *matching_keys], limit_page_length=0)

	deleted_count = 0
	for record in frappe_records:
		# Build key tuple for this Frappe record (normalized for comparison)
		frappe_key = tuple(_normalize_key_value(record.get(k)) for k in matching_keys)

		if frappe_key not in wp_key_set:
			# This record no longer exists in WordPress - delete it
			frappe.delete_doc(frappe_doctype, record.name, force=True, ignore_permissions=True)
			deleted_count += 1

	if deleted_count > 0:
		frappe.db.commit()

	return deleted_count


def run_scheduled_syncs():
	"""
	Scheduler entry point: sync all tables that are due.

	Called by Frappe scheduler based on hooks.py configuration.
	Checks each WP Table with auto_sync_active="Yes" and syncs
	if enough time has passed since last_synced.
	"""
	# Get all tables eligible for auto-sync
	tables = frappe.get_all(
		"WP Tables",
		filters={
			"auto_sync_active": "Yes",
			"mirror_status": "Mirrored",
		},
		fields=["name", "table_name", "sync_frequency", "last_synced"],
	)

	now = now_datetime()

	for table_info in tables:
		try:
			# Check if sync is due
			sync_frequency = table_info.sync_frequency or 60  # Default 60 minutes
			last_synced = table_info.last_synced

			if last_synced:
				if isinstance(last_synced, str):
					last_synced = datetime.fromisoformat(last_synced)
				time_since_sync = (now - last_synced).total_seconds() / 60
				if time_since_sync < sync_frequency:
					continue  # Not due yet

			# Sync is due - run it
			wp_table_doc = frappe.get_doc("WP Tables", table_info.name)
			_run_sync_with_status(wp_table_doc)

		except Exception as e:
			# Log error but continue with other tables
			frappe.log_error(title=f"Scheduled Sync Error: {table_info.table_name}", message=str(e))


def _run_sync_with_status(wp_table_doc):
	"""
	Run sync and update status fields on the WP Tables document.

	Args:
		wp_table_doc: WP Tables document
	"""
	# Set status to Running
	wp_table_doc.last_sync_status = "Running"
	wp_table_doc.last_sync_log = "Sync started..."
	wp_table_doc.save()
	frappe.db.commit()

	try:
		result = sync_table(wp_table_doc)

		# Check for anomaly: WP has rows but Frappe table is empty after sync
		# This indicates a bug - don't update last_synced so next sync does full pull
		total_wp_rows = result.get("total_wp_rows", 0) or result.get("rows_inserted", 0)
		frappe_count = frappe.db.count(wp_table_doc.frappe_doctype)

		if total_wp_rows > 0 and frappe_count == 0:
			# Something went wrong - Frappe table is empty but WP has data
			wp_table_doc.last_sync_status = "Warning"
			wp_table_doc.last_sync_log = (
				f"ANOMALY: WP has {total_wp_rows} rows but Frappe table is empty. "
				f"last_synced NOT updated - next sync will do full pull. "
				f"Check matching keys and column mapping."
			)
			# Don't update last_synced - leave it as-is or null
			wp_table_doc.save()
			frappe.db.commit()
			return

		# Update status on success
		wp_table_doc.last_synced = now_datetime()
		wp_table_doc.last_sync_status = "Success"

		# Build summary log
		if result.get("method") == "Truncate & Replace":
			wp_table_doc.last_sync_log = f"Truncate & Replace: {result.get('rows_inserted', 0)} rows inserted"
		else:
			wp_table_doc.last_sync_log = (
				f"TS Compare: {result.get('rows_upserted', 0)} upserted, "
				f"{result.get('rows_deleted', 0)} deleted, "
				f"{result.get('total_wp_rows', 0)} total WP rows, "
				f"{frappe_count} in Frappe"
			)

		wp_table_doc.save()
		frappe.db.commit()

	except Exception as e:
		# Update status on error
		wp_table_doc.last_sync_status = "Error"
		wp_table_doc.last_sync_log = str(e)[:500]  # Truncate long errors
		wp_table_doc.save()
		frappe.db.commit()

		frappe.log_error(title=f"Sync Error: {wp_table_doc.table_name}", message=str(e))
		raise
