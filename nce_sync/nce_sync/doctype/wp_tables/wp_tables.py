# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

import json

import frappe
from frappe import _
from frappe.model.document import Document

# Core Frappe DocType names - never drop these tables (safety guard)
_NEVER_DROP_DOCTYPES = frozenset({
	"DocType", "DocField", "DocPerm", "DocType Action", "DocType Link",
	"User", "User Permission", "Role", "Module Def", "File",
	"Error Log", "Error Snapshot", "Scheduled Job Log", "Activity Log",
	"Singles", "DefaultValue", "Property Setter", "Custom Field",
	"Workflow", "Workflow State", "Workflow Action", "Workflow Transition",
})


def _is_safe_to_drop_table(doctype_name):
	"""Return False if dropping this table could harm core Frappe."""
	if not doctype_name or "`" in doctype_name or ";" in doctype_name:
		return False
	if doctype_name in _NEVER_DROP_DOCTYPES:
		return False
	# Also block any non-custom DocType (core/system)
	if frappe.db.exists("DocType", doctype_name):
		is_custom = frappe.db.get_value("DocType", doctype_name, "custom")
		if not is_custom:
			return False
	return True


def _delete_mirrored_doctype(doctype_name):
	"""
	Fully remove a mirrored DocType: workspace shortcut, DocType record, and DB table.
	Frappe often leaves orphaned tables when deleting DocTypes, so we explicitly drop
	the table first, then delete the DocType. This guarantees the table is gone.
	Never drops core Frappe tables (guard rail).
	"""
	if not _is_safe_to_drop_table(doctype_name):
		return
	from nce_sync.utils.workspace_utils import remove_from_workspace

	remove_from_workspace(doctype_name)

	# Drop table FIRST - guarantees it's gone even if DocType delete fails
	table_name = f"tab{doctype_name}"
	try:
		frappe.db.sql(f"DROP TABLE IF EXISTS `{table_name}`")
		frappe.db.commit()
	except Exception as e:
		frappe.log_error(title=f"Drop table failed: {table_name}", message=str(e))

	# Delete DocType record
	if frappe.db.exists("DocType", doctype_name):
		try:
			frappe.delete_doc("DocType", doctype_name, force=True, ignore_permissions=True)
			frappe.db.commit()
		except Exception as e:
			frappe.log_error(title=f"Delete DocType failed: {doctype_name}", message=str(e))
			# Table is already dropped, so user can re-mirror; DocType orphan may need manual cleanup


class WPTables(Document):
	"""Tracks WordPress tables selected for mirroring."""

	def autoname(self):
		"""Set document name from nce_name if provided, otherwise use table_name."""
		self.name = self.nce_name or self.table_name

	def on_trash(self):
		"""Full cascade cleanup: delete Sync Logs, mirrored DocType, and workspace shortcut."""
		# Delete associated Sync Log records
		sync_logs = frappe.get_all("Sync Log", filters={"wp_table": self.name}, pluck="name")
		for log_name in sync_logs:
			frappe.delete_doc("Sync Log", log_name, force=True, ignore_permissions=True)

		# Delete mirrored DocType (table + record) - drop table first so it always works
		if self.frappe_doctype:
			_delete_mirrored_doctype(self.frappe_doctype)

	def validate(self):
		"""Validate and enforce source-of-truth hierarchy."""
		if self.nce_name:
			self._validate_doctype_name(self.nce_name)

	def _validate_doctype_name(self, name):
		"""
		Check if the proposed DocType name conflicts with existing DocTypes
		or database tables (which may belong to Frappe core or other apps).
		"""
		# Skip if this table already owns this DocType
		if self.frappe_doctype == name:
			return

		# Check if DocType already exists in the registry
		if frappe.db.exists("DocType", name):
			is_custom = frappe.db.get_value("DocType", name, "custom")
			if not is_custom:
				frappe.throw(
					_(
						"'{0}' is a system DocType and cannot be used. Please choose a different name."
					).format(name)
				)
			else:
				other_table = frappe.db.get_value(
					"WP Tables", {"frappe_doctype": name, "name": ["!=", self.name]}, "name"
				)
				if other_table:
					frappe.throw(
						_(
							"'{0}' is already used by another mirrored table ({1}). Please choose a different name."
						).format(name, other_table)
					)
				else:
					frappe.throw(
						_(
							"A DocType named '{0}' already exists. Please choose a different name or delete the existing DocType first."
						).format(name)
					)

		# Check if a database table with this name already exists (possibly in use by another app)
		if frappe.db.sql("SHOW TABLES LIKE %s", f"tab{name}"):
			frappe.throw(
				_(
					"'{0}' cannot be used — it already exists, possibly in use by another app. Please choose a different name."
				).format(name)
			)

	@frappe.whitelist()
	def preview_schema(self):
		"""Introspect table schema and return proposed field mappings for user review."""
		from nce_sync.utils.schema_mirror import preview_table_schema

		wp_conn = frappe.get_single("WordPress Connection")
		if not wp_conn:
			frappe.throw(_("WordPress Connection not configured"))

		return preview_table_schema(wp_conn, self)

	@frappe.whitelist()
	def mirror_schema(self, field_overrides=None, label_overrides=None, matching_fields=None, name_field_column=None, auto_generated_columns=None, modified_ts_field=None, created_ts_field=None):
		"""Mirror this specific table's schema to a Frappe DocType."""
		try:
			from nce_sync.utils.schema_mirror import mirror_table_schema

			if field_overrides and isinstance(field_overrides, str):
				field_overrides = json.loads(field_overrides)

			if label_overrides and isinstance(label_overrides, str):
				label_overrides = json.loads(label_overrides)

			# Matching fields should already be saved by JS before this is called
			# But update if provided and different (belt and suspenders)
			if matching_fields and matching_fields != self.matching_fields:
				self.matching_fields = matching_fields
				self.save()

			# Validate DocType name before mirroring
			doctype_name = self.nce_name or self.table_name
			self._validate_doctype_name(doctype_name)

			wp_conn = frappe.get_single("WordPress Connection")
			if not wp_conn:
				frappe.throw(_("WordPress Connection not configured"))

			mirror_table_schema(
				wp_conn,
				self,
				field_overrides=field_overrides,
				label_overrides=label_overrides,
				name_field_column=name_field_column or None,
				auto_generated_columns=auto_generated_columns or None,
				modified_ts_field=modified_ts_field or None,
				created_ts_field=created_ts_field or None,
			)

			frappe.msgprint(
				_("Successfully mirrored table: {0}").format(self.table_name),
				indicator="green",
			)

		except Exception as e:
			import traceback

			self.mirror_status = "Error"
			self.error_log = traceback.format_exc()
			self.save()
			frappe.log_error(title=f"Mirror Error: {self.table_name}", message=traceback.format_exc())
			frappe.throw(_("Failed to mirror table: {0}").format(str(e)))

	@frappe.whitelist()
	def delete_mirror(self):
		"""
		Delete the generated DocType and remove from workspace.
		Resets this WP Tables entry back to Pending so it can be re-mirrored.
		"""
		doctype_name = self.frappe_doctype
		if not doctype_name:
			frappe.throw(_("No mirrored DocType to delete"))

		_delete_mirrored_doctype(doctype_name)

		# Reset this WP Tables entry
		self.frappe_doctype = None
		self.mirror_status = "Pending"
		self.error_log = None
		self.save()

		frappe.msgprint(
			_("Deleted DocType '{0}' and removed from workspace. Ready to re-mirror.").format(doctype_name),
			indicator="green",
		)

	@frappe.whitelist()
	def remove_table(self):
		"""
		Full cleanup: delete the generated DocType, remove from workspace,
		and delete this WP Tables record itself.
		"""
		doctype_name = self.frappe_doctype
		if doctype_name:
			_delete_mirrored_doctype(doctype_name)

		# Delete this WP Tables record
		table_name = self.table_name
		frappe.delete_doc("WP Tables", self.name, force=True, ignore_permissions=True)
		frappe.db.commit()

		frappe.msgprint(
			_("Removed table '{0}' and all associated data.").format(table_name),
			indicator="green",
		)

	@frappe.whitelist()
	def add_to_workspace(self):
		"""Add the mirrored DocType as a shortcut in the Tables workspace."""
		from nce_sync.utils.workspace_utils import add_to_workspace

		if not self.frappe_doctype:
			frappe.throw(_("No mirrored DocType to add"))

		add_to_workspace(self.frappe_doctype, label=self.nce_name or self.frappe_doctype)
		frappe.msgprint(
			_("Added '{0}' to the workspace.").format(self.frappe_doctype),
			indicator="green",
			alert=True,
		)

	@frappe.whitelist()
	def regenerate_column_mapping(self):
		"""
		Regenerate the column mapping from the WordPress table schema.
		Useful for tables mirrored before column_mapping was added.
		Also detects virtual/generated columns for reverse sync protection.
		"""
		import json

		from nce_sync.utils.schema_mirror import get_table_schema, get_wp_connection, sanitize_fieldname

		wp_conn = frappe.get_single("WordPress Connection")
		if not wp_conn:
			frappe.throw(_("WordPress Connection not configured"))

		conn = get_wp_connection(wp_conn)
		schema = get_table_schema(conn, self.table_name)
		conn.close()

		# Build column mapping: WP column name -> {fieldname, is_virtual[, is_name]}
		# When name_field_column is set, that column maps to "name" with is_name=True
		column_mapping = {}
		virtual_count = 0
		name_field_column = getattr(self, "name_field_column", None)
		for col in schema["columns"]:
			wp_col_name = col["COLUMN_NAME"]
			extra = col.get("EXTRA", "") or ""
			is_virtual = "VIRTUAL" in extra.upper() or "GENERATED" in extra.upper()
			if name_field_column and wp_col_name == name_field_column:
				column_mapping[wp_col_name] = {
					"fieldname": "name",
					"is_virtual": is_virtual,
					"is_name": True,
				}
			else:
				frappe_fieldname = sanitize_fieldname(wp_col_name.lower())
				column_mapping[wp_col_name] = {
					"fieldname": frappe_fieldname,
					"is_virtual": is_virtual,
				}
			if is_virtual:
				virtual_count += 1

		self.column_mapping = json.dumps(column_mapping)
		self.save()

		msg = _("Column mapping regenerated: {0} columns mapped").format(len(column_mapping))
		if virtual_count > 0:
			msg += _(", {0} virtual/computed columns detected").format(virtual_count)
		frappe.msgprint(msg, indicator="green")

	@frappe.whitelist()
	def truncate_data(self):
		"""
		Delete all records from the mirrored Frappe DocType.
		The DocType structure remains intact.
		"""
		if not self.frappe_doctype:
			frappe.throw(_("No Frappe DocType associated with this table"))

		frappe.db.delete(self.frappe_doctype)
		frappe.db.commit()

		# Reset sync status since data is gone
		self.last_synced = None
		self.last_sync_status = None
		self.last_sync_log = "Data truncated manually"
		self.save()

	@frappe.whitelist()
	def debug_sync_one_row(self):
		"""
		Debug: Sync just the first row and show detailed info about what's happening.
		"""
		from nce_sync.utils.schema_mirror import get_wp_connection

		if not self.frappe_doctype:
			frappe.throw(_("No Frappe DocType associated with this table"))

		wp_conn = frappe.get_single("WordPress Connection")
		conn = get_wp_connection(wp_conn)

		cursor = conn.cursor()

		# Get actual column names from information_schema
		cursor.execute(
			"""
			SELECT COLUMN_NAME
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
			ORDER BY ORDINAL_POSITION
		""",
			(self.table_name,),
		)
		schema_columns = [r["COLUMN_NAME"] for r in cursor.fetchall()]

		cursor.execute(f"SELECT * FROM `{self.table_name}` LIMIT 1")
		row = cursor.fetchone()
		cursor.close()
		conn.close()

		if not row:
			frappe.throw(_("No rows in source table"))

		# Get Frappe DocType field info
		meta = frappe.get_meta(self.frappe_doctype)
		frappe_fields = {df.fieldname: df.fieldtype for df in meta.fields}

		# Build debug info
		debug_info = []

		debug_info.append("=== Schema Column Names (from information_schema) ===")
		for col in schema_columns:
			debug_info.append(f"  Schema: '{col}'")

		debug_info.append("\n=== WordPress Row Keys (from SELECT *) ===")
		for key in row.keys():
			debug_info.append(f"  WP: '{key}' = {repr(row[key])[:50]}")

		debug_info.append(f"\n=== Frappe DocType Fields ({len(frappe_fields)} fields) ===")
		for fname, ftype in frappe_fields.items():
			debug_info.append(f"  Frappe: '{fname}' ({ftype})")

		debug_info.append("\n=== Field Matching (with lowercase) ===")
		matched = 0
		unmatched_wp = []
		for key in row.keys():
			lowercase_key = key.lower()
			if lowercase_key in frappe_fields:
				debug_info.append(f"  MATCH: WP '{key}' -> Frappe '{lowercase_key}'")
				matched += 1
			else:
				unmatched_wp.append(key)
				debug_info.append(f"  NO MATCH: WP '{key}' (lowercase: '{lowercase_key}') not in Frappe")

		debug_info.append("\n=== Summary ===")
		debug_info.append(f"Matched: {matched}/{len(row)}")
		if unmatched_wp:
			debug_info.append(f"Unmatched WP columns: {unmatched_wp}")
		else:
			debug_info.append("All columns matched!")

		frappe.msgprint("<pre>" + "\n".join(debug_info) + "</pre>", title="Debug Sync Info")

	@frappe.whitelist()
	def sync_now(self):
		"""
		Manual trigger for syncing this table's data from WordPress to Frappe.
		Enqueues the sync as a background job so the user can keep working.
		Progress is reported via toast notifications.
		"""
		if self.mirror_status != "Mirrored":
			frappe.throw(_("Table must be mirrored before syncing"))

		if not self.frappe_doctype:
			frappe.throw(_("No Frappe DocType associated with this table"))

		frappe.enqueue(
			"nce_sync.utils.data_sync.run_sync_for_table",
			queue="long",
			timeout=3600,
			wp_table_name=self.name,
			user=frappe.session.user,
		)

		frappe.msgprint(
			_("Sync started in background for {0}. You'll see progress toasts in the bottom-right.").format(
				self.nce_name or self.table_name
			),
			indicator="blue",
			alert=True,
		)
