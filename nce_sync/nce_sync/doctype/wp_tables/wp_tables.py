# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

import json

import frappe
from frappe import _
from frappe.model.document import Document


class WPTables(Document):
	"""Tracks WordPress tables selected for mirroring."""

	def autoname(self):
		"""Set document name from nce_name if provided, otherwise use table_name."""
		self.name = self.nce_name or self.table_name

	def validate(self):
		"""Validate and enforce source-of-truth hierarchy."""
		# Check if NCE Name conflicts with existing DocType
		if self.nce_name:
			self._validate_doctype_name(self.nce_name)

		# Rename document if nce_name changed and differs from current name
		desired_name = self.nce_name or self.table_name
		if self.name and self.name != desired_name and not self.is_new():
			self._rename_to(desired_name)

	def _rename_to(self, new_name):
		"""Rename this document to new_name."""
		if frappe.db.exists("WP Tables", new_name):
			frappe.throw(_("A WP Table with name '{0}' already exists").format(new_name))

		# Use Frappe's rename_doc
		frappe.rename_doc("WP Tables", self.name, new_name, force=True)
		self.name = new_name

	def _validate_doctype_name(self, name):
		"""
		Check if the proposed DocType name conflicts with existing DocTypes.
		Warns about system DocTypes and other conflicts.
		"""
		# Skip if this table already owns this DocType
		if self.frappe_doctype == name:
			return

		# Check if DocType already exists
		if frappe.db.exists("DocType", name):
			# Check if it's a system/core DocType (not custom)
			is_custom = frappe.db.get_value("DocType", name, "custom")
			if not is_custom:
				frappe.throw(
					_(
						"'{0}' is a Frappe system DocType and cannot be used. Please choose a different name."
					).format(name)
				)
			else:
				# It's a custom DocType - check if another WP Table owns it
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

	@frappe.whitelist()
	def preview_schema(self):
		"""Introspect table schema and return proposed field mappings for user review."""
		from nce_sync.utils.schema_mirror import preview_table_schema

		wp_conn = frappe.get_single("WordPress Connection")
		if not wp_conn:
			frappe.throw(_("WordPress Connection not configured"))

		return preview_table_schema(wp_conn, self)

	@frappe.whitelist()
	def mirror_schema(self, field_overrides=None, label_overrides=None, matching_fields=None):
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
				wp_conn, self, field_overrides=field_overrides, label_overrides=label_overrides
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
		from nce_sync.utils.workspace_utils import remove_from_workspace

		doctype_name = self.frappe_doctype
		if not doctype_name:
			frappe.throw(_("No mirrored DocType to delete"))

		# Remove from workspace first
		remove_from_workspace(doctype_name)

		# Delete the generated DocType
		if frappe.db.exists("DocType", doctype_name):
			frappe.delete_doc("DocType", doctype_name, force=True, ignore_permissions=True)
			frappe.db.commit()

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
		from nce_sync.utils.workspace_utils import remove_from_workspace

		doctype_name = self.frappe_doctype

		# Remove from workspace
		if doctype_name:
			remove_from_workspace(doctype_name)

			# Delete the generated DocType
			if frappe.db.exists("DocType", doctype_name):
				frappe.delete_doc("DocType", doctype_name, force=True, ignore_permissions=True)

		# Delete this WP Tables record
		table_name = self.table_name
		frappe.delete_doc("WP Tables", self.name, force=True, ignore_permissions=True)
		frappe.db.commit()

		frappe.msgprint(
			_("Removed table '{0}' and all associated data.").format(table_name),
			indicator="green",
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

		# Build column mapping: WP column name -> {fieldname, is_virtual}
		# Sanitize fieldnames to handle Frappe restricted names (e.g., 'name' -> 'name_field')
		column_mapping = {}
		virtual_count = 0
		for col in schema["columns"]:
			wp_col_name = col["COLUMN_NAME"]
			frappe_fieldname = sanitize_fieldname(wp_col_name.lower())
			extra = col.get("EXTRA", "") or ""
			is_virtual = "VIRTUAL" in extra.upper() or "GENERATED" in extra.upper()
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
		)

		frappe.msgprint(
			_("Sync started in background for {0}. You'll see progress toasts in the bottom-right.").format(
				self.nce_name or self.table_name
			),
			indicator="blue",
			alert=True,
		)
