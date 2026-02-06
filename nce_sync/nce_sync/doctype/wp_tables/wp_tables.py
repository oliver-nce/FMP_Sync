# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

import json

import frappe
from frappe import _
from frappe.model.document import Document


class WPTables(Document):
	"""Tracks WordPress tables selected for mirroring."""

	def validate(self):
		"""Validate and enforce source-of-truth hierarchy."""
		# Source-of-truth: User values > Auto-detected > Defaults
		pass

	@frappe.whitelist()
	def preview_schema(self):
		"""Introspect table schema and return proposed field mappings for user review."""
		from nce_sync.utils.schema_mirror import preview_table_schema

		wp_conn = frappe.get_single("WordPress Connection")
		if not wp_conn:
			frappe.throw(_("WordPress Connection not configured"))

		return preview_table_schema(wp_conn, self)

	@frappe.whitelist()
	def mirror_schema(self, field_overrides=None):
		"""Mirror this specific table's schema to a Frappe DocType."""
		try:
			from nce_sync.utils.schema_mirror import mirror_table_schema

			if field_overrides and isinstance(field_overrides, str):
				field_overrides = json.loads(field_overrides)

			wp_conn = frappe.get_single("WordPress Connection")
			if not wp_conn:
				frappe.throw(_("WordPress Connection not configured"))

			mirror_table_schema(wp_conn, self, field_overrides=field_overrides)

			frappe.msgprint(
				_("Successfully mirrored table: {0}").format(self.table_name),
				indicator="green",
			)

		except Exception as e:
			self.mirror_status = "Error"
			self.error_log = str(e)
			self.save()
			frappe.log_error(title=f"Mirror Error: {self.table_name}", message=str(e))
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
