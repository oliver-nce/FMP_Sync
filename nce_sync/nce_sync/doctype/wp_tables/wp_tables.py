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
		# Never overwrite user-entered values with auto-detection
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

			# Parse field_overrides if passed as JSON string
			if field_overrides and isinstance(field_overrides, str):
				field_overrides = json.loads(field_overrides)

			# Get WordPress Connection
			wp_conn = frappe.get_single("WordPress Connection")
			if not wp_conn:
				frappe.throw(_("WordPress Connection not configured"))

			# Mirror this table with optional field overrides
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
