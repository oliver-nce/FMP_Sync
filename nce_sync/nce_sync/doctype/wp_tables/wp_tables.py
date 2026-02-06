# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.model.document import Document


class WPTables(Document):
	"""Tracks WordPress tables selected for mirroring."""

	def validate(self):
		"""Validate and enforce source-of-truth hierarchy."""
		# Source-of-truth: User values > Auto-detected > Defaults
		# Never overwrite user-entered values with auto-detection

		# If timestamp fields are being set programmatically (e.g., by auto-detection),
		# check if user has already set them. If so, preserve user values.
		# This logic is primarily defensive; auto-detection in schema_mirror.py
		# should respect existing values.

		# No specific validation needed here beyond what's in JSON
		pass

	@frappe.whitelist()
	def mirror_schema(self):
		"""Mirror this specific table's schema to a Frappe DocType."""
		try:
			from nce_sync.utils.schema_mirror import mirror_table_schema

			# Get WordPress Connection
			wp_conn = frappe.get_single("WordPress Connection")
			if not wp_conn:
				frappe.throw(_("WordPress Connection not configured"))

			# Mirror this table
			mirror_table_schema(wp_conn, self)

			frappe.msgprint(_("Successfully mirrored table: {0}").format(self.table_name), indicator="green")

		except Exception as e:
			self.mirror_status = "Error"
			self.error_log = str(e)
			self.save()
			frappe.log_error(title=f"Mirror Error: {self.table_name}", message=str(e))
			frappe.throw(_("Failed to mirror table: {0}").format(str(e)))
