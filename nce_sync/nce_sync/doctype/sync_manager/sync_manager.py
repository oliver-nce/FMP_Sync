# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document


class SyncManager(Document):
	@frappe.whitelist()
	def load_wp_tables(self):
		"""
		Populate the tables_to_sync child table with all mirrored WP Tables.
		Only adds tables that aren't already in the list.
		"""
		# Get all mirrored WP Tables
		wp_tables = frappe.get_all(
			"WP Tables",
			filters={"mirror_status": "Mirrored"},
			fields=["name", "table_name", "frappe_doctype"],
		)

		# Get existing table names in the list
		existing = {row.wp_table for row in self.tables_to_sync}

		# Add missing tables
		for table in wp_tables:
			if table.name not in existing:
				self.append(
					"tables_to_sync",
					{
						"wp_table": table.name,
						"table_name": table.table_name,
						"frappe_doctype": table.frappe_doctype,
						"enabled": 1,
					},
				)

		self.save()
