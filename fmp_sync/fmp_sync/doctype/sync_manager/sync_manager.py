# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.model.document import Document


class SyncManager(Document):
	@frappe.whitelist()
	def run_sync_now(self):
		"""Enqueue an immediate sync for all enabled mirrored tables."""
		from fmp_sync.utils.data_sync import run_sync_for_table

		tables = frappe.get_all(
			"FM Tables",
			filters={"auto_sync_active": 1, "mirror_status": ["in", ["Mirrored", "Linked"]]},
			pluck="name",
		)

		if not tables:
			frappe.msgprint(_("No tables with auto-sync enabled"))
			return _("No tables to sync")

		user = frappe.session.user
		for table_name in tables:
			frappe.enqueue(
				run_sync_for_table,
				fm_table_name=table_name,
				user=user,
				queue="default",
				is_async=True,
			)

		return _("{0} sync job(s) queued").format(len(tables))

	@frappe.whitelist()
	def load_fm_tables(self):
		"""
		Populate the tables_to_sync child table with all mirrored FM Tables.
		Only adds tables that aren't already in the list.
		"""
		# Get all mirrored FM Tables
		fm_tables = frappe.get_all(
			"FM Tables",
			filters={"mirror_status": ["in", ["Mirrored", "Linked"]]},
			fields=["name", "table_name", "frappe_doctype"],
		)

		# Get existing table names in the list
		existing = {row.fm_table for row in self.tables_to_sync}

		# Add missing tables
		for table in fm_tables:
			if table.name not in existing:
				self.append(
					"tables_to_sync",
					{
						"fm_table": table.name,
						"table_name": table.table_name,
						"frappe_doctype": table.frappe_doctype,
						"enabled": 1,
					},
				)

		self.save()
