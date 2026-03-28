# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""Add listen_for_changes column to FM Tables table."""

import frappe


def execute():
	"""Add the listen_for_changes column to the `tabFM Tables` table."""
	if not frappe.db.table_exists("FM Tables"):
		return

	# Check if the column already exists
	if frappe.db.has_column("FM Tables", "listen_for_changes"):
		return

	# Add the column with default value 0
	frappe.db.sql(
		"""
		ALTER TABLE `tabFM Tables`
		ADD COLUMN `listen_for_changes` TINYINT(1) NOT NULL DEFAULT 0
		"""
	)
	frappe.db.commit()
