# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""Create the Table Links page if it does not exist."""

import frappe


def execute():
	if frappe.db.exists("Page", "table-links"):
		return

	page = frappe.new_doc("Page")
	page.page_name = "table-links"
	page.title = "Table Links"
	page.module = "FMP Sync"
	page.standard = "Yes"
	page.icon = "link"
	page.insert(ignore_permissions=True)
	frappe.db.commit()
