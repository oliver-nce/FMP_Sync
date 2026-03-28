# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""App install hooks: ensure Single rows exist after migrate."""

import frappe


def ensure_fmp_sync_settings_row():
	"""Create the FMP Sync Settings singleton row if missing (temp name counter)."""
	if not frappe.db.exists("DocType", "FMP Sync Settings"):
		return
	if frappe.db.exists("FMP Sync Settings", "FMP Sync Settings"):
		return
	doc = frappe.new_doc("FMP Sync Settings")
	doc.temp_name_counter = 0
	doc.insert(ignore_permissions=True)
	frappe.db.commit()


def after_install():
	"""Run after `bench install-app` / first install."""
	ensure_fmp_sync_settings_row()
