# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""App install hooks: ensure Single rows and Tables workspace exist after migrate."""

import json
import os
import traceback

import frappe

_WORKSPACE_NAME = "Tables"
_WORKSPACE_JSON = (
	"fmp_sync",
	"workspace",
	"Tables",
	"tables.json",
)


def import_tables_workspace(*, replace_existing=False):
	"""Load the Tables Workspace from bundled JSON (v15 Desk sidebar / /app/tables).

	If the row already exists, we skip by default so local edits are preserved.
	Pass replace_existing=True to delete and re-insert from JSON (repair / one-time patch).
	"""
	if not frappe.db.exists("DocType", "Workspace"):
		return
	path = frappe.get_app_path("fmp_sync", *_WORKSPACE_JSON)
	if not os.path.exists(path):
		frappe.log_error(
			title="FMP Sync: Tables workspace JSON missing",
			message=f"Expected file not found: {path}",
		)
		return

	exists = frappe.db.exists("Workspace", _WORKSPACE_NAME)
	if exists and not replace_existing:
		return
	if exists and replace_existing:
		try:
			frappe.delete_doc("Workspace", _WORKSPACE_NAME, force=True, ignore_permissions=True)
			frappe.db.commit()
		except Exception:
			frappe.log_error(title="FMP Sync: failed to remove old Tables workspace")
			raise

	try:
		with open(path, encoding="utf-8") as f:
			data = json.load(f)
		for row in data.get("shortcuts") or []:
			row.setdefault("doctype", "Workspace Shortcut")
		for row in data.get("links") or []:
			row.setdefault("doctype", "Workspace Link")
		doc = frappe.get_doc(data)
		doc.flags.ignore_links = True
		doc.insert(ignore_permissions=True)
		frappe.db.commit()
		frappe.clear_cache()
	except Exception:
		frappe.log_error(
			title="FMP Sync: Tables workspace import failed",
			message=traceback.format_exc(),
		)
		raise


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
	import_tables_workspace()
