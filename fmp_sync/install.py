# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""App install hooks: ensure Single rows and Tables workspace exist after migrate."""

import json
import os

import frappe

_WORKSPACE_NAME = "Tables"
_WORKSPACE_JSON = (
	"fmp_sync",
	"fmp_sync",
	"workspace",
	"Tables",
	"tables.json",
)


def import_tables_workspace():
	"""Insert the Tables Workspace from bundled JSON if missing (Desk sidebar / /app/tables)."""
	if not frappe.db.exists("DocType", "Workspace"):
		return
	path = frappe.get_app_path("fmp_sync", *_WORKSPACE_JSON)
	if not os.path.exists(path):
		return
	if frappe.db.exists("Workspace", _WORKSPACE_NAME):
		return
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
