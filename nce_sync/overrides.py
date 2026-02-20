# Copyright (c) 2026, Oliver Reid and contributors
# License: MIT. See LICENSE

"""
Override Frappe methods for NCE Sync.
"""

from json import loads

import frappe
from frappe.desk import desktop as desktop_module

_original_get_desktop_page = desktop_module.get_desktop_page


@frappe.whitelist()
def get_desktop_page(page):
	"""Wrap get_desktop_page to sync Tables workspace shortcuts on load."""
	page_data = loads(page) if isinstance(page, str) else page
	if page_data.get("name") == "Tables":
		from nce_sync.utils.workspace_utils import sync_tables_workspace_shortcuts

		sync_tables_workspace_shortcuts()
	return _original_get_desktop_page(page)
