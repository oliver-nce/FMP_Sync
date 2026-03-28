# Copyright (c) 2026, Oliver Reid and contributors
# License: MIT. See LICENSE

"""
Override Frappe methods for FMP Sync.
"""

from json import dumps, loads

import frappe
from frappe.desk import desktop as desktop_module

_original_get_desktop_page = desktop_module.get_desktop_page

VERSION_BLOCK_ID = "fmp_version"


@frappe.whitelist()
def get_desktop_page(page):
	"""Wrap get_desktop_page to sync Tables workspace shortcuts on load."""
	page_data = loads(page) if isinstance(page, str) else page
	is_tables = page_data.get("name") == "Tables"

	if is_tables:
		from fmp_sync.utils.workspace_utils import sync_tables_workspace_shortcuts
		sync_tables_workspace_shortcuts()

	result = _original_get_desktop_page(page)

	if is_tables:
		_inject_version(result)

	return result


def _inject_version(result):
	"""Append a small version label to the bottom of the Tables workspace."""
	from fmp_sync import __version__

	page_doc = result.get("page") if isinstance(result, dict) else None
	if not page_doc:
		return

	content_str = page_doc.get("content", "[]")
	try:
		blocks = loads(content_str) if isinstance(content_str, str) else content_str
	except Exception:
		return

	blocks = [b for b in blocks if b.get("id") != VERSION_BLOCK_ID]
	blocks.append({
		"id": VERSION_BLOCK_ID,
		"type": "header",
		"data": {
			"text": (
				'<span class="text-muted" style="font-size:11px;">'
				f"FMP Sync v{__version__}"
				"</span>"
			),
			"col": 12,
		},
	})
	page_doc["content"] = dumps(blocks)
