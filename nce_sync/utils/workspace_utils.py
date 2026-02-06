# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""
Workspace utilities for NCE_Sync.
Adds/removes dynamically mirrored DocTypes as shortcut cards on the NCE Sync workspace.
"""

import json

import frappe

WORKSPACE_NAME = "NCE Sync"


def add_to_workspace(doctype_name, label=None):
	"""
	Add a mirrored DocType as a shortcut card to the NCE Sync workspace content.

	Added to content (shortcut cards) only — NOT to links — so that
	mirrored DocTypes appear on the workspace page but do NOT appear
	in the magic menu / awesomebar.

	Args:
		doctype_name: Name of the DocType
		label: Optional custom label (defaults to doctype_name)
	"""
	if not frappe.db.exists("Workspace", WORKSPACE_NAME):
		return  # Workspace not yet installed; will be created by bench migrate

	workspace = frappe.get_doc("Workspace", WORKSPACE_NAME)

	# Parse existing content
	try:
		content = json.loads(workspace.content) if workspace.content else []
	except json.JSONDecodeError:
		content = []

	# Check if shortcut already exists for this DocType
	for item in content:
		if item.get("type") == "shortcut" and item.get("data", {}).get("shortcut_name") == doctype_name:
			return  # Already exists

	# Add new shortcut card entry to content
	shortcut_entry = {
		"id": frappe.generate_hash(length=10),
		"type": "shortcut",
		"data": {
			"shortcut_name": doctype_name,
			"col": 4,
		},
	}
	content.append(shortcut_entry)

	# Also add to the shortcuts child table (needed for content shortcut references)
	workspace.append(
		"shortcuts",
		{
			"label": label or doctype_name,
			"link_to": doctype_name,
			"type": "DocType",
			"doc_view": "List",
			"color": "Grey",
			"stats_filter": "[]",
		},
	)

	# Update workspace content
	workspace.content = json.dumps(content)
	workspace.save(ignore_permissions=True)
	frappe.db.commit()

	frappe.clear_cache()


def remove_from_workspace(doctype_name):
	"""
	Remove a mirrored DocType's shortcut card from the NCE Sync workspace.

	Args:
		doctype_name: Name of the DocType to remove
	"""
	if not frappe.db.exists("Workspace", WORKSPACE_NAME):
		return

	workspace = frappe.get_doc("Workspace", WORKSPACE_NAME)

	# Remove from content JSON
	try:
		content = json.loads(workspace.content) if workspace.content else []
	except json.JSONDecodeError:
		content = []

	content = [
		item
		for item in content
		if not (item.get("type") == "shortcut" and item.get("data", {}).get("shortcut_name") == doctype_name)
	]

	# Remove from shortcuts child table
	workspace.shortcuts = [s for s in workspace.shortcuts if s.link_to != doctype_name]

	# Save
	workspace.content = json.dumps(content)
	workspace.save(ignore_permissions=True)
	frappe.db.commit()

	frappe.clear_cache()
