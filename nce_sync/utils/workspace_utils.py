# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""
Workspace management utilities for NCE_Sync.
Handles workspace creation and DocType shortcut card management.
"""

import json

import frappe
from frappe import _


def ensure_workspace():
	"""
	Ensure the NCE Sync workspace exists.
	Creates it if it doesn't exist, returns it if it does.

	Returns:
		Workspace document
	"""
	workspace_name = "NCE Sync"

	if frappe.db.exists("Workspace", workspace_name):
		return frappe.get_doc("Workspace", workspace_name)

	# Create new workspace
	workspace = frappe.get_doc(
		{
			"doctype": "Workspace",
			"name": workspace_name,
			"title": workspace_name,
			"module": "NCE Sync",
			"icon": "database",
			"is_standard": 0,
			"public": 1,
			"content": json.dumps([]),  # Empty content initially
			"links": [],  # Empty links - this keeps DocTypes out of sidebar/magic menu
		}
	)

	workspace.insert(ignore_permissions=True)
	frappe.db.commit()

	return workspace


def add_to_workspace(doctype_name, label=None):
	"""
	Add a DocType as a shortcut card to the NCE Sync workspace page content.

	Args:
		doctype_name: Name of the DocType
		label: Optional custom label (defaults to doctype_name)
	"""
	workspace = ensure_workspace()

	# Parse existing content
	try:
		content = json.loads(workspace.content) if workspace.content else []
	except json.JSONDecodeError:
		content = []

	# Check if already exists
	for item in content:
		if item.get("type") == "shortcut" and item.get("link_to") == doctype_name:
			return  # Already exists

	# Add new shortcut card
	shortcut = {
		"type": "shortcut",
		"label": label or doctype_name,
		"link_to": doctype_name,
		"link_type": "DocType",
		"color": "blue",
	}

	content.append(shortcut)

	# Update workspace
	workspace.content = json.dumps(content)
	workspace.save(ignore_permissions=True)
	frappe.db.commit()

	# Clear cache
	frappe.clear_cache()


def initialize_workspace_on_install():
	"""
	Called after app installation.
	Creates the NCE Sync workspace and adds core DocTypes.
	"""
	# Ensure workspace exists
	ensure_workspace()

	# Add core app DocTypes to workspace
	add_to_workspace("WordPress Connection")
	add_to_workspace("WP Tables")

	frappe.msgprint(_("NCE Sync workspace created successfully"), indicator="green")
