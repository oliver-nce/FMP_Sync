# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""
API endpoints for NCE Sync app.
"""

import frappe
from frappe import _


@frappe.whitelist()
def toggle_auto_sync(table_names):
	"""
	Toggle the auto_sync_active field for the specified WP Tables.

	Args:
		table_names: List of WP Tables names (or JSON string)

	Returns:
		str: Summary message of changes made
	"""
	if isinstance(table_names, str):
		import json

		table_names = json.loads(table_names)

	if not table_names:
		frappe.throw(_("No tables specified"))

	enabled_count = 0
	disabled_count = 0

	for table_name in table_names:
		doc = frappe.get_doc("WP Tables", table_name)

		if doc.auto_sync_active:
			doc.auto_sync_active = 0
			disabled_count += 1
		else:
			doc.auto_sync_active = 1
			enabled_count += 1

		doc.save(ignore_permissions=True)

	frappe.db.commit()

	# Build response message
	parts = []
	if enabled_count:
		parts.append(f"{enabled_count} enabled")
	if disabled_count:
		parts.append(f"{disabled_count} disabled")

	return _("Auto sync: {0}").format(", ".join(parts))
