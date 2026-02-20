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


@frappe.whitelist()
def get_table_links_grid_data():
	"""
	Return mirrored tables and their Link field relationships for the grid UI.
	Data is derived from actual DocType metas (no stored copy).

	Returns:
		dict: {
			"tables": [{"doctype": "Events", "label": "Events"}, ...],
			"links": {"Events": {"Venues": [{"field": "venue_id", "label": "Venue"}]}}
		}
	"""
	tables = frappe.get_all(
		"WP Tables",
		filters={"mirror_status": "Mirrored", "frappe_doctype": ["!=", ""]},
		fields=["frappe_doctype", "nce_name", "table_name"],
		order_by="frappe_doctype",
	)

	# Build list of mirrored DocTypes with display labels
	doctypes = []
	seen = set()
	for row in tables:
		dt = row.get("frappe_doctype")
		if not dt or dt in seen:
			continue
		seen.add(dt)
		label = row.get("nce_name") or row.get("table_name") or dt
		doctypes.append({"doctype": dt, "label": label})

	# Scan each DocType's meta for Link fields pointing to other mirrored tables
	mirrored_set = {d["doctype"] for d in doctypes}
	links = {}  # source_doctype -> { target_doctype -> [{"field", "label"}, ...] }

	for d in doctypes:
		source = d["doctype"]
		try:
			meta = frappe.get_meta(source)
		except Exception:
			continue

		for df in meta.fields:
			if df.fieldtype != "Link" or not df.options:
				continue
			target = df.options
			if target not in mirrored_set or target == source:
				continue

			if source not in links:
				links[source] = {}
			if target not in links[source]:
				links[source][target] = []
			links[source][target].append({"field": df.fieldname, "label": df.label or df.fieldname})

	return {"tables": doctypes, "links": links}
