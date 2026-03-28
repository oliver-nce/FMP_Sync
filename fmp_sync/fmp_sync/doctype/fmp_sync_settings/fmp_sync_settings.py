# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

from frappe.model.document import Document


class FMPSyncSettings(Document):
	"""Singleton: internal counters and flags for FMP Sync (e.g. temp Frappe names)."""

	pass
