# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""Ensure FMP Sync Settings singleton exists (for sites upgrading into this DocType)."""

import frappe


def execute():
	from fmp_sync.install import ensure_fmp_sync_settings_row

	ensure_fmp_sync_settings_row()
