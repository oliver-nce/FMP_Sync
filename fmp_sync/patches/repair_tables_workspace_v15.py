# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""One-time v15 repair: rebuild Tables workspace from JSON (sidebar not showing)."""

import frappe


def execute():
	from fmp_sync.install import import_tables_workspace

	import_tables_workspace(replace_existing=True)
