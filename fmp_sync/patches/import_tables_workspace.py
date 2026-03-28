# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""Ensure the Tables Workspace exists (module JSON is not always synced to tabWorkspace)."""

import frappe


def execute():
	from fmp_sync.install import import_tables_workspace

	import_tables_workspace()
