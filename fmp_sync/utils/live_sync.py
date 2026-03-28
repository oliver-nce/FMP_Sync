# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""
Live write-back from Frappe to FileMaker — STUB.

This module will eventually handle the on_update / after_insert wildcard hook
for mirrored DocTypes, pushing changes back to FileMaker via the OData API.

Currently a no-op placeholder.  The cache helpers and hook handler are kept
so that hooks.py and FM Tables can reference them without errors, but
push_record_to_fm() does nothing.

TODO: Implement OData PATCH/POST write-back when reverse sync is needed.
"""

import frappe
from frappe import _

CACHE_KEY = "fmp_sync:listen_for_changes_tables"

# Frappe system fields that should never be pushed back to FM
SKIP_FIELDS = frozenset(
	{
		"name",
		"owner",
		"creation",
		"modified",
		"modified_by",
		"docstatus",
		"idx",
		"_user_tags",
		"_comments",
		"_assign",
		"_liked_by",
	}
)


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


def _get_listen_map():
	"""
	Return a dict of {frappe_doctype: fm_table_name} for all FM Tables
	with listen_for_changes = 1 and mirror_status = "Mirrored".

	Result is cached in Redis; invalidated (via clear_sql_direct_cache) whenever
	a FM Tables record is saved or trashed.
	"""
	cached = frappe.cache().get_value(CACHE_KEY)
	if cached is not None:
		return cached

	try:
		rows = frappe.get_all(
			"FM Tables",
			filters={"mirror_status": "Mirrored", "listen_for_changes": 1},
			fields=["name", "frappe_doctype"],
		)
	except Exception:
		# Column may not exist yet during migration — treat as empty
		return {}
	mapping = {r.frappe_doctype: r.name for r in rows if r.frappe_doctype}
	frappe.cache().set_value(CACHE_KEY, mapping)
	return mapping


def clear_sql_direct_cache():
	"""Invalidate the listen-for-changes table map so it is rebuilt on next access."""
	frappe.cache().delete_value(CACHE_KEY)


# ---------------------------------------------------------------------------
# Hook handler
# ---------------------------------------------------------------------------


def on_record_change(doc, method):
	"""
	Wildcard doc_events handler (on_update / after_insert).

	Bails early for:
	- Records being saved by the inbound scheduled sync (frappe.flags.in_sync)
	- DocTypes that do not have listen_for_changes enabled

	Currently a NO-OP stub — enqueues push_record_to_fm which does nothing.
	"""
	if getattr(frappe.flags, "in_sync", False):
		return

	listen_map = _get_listen_map()
	if doc.doctype not in listen_map:
		return

	fm_table_name = listen_map[doc.doctype]

	frappe.enqueue(
		push_record_to_fm,
		fm_table_name=fm_table_name,
		doctype=doc.doctype,
		docname=doc.name,
		queue="short",
		is_async=True,
	)


# ---------------------------------------------------------------------------
# Background job — STUB
# ---------------------------------------------------------------------------


def push_record_to_fm(fm_table_name, doctype, docname):
	"""
	Background job: push one Frappe record to the matching FileMaker table.

	STUB — does nothing.  Will be implemented with OData PATCH/POST when
	Frappe → FileMaker write-back is needed.

	Args:
		fm_table_name: Name of the FM Tables document
		doctype: Frappe DocType name
		docname: Frappe document name
	"""
	# TODO: Implement OData PATCH (update) / POST (insert) write-back.
	#
	# OData write-back outline:
	#   1. Build JSON body from Frappe doc fields (skip system fields + auto-generated)
	#   2. If docname is a negative temp name → POST to {base_url}/{table_name}
	#      Read back the new ROWID from the response, rename Frappe doc.
	#   3. Otherwise → PATCH to {base_url}/{table_name}('{docname}')
	#
	# For now, log a debug message and return.
	frappe.logger("fmp_sync").debug(
		f"live_sync stub: push_record_to_fm called for {doctype}/{docname} "
		f"(FM table: {fm_table_name}) — no-op"
	)


# Backward compat alias — old name from WP version
push_record_to_wp = push_record_to_fm
