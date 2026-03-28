# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""
Reverse-sync utilities: Frappe → FileMaker — STUB.

This module will eventually handle pushing new/updated Frappe records back
to FileMaker via the OData API (POST for inserts, PATCH for updates).

Currently all core functions are no-op placeholders.  The temp-name helpers
and before_insert hook are kept functional so that new records created in
Frappe get a temporary negative-integer name that won't collide with
real FM ROWIDs.

TODO: Implement OData POST/PATCH write-back when reverse sync is needed.
"""

import json
import re

import frappe
from frappe import _

# ---------------------------------------------------------------------------
# Temp-name counter key stored in Frappe's "Singles" / cache
# ---------------------------------------------------------------------------
_COUNTER_DOCTYPE = "FMP Sync Settings"
_COUNTER_FIELD = "temp_name_counter"


def _next_temp_name():
	"""
	Return the next negative integer to use as a temporary Frappe document name.

	Uses `frappe.db` with an atomic decrement stored in the
	"FMP Sync Settings" singleton.  Falls back to a DB-level MIN if
	the singleton doesn't exist yet.
	"""
	try:
		current = frappe.db.get_single_value(_COUNTER_DOCTYPE, _COUNTER_FIELD) or 0
		current = int(current)
	except Exception:
		# Singleton or field missing – derive from actual min name in use
		current = 0

	if current >= 0:
		# Bootstrap from the lowest negative name already in any mirrored table
		current = 0

	new_val = current - 1
	try:
		frappe.db.set_single_value(_COUNTER_DOCTYPE, _COUNTER_FIELD, new_val)
	except Exception:
		pass  # non-critical – worst case we get a collision, handled below

	return str(new_val)


def _is_mirrored_doctype(doctype):
	"""Return the FM Tables doc if *doctype* is a mirrored FMP Sync table."""
	if not frappe.db.table_exists("FM Tables"):
		return None
	try:
		return frappe.db.get_value(
			"FM Tables",
			{"frappe_doctype": doctype, "mirror_status": "Mirrored"},
			["name", "frappe_doctype", "name_field_column"],
			as_dict=True,
		)
	except Exception:
		return None


# ---------------------------------------------------------------------------
# Frappe before_insert hook
# ---------------------------------------------------------------------------


def assign_temp_name(doc, method=None):
	"""
	before_insert hook – assign a negative auto-decrementing integer name to
	any new document belonging to a mirrored DocType that uses name_field_column
	(i.e. the FM primary-key column maps to Frappe's `name` field).

	This keeps the record locally visible while we wait to push it to FileMaker
	and learn its real auto-increment ID.
	"""
	fm_table_ref = _is_mirrored_doctype(doc.doctype)
	if not fm_table_ref:
		return
	if not fm_table_ref.get("name_field_column"):
		return  # Only applies when FM PK is mapped to Frappe name

	# Only intercept if the name hasn't been set or looks like a Frappe hash
	# (i.e. the user hasn't supplied a real ID)
	current_name = getattr(doc, "name", None) or ""
	if current_name and not _looks_like_temp_or_hash(current_name):
		return  # Already has a real ID – leave it alone

	temp_name = _next_temp_name()
	# Ensure uniqueness in case of counter drift
	doctype = doc.doctype
	while frappe.db.exists(doctype, temp_name):
		temp_name = str(int(temp_name) - 1)

	doc.name = temp_name


def _looks_like_temp_or_hash(name):
	"""
	Return True if *name* looks like a Frappe-generated hash or a temp negative
	integer that we should replace.
	"""
	if not name:
		return True
	# Negative integers are our own temp names
	try:
		return int(name) < 0
	except (ValueError, TypeError):
		pass
	# Frappe hash format: 10 hex chars
	return bool(re.fullmatch(r"[0-9a-f]{10}", name.lower()))


def _is_temp_name(name):
	"""Return True if *name* is a negative integer (temp name awaiting FM INSERT)."""
	try:
		return int(name) < 0
	except (ValueError, TypeError):
		return False


# ---------------------------------------------------------------------------
# Core reverse-sync functions — STUBS
# ---------------------------------------------------------------------------


def sync_frappe_to_fm(fm_table_doc, user=None):
	"""
	Push all pending Frappe → FileMaker changes for a mirrored table.

	STUB — returns zero counts.  Will be implemented with OData POST/PATCH
	when reverse sync is needed.

	TODO: Implement with OData API:
	  - POST {base_url}/{table_name} for new records (negative temp name)
	  - PATCH {base_url}/{table_name}(ROWID) for existing records
	  - Read back new ROWID from POST response, rename Frappe doc

	Args:
		fm_table_doc: FM Tables document
		user: optional user ID for realtime progress events

	Returns:
		dict with inserted/updated/errors counts (all zero for now)
	"""
	frappe.logger("fmp_sync").debug(
		f"reverse_sync stub: sync_frappe_to_fm called for {fm_table_doc.table_name} — no-op"
	)
	return {"inserted": 0, "updated": 0, "errors": 0}


def insert_record_to_fm(fm_conn_doc, fm_table_doc, frappe_doc):
	"""
	INSERT one Frappe document into FileMaker via OData POST.

	STUB — raises NotImplementedError.

	TODO: Implement OData POST to {base_url}/{table_name}
	"""
	raise NotImplementedError(
		"Reverse sync INSERT not yet implemented. "
		"OData POST write-back is planned for a future release."
	)


def update_record_in_fm(fm_conn_doc, fm_table_doc, frappe_doc):
	"""
	UPDATE one existing Frappe document in FileMaker via OData PATCH.

	STUB — raises NotImplementedError.

	TODO: Implement OData PATCH to {base_url}/{table_name}('{ROWID}')
	"""
	raise NotImplementedError(
		"Reverse sync UPDATE not yet implemented. "
		"OData PATCH write-back is planned for a future release."
	)


# Backward compat aliases — old names from WP version
sync_frappe_to_wp = sync_frappe_to_fm
insert_record_to_wp = insert_record_to_fm
update_record_in_wp = update_record_in_fm
