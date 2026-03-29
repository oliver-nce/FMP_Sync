# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

import json
import shlex

import frappe
from frappe import _
from frappe.model.document import Document

# Core Frappe DocType names - never drop these tables (safety guard)
_NEVER_DROP_DOCTYPES = frozenset(
	{
		"DocType",
		"DocField",
		"DocPerm",
		"DocType Action",
		"DocType Link",
		"User",
		"User Permission",
		"Role",
		"Module Def",
		"File",
		"Error Log",
		"Error Snapshot",
		"Scheduled Job Log",
		"Activity Log",
		"Singles",
		"DefaultValue",
		"Property Setter",
		"Custom Field",
		"Workflow",
		"Workflow State",
		"Workflow Action",
		"Workflow Transition",
	}
)


def _is_safe_to_drop_table(doctype_name):
	"""Return False if dropping this table could harm core Frappe."""
	if not doctype_name or "`" in doctype_name or ";" in doctype_name:
		return False
	if doctype_name in _NEVER_DROP_DOCTYPES:
		return False
	# Also block any non-custom DocType (core/system)
	if frappe.db.exists("DocType", doctype_name):
		is_custom = frappe.db.get_value("DocType", doctype_name, "custom")
		if not is_custom:
			return False
	return True


def _collect_soft_dependencies(doctype_name):
	"""
	Return a dict of all soft-dependency document names that reference doctype_name.
	These are artifacts Frappe does NOT clean up automatically on DocType deletion.
	"""

	def names(dt, filters):
		return frappe.get_all(dt, filters=filters, pluck="name")

	return {
		"Report": names("Report", {"ref_doctype": doctype_name}),
		"Dashboard Chart": names("Dashboard Chart", {"document_type": doctype_name}),
		"Number Card": names("Number Card", {"document_type": doctype_name}),
		"Client Script": names("Client Script", {"dt": doctype_name}),
		"Kanban Board": names("Kanban Board", {"reference_doctype": doctype_name}),
		"Print Format": names("Print Format", {"doc_type": doctype_name}),
	}


def _delete_mirrored_doctype(doctype_name):
	"""
	Fully remove a mirrored DocType and all its soft dependencies in the correct order
	so that Frappe's workspace validation never encounters a stale reference.

	Order:
	  1. Collect all soft-dependency artifacts (Reports, Charts, Scripts, etc.)
	  2. Remove their workspace links WHILE they still exist (validation passes)
	  3. Delete the artifacts
	  4. Remove the DocType workspace shortcut
	  5. Delete the DocType — Frappe drops the table and cleans hard dependencies
	"""
	if not _is_safe_to_drop_table(doctype_name):
		return

	from fmp_sync.utils.workspace_utils import remove_from_workspace

	# Step 1: collect everything that references this DocType
	deps = _collect_soft_dependencies(doctype_name)

	# Step 2 + 4: clean workspace (shortcuts + all artifact links) before anything is deleted
	remove_from_workspace(doctype_name, soft_deps=deps)

	# Step 3: delete soft-dependency artifacts now that workspace is clean
	for doctype, names in deps.items():
		for name in names:
			try:
				frappe.delete_doc(doctype, name, force=True, ignore_permissions=True)
			except Exception as e:
				frappe.log_error(title=f"Delete {doctype} '{name}' failed", message=str(e))
	if any(deps.values()):
		frappe.db.commit()

	# Step 5: delete the DocType record
	if frappe.db.exists("DocType", doctype_name):
		try:
			frappe.delete_doc("DocType", doctype_name, force=True, ignore_permissions=True)
			frappe.db.commit()
		except Exception as e:
			frappe.log_error(title=f"Delete DocType failed: {doctype_name}", message=str(e))

	# Step 6: explicitly drop the DB table (Frappe doesn't always do this reliably)
	try:
		frappe.db.sql(f"DROP TABLE IF EXISTS `tab{doctype_name}`")
		frappe.db.commit()
	except Exception as e:
		frappe.log_error(title=f"Drop table failed: tab{doctype_name}", message=str(e))


class FMTables(Document):
	"""Tracks FileMaker tables selected for mirroring."""

	def autoname(self):
		"""Set document name from fmp_name if provided, otherwise use table_name."""
		self.name = self.fmp_name or self.table_name

	def on_update(self):
		"""Invalidate the listen-for-changes cache so any toggle takes effect immediately."""
		from fmp_sync.utils.live_sync import clear_sql_direct_cache

		clear_sql_direct_cache()

	def on_trash(self):
		"""Full cascade cleanup: delete Sync Logs, mirrored DocType (Mirror mode only), workspace shortcut, and clear live-sync cache."""
		from fmp_sync.utils.live_sync import clear_sql_direct_cache

		clear_sql_direct_cache()
		# Delete associated Sync Log records
		sync_logs = frappe.get_all("Sync Log", filters={"fm_table": self.name}, pluck="name")
		for log_name in sync_logs:
			frappe.delete_doc("Sync Log", log_name, force=True, ignore_permissions=True)

		# Delete mirrored DocType (table + record) - drop table first so it always works
		if self.frappe_doctype and self.doctype_source != "Native":
			_delete_mirrored_doctype(self.frappe_doctype)

	def validate(self):
		"""Validate and enforce source-of-truth hierarchy."""
		if self.doctype_source == "Native":
			# Native entries only need a valid existing DocType — no FM table required
			if not self.frappe_doctype:
				frappe.throw(
					_(
						"Frappe DocType is required for Native mode. "
						"Select the existing DocType you want to link."
					)
				)
			if not frappe.db.exists("DocType", self.frappe_doctype):
				frappe.throw(
					_("DocType '{0}' does not exist. Please select a valid existing DocType.").format(
						self.frappe_doctype
					)
				)
		else:
			# Mirror mode — existing validation
			if self.fmp_name:
				self._validate_doctype_name(self.fmp_name)

	def _validate_doctype_name(self, name):
		"""
		Check if the proposed DocType name conflicts with existing DocTypes
		or database tables (which may belong to Frappe core or other apps).
		"""
		# Skip if this table already owns this DocType
		if self.frappe_doctype == name:
			return

		# Check if DocType already exists in the registry
		if frappe.db.exists("DocType", name):
			is_custom = frappe.db.get_value("DocType", name, "custom")
			if not is_custom:
				frappe.throw(
					_("'{0}' is a system DocType and cannot be used. Please choose a different name.").format(
						name
					)
				)
			else:
				other_table = frappe.db.get_value(
					"FM Tables", {"frappe_doctype": name, "name": ["!=", self.name]}, "name"
				)
				if other_table:
					frappe.throw(
						_(
							"'{0}' is already used by another mirrored table ({1}). Please choose a different name."
						).format(name, other_table)
					)
				else:
					frappe.throw(
						_(
							"A DocType named '{0}' already exists. Please choose a different name or delete the existing DocType first."
						).format(name)
					)

		# Check if a database table with this name already exists (possibly in use by another app)
		# Skip if this FM Tables entry owns that name (e.g. orphan table from a previous re-mirror)
		if frappe.db.sql("SHOW TABLES LIKE %s", f"tab{name}"):
			is_own_orphan = self.fmp_name == name or self.frappe_doctype == name
			if not is_own_orphan:
				frappe.throw(
					_(
						"'{0}' cannot be used — it already exists, possibly in use by another app. Please choose a different name."
					).format(name)
				)

	@frappe.whitelist()
	def preview_schema(self, table_name_override=None):
		"""Introspect table schema and return proposed field mappings for user review."""
		from fmp_sync.utils.schema_mirror import preview_table_schema

		fm_conn = frappe.get_single("FileMaker Connection")
		if not fm_conn:
			frappe.throw(_("FileMaker Connection not configured"))

		original_table_name = self.table_name
		if table_name_override:
			self.table_name = table_name_override

		try:
			return preview_table_schema(fm_conn, self)
		finally:
			self.table_name = original_table_name

	@frappe.whitelist()
	def link_external_doctype(self):
		"""
		Link an existing Native DocType: set mirror_status to 'Linked'.
		No FM table or column mapping is involved.
		"""
		if not self.frappe_doctype:
			frappe.throw(_("Frappe DocType is required."))
		if not frappe.db.exists("DocType", self.frappe_doctype):
			frappe.throw(_("DocType '{0}' does not exist.").format(self.frappe_doctype))

		self.mirror_status = "Linked"
		self.error_log = None
		self.save()

		frappe.msgprint(
			_("Native DocType '{0}' linked successfully.").format(self.frappe_doctype),
			indicator="green",
		)

	@frappe.whitelist()
	def unlink_external_doctype(self):
		"""
		Remove the Native link. Resets mirror_status to Pending.
		Does NOT delete the Frappe DocType — it belongs to another app.
		"""
		self.mirror_status = "Pending"
		self.error_log = None
		self.save()

		frappe.msgprint(
			_("Native DocType unlinked. This entry is now in Pending state."),
			indicator="orange",
		)

	@frappe.whitelist()
	def mirror_schema(
		self,
		field_overrides=None,
		label_overrides=None,
		fieldname_overrides=None,
		matching_fields=None,
		name_field_column=None,
		auto_generated_columns=None,
		modified_ts_field=None,
		created_ts_field=None,
		user_skipped_columns=None,
	):
		"""Mirror this specific table's schema to a Frappe DocType."""
		try:
			from fmp_sync.utils.schema_mirror import mirror_table_schema

			if field_overrides and isinstance(field_overrides, str):
				field_overrides = json.loads(field_overrides)

			if label_overrides and isinstance(label_overrides, str):
				label_overrides = json.loads(label_overrides)

			if fieldname_overrides and isinstance(fieldname_overrides, str):
				fieldname_overrides = json.loads(fieldname_overrides)

			# Matching fields should already be saved by JS before this is called
			# But update if provided and different (belt and suspenders)
			if matching_fields and matching_fields != self.matching_fields:
				self.matching_fields = matching_fields
				self.save()

			# Validate DocType name before mirroring
			doctype_name = self.fmp_name or self.table_name
			self._validate_doctype_name(doctype_name)

			fm_conn = frappe.get_single("FileMaker Connection")
			if not fm_conn:
				frappe.throw(_("FileMaker Connection not configured"))

			mirror_table_schema(
				fm_conn,
				self,
				field_overrides=field_overrides,
				label_overrides=label_overrides,
				fieldname_overrides=fieldname_overrides,
				name_field_column=name_field_column or None,
				auto_generated_columns=auto_generated_columns or None,
				modified_ts_field=modified_ts_field or None,
				created_ts_field=created_ts_field or None,
				user_skipped_columns=user_skipped_columns or None,
			)

			frappe.msgprint(
				_("Successfully mirrored table: {0}").format(self.table_name),
				indicator="green",
			)

		except Exception as e:
			import traceback

			self.mirror_status = "Error"
			self.error_log = traceback.format_exc()
			self.save()
			frappe.log_error(title=f"Mirror Error: {self.table_name}", message=traceback.format_exc())
			frappe.throw(_("Failed to mirror table: {0}").format(str(e)))

	@frappe.whitelist()
	def delete_mirror(self):
		"""
		Delete the generated DocType and remove from workspace.
		Resets this FM Tables entry back to Pending so it can be re-mirrored.
		"""
		doctype_name = self.frappe_doctype
		if not doctype_name:
			frappe.throw(_("No mirrored DocType to delete"))

		_delete_mirrored_doctype(doctype_name)

		# Reset this FM Tables entry
		self.frappe_doctype = None
		self.mirror_status = "Pending"
		self.error_log = None
		self.save()

		frappe.msgprint(
			_("Deleted DocType '{0}' and removed from workspace. Ready to re-mirror.").format(doctype_name),
			indicator="green",
		)

	@frappe.whitelist()
	def remove_table(self):
		"""
		Full cleanup: delete the generated DocType, remove from workspace,
		and delete this FM Tables record itself.
		"""
		doctype_name = self.frappe_doctype
		if doctype_name:
			_delete_mirrored_doctype(doctype_name)

		# Delete this FM Tables record
		table_name = self.table_name
		frappe.delete_doc("FM Tables", self.name, force=True, ignore_permissions=True)
		frappe.db.commit()

		frappe.msgprint(
			_("Removed table '{0}' and all associated data.").format(table_name),
			indicator="green",
		)

	@frappe.whitelist()
	def add_to_workspace(self):
		"""Add the mirrored DocType as a shortcut in the Tables workspace."""
		from fmp_sync.utils.workspace_utils import add_to_workspace

		if not self.frappe_doctype:
			frappe.throw(_("No mirrored DocType to add"))

		add_to_workspace(self.frappe_doctype, label=self.fmp_name or self.frappe_doctype)
		frappe.msgprint(
			_("Added '{0}' to the workspace.").format(self.frappe_doctype),
			indicator="green",
			alert=True,
		)

	@frappe.whitelist()
	def regenerate_column_mapping(self):
		"""
		Regenerate the column mapping from the FileMaker table schema (fm_schema cache on FileMaker Connection).
		Useful for tables mirrored before column_mapping was added.
		Also detects auto-generated and stored calculation fields.
		"""
		import json

		from fmp_sync.utils.schema_mirror import (
			get_fm_session,
			get_table_schema,
			classify_field,
			resolve_fieldname,
		)

		fm_conn = frappe.get_single("FileMaker Connection")
		if not fm_conn:
			frappe.throw(_("FileMaker Connection not configured"))

		session, base_url = get_fm_session(fm_conn)
		schema = get_table_schema((session, base_url), self.table_name, fm_conn_doc=fm_conn)
		session.close()

		# Build column mapping: FM field name -> {fieldname, is_stored_calc[, is_name, is_auto_generated]}
		column_mapping = {}
		skipped = []
		stored_calcs = []
		name_field_column = getattr(self, "name_field_column", None)

		user_skip_lower = set()
		skip_raw = getattr(self, "user_skipped_columns", None) or ""
		for part in skip_raw.split(","):
			p = part.strip()
			if p:
				user_skip_lower.add(p.lower())

		for col in schema["columns"]:
			fm_col_name = col["COLUMN_NAME"]
			if fm_col_name.lower() in user_skip_lower:
				continue
			classification = classify_field(col)

			if classification != "include":
				skipped.append({"field": fm_col_name, "reason": classification})
				continue

			is_auto_gen = bool(col.get("AUTO_GENERATED"))
			is_stored_calc = bool(col.get("COMPUTED")) and not is_auto_gen

			if name_field_column and fm_col_name == name_field_column:
				column_mapping[fm_col_name] = {
					"fieldname": "name",
					"is_name": True,
					"is_auto_generated": is_auto_gen,
				}
			else:
				frappe_fieldname = resolve_fieldname(fm_col_name)
				entry = {
					"fieldname": frappe_fieldname,
					"is_auto_generated": is_auto_gen,
				}
				if is_stored_calc:
					entry["is_stored_calc"] = True
					stored_calcs.append(fm_col_name)
				column_mapping[fm_col_name] = entry

		self.column_mapping = json.dumps(column_mapping)
		if skipped:
			self.skipped_fields = json.dumps(skipped)
		if stored_calcs:
			self.stored_calc_fields = json.dumps(stored_calcs)
		self.save()

		msg = _("Column mapping regenerated: {0} columns mapped").format(len(column_mapping))
		if skipped:
			msg += _(", {0} fields skipped").format(len(skipped))
		if stored_calcs:
			msg += _(", {0} stored calcs imported as ordinary fields").format(len(stored_calcs))
		frappe.msgprint(msg, indicator="green")

	@frappe.whitelist()
	def truncate_data(self):
		"""
		Delete all records from the mirrored Frappe DocType.
		The DocType structure remains intact.
		"""
		if not self.frappe_doctype:
			frappe.throw(_("No Frappe DocType associated with this table"))

		frappe.db.delete(self.frappe_doctype)
		frappe.db.commit()

		# Reset sync status since data is gone
		self.last_synced = None
		self.last_sync_status = None
		self.last_sync_log = "Data truncated manually"
		self.save()

	@frappe.whitelist()
	def remap_schema(
		self,
		new_table_name=None,
		field_overrides=None,
		label_overrides=None,
		fieldname_overrides=None,
		matching_fields=None,
		name_field_column=None,
		auto_generated_columns=None,
		modified_ts_field=None,
		created_ts_field=None,
		user_skipped_columns=None,
	):
		"""
		Remap an existing mirrored DocType to a (possibly renamed) source table.
		Truncates data, updates source reference, adds any new columns, rebuilds
		the column mapping, then leaves the DocType ready for a fresh sync.
		The DocType and its SQL table are preserved so other apps' references stay intact.
		"""
		from fmp_sync.utils.schema_mirror import mirror_table_schema

		if not self.frappe_doctype:
			frappe.throw(_("No mirrored DocType to remap"))

		if field_overrides and isinstance(field_overrides, str):
			field_overrides = json.loads(field_overrides)
		if label_overrides and isinstance(label_overrides, str):
			label_overrides = json.loads(label_overrides)
		if fieldname_overrides and isinstance(fieldname_overrides, str):
			fieldname_overrides = json.loads(fieldname_overrides)

		# Update source table name if it changed
		if new_table_name and new_table_name != self.table_name:
			self.table_name = new_table_name
			self.save()

		# Update matching fields if provided
		if matching_fields and matching_fields != self.matching_fields:
			self.matching_fields = matching_fields
			self.save()

		# Truncate existing data
		frappe.db.delete(self.frappe_doctype)
		frappe.db.commit()

		fm_conn = frappe.get_single("FileMaker Connection")
		if not fm_conn:
			frappe.throw(_("FileMaker Connection not configured"))

		# Re-mirror: detects existing DocType and calls update_existing_doctype
		# which adds new columns without removing existing ones
		mirror_table_schema(
			fm_conn,
			self,
			field_overrides=field_overrides,
			label_overrides=label_overrides,
			fieldname_overrides=fieldname_overrides,
			name_field_column=name_field_column or None,
			auto_generated_columns=auto_generated_columns or None,
			modified_ts_field=modified_ts_field or None,
			created_ts_field=created_ts_field or None,
			user_skipped_columns=user_skipped_columns or None,
		)

		# Reset sync status
		self.last_synced = None
		self.last_sync_status = None
		self.last_sync_log = "Schema remapped — ready for sync"
		self.save()

		frappe.msgprint(
			_("Remapped '{0}' to source table '{1}'. Data cleared — run Sync Now to repopulate.").format(
				self.frappe_doctype, self.table_name
			),
			indicator="green",
		)

	@frappe.whitelist()
	def debug_sync_one_row(self):
		"""
		Debug: Fetch one row via OData and show detailed info about field matching.
		"""
		from fmp_sync.utils.schema_mirror import get_fm_session

		if not self.frappe_doctype:
			frappe.throw(_("No Frappe DocType associated with this table"))

		fm_conn = frappe.get_single("FileMaker Connection")
		session, base_url = get_fm_session(fm_conn)

		# Fetch one row via OData (FM-safe query encoding — no + for spaces)
		from fmp_sync.fmp_sync.doctype.filemaker_connection.filemaker_connection import _fm_odata_url

		url = _fm_odata_url(f"{base_url}/{self.table_name}", {"$top": "1"})
		resp = session.get(url, timeout=30)
		resp.raise_for_status()
		data = resp.json()
		rows = data.get("value", [])
		session.close()

		if not rows:
			frappe.throw(_("No rows in source table"))

		row = rows[0]

		# Get Frappe DocType field info
		meta = frappe.get_meta(self.frappe_doctype)
		frappe_fields = {df.fieldname: df.fieldtype for df in meta.fields}

		# Build debug info
		debug_info = []

		debug_info.append("=== OData Row Keys (from GET $top=1) ===")
		for key in row.keys():
			if key.startswith("@"):
				continue  # Skip OData metadata
			debug_info.append(f"  FM: '{key}' = {repr(row[key])[:80]}")

		debug_info.append(f"\n=== Frappe DocType Fields ({len(frappe_fields)} fields) ===")
		for fname, ftype in frappe_fields.items():
			debug_info.append(f"  Frappe: '{fname}' ({ftype})")

		debug_info.append("\n=== Field Matching (FM field → lowercase → Frappe) ===")
		matched = 0
		unmatched_fm = []
		for key in row.keys():
			if key.startswith("@"):
				continue
			lowercase_key = key.lower()
			if lowercase_key in frappe_fields:
				debug_info.append(f"  MATCH: FM '{key}' -> Frappe '{lowercase_key}'")
				matched += 1
			else:
				unmatched_fm.append(key)
				debug_info.append(f"  NO MATCH: FM '{key}' (lowercase: '{lowercase_key}') not in Frappe")

		# Check column_mapping for remapped fields
		if self.column_mapping:
			cm = json.loads(self.column_mapping)
			debug_info.append(f"\n=== Column Mapping ({len(cm)} entries) ===")
			for fm_col, info in cm.items():
				if isinstance(info, dict):
					debug_info.append(f"  {fm_col} → {info.get('fieldname', '?')}")
				else:
					debug_info.append(f"  {fm_col} → {info}")

		non_meta_keys = [k for k in row.keys() if not k.startswith("@")]
		debug_info.append("\n=== Summary ===")
		debug_info.append(f"Matched: {matched}/{len(non_meta_keys)}")
		if unmatched_fm:
			debug_info.append(f"Unmatched FM fields: {unmatched_fm}")
		else:
			debug_info.append("All columns matched!")

		frappe.msgprint("<pre>" + "\n".join(debug_info) + "</pre>", title="Debug Sync Info")

	@frappe.whitelist()
	def sync_now(self):
		"""
		Manual trigger for syncing this table's data from FileMaker to Frappe.
		Enqueues the sync as a background job so the user can keep working.
		Progress is reported via toast notifications.
		"""
		if self.mirror_status != "Mirrored":
			frappe.throw(_("Table must be mirrored before syncing"))

		if not self.frappe_doctype:
			frappe.throw(_("No Frappe DocType associated with this table"))

		frappe.enqueue(
			"fmp_sync.utils.data_sync.run_sync_for_table",
			queue="long",
			timeout=3600,
			fm_table_name=self.name,
			user=frappe.session.user,
		)

		frappe.msgprint(
			_("Sync started in background for {0}. You'll see progress toasts in the bottom-right.").format(
				self.fmp_name or self.table_name
			),
			indicator="blue",
			alert=True,
		)

	@frappe.whitelist()
	def get_sync_curl(self):
		"""Shell-safe curl for the first OData page sync would use ($top=500, $select from mapping).

		Does not run sync. Password is embedded for local terminal testing only.
		"""
		from fmp_sync.fmp_sync.doctype.filemaker_connection.filemaker_connection import _fm_odata_url
		from fmp_sync.utils.data_sync import _build_odata_select

		SYNC_CURL_TOP = 500

		if self.doctype_source == "Native":
			frappe.throw(_("Sync curl is only available for mirrored FileMaker tables, not Native-linked DocTypes."))

		if not self.table_name:
			frappe.throw(_("Table name is required."))

		if self.mirror_status not in ("Mirrored", "Linked"):
			frappe.throw(_("Table must be Mirrored or Linked."))

		fm_conn = frappe.get_single("FileMaker Connection")
		if not fm_conn:
			frappe.throw(_("FileMaker Connection not configured"))

		column_mapping = {}
		if self.column_mapping:
			column_mapping = json.loads(self.column_mapping)

		select_fields = _build_odata_select(column_mapping)
		params = {"$top": str(SYNC_CURL_TOP)}
		if select_fields:
			params["$select"] = select_fields

		base_url = fm_conn.get_odata_base_url()
		url = f"{base_url}/{self.table_name}"
		req_url = _fm_odata_url(url, params)

		user = fm_conn.username or ""
		pwd = fm_conn.get_password("password") or ""
		cred = shlex.quote(f"{user}:{pwd}")
		url_q = shlex.quote(req_url)
		curl = "\n".join(
			[
				f"curl -sS -u {cred} {url_q} \\",
				"  -H 'Accept: application/json' \\",
				"  -H 'OData-Version: 4.0' \\",
				"  -H 'Accept-Encoding: identity' \\",
				r"  -w '\nhttp_code:%{http_code} size:%{size_download}\n'",
			]
		)
		return {"curl": curl}
