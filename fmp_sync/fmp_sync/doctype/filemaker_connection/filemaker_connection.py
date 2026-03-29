# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""
FileMaker Connection — Single DocType for FM Server OData credentials.

Stores host, port, database, username, password, SSL settings.
Provides test_connection (GET service document) and discover_tables
(GET FileMaker_Tables, filter to base tables).
"""

import json

import frappe
import requests
from frappe import _
from frappe.model.document import Document

from fmp_sync.utils.schema_mirror import _fm_field_class_computed, _fm_fieldtype_to_edm, _odata_get


def _fm_odata_follow_pages(session, url, params=None):
	"""Collect all OData pages; return None if the first response is 404."""
	resp = session.get(url, params=params, timeout=30)
	if resp.status_code == 401:
		frappe.throw(_("OData authentication failed (401). Check credentials."))
	if resp.status_code == 403:
		frappe.throw(_("OData access forbidden (403). Check fmodata privilege."))
	if resp.status_code == 404:
		return None
	resp.raise_for_status()
	data = resp.json()
	rows = list(data.get("value", []))
	while True:
		next_link = data.get("@odata.nextLink") or data.get("odata.nextLink")
		if not next_link:
			break
		data = _odata_get(session, next_link)
		rows.extend(data.get("value", []))
	return rows


def _fm_odata_follow_pages_required(session, url, params=None):
	rows = _fm_odata_follow_pages(session, url, params)
	if rows is None:
		frappe.throw(_("OData resource not found (404): {0}").format(url))
	return rows


def _fetch_fm_schema(session, base_url):
	"""Build full FM schema JSON (tables + fields) via OData system entities."""
	tables_url = f"{base_url}/FileMaker_Tables"
	table_rows = _fm_odata_follow_pages_required(
		session,
		tables_url,
		{"$select": "TableName,BaseTableName,TableId"},
	)
	field_params_bt = {"$select": "BaseTableName,FieldName,FieldType,FieldClass,FieldReps"}
	field_params_f = {"$select": "TableName,FieldName,FieldType,FieldClass,FieldReps"}

	field_rows = _fm_odata_follow_pages(
		session, f"{base_url}/FileMaker_BaseTableFields", field_params_bt
	)
	group_by_base = field_rows is not None
	if not group_by_base:
		field_rows = _fm_odata_follow_pages_required(
			session, f"{base_url}/FileMaker_Fields", field_params_f
		)

	fields_by_base = {}
	fields_by_table = {}
	if group_by_base:
		for row in field_rows:
			bt = row.get("BaseTableName") or row.get("baseTableName")
			if not bt:
				continue
			fields_by_base.setdefault(bt, []).append(row)
	else:
		for row in field_rows:
			tn = row.get("TableName") or row.get("tableName")
			if not tn:
				continue
			fields_by_table.setdefault(tn, []).append(row)

	tables_out = []
	iso = frappe.utils.now_datetime().isoformat()
	sort_key = lambda r: (r.get("TableName") or r.get("tableName") or "")
	for tr in sorted(table_rows, key=sort_key):
		tn = tr.get("TableName") or tr.get("tableName")
		bn = tr.get("BaseTableName") or tr.get("baseTableName")
		if not tn or tn.startswith("FileMaker_"):
			continue
		is_base = (tn == bn) if bn else True
		if group_by_base:
			raw_fields = fields_by_base.get(bn, [])
		else:
			raw_fields = fields_by_table.get(tn, [])
		fm_fields = []
		for row in raw_fields:
			fn = row.get("FieldName") or row.get("fieldName")
			if not fn:
				continue
			ft = row.get("FieldType") or row.get("fieldType")
			fcl = row.get("FieldClass") or row.get("fieldClass") or "Normal"
			freps = row.get("FieldReps") or row.get("fieldReps") or 1
			try:
				freps = int(freps)
			except (TypeError, ValueError):
				freps = 1
			edm = _fm_fieldtype_to_edm(ft)
			computed = _fm_field_class_computed(fcl)
			fm_fields.append({
				"COLUMN_NAME": fn,
				"EDM_TYPE": edm,
				"COLUMN_TYPE": edm.replace("Edm.", ""),
				"IS_NULLABLE": "YES",
				"MAX_LENGTH": None,
				"COMPUTED": computed,
				"AUTO_GENERATED": False,
				"VERSION_ID": False,
				"MAX_REPETITIONS": freps,
				"FIELD_CLASS": fcl,
			})
		tables_out.append({
			"table_name": tn,
			"base_table_name": bn or tn,
			"is_base_table": is_base,
			"fields": fm_fields,
		})
	return {"fetched_at": iso, "tables": tables_out}


class FileMakerConnection(Document):
	"""Single DocType to manage FileMaker Server OData connection."""

	def onload(self):
		"""Normalize schema-cache fields for Desk (read-only + empty = hidden in Frappe).

		These assignments only affect the document sent to the browser for this request;
		they do not save to the database.
		"""
		if self.fm_schema is None or self.fm_schema == "":
			self.fm_schema = {}
		elif isinstance(self.fm_schema, str):
			try:
				self.fm_schema = json.loads(self.fm_schema)
			except Exception:
				self.fm_schema = {}

		raw = self.get("fm_schema_fetched_at")
		placeholder = _("Not cached yet")
		if raw is None or (isinstance(raw, str) and not str(raw).strip()):
			self.fm_schema_fetched_at = placeholder

	def get_odata_base_url(self):
		"""Build the OData base URL from connection settings.

		Returns:
			str: e.g. https://fms.example.com:443/fmi/odata/v4/MyDatabase
		"""
		protocol = "https" if self.use_ssl else "http"
		port = self.port or 443
		# Omit port from URL if it's the default for the protocol
		if (protocol == "https" and port == 443) or (protocol == "http" and port == 80):
			host_part = self.host
		else:
			host_part = f"{self.host}:{port}"
		return f"{protocol}://{host_part}/fmi/odata/v4/{self.database}"

	def get_odata_session(self):
		"""Create a requests.Session configured with Basic Auth and SSL settings.

		Returns:
			tuple: (requests.Session, base_url string)
		"""
		session = requests.Session()
		session.auth = (self.username, self.get_password("password"))
		session.verify = bool(self.verify_ssl)
		session.headers.update({
			"Accept": "application/json",
			"OData-Version": "4.0",
		})
		base_url = self.get_odata_base_url()
		return session, base_url

	@frappe.whitelist()
	def test_connection(self):
		"""Test the FileMaker OData connection by fetching the service document.

		GET /fmi/odata/v4/{database}
		A 200 response with a valid JSON service document confirms connectivity,
		authentication, and that the fmodata privilege is enabled.
		"""
		try:
			session, base_url = self.get_odata_session()

			# GET service document
			resp = session.get(base_url, timeout=15)

			if resp.status_code == 401:
				raise Exception(
					"Authentication failed (401). Check username/password "
					"and ensure the account has the fmodata extended privilege."
				)
			if resp.status_code == 403:
				raise Exception(
					"Access forbidden (403). The fmodata extended privilege "
					"may not be enabled for this account."
				)
			if resp.status_code == 404:
				raise Exception(
					f"Database not found (404). Check that '{self.database}' "
					"is hosted and OData is enabled on the server."
				)

			resp.raise_for_status()

			data = resp.json()
			session.close()

			# Count available entity sets (tables)
			entity_sets = data.get("value", [])
			table_count = len(entity_sets)

			# Try to extract server version from $metadata annotations
			server_info = self._detect_server_info(session, base_url)

			self.odata_base_url = base_url
			self.connection_status = "Connected"
			self.connection_message = _(
				"Connected. {0} table(s) available via OData."
			).format(table_count)
			if server_info:
				self.server_product_info = server_info
			self.save()

			frappe.msgprint(
				_("Connection successful! {0} table(s) found.").format(table_count),
				indicator="green",
			)

		except requests.exceptions.SSLError as e:
			self.connection_status = "Failed"
			self.connection_message = (
				f"SSL Error: {str(e)[:200]}. "
				"If using a self-signed certificate, uncheck 'Verify SSL Certificate'."
			)
			self.save()
			frappe.throw(self.connection_message)

		except requests.exceptions.ConnectionError as e:
			self.connection_status = "Failed"
			self.connection_message = (
				f"Connection error: cannot reach {self.host}:{self.port or 443}. "
				f"Check hostname and port. ({str(e)[:150]})"
			)
			self.save()
			frappe.throw(self.connection_message)

		except Exception as e:
			self.connection_status = "Failed"
			self.connection_message = str(e)[:500]
			self.save()
			frappe.throw(_("Connection failed: {0}").format(str(e)[:300]))

	def _detect_server_info(self, session, base_url):
		"""Try to read server/product version from $metadata annotations.

		Returns:
			str or None: Server version string if detected
		"""
		try:
			resp = session.get(f"{base_url}/$metadata", timeout=15)
			if resp.status_code == 200:
				# The $metadata XML may contain a product annotation
				# e.g. <Annotation Term="..." String="FileMaker Server 21.0.1"/>
				text = resp.text
				# Quick scan for product version in the XML
				import re

				match = re.search(r'FileMaker[^"<]*\d+\.\d+[^"<]*', text)
				if match:
					return match.group(0).strip()
		except Exception:
			pass
		return None

	@frappe.whitelist()
	def refresh_fm_schema(self):
		"""Fetch FileMaker_Tables + fields OData, store JSON on this connection (singleton)."""
		session, base_url = self.get_odata_session()
		try:
			result = _fetch_fm_schema(session, base_url)
		finally:
			session.close()
		# Assign dict — Frappe JSON field stores it correctly (avoid double-encoded strings).
		self.fm_schema = result
		self.fm_schema_fetched_at = frappe.utils.format_datetime(frappe.utils.now_datetime())
		self.save()
		table_count = len(result.get("tables", []))
		field_count = sum(len(t["fields"]) for t in result.get("tables", []))
		frappe.msgprint(
			_("Schema cached: {0} table(s), {1} field(s).").format(table_count, field_count),
			indicator="green",
		)
		return {"table_count": table_count, "field_count": field_count}

	@frappe.whitelist()
	def discover_tables(self):
		"""Discover all base tables from FileMaker via OData.

		Queries the service document for entity set names.
		Optionally queries FileMaker_Tables to filter to base tables only
		(where tableName == BaseTableName).

		Returns:
			list of dicts: [{"table_name": "...", "table_type": "BASE TABLE"}, ...]
		"""
		try:
			session, base_url = self.get_odata_session()

			# Step 1: Get entity sets from service document
			resp = session.get(base_url, timeout=15)
			resp.raise_for_status()
			data = resp.json()

			entity_sets = data.get("value", [])
			all_tables = {es.get("name"): "TABLE" for es in entity_sets}

			# Step 2: Try FileMaker_Tables to identify base tables
			base_tables = set()
			try:
				fm_tables_url = f"{base_url}/FileMaker_Tables"
				fm_resp = session.get(fm_tables_url, timeout=15)
				if fm_resp.status_code == 200:
					fm_data = fm_resp.json()
					for row in fm_data.get("value", []):
						table_name = row.get("TableName") or row.get("tableName")
						base_name = row.get("BaseTableName") or row.get("baseTableName")
						if table_name and base_name and table_name == base_name:
							base_tables.add(table_name)
			except Exception:
				# FileMaker_Tables may not be available — fall back to all entity sets
				pass

			session.close()

			# Build result: mark base tables vs table occurrences
			result = []
			for table_name, table_type in sorted(all_tables.items()):
				# Skip system tables in the result list
				if table_name.startswith("FileMaker_"):
					continue
				entry = {
					"table_name": table_name,
					"table_type": "BASE TABLE" if (
						table_name in base_tables or not base_tables
					) else "TABLE OCCURRENCE",
				}
				result.append(entry)

			try:
				self.refresh_fm_schema()
			except Exception:
				frappe.log_error(title="FMP Sync: Schema cache refresh failed")

			return result

		except Exception as e:
			frappe.throw(_("Failed to discover tables: {0}").format(str(e)[:300]))

	@frappe.whitelist()
	def mirror_all(self):
		"""Mirror all selected FM_Tables to Frappe DocTypes."""
		try:
			from fmp_sync.utils.schema_mirror import mirror_table_schema

			fm_tables = frappe.get_all("FM Tables", fields=["name"])

			if not fm_tables:
				frappe.msgprint(_("No tables selected for mirroring."), indicator="orange")
				return

			success_count = 0
			error_count = 0

			for table in fm_tables:
				try:
					doc = frappe.get_doc("FM Tables", table.name)
					mirror_table_schema(self, doc)
					success_count += 1
				except Exception as e:
					error_count += 1
					frappe.log_error(title=f"Mirror Error: {table.name}", message=str(e))

			if error_count == 0:
				frappe.msgprint(
					_("Successfully mirrored {0} table(s)").format(success_count),
					indicator="green",
				)
			else:
				frappe.msgprint(
					_(
						"Mirrored {0} table(s) with {1} error(s). Check Error Log for details."
					).format(success_count, error_count),
					indicator="orange",
				)

		except Exception as e:
			frappe.throw(_("Mirror operation failed: {0}").format(str(e)))
