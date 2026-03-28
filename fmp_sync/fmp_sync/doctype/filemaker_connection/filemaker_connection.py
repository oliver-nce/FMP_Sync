# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""
FileMaker Connection — Single DocType for FM Server OData credentials.

Stores host, port, database, username, password, SSL settings.
Provides test_connection (GET service document) and discover_tables
(GET FileMaker_Tables, filter to base tables).
"""

import frappe
import requests
from frappe import _
from frappe.model.document import Document


class FileMakerConnection(Document):
	"""Single DocType to manage FileMaker Server OData connection."""

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
