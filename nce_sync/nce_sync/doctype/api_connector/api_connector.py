# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

import frappe
import requests
from frappe.model.document import Document
from frappe.utils import now_datetime


class APIConnector(Document):
	pass


@frappe.whitelist()
def test_connection(connector_name):
	"""Test an API Connector by sending a request to its base_url."""
	doc = frappe.get_doc("API Connector", connector_name)

	try:
		headers = {}
		auth = None

		if doc.auth_type == "API Key":
			api_key = doc.get_password("api_key") if doc.api_key else None
			if api_key:
				headers["X-API-Key"] = api_key

		elif doc.auth_type == "Basic Auth":
			username = doc.username or ""
			password = doc.get_password("password") if doc.password else ""
			auth = (username, password)

		elif doc.auth_type == "Bearer Token":
			token = doc.get_password("bearer_token") if doc.bearer_token else None
			if token:
				headers["Authorization"] = f"Bearer {token}"

		if doc.custom_headers:
			import json
			try:
				extra = json.loads(doc.custom_headers)
				headers.update(extra)
			except json.JSONDecodeError:
				pass

		timeout = doc.timeout_seconds or 30
		resp = requests.get(doc.base_url, headers=headers, auth=auth, timeout=timeout)

		success = resp.status_code < 400
		doc.db_set("last_tested", now_datetime())
		doc.db_set("last_test_result", "Success" if success else "Failed")
		doc.db_set("last_error", "" if success else f"HTTP {resp.status_code}: {resp.text[:500]}")

		return {"success": success, "status_code": resp.status_code}

	except Exception as e:
		doc.db_set("last_tested", now_datetime())
		doc.db_set("last_test_result", "Failed")
		doc.db_set("last_error", str(e)[:500])
		return {"success": False, "error": str(e)}
