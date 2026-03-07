# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

import json

import frappe
import requests
from frappe.model.document import Document
from frappe.utils import now_datetime

VALID_SERVICES = [
	"WordPress", "WooCommerce", "Google Sheets", "Google Maps",
	"Authorize.net", "Stripe", "SendGrid", "Twilio", "Anthropic", "Klaviyo",
]
VALID_AUTH_TYPES = ["API Key", "Basic Auth", "Bearer Token", "OAuth2", "None"]
VALID_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"]


class APIConnector(Document):
	pass


@frappe.whitelist()
def get_credential(connector_name, fieldname):
	"""Return the decrypted value of a Password field for copying to clipboard."""
	allowed = {"api_key", "api_secret", "password", "bearer_token", "oauth_refresh_token"}
	if fieldname not in allowed:
		frappe.throw("Invalid credential field")

	doc = frappe.get_doc("API Connector", connector_name)
	doc.check_permission("read")

	value = doc.get_password(fieldname) if getattr(doc, fieldname, None) else None
	return value or ""


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


@frappe.whitelist()
def ai_discover_connector(service_name):
	"""Use the Anthropic connector's API key to research an API service."""
	if not frappe.db.exists("API Connector", "Anthropic"):
		frappe.throw("No 'Anthropic' connector found. Create one with a valid API key first.")

	anthropic_doc = frappe.get_doc("API Connector", "Anthropic")
	api_key = anthropic_doc.get_password("api_key") if anthropic_doc.api_key else None
	if not api_key:
		frappe.throw("The Anthropic connector has no API Key configured.")

	prompt = (
		f'Research the "{service_name}" REST API and return ONLY a valid JSON object '
		f"(no markdown, no explanation) with this structure:\n\n"
		f'{{\n'
		f'  "connector_name": "{service_name}",\n'
		f'  "service": "<if it matches one of [{", ".join(VALID_SERVICES)}] use that '
		f'exact name; otherwise Custom>",\n'
		f'  "base_url": "<root API URL>",\n'
		f'  "auth_type": "<one of: {", ".join(VALID_AUTH_TYPES)}>",\n'
		f'  "timeout_seconds": 30,\n'
		f'  "max_retries": 3,\n'
		f'  "rate_limit_rpm": 0,\n'
		f'  "notes": "<HTML with brief credential-setup instructions>",\n'
		f'  "endpoints": [\n'
		f'    {{\n'
		f'      "endpoint_name": "<Human-readable name>",\n'
		f'      "endpoint_key": "<snake_case identifier>",\n'
		f'      "http_method": "<one of: {", ".join(VALID_METHODS)}>",\n'
		f'      "content_type": "application/json",\n'
		f'      "path": "<URL path appended to base_url>",\n'
		f'      "description": "<What this endpoint does>",\n'
		f'      "documentation_url": "<URL to docs for this endpoint>",\n'
		f'      "sample_submission": {{}},\n'
		f'      "sample_response": {{}}\n'
		f"    }}\n"
		f"  ]\n"
		f"}}\n\n"
		f"Include the 5-10 most commonly used endpoints. "
		f"sample_submission / sample_response must be JSON objects (or null for GET). "
		f"Return ONLY valid JSON."
	)

	resp = requests.post(
		"https://api.anthropic.com/v1/messages",
		headers={
			"x-api-key": api_key,
			"anthropic-version": "2023-06-01",
			"content-type": "application/json",
		},
		json={
			"model": "claude-sonnet-4-20250514",
			"max_tokens": 4096,
			"messages": [{"role": "user", "content": prompt}],
		},
		timeout=90,
	)

	if resp.status_code != 200:
		frappe.throw(f"Anthropic API error (HTTP {resp.status_code}): {resp.text[:500]}")

	text = resp.json().get("content", [{}])[0].get("text", "")
	text = text.strip()
	if text.startswith("```"):
		text = "\n".join(text.split("\n")[1:])
		if text.rstrip().endswith("```"):
			text = text.rstrip()[:-3]
		text = text.strip()

	try:
		data = json.loads(text)
	except json.JSONDecodeError as e:
		frappe.throw(f"Failed to parse AI response as JSON: {e}\n\nRaw: {text[:1000]}")

	for ep in data.get("endpoints", []):
		for field in ("sample_submission", "sample_response"):
			val = ep.get(field)
			if val is not None and not isinstance(val, str):
				ep[field] = json.dumps(val, indent=2)
			elif val is None:
				ep[field] = ""

	return data


@frappe.whitelist()
def create_connector_from_ai(connector_data):
	"""Create an API Connector document from AI-discovered data."""
	if isinstance(connector_data, str):
		connector_data = json.loads(connector_data)

	name = connector_data.get("connector_name", "").strip()
	if not name:
		frappe.throw("Connector name is required.")
	if frappe.db.exists("API Connector", name):
		frappe.throw(f"A connector named '{name}' already exists.")

	doc = frappe.new_doc("API Connector")
	doc.connector_name = name
	doc.service = connector_data.get("service", "Custom")
	doc.base_url = connector_data.get("base_url", "")
	doc.auth_type = connector_data.get("auth_type", "API Key")
	doc.timeout_seconds = connector_data.get("timeout_seconds", 30)
	doc.max_retries = connector_data.get("max_retries", 3)
	doc.rate_limit_rpm = connector_data.get("rate_limit_rpm", 0)
	doc.notes = connector_data.get("notes", "")
	doc.status = "Inactive"

	for ep in connector_data.get("endpoints", []):
		doc.append("endpoints", {
			"endpoint_name": ep.get("endpoint_name", ""),
			"endpoint_key": ep.get("endpoint_key", ""),
			"http_method": ep.get("http_method", "GET"),
			"content_type": ep.get("content_type", "application/json"),
			"path": ep.get("path", ""),
			"description": ep.get("description", ""),
			"documentation_url": ep.get("documentation_url", ""),
			"sample_submission": ep.get("sample_submission", ""),
			"sample_response": ep.get("sample_response", ""),
		})

	doc.insert()
	return {"name": doc.name, "endpoint_count": len(doc.endpoints)}
