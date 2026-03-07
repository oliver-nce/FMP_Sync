// Copyright (c) 2026, Oliver Reid and contributors
// For license information, please see license.txt

frappe.listview_settings["API Connector"] = {
	onload(listview) {
		let $btn = listview.page.add_inner_button(
			__("AI Discover"),
			() => ai_discover_dialog(listview)
		);
		$btn.append(
			' <span style="background:#7c3aed;color:#fff;font-size:9px;' +
				"padding:1px 5px;border-radius:3px;font-weight:600;" +
				'vertical-align:middle;margin-left:2px;">AI</span>'
		);
	},
};

function ai_discover_dialog(listview) {
	let d = new frappe.ui.Dialog({
		title: __("AI Discover API Connector"),
		fields: [
			{
				fieldtype: "Data",
				fieldname: "service_name",
				label: __("API Service Name"),
				description: __("e.g. Mailchimp, HubSpot, Slack, OpenAI …"),
				reqd: 1,
			},
		],
		primary_action_label: __("Discover"),
		primary_action(values) {
			d.hide();
			frappe.call({
				method: "nce_sync.nce_sync.doctype.api_connector.api_connector.ai_discover_connector",
				args: { service_name: values.service_name },
				freeze: true,
				freeze_message: __("AI is researching {0} API…", [values.service_name]),
				callback(r) {
					if (r.message) {
						show_discovery_results(r.message, listview);
					}
				},
			});
		},
	});
	d.show();
}

function show_discovery_results(data, listview) {
	let endpoint_rows = (data.endpoints || [])
		.map(
			(ep, i) =>
				"<tr>" +
				`<td>${i + 1}</td>` +
				`<td><strong>${frappe.utils.escape_html(ep.endpoint_name)}</strong></td>` +
				`<td><code>${ep.http_method}</code></td>` +
				`<td><code>${frappe.utils.escape_html(ep.path)}</code></td>` +
				`<td>${frappe.utils.escape_html(ep.description || "")}</td>` +
				"</tr>"
		)
		.join("");

	let html =
		'<div style="margin-bottom:15px;">' +
		"<h5>Connector Details</h5>" +
		'<table class="table table-bordered" style="font-size:13px;">' +
		`<tr><td style="width:140px;font-weight:600;">Name</td><td>${frappe.utils.escape_html(data.connector_name)}</td></tr>` +
		`<tr><td style="font-weight:600;">Service</td><td>${frappe.utils.escape_html(data.service)}</td></tr>` +
		`<tr><td style="font-weight:600;">Base URL</td><td><code>${frappe.utils.escape_html(data.base_url)}</code></td></tr>` +
		`<tr><td style="font-weight:600;">Auth Type</td><td>${frappe.utils.escape_html(data.auth_type)}</td></tr>` +
		`<tr><td style="font-weight:600;">Rate Limit</td><td>${data.rate_limit_rpm || "Not set"} req/min</td></tr>` +
		"</table></div>" +
		"<div>" +
		`<h5>Endpoints (${(data.endpoints || []).length})</h5>` +
		'<div style="max-height:300px;overflow-y:auto;">' +
		'<table class="table table-bordered table-striped" style="font-size:12px;">' +
		"<thead><tr><th>#</th><th>Name</th><th>Method</th><th>Path</th><th>Description</th></tr></thead>" +
		`<tbody>${endpoint_rows}</tbody></table></div></div>`;

	let confirm_dialog = new frappe.ui.Dialog({
		title:
			__("AI Discovery Results — {0}", [data.connector_name]) +
			' <span style="background:#7c3aed;color:#fff;font-size:9px;' +
			"padding:1px 5px;border-radius:3px;font-weight:600;" +
			'vertical-align:middle;margin-left:4px;">AI</span>',
		size: "extra-large",
		fields: [{ fieldtype: "HTML", fieldname: "results_html" }],
		primary_action_label: __("Create Connector"),
		primary_action() {
			confirm_dialog.hide();
			create_connector_from_ai(data, listview);
		},
		secondary_action_label: __("Cancel"),
	});

	confirm_dialog.fields_dict.results_html.$wrapper.html(html);
	confirm_dialog.show();
}

function create_connector_from_ai(data, listview) {
	frappe.call({
		method: "nce_sync.nce_sync.doctype.api_connector.api_connector.create_connector_from_ai",
		args: { connector_data: data },
		freeze: true,
		freeze_message: __("Creating {0} connector…", [data.connector_name]),
		callback(r) {
			if (r.message) {
				frappe.show_alert(
					{
						message: __("Created {0} with {1} endpoints", [
							r.message.name,
							r.message.endpoint_count,
						]),
						indicator: "green",
					},
					7
				);
				listview.refresh();
				frappe.set_route("Form", "API Connector", r.message.name);
			}
		},
	});
}
