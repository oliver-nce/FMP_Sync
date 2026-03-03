// Copyright (c) 2026, Oliver Reid and contributors
// For license information, please see license.txt

frappe.ui.form.on("API Connector", {
	refresh(frm) {
		if (!frm.is_new()) {
			frm.add_custom_button(__("Test Connection"), function () {
				frappe.call({
					method: "nce_sync.nce_sync.doctype.api_connector.api_connector.test_connection",
					args: { connector_name: frm.doc.name },
					freeze: true,
					freeze_message: __("Testing connection..."),
					callback: function (r) {
						frm.reload_doc();
						if (r.message && r.message.success) {
							frappe.show_alert({
								message: __("Connection successful"),
								indicator: "green",
							});
						} else {
							frappe.show_alert({
								message: __("Connection failed: {0}", [
									r.message ? r.message.error : "Unknown error",
								]),
								indicator: "red",
							});
						}
					},
				});
			});
		}
	},
});
