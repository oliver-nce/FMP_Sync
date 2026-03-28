// Copyright (c) 2026, Oliver Reid and contributors
// For license information, please see license.txt

frappe.ui.form.on("Sync Manager", {
	refresh(frm) {
		frm.add_custom_button(__("Load FM Tables"), function () {
			frm.call({
				method: "load_fm_tables",
				doc: frm.doc,
				callback: function (r) {
					frm.refresh_field("tables_to_sync");
					frappe.msgprint(__("FM Tables loaded"));
				},
			});
		});

		frm.add_custom_button(__("Run Sync Now"), function () {
			frappe.confirm(
				__("Sync all enabled tables now?"),
				function () {
					frm.call({
						method: "run_sync_now",
						doc: frm.doc,
						callback: function (r) {
							frappe.show_alert({
								message: r.message || __("Sync jobs queued"),
								indicator: "green",
							});
							frm.reload_doc();
						},
					});
				}
			);
		}, __("Actions"));
	},
});
