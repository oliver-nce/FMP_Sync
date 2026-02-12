// Copyright (c) 2026, Oliver Reid and contributors
// For license information, please see license.txt

frappe.ui.form.on("Sync Manager", {
	refresh(frm) {
		// Add button to populate tables from WP Tables
		frm.add_custom_button(__("Load WP Tables"), function () {
			frm.call({
				method: "load_wp_tables",
				doc: frm.doc,
				callback: function (r) {
					frm.refresh_field("tables_to_sync");
					frappe.msgprint(__("WP Tables loaded"));
				},
			});
		});
	},
});
