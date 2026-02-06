// Copyright (c) 2026, Oliver Reid and contributors
// For license information, please see license.txt

frappe.ui.form.on("WP Tables", {
	refresh: function (frm) {
		// Mirror Schema button
		if (!frm.is_new()) {
			frm.add_custom_button(__("Mirror Schema"), function () {
				frappe.confirm(__("Mirror this table's schema to a Frappe DocType?"), function () {
					frappe.call({
						method: "mirror_schema",
						doc: frm.doc,
						freeze: true,
						freeze_message: __("Mirroring schema..."),
						callback: function (r) {
							frm.reload_doc();
						},
					});
				});
			});
		}
	},
});
