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

			if (frm.doc.notes) {
				frm.add_custom_button(__("Setup Guide"), function () {
					show_setup_guide(frm);
				});
			}
		}
	},
});

function show_setup_guide(frm) {
	if (frm._setup_guide_dialog && frm._setup_guide_dialog.display) {
		frm._setup_guide_dialog.show();
		return;
	}

	let d = new frappe.ui.Dialog({
		title: __("{0} — Setup Guide", [frm.doc.connector_name]),
		size: "large",
		fields: [
			{
				fieldtype: "HTML",
				fieldname: "guide_content",
			},
		],
		primary_action_label: __("Close"),
		primary_action() {
			d.hide();
		},
	});

	d.fields_dict.guide_content.$wrapper.html(
		'<div class="setup-guide-content" style="' +
			"padding: 15px; " +
			"font-size: 14px; " +
			"line-height: 1.6; " +
			"max-height: 70vh; " +
			"overflow-y: auto;" +
			'">' +
			frm.doc.notes +
			"</div>"
	);

	d.$wrapper.find(".modal-dialog").css({
		"max-width": "700px",
		cursor: "move",
	});

	// Make draggable
	let modal_dialog = d.$wrapper.find(".modal-dialog");
	let header = d.$wrapper.find(".modal-header");
	let isDragging = false;
	let offsetX, offsetY;

	// Remove modal centering so absolute positioning works
	d.$wrapper.find(".modal").css({
		display: "flex",
		"align-items": "flex-start",
		"justify-content": "flex-end",
		"padding-top": "60px",
		"padding-right": "20px",
	});

	// Allow clicking behind the dialog
	d.$wrapper.find(".modal-backdrop").css("display", "none");
	d.$wrapper.find(".modal").css("pointer-events", "none");
	modal_dialog.css("pointer-events", "auto");

	header.on("mousedown", function (e) {
		isDragging = true;
		let rect = modal_dialog[0].getBoundingClientRect();
		offsetX = e.clientX - rect.left;
		offsetY = e.clientY - rect.top;
		modal_dialog.css("transition", "none");
		e.preventDefault();
	});

	$(document).on("mousemove.setupguide", function (e) {
		if (!isDragging) return;
		modal_dialog.css({
			position: "fixed",
			left: e.clientX - offsetX + "px",
			top: e.clientY - offsetY + "px",
			margin: 0,
			transform: "none",
		});
	});

	$(document).on("mouseup.setupguide", function () {
		isDragging = false;
	});

	d.onhide = function () {
		$(document).off("mousemove.setupguide");
		$(document).off("mouseup.setupguide");
	};

	frm._setup_guide_dialog = d;
	d.show();
}
