// Copyright (c) 2026, Oliver Reid and contributors
// For license information, please see license.txt

// Available Frappe field types for the dropdown
const FRAPPE_FIELD_TYPES = [
	"Data",
	"Small Text",
	"Text",
	"Long Text",
	"Int",
	"Float",
	"Check",
	"Date",
	"Datetime",
	"Time",
	"Select",
	"JSON",
	"Password",
	"Currency",
	"Percent",
	"Rating",
];

frappe.ui.form.on("WP Tables", {
	refresh: function (frm) {
		// Mirror Schema button
		if (!frm.is_new()) {
			frm.add_custom_button(__("Mirror Schema"), function () {
				// Step 1: Preview schema first
				frappe.call({
					method: "preview_schema",
					doc: frm.doc,
					freeze: true,
					freeze_message: __("Introspecting table schema..."),
					callback: function (r) {
						if (r.message) {
							show_preview_dialog(frm, r.message);
						}
					},
				});
			});
		}
	},
});

function show_preview_dialog(frm, preview_data) {
	let fields = preview_data.fields;
	let doctype_name = preview_data.doctype_name;

	let d = new frappe.ui.Dialog({
		title: __("Review Field Types — {0}", [doctype_name]),
		size: "extra-large",
		fields: [
			{
				fieldtype: "HTML",
				fieldname: "field_preview",
			},
		],
		primary_action_label: __("Confirm & Create"),
		primary_action: function () {
			// Collect user selections
			let field_overrides = {};
			let has_changes = false;

			d.$wrapper.find(".field-type-select").each(function () {
				let col_name = $(this).data("column");
				let selected = $(this).val();
				let original = $(this).data("original");

				if (selected !== original) {
					has_changes = true;
				}
				// Always send all field types (user may have confirmed defaults)
				field_overrides[col_name] = selected;
			});

			d.hide();

			// Step 2: Mirror with user-confirmed field types
			frappe.call({
				method: "mirror_schema",
				doc: frm.doc,
				args: {
					field_overrides: JSON.stringify(field_overrides),
				},
				freeze: true,
				freeze_message: __("Creating DocType..."),
				callback: function (r) {
					frappe.set_route("Workspace", "NCE Sync");
				},
			});
		},
	});

	// Build the preview table
	let html = `
		<div style="margin-bottom: 10px;">
			<span class="text-muted">${__(
				"Review the proposed field types below. Adjust any that look incorrect before creating the DocType."
			)}</span>
		</div>
		<div style="max-height: 500px; overflow-y: auto;">
			<table class="table table-bordered table-sm" style="font-size: 13px;">
				<thead style="position: sticky; top: 0; background: var(--fg-color, #fff); z-index: 1;">
					<tr>
						<th style="width: 25%;">${__("Column")}</th>
						<th style="width: 20%;">${__("DB Type")}</th>
						<th style="width: 25%;">${__("Frappe Type")}</th>
						<th style="width: 15%;">${__("Nullable")}</th>
						<th style="width: 15%;">${__("Keys")}</th>
					</tr>
				</thead>
				<tbody>
	`;

	fields.forEach(function (f) {
		// Build keys badges
		let keys = [];
		if (f.is_primary_key) keys.push('<span class="badge badge-danger">PK</span>');
		if (f.is_unique) keys.push('<span class="badge badge-warning">UQ</span>');
		if (f.is_indexed) keys.push('<span class="badge badge-info">IDX</span>');
		let keys_html = keys.length > 0 ? keys.join(" ") : "—";

		// Build select dropdown for Frappe type
		let options_html = FRAPPE_FIELD_TYPES.map(function (ft) {
			let selected = ft === f.proposed_fieldtype ? "selected" : "";
			return `<option value="${ft}" ${selected}>${ft}</option>`;
		}).join("");

		// Highlight if the DB type suggests the auto-detection might be off
		// (e.g., longtext for a view column that could be anything)
		let row_class = f.db_type === "longtext" ? 'style="background: #fff8e1;"' : "";

		html += `
			<tr ${row_class}>
				<td><strong>${f.column_name}</strong><br><small class="text-muted">${f.label}</small></td>
				<td><code>${f.db_type}</code></td>
				<td>
					<select class="form-control form-control-sm field-type-select"
						data-column="${f.column_name}"
						data-original="${f.proposed_fieldtype}">
						${options_html}
					</select>
				</td>
				<td>${f.is_nullable === "YES" ? "Yes" : "<strong>No</strong>"}</td>
				<td>${keys_html}</td>
			</tr>
		`;
	});

	html += `
				</tbody>
			</table>
		</div>
	`;

	d.fields_dict.field_preview.$wrapper.html(html);

	// Highlight changed dropdowns
	d.$wrapper.on("change", ".field-type-select", function () {
		let original = $(this).data("original");
		if ($(this).val() !== original) {
			$(this).css("border-color", "#f0ad4e");
			$(this).css("background-color", "#fff8e1");
		} else {
			$(this).css("border-color", "");
			$(this).css("background-color", "");
		}
	});

	d.show();
}
