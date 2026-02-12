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
		if (frm.is_new()) return;

		// Mirror Schema button — always available
		frm.add_custom_button(
			__("Mirror Schema"),
			function () {
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
			},
			__("Actions")
		);

		// Only show sync and cleanup buttons when a mirror exists
		if (frm.doc.mirror_status === "Mirrored" && frm.doc.frappe_doctype) {
			// Sync Now button - primary action for mirrored tables
			frm.add_custom_button(
				__("Sync Now"),
				function () {
					frappe.call({
						method: "sync_now",
						doc: frm.doc,
						freeze: true,
						freeze_message: __("Syncing data from WordPress..."),
						callback: function (r) {
							frm.reload_doc();
						},
						error: function (r) {
							// Reload to show error status
							frm.reload_doc();
						},
					});
				},
				__("Actions")
			);

			frm.add_custom_button(
				__("Regen Column Map"),
				function () {
					frappe.call({
						method: "regenerate_column_mapping",
						doc: frm.doc,
						freeze: true,
						freeze_message: __("Regenerating column mapping..."),
						callback: function () {
							frm.reload_doc();
						},
					});
				},
				__("Actions")
			);

			frm.add_custom_button(
				__("Debug Sync"),
				function () {
					frappe.call({
						method: "debug_sync_one_row",
						doc: frm.doc,
						freeze: true,
						freeze_message: __("Checking first row..."),
						callback: function () {},
					});
				},
				__("Actions")
			);

			frm.add_custom_button(
				__("Truncate Data"),
				function () {
					frappe.confirm(
						__(
							"This will delete ALL records from '{0}' in Frappe. The DocType structure will remain. Continue?",
							[frm.doc.frappe_doctype]
						),
						function () {
							frappe.call({
								method: "truncate_data",
								doc: frm.doc,
								freeze: true,
								freeze_message: __("Deleting all records..."),
								callback: function () {
									frm.reload_doc();
									frappe.show_alert({
										message: __("All records deleted"),
										indicator: "green",
									});
								},
							});
						}
					);
				},
				__("Actions")
			);

			frm.add_custom_button(
				__("Re-mirror"),
				function () {
					frappe.confirm(
						__(
							"This will delete the DocType '{0}' and remove it from the workspace. You can then mirror again with different settings. Continue?",
							[frm.doc.frappe_doctype]
						),
						function () {
							frappe.call({
								method: "delete_mirror",
								doc: frm.doc,
								freeze: true,
								freeze_message: __("Deleting mirror..."),
								callback: function () {
									frm.reload_doc();
								},
							});
						}
					);
				},
				__("Actions")
			);

			frm.add_custom_button(
				__("Remove Table"),
				function () {
					frappe.confirm(
						__(
							"This will permanently delete the DocType '{0}', remove it from the workspace, and delete this WP Tables entry. This cannot be undone. Continue?",
							[frm.doc.frappe_doctype]
						),
						function () {
							frappe.call({
								method: "remove_table",
								doc: frm.doc,
								freeze: true,
								freeze_message: __("Removing table..."),
								callback: function () {
									window.location.href = "/app/nce-sync";
								},
							});
						}
					);
				},
				__("Actions")
			);
		}
	},
});

function show_preview_dialog(frm, preview_data) {
	let fields = preview_data.fields;
	let doctype_name = preview_data.doctype_name;
	let previous_matching = preview_data.previous_matching_fields || [];

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
			// Collect field type overrides
			let field_overrides = {};
			d.$wrapper.find(".field-type-select").each(function () {
				let col_name = $(this).data("column");
				let selected = $(this).val();
				// Always send all field types (user may have confirmed defaults)
				field_overrides[col_name] = selected;
			});

			// Collect label overrides
			let label_overrides = {};
			d.$wrapper.find(".field-label-input").each(function () {
				let col_name = $(this).data("column");
				let label = $(this).val().trim();
				let original = $(this).data("original");
				// Only send if changed from default
				if (label && label !== original) {
					label_overrides[col_name] = label;
				}
			});

			// Collect matching fields (up to 3 selected checkboxes)
			let matching_fields = [];
			d.$wrapper.find(".matching-field-checkbox:checked").each(function () {
				matching_fields.push($(this).data("column"));
			});

			// Validate: max 3 matching fields
			if (matching_fields.length > 3) {
				frappe.msgprint(__("Please select a maximum of 3 matching fields."));
				return;
			}

			// Disable button to prevent double-clicks while processing
			d.get_primary_btn().prop("disabled", true).text(__("Creating…"));

			// Mirror with user-confirmed field types, labels, and matching fields
			frappe.call({
				method: "mirror_schema",
				doc: frm.doc,
				args: {
					field_overrides: JSON.stringify(field_overrides),
					label_overrides: JSON.stringify(label_overrides),
					matching_fields: matching_fields.join(","),
				},
				freeze: true,
				freeze_message: __("Creating DocType..."),
				callback: function (r) {
					d.hide();
					window.location.href = "/app/nce-sync";
				},
				error: function (r) {
					// Re-enable button so the user can retry after fixing the issue
					d.get_primary_btn().prop("disabled", false).text(__("Confirm & Create"));
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
			<br>
			<span class="text-muted"><strong>${__("Matching Fields:")}</strong> ${__(
		"Select up to 3 fields to use for matching records during sync (useful when the table lacks unique keys)."
	)}</span>
		</div>
		<div style="max-height: 500px; overflow-y: auto;">
			<table class="table table-bordered table-sm" style="font-size: 13px;">
				<thead style="position: sticky; top: 0; background: var(--fg-color, #fff); z-index: 1;">
					<tr>
						<th style="width: 4%;">${__("Match")}</th>
						<th style="width: 16%;">${__("Column")}</th>
						<th style="width: 14%;">${__("DB Type")}</th>
						<th style="width: 18%;">${__("Frappe Type")}</th>
						<th style="width: 8%;">${__("Nullable")}</th>
						<th style="width: 18%;">${__("Keys")}</th>
						<th style="width: 22%;">${__("Label")}</th>
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
		if (f.is_virtual) keys.push('<span class="badge badge-secondary">VIRTUAL</span>');
		let keys_html = keys.length > 0 ? keys.join(" ") : "—";

		// Build select dropdown for Frappe type
		let options_html = FRAPPE_FIELD_TYPES.map(function (ft) {
			let selected = ft === f.proposed_fieldtype ? "selected" : "";
			return `<option value="${ft}" ${selected}>${ft}</option>`;
		}).join("");

		// Highlight if the DB type suggests the auto-detection might be off
		// (e.g., longtext for a view column that could be anything)
		let row_class = f.db_type === "longtext" ? 'style="background: #fff8e1;"' : "";

		// Pre-check matching field checkbox:
		// 1. If previously selected by user (from saved matching_fields)
		// 2. Or if it's a PK or unique field (for new mirrors)
		let checked = "";
		if (previous_matching.length > 0) {
			// Use previous user selection
			checked = previous_matching.includes(f.column_name.toLowerCase()) ? "checked" : "";
		} else {
			// Default: check PK and unique fields
			checked = f.is_primary_key || f.is_unique ? "checked" : "";
		}

		html += `
			<tr ${row_class}>
				<td style="text-align: center;">
					<input type="checkbox" class="matching-field-checkbox"
						data-column="${f.column_name}" ${checked}>
				</td>
				<td><strong>${f.column_name}</strong></td>
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
				<td>
					<input type="text" class="form-control form-control-sm field-label-input"
						data-column="${f.column_name}"
						data-original="${f.label}"
						value="${f.label}">
				</td>
			</tr>
		`;
	});

	html += `
				</tbody>
			</table>
		</div>
	`;

	d.fields_dict.field_preview.$wrapper.html(html);

	// Limit matching field selection to 3
	d.$wrapper.on("change", ".matching-field-checkbox", function () {
		let checked_count = d.$wrapper.find(".matching-field-checkbox:checked").length;
		if (checked_count > 3) {
			$(this).prop("checked", false);
			frappe.show_alert({
				message: __("Maximum 3 matching fields allowed"),
				indicator: "orange",
			});
		}
	});

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

	// Highlight changed labels
	d.$wrapper.on("input", ".field-label-input", function () {
		let original = $(this).data("original");
		if ($(this).val().trim() !== original) {
			$(this).css("border-color", "#f0ad4e");
			$(this).css("background-color", "#fff8e1");
		} else {
			$(this).css("border-color", "");
			$(this).css("background-color", "");
		}
	});

	d.show();
}
