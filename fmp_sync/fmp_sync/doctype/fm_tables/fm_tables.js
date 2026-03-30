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

const RESERVED_FIELDNAMES = [
	"name",
	"parent",
	"creation",
	"owner",
	"modified",
	"modified_by",
	"parentfield",
	"parenttype",
	"file_list",
	"flags",
	"docstatus",
];

function _scrub_fieldname(label) {
	return (label || "")
		.toLowerCase()
		.replace(/\s+/g, "_")
		.replace(/[^a-z0-9_]/g, "");
}

/** Match server normalize_frappe_fieldname_fragment for editable Frappe fieldnames. */
function _normalize_fmp_fieldname_input(s) {
	return String(s || "")
		.trim()
		.toLowerCase()
		.replace(/[^a-z0-9_]/g, "")
		.replace(/_+/g, "_")
		.replace(/^_|_$/g, "");
}

function _label_from_fmp_fieldname(fn) {
	const n = _normalize_fmp_fieldname_input(fn);
	if (!n) {
		return "";
	}
	return n
		.split("_")
		.filter(Boolean)
		.map((w) => w.charAt(0).toUpperCase() + w.slice(1))
		.join(" ");
}

/** Copy-paste curl for OData page. Does not run sync; password visible. */
function show_sync_curl_dialog(frm, curl, top) {
	top = top || 500;
	const d = new frappe.ui.Dialog({
		title: __("Test curl — OData page ($top={0})", [top]),
		fields: [
			{
				fieldtype: "HTML",
				fieldname: "fetch_actions",
				options:
					'<p style="margin-bottom:10px">' +
					'<button type="button" class="btn btn-primary btn-sm" id="fmp-sync-fetch-copy-rows">' +
					__("Fetch & copy JSON (first {0})", [top]) +
					"</button> " +
					'<button type="button" class="btn btn-warning btn-sm" id="fmp-sync-import-first-page">' +
					__("Import first {0} into Frappe", [top]) +
					"</button></p>" +
					"<p class='text-muted small' style='margin-bottom:0'>" +
					__(
						"Same OData request as sync: $top={0} and $select from your mapping. Copy JSON only reads data. Import upserts into the mirrored DocType (matching keys updated like TS Compare) — not a full sync (no orphan deletes).",
						[top],
					) +
					"</p>",
			},
			{
				fieldtype: "HTML",
				fieldname: "warn",
				options:
					"<p class='text-danger' style='margin-bottom:10px'>" +
					__(
						"Password is shown in the command below. For local testing only. This dialog does not start a sync — use Sync Now when you are ready.",
					) +
					"</p>" +
					"<p class='text-muted small'>" +
					__(
						"Matches the first OData GET for this table: $top={0} and $select from your column mapping (same as Truncate & Replace first page).",
						[top],
					) +
					"</p>",
			},
			{
				fieldtype: "HTML",
				fieldname: "curl_wrap",
				options:
					'<textarea id="fmp-sync-curl-ta" class="form-control" readonly style="width:100%;min-height:180px;font-family:monospace;font-size:12px;white-space:pre;overflow:auto"></textarea>' +
					'<p style="margin-top:8px"><button type="button" class="btn btn-default btn-sm" id="fmp-sync-curl-copy">' +
					__("Copy to clipboard") +
					"</button></p>",
			},
		],
		primary_action_label: __("Close"),
		primary_action: function () {
			d.hide();
		},
	});
	d.show();
	const $ta = d.$wrapper.find("#fmp-sync-curl-ta");
	$ta.val(curl);

	const copyTextToClipboard = function (text, successMsg) {
		const done = function () {
			frappe.show_alert({
				message: successMsg || __("Copied"),
				indicator: "green",
			});
		};
		if (navigator.clipboard && navigator.clipboard.writeText) {
			navigator.clipboard.writeText(text).then(done, function () {
				try {
					const ta = document.createElement("textarea");
					ta.value = text;
					document.body.appendChild(ta);
					ta.select();
					document.execCommand("copy");
					document.body.removeChild(ta);
					done();
				} catch (e) {
					frappe.msgprint(__("Could not copy automatically — select text manually."));
				}
			});
		} else {
			try {
				const ta = document.createElement("textarea");
				ta.value = text;
				document.body.appendChild(ta);
				ta.select();
				document.execCommand("copy");
				document.body.removeChild(ta);
				done();
			} catch (e) {
				frappe.msgprint(__("Could not copy automatically."));
			}
		}
	};

	d.$wrapper.find("#fmp-sync-fetch-copy-rows").on("click", function () {
		frappe.call({
			method: "fetch_sync_first_page_for_clipboard",
			doc: frm.doc,
			freeze: true,
			freeze_message: __("Fetching from FileMaker…"),
			callback: function (r) {
				const msg = r.message;
				if (!msg || typeof msg.text !== "string") {
					frappe.msgprint(__("No data returned."));
					return;
				}
				copyTextToClipboard(
					msg.text,
					__("Copied {0} row(s) as JSON", [msg.row_count != null ? msg.row_count : "?"]),
				);
			},
		});
	});

	d.$wrapper.find("#fmp-sync-import-first-page").on("click", function () {
		const dt = frm.doc.frappe_doctype || __("this DocType");
		frappe.confirm(
			__(
				"Fetch the same OData page as sync ($top=500) and upsert into \"{0}\"? Existing rows with the same matching keys will be updated. This is not a full sync (orphan rows are not deleted).",
				[dt],
			),
			function () {
				frappe.call({
					method: "import_first_500_rows_to_frappe",
					doc: frm.doc,
					freeze: true,
					freeze_message: __("Importing from FileMaker…"),
					callback: function (r) {
						const m = r.message || {};
						const parts = [
							__("Fetched: {0}", [m.fetched]),
							__("New: {0}", [m.inserted]),
							__("Updated: {0}", [m.updated]),
						];
						if (m.skipped) {
							parts.push(__("Skipped: {0}", [m.skipped]));
						}
						frappe.msgprint({
							title: __("Import finished"),
							message: parts.join(" · "),
							indicator: m.skipped ? "orange" : "green",
						});
						frm.reload_doc();
					},
				});
			},
		);
	});

	d.$wrapper.find("#fmp-sync-curl-copy").on("click", function () {
		const t = $ta.get(0);
		t.focus();
		t.select();
		t.setSelectionRange(0, curl.length);
		copyTextToClipboard(curl, __("Copied"));
	});
}

frappe.ui.form.on("FM Tables", {
	after_save: function (frm) {
		let desired = (
			frm.doc.fmp_name ||
			frm.doc.table_name ||
			frm.doc.frappe_doctype ||
			""
		).trim();
		if (desired && frm.doc.name !== desired) {
			frappe
				.xcall("frappe.client.rename_doc", {
					doctype: "FM Tables",
					old_name: frm.doc.name,
					new_name: desired,
				})
				.then(() => {
					frappe.set_route("Form", "FM Tables", desired);
				})
				.catch((e) => {
					frappe.msgprint({
						title: __("Rename Failed"),
						message: e.message || __("Could not rename to {0}", [desired]),
						indicator: "red",
					});
				});
		}
	},

	refresh: function (frm) {
		if (frm.is_new()) return;

		// Set form title badge based on sync + mirror state
		if (frm.doc.mirror_status === "Pending") {
			frm.page.set_indicator(__("Pending"), "orange");
		} else if (frm.doc.last_sync_status === "Running") {
			frm.page.set_indicator(__("Syncing"), "blue");
		} else if (frm.doc.last_sync_status === "Error") {
			frm.page.set_indicator(__("Sync Error"), "red");
		} else if (frm.doc.last_sync_status === "Success") {
			frm.page.set_indicator(__("Synced"), "green");
		} else if (frm.doc.mirror_status === "Mirrored") {
			frm.page.set_indicator(__("Mirrored"), "blue");
		} else if (frm.doc.mirror_status === "Linked") {
			frm.page.set_indicator(__("Linked"), "purple");
		}

		let is_mirrored =
			(frm.doc.mirror_status === "Mirrored" || frm.doc.mirror_status === "Linked") &&
			frm.doc.frappe_doctype;

		// Mirror Schema — only when not yet mirrored (and not Native mode)
		if (!is_mirrored && frm.doc.doctype_source !== "Native") {
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
				__("Actions"),
			);
		}

		// Native mode: Link / Unlink buttons
		if (frm.doc.doctype_source === "Native") {
			if (!is_mirrored) {
				frm.add_custom_button(
					__("Link DocType"),
					function () {
						if (!frm.doc.frappe_doctype) {
							frappe.msgprint(__("Please select a Frappe DocType first."));
							return;
						}
						frm.save().then(function () {
							frappe.call({
								method: "link_external_doctype",
								doc: frm.doc,
								freeze: true,
								freeze_message: __("Linking..."),
								callback: function () {
									frm.reload_doc();
								},
							});
						});
					},
					__("Actions"),
				);
			} else {
				frm.add_custom_button(
					__("Unlink"),
					function () {
						frappe.confirm(
							__(
								"This will unlink the Native DocType from this entry. The DocType itself will NOT be deleted. Continue?",
							),
							function () {
								frappe.call({
									method: "unlink_external_doctype",
									doc: frm.doc,
									freeze: true,
									freeze_message: __("Unlinking..."),
									callback: function () {
										frm.reload_doc();
									},
								});
							},
						);
					},
					__("Actions"),
				);
			}
		}

		if (is_mirrored) {
			// Sync Now
			frm.add_custom_button(
				__("Sync Now"),
				function () {
					frappe.call({
						method: "sync_now",
						doc: frm.doc,
						callback: function () {
							show_sync_progress_dialog(frm);
						},
						error: function () {
							frm.reload_doc();
						},
					});
				},
				__("Actions"),
			);

			// Show curl for first OData page (mirrored FM tables only; does not run sync)
			if (frm.doc.doctype_source !== "Native") {
				frm.add_custom_button(
					__("Show Sync Curl"),
					function () {
						const top_dialog = new frappe.ui.Dialog({
							title: __("Choose $top (row limit)"),
							fields: [
								{
									fieldtype: "Int",
									fieldname: "top_value",
									label: __("$top"),
									default: 500,
									description: __(
										"Number of rows to request from FileMaker. Common values: 1 (connectivity test), 10, 50, 100, 500, 1000, 5000.",
									),
								},
							],
							primary_action_label: __("Build Curl"),
							primary_action: function (values) {
								const chosen_top = cint(values.top_value);
								if (chosen_top < 1) {
									frappe.msgprint(__("$top must be at least 1."));
									return;
								}
								top_dialog.hide();
								frappe.call({
									method: "get_sync_curl",
									doc: frm.doc,
									args: { top: chosen_top },
									freeze: true,
									freeze_message: __("Building curl…"),
									callback: function (r) {
										const curl = r.message && r.message.curl;
										if (curl) {
											show_sync_curl_dialog(frm, curl, chosen_top);
										}
									},
								});
							},
						});
						top_dialog.show();
					},
					__("Actions"),
				);
			}

			// Truncate Data — clears all records, keeps DocType structure
			frm.add_custom_button(
				__("Truncate Data"),
				function () {
					frappe.confirm(
						__(
							"This will delete ALL records from '{0}' in Frappe. The DocType structure will remain. Continue?",
							[frm.doc.frappe_doctype],
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
						},
					);
				},
				__("Actions"),
			);

			// Remap — re-read source schema, add new columns, rebuild mapping, truncate + repopulate
			frm.add_custom_button(
				__("Remap"),
				function () {
					let d = new frappe.ui.Dialog({
						title: __("Remap — {0}", [frm.doc.frappe_doctype]),
						fields: [
							{
								fieldtype: "HTML",
								fieldname: "info",
								options: `<p class="text-muted">${__(
									"This will truncate all data in '{0}', re-read the source schema (adding any new columns), and rebuild the column mapping. The DocType and its table are preserved.",
									[frm.doc.frappe_doctype],
								)}</p>`,
							},
							{
								fieldtype: "Data",
								fieldname: "new_table_name",
								label: __("Source Table Name"),
								default: frm.doc.table_name,
								description: __(
									"Change this if the FileMaker table has been renamed.",
								),
							},
						],
						primary_action_label: __("Continue"),
						primary_action: function (values) {
							d.hide();
							let new_name = (values.new_table_name || "").trim();
							let table_name_override =
								new_name && new_name !== frm.doc.table_name ? new_name : undefined;

							frappe.call({
								method: "preview_schema",
								doc: frm.doc,
								args: { table_name_override: table_name_override },
								freeze: true,
								freeze_message: __("Introspecting table schema..."),
								callback: function (r) {
									if (r.message) {
										show_preview_dialog(frm, r.message, "remap", new_name);
									}
								},
							});
						},
					});
					d.show();
				},
				__("Actions"),
			);

			// Reconfigure — full teardown (Mirror mode only)
			if (frm.doc.doctype_source !== "Native") {
				frm.add_custom_button(
					__("Reconfigure"),
					function () {
						frappe.confirm(
							__(
								"This will delete the DocType '{0}', its data, and remove it from the workspace. You can then Mirror Schema again with different settings. Continue?",
								[frm.doc.frappe_doctype],
							),
							function () {
								frappe.call({
									method: "delete_mirror",
									doc: frm.doc,
									freeze: true,
									freeze_message: __("Reconfiguring..."),
									callback: function () {
										frappe.ui.toolbar.clear_cache();
										frm.reload_doc();
									},
								});
							},
						);
					},
					__("Actions"),
				);
			}

			// "Add to Workspace" shown outside Actions menu when shortcut is missing
			frappe
				.xcall("fmp_sync.utils.workspace_utils.is_in_workspace", {
					doctype_name: frm.doc.frappe_doctype,
				})
				.then((in_ws) => {
					if (!in_ws) {
						let btn = frm.add_custom_button(__("Add to Workspace"), function () {
							frappe.call({
								method: "add_to_workspace",
								doc: frm.doc,
								callback: function () {
									frappe.ui.toolbar.clear_cache();
									frm.reload_doc();
								},
							});
						});
						btn.css({
							"background-color": "pink",
							color: "red",
							"font-weight": "bold",
						});
					}
				});
		}
	},
});

function show_preview_dialog(frm, preview_data, mode, new_table_name) {
	mode = mode || "mirror";
	let fields = preview_data.fields.slice();
	fields.sort(function (a, b) {
		return String(a.column_name).localeCompare(String(b.column_name), undefined, {
			sensitivity: "base",
		});
	});
	let doctype_name = preview_data.doctype_name;
	let previous_user_skipped = preview_data.previous_user_skipped_columns || [];
	let previous_matching = preview_data.previous_matching_fields || [];
	let previous_name_column = preview_data.previous_name_field_column || null;

	let dialog_title =
		mode === "remap"
			? __("Remap Schema — {0}", [doctype_name])
			: __("Review Field Types — {0}", [doctype_name]);
	let action_label = mode === "remap" ? __("Confirm & Remap") : __("Confirm & Create");

	let d = new frappe.ui.Dialog({
		title: dialog_title,
		size: "extra-large",
		fields: [
			{
				fieldtype: "HTML",
				fieldname: "field_preview",
			},
		],
		primary_action_label: action_label,
		primary_action: function () {
			// Collect field type overrides (skip rows marked Skip)
			let field_overrides = {};
			d.$wrapper.find(".field-type-select").each(function () {
				if ($(this).prop("disabled")) {
					return;
				}
				let col_name = $(this).data("column");
				let selected = $(this).val();
				field_overrides[col_name] = selected;
			});

			let fieldname_overrides = {};
			let fn_collect_ok = true;
			d.$wrapper.find(".field-name-input").each(function () {
				const row = $(this).closest("tr");
				if (row.find(".skip-field-checkbox").is(":checked")) {
					return;
				}
				if ($(this).prop("disabled")) {
					return;
				}
				const col_name = $(this).data("column");
				const fn = _normalize_fmp_fieldname_input($(this).val());
				if (!fn) {
					fn_collect_ok = false;
					frappe.msgprint(__("Field name cannot be empty (column {0}).", [col_name]));
					return false;
				}
				fieldname_overrides[col_name] = fn;
			});
			if (!fn_collect_ok) {
				return;
			}
			let seenFn = Object.create(null);
			for (const col of Object.keys(fieldname_overrides)) {
				const v = fieldname_overrides[col];
				if (seenFn[v]) {
					frappe.msgprint(
						__("Duplicate Frappe field names: {0}. Change one of them.", [v]),
					);
					return;
				}
				seenFn[v] = col;
			}

			// Label overrides only when different from auto title-case of Field name
			let label_overrides = {};
			d.$wrapper.find(".field-label-input").each(function () {
				if ($(this).prop("disabled")) {
					return;
				}
				const row = $(this).closest("tr");
				if (row.find(".skip-field-checkbox").is(":checked")) {
					return;
				}
				const col_name = $(this).data("column");
				const label = $(this).val().trim();
				const fnRaw = row.find(".field-name-input").val();
				const autoLabel = _label_from_fmp_fieldname(fnRaw);
				if (label && autoLabel && label !== autoLabel) {
					label_overrides[col_name] = label;
				} else if (label && !autoLabel) {
					label_overrides[col_name] = label;
				}
			});

			// Collect "Use as Name" selection (mutually exclusive radio)
			let name_field_column = d.$wrapper.find(".name-field-radio:checked").val() || "";

			// Collect matching fields (up to 3 selected checkboxes)
			let matching_fields = [];
			d.$wrapper.find(".matching-field-checkbox:checked").each(function () {
				matching_fields.push($(this).data("column"));
			});

			// Collect auto-generated columns
			let auto_generated_columns = [];
			d.$wrapper.find(".auto-generated-checkbox:checked").each(function () {
				auto_generated_columns.push($(this).data("column"));
			});

			// Collect timestamp field selections
			let modified_ts_field = d.$wrapper.find(".mod-ts-radio:checked").val() || "";
			let created_ts_field = d.$wrapper.find(".crt-ts-radio:checked").val() || "";

			let user_skipped = [];
			d.$wrapper.find(".skip-field-checkbox:checked").each(function () {
				user_skipped.push($(this).data("column"));
			});
			let skip_lower = new Set(user_skipped.map((c) => String(c).toLowerCase()));
			function _col_skipped(col) {
				return col && skip_lower.has(String(col).toLowerCase());
			}
			if (_col_skipped(modified_ts_field)) {
				frappe.msgprint(__("The modified timestamp column cannot be skipped."));
				return;
			}
			if (_col_skipped(created_ts_field)) {
				frappe.msgprint(__("The created timestamp column cannot be skipped."));
				return;
			}
			if (_col_skipped(name_field_column)) {
				frappe.msgprint(__("The Frappe ID column cannot be skipped."));
				return;
			}
			for (let mi = 0; mi < matching_fields.length; mi++) {
				if (_col_skipped(matching_fields[mi])) {
					frappe.msgprint(
						__(
							"Matching field '{0}' cannot be skipped. Uncheck Skip or change matching fields.",
							[matching_fields[mi]],
						),
					);
					return;
				}
			}

			// Validate reserved column labels
			let reserved_errors = [];
			d.$wrapper.find(".field-label-input.reserved-source-col").each(function () {
				if ($(this).prop("disabled")) {
					return;
				}
				let col_name = $(this).data("column");
				let label = $(this).val().trim();
				let scrubbed = _scrub_fieldname(label);
				if (!label || RESERVED_FIELDNAMES.includes(scrubbed)) {
					reserved_errors.push(col_name);
					$(this).css({ "border-color": "#dc3545", "background-color": "#fff0f0" });
				}
			});
			if (reserved_errors.length > 0) {
				frappe.msgprint(
					__(
						"Reserved column(s) need a unique label: <strong>{0}</strong>.<br>Choose a label that doesn't resolve to a reserved name (e.g. 'Event Name' instead of 'Name').",
						[reserved_errors.join(", ")],
					),
				);
				return;
			}

			// Validate: max 3 matching fields (when not using Name)
			if (!name_field_column && matching_fields.length > 3) {
				frappe.msgprint(__("Please select a maximum of 3 matching fields."));
				return;
			}
			if (!name_field_column && matching_fields.length === 0) {
				frappe.msgprint(
					__("Please select at least one matching field, or use a column as Name."),
				);
				return;
			}

			// Validate: Modified TS is mandatory
			if (!modified_ts_field) {
				frappe.msgprint(__("Please select a Modified Timestamp field (Mod TS column)."));
				return;
			}

			// Disable button to prevent double-clicks while processing
			let busy_text = mode === "remap" ? __("Remapping…") : __("Creating…");
			d.get_primary_btn().prop("disabled", true).text(busy_text);

			let call_method = mode === "remap" ? "remap_schema" : "mirror_schema";
			let freeze_msg =
				mode === "remap" ? __("Remapping schema...") : __("Creating DocType...");

			let call_args = {
				field_overrides: JSON.stringify(field_overrides),
				label_overrides: JSON.stringify(label_overrides),
				fieldname_overrides: JSON.stringify(fieldname_overrides),
				matching_fields: matching_fields.join(","),
				name_field_column: name_field_column || undefined,
				auto_generated_columns: auto_generated_columns.join(",") || undefined,
				modified_ts_field: modified_ts_field || undefined,
				created_ts_field: created_ts_field || undefined,
				user_skipped_columns:
					user_skipped.length > 0 ? user_skipped.join(",") : undefined,
			};
			if (mode === "remap" && new_table_name && new_table_name !== frm.doc.table_name) {
				call_args.new_table_name = new_table_name;
			}

			frappe.call({
				method: call_method,
				doc: frm.doc,
				args: call_args,
				freeze: true,
				freeze_message: freeze_msg,
				callback: function (r) {
					d.hide();
					frm.reload_doc();
				},
				error: function (r) {
					d.get_primary_btn().prop("disabled", false).text(action_label);
				},
			});
		},
	});

	// Build the preview table
	let html = `
		<div style="margin-bottom: 10px;">
			<span class="text-muted">${__(
				"Review the proposed field types below. Adjust any that look incorrect before creating the DocType.",
			)}</span>
			<br>
			<span class="text-muted"><strong>${__("Matching Fields:")}</strong> ${__(
				"Select up to 3 fields to use for matching records during sync (useful when the table lacks unique keys).",
			)}</span>
		<br>
		<span class="text-muted"><strong>${__("Frappe ID:")}</strong> ${__(
			"Select one column to use as Frappe's record ID (skips field creation, enables fast direct lookup).",
		)}</span>
		<br>
		<span class="text-muted"><strong>${__("Auto:")}</strong> ${__(
			"Mark columns that are auto-generated by the source (e.g. auto_increment). These will be skipped when writing records back to the source.",
		)}</span>
		<br>
		<span class="text-muted"><strong>${__("Mod TS / Created TS:")}</strong> ${__(
			"Pick the modified-timestamp field (required) and optionally the created-timestamp field. Only datetime/timestamp columns are selectable.",
		)}</span>
		<br>
		<span class="text-muted"><strong>${__("Skip:")}</strong> ${__(
			"Exclude a column from the Frappe DocType and from FM→Frappe sync (OData $select). You cannot skip columns used as Frappe ID, matching fields, or modified timestamp.",
		)}</span>
		<div class="fmp-skip-toolbar" style="margin: 8px 0 4px 0; display: flex; flex-wrap: wrap; align-items: center; gap: 8px;">
			<span style="font-weight: 600; color: var(--text-color, inherit);">${__("Skip column checkboxes:")}</span>
			<button type="button" class="btn btn-xs btn-primary fmp-skip-all">${__("Select all")}</button>
			<button type="button" class="btn btn-xs btn-default fmp-skip-none">${__("Select none")}</button>
		</div>
		<p class="text-muted small" style="margin: 0 0 8px 0;">${__(
			"Tip: Scroll the table horizontally if you do not see Field name or Label.",
		)}</p>
		<p style="margin: 10px 0 0 0;">
			<strong>${__("All strings → Data?")}</strong>
			<label class="small" style="margin-left: 10px; font-weight: normal;">
				<input type="radio" name="fmp-all-str-data" value="N" checked> ${__("No")}
			</label>
			<label class="small" style="margin-left: 10px; font-weight: normal;">
				<input type="radio" name="fmp-all-str-data" value="Y"> ${__("Yes")}
			</label>
			<span class="text-muted" style="margin-left: 10px;">${__(
				"If Yes, only FM/OData text fields (e.g. String) use Data; numbers and dates are unchanged. No restores the proposed type per column.",
			)}</span>
		</p>
		</div>
		<div class="fmp-schema-preview-scroll" style="min-height: 220px; max-height: min(75vh, 900px); overflow: auto; width: 100%;">
			<style>
				.fmp-schema-preview-table { table-layout: auto; width: max-content; min-width: 100%; font-size: 13px; }
				.fmp-schema-preview-table thead th {
					vertical-align: top;
					white-space: nowrap;
					background: var(--fg-color, #fff);
				}
				.fmp-schema-preview-table .fmp-skip-th-btns { white-space: normal; min-width: 5.5rem; }
				.fmp-schema-preview-table .fmp-skip-th-btns .btn { display: block; width: 100%; margin-top: 4px; font-weight: 600; }
			</style>
			<table class="table table-bordered table-sm fmp-schema-preview-table">
				<thead style="position: sticky; top: 0; z-index: 2; box-shadow: 0 1px 0 var(--border-color, #ddd);">
					<tr>
						<th class="fmp-skip-th-btns" title="${__(
							"Exclude from DocType and sync",
						)}">
							${__("Skip")}
							<button type="button" class="btn btn-xs btn-primary fmp-skip-all">${__("All")}</button>
							<button type="button" class="btn btn-xs btn-default fmp-skip-none">${__("None")}</button>
						</th>
						<th>${__("Match")}</th>
						<th title="${__("Map this column directly to Frappe\'s record ID (name field)")}">${__("Frappe ID")}</th>
						<th>${__("Auto")}</th>
						<th title="${__("Modified timestamp — required")}"><span style="color:#d44;">${__("Mod TS")}</span></th>
						<th title="${__("Created timestamp — optional")}">${__("Crt TS")}</th>
						<th>${__("Column")}</th>
						<th style="min-width: 9rem;" title="${__(
							"Internal Frappe fieldname (lowercase, a-z 0-9 _). Label defaults from this unless you edit the label.",
						)}"><strong>${__("Field name")}</strong></th>
						<th>${__("DB Type")}</th>
						<th style="min-width: 8rem;">${__("Frappe Type")}</th>
						<th>${__("Nullable")}</th>
						<th>${__("Keys")}</th>
						<th style="min-width: 10rem;">${__("Label")}</th>
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
		// When "Use as Name" is selected for a column, Match is disabled (that column IS the match key)
		let checked = "";
		if (previous_matching.length > 0) {
			// Use previous user selection
			checked = previous_matching.includes(f.column_name.toLowerCase()) ? "checked" : "";
		} else {
			// Default: check PK and unique fields
			checked = f.is_primary_key || f.is_unique ? "checked" : "";
		}

		// Pre-select "Use as Name" radio: previous selection, or only PK if exactly one
		let pk_count = fields.filter((x) => x.is_primary_key).length;
		let name_checked = "";
		if (previous_name_column && previous_name_column === f.column_name) {
			name_checked = "checked";
		} else if (!previous_name_column && pk_count === 1 && f.is_primary_key) {
			name_checked = "checked";
		}

		// Pre-check "Auto" if column is auto_increment (or user previously set it)
		let previous_auto_generated = preview_data.previous_auto_generated_columns || [];
		let auto_checked = "";
		if (previous_auto_generated.includes(f.column_name.toLowerCase())) {
			auto_checked = "checked";
		} else if (!previous_auto_generated.length && f.is_auto_increment) {
			auto_checked = "checked";
		}

		let skip_checked =
			previous_user_skipped.indexOf(f.column_name.toLowerCase()) >= 0 ? "checked" : "";

		// Timestamp radio buttons — only enabled for datetime/timestamp columns
		let is_datetime = ["datetime", "timestamp", "Datetime"].some((t) =>
			f.db_type.toLowerCase().includes(t.toLowerCase()),
		);
		let previous_mod_ts = (preview_data.previous_modified_ts || "").toLowerCase();
		let previous_crt_ts = (preview_data.previous_created_ts || "").toLowerCase();
		let col_lower = f.column_name.toLowerCase();

		let mod_ts_cell = "";
		let crt_ts_cell = "";
		if (is_datetime) {
			let mod_checked = previous_mod_ts && previous_mod_ts === col_lower ? "checked" : "";
			// Default: pre-select first field named like "post_modified" / "updated_at" / "modified"
			if (!mod_checked && !previous_mod_ts) {
				if (/modif|updated/.test(col_lower)) mod_checked = "checked";
			}
			let crt_checked = previous_crt_ts && previous_crt_ts === col_lower ? "checked" : "";
			if (!crt_checked && !previous_crt_ts) {
				if (/creat|post_date(?!_gmt)/.test(col_lower)) crt_checked = "checked";
			}
			mod_ts_cell = `<input type="radio" name="mod_ts_radio" class="mod-ts-radio"
				value="${f.column_name}" data-column="${f.column_name}" ${mod_checked}>`;
			crt_ts_cell = `<input type="radio" name="crt_ts_radio" class="crt-ts-radio"
				value="${f.column_name}" data-column="${f.column_name}" ${crt_checked}>`;
		} else {
			mod_ts_cell = `<span style="color:#ccc;" title="${__("Not a datetime column")}">—</span>`;
			crt_ts_cell = `<span style="color:#ccc;" title="${__("Not a datetime column")}">—</span>`;
		}

		let is_reserved_col = RESERVED_FIELDNAMES.includes(f.column_name.toLowerCase());
		let reserved_cls = is_reserved_col ? " reserved-source-col" : "";
		let label_styles = "";
		let label_readonly = "";
		let reserved_hint = "";

		// In remap mode, existing columns get a read-only label
		let is_locked = mode === "remap" && f.is_existing;
		if (is_locked) {
			label_styles = "opacity:0.6;cursor:not-allowed;";
			label_readonly = " readonly";
		}

		if (is_reserved_col && !is_locked) {
			let scrubbed = _scrub_fieldname(f.label);
			let still_bad = RESERVED_FIELDNAMES.includes(scrubbed);
			label_styles += still_bad
				? "border-color:#dc3545;background-color:#fff0f0;"
				: "border-color:#28a745;background-color:#f0fff0;";
			reserved_hint = `<div class="reserved-label-hint text-danger" style="font-size:11px;margin-top:2px;${still_bad ? "" : "display:none;"}">⚠ "${f.column_name}" is reserved — choose a unique label</div>`;
		}
		let label_style_attr = label_styles ? ` style="${label_styles}"` : "";

		let pfn = String(f.proposed_fieldname || "");
		let pfn_esc = frappe.utils.escape_html(pfn);
		let autoLbl = _label_from_fmp_fieldname(f.proposed_fieldname);
		let labelSynced =
			String(f.label || "").trim() === String(autoLbl || "").trim() ? "1" : "0";
		let fn_readonly_attr = is_locked ? " readonly" : "";
		let label_esc = frappe.utils.escape_html(String(f.label || ""));

		let db_type_attr = frappe.utils.escape_html(String(f.db_type || ""));
		html += `
			<tr ${row_class} data-db-type="${db_type_attr}">
				<td style="text-align: center;">
					<input type="checkbox" class="skip-field-checkbox"
						data-column="${f.column_name}" ${skip_checked}>
				</td>
				<td style="text-align: center;">
					<input type="checkbox" class="matching-field-checkbox"
						data-column="${f.column_name}" ${checked}>
				</td>
				<td style="text-align: center;">
					<input type="radio" name="name_field_radio" class="name-field-radio"
						value="${f.column_name}" data-column="${f.column_name}" ${name_checked}>
				</td>
				<td style="text-align: center;">
					<input type="checkbox" class="auto-generated-checkbox"
						data-column="${f.column_name}" ${auto_checked}>
				</td>
				<td style="text-align: center;">${mod_ts_cell}</td>
				<td style="text-align: center;">${crt_ts_cell}</td>
				<td><strong>${f.column_name}</strong></td>
				<td style="min-width: 9rem;">
					<input type="text" class="form-control form-control-sm field-name-input"
						data-column="${f.column_name}"
						data-fn-locked="${is_locked ? "1" : "0"}"
						data-original="${pfn_esc}"
						data-label-synced="${labelSynced}"
						value="${pfn_esc}"${fn_readonly_attr}>
				</td>
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
				<input type="text" class="form-control form-control-sm field-label-input${reserved_cls}"
					data-column="${f.column_name}"
					data-original="${label_esc}"
					value="${label_esc}"${label_style_attr}${label_readonly}>
				${reserved_hint}
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

	function refresh_skip_row_state($cb) {
		const row = $cb.closest("tr");
		const skip = $cb.is(":checked");
		row.find(".matching-field-checkbox, .auto-generated-checkbox").prop("disabled", skip);
		row.find(".name-field-radio, .mod-ts-radio, .crt-ts-radio").prop("disabled", skip);
		row.find(".field-type-select, .field-label-input, .field-name-input").prop("disabled", skip);
		row.css("opacity", skip ? "0.75" : "");
		if (skip) {
			row.find(".matching-field-checkbox").prop("checked", false);
			row.find(".auto-generated-checkbox").prop("checked", false);
			row.find(".name-field-radio").prop("checked", false);
			row.find(".mod-ts-radio").prop("checked", false);
			row.find(".crt-ts-radio").prop("checked", false);
		}
	}

	// When "Frappe ID" radio is selected: grey out Frappe Type on the ID column (no separate field).
	// Rows marked Skip stay disabled.
	function _refresh_frappe_id_state() {
		let $checked_radio = d.$wrapper.find(".name-field-radio:checked");
		let name_selected = $checked_radio.length > 0;
		let id_col = name_selected ? $checked_radio.val() : null;

		d.$wrapper.find(".matching-field-checkbox").each(function () {
			let skip = $(this).closest("tr").find(".skip-field-checkbox").is(":checked");
			$(this).prop("disabled", skip);
		});

		d.$wrapper.find(".field-type-select").each(function () {
			let col = $(this).data("column");
			let skip = $(this).closest("tr").find(".skip-field-checkbox").is(":checked");
			if (skip) {
				$(this)
					.prop("disabled", true)
					.css({ opacity: "0.45", "pointer-events": "none" })
					.removeAttr("title");
				return;
			}
			if (id_col && col === id_col) {
				$(this)
					.val("Data")
					.prop("disabled", true)
					.css({ opacity: "0.45", "pointer-events": "none" })
					.attr(
						"title",
						__(
							"This column maps to Frappe's name field (varchar) — no separate field is created",
						),
					);
			} else {
				$(this)
					.prop("disabled", false)
					.css({ opacity: "", "pointer-events": "" })
					.removeAttr("title")
					.val($(this).data("original"));
			}
		});
		d.$wrapper.find(".field-name-input").each(function () {
			const col = $(this).data("column");
			const skip = $(this).closest("tr").find(".skip-field-checkbox").is(":checked");
			const wasLocked = $(this).attr("data-fn-locked") === "1";
			if (skip) {
				return;
			}
			if (id_col && col === id_col) {
				$(this).val("name").prop("disabled", true).prop("readonly", false).attr("title", __("Maps to Frappe record ID (name)"));
			} else {
				$(this)
					.prop("disabled", false)
					.removeAttr("title")
					.val($(this).data("original"))
					.prop("readonly", wasLocked);
			}
		});
		_apply_all_strings_data_mode();
	}

	d.fields_dict.field_preview.$wrapper.on("click", ".fmp-skip-all", function (e) {
		e.preventDefault();
		e.stopPropagation();
		d.$wrapper.find(".skip-field-checkbox").prop("checked", true);
		d.$wrapper.find(".skip-field-checkbox").each(function () {
			refresh_skip_row_state($(this));
		});
		_refresh_frappe_id_state();
	});
	d.fields_dict.field_preview.$wrapper.on("click", ".fmp-skip-none", function (e) {
		e.preventDefault();
		e.stopPropagation();
		d.$wrapper.find(".skip-field-checkbox").prop("checked", false);
		d.$wrapper.find(".skip-field-checkbox").each(function () {
			refresh_skip_row_state($(this));
		});
		_refresh_frappe_id_state();
	});

	/** True only for FM text / OData string — not numeric, date, time, or boolean. */
	function _is_fm_text_db_type(db) {
		const raw = String(db || "").trim();
		const lower = raw.toLowerCase();
		if (!lower) {
			return false;
		}
		// OData full type
		if (lower.startsWith("edm.")) {
			return lower === "edm.string";
		}
		// Short EDM name from schema (COLUMN_TYPE without Edm. prefix)
		const nonText = new Set([
			"int64",
			"int32",
			"int16",
			"decimal",
			"double",
			"single",
			"boolean",
			"byte",
			"sbyte",
			"date",
			"datetimeoffset",
			"timeofday",
			"guid",
			"binary",
			"stream",
			// SQL / other whole-type names (not FM OData short names)
			"int",
			"integer",
			"bigint",
			"smallint",
			"tinyint",
			"float",
			"numeric",
			"real",
			"money",
			"bit",
			"datetime",
			"timestamp",
			"smalldatetime",
			"datetime2",
			"time",
			"year",
		]);
		if (nonText.has(lower)) {
			return false;
		}
		if (lower === "string") {
			return true;
		}
		// SQL-style text (non-OData paths)
		if (/\b(varchar|nvarchar|text|longtext|mediumtext|tinytext|clob|nclob)\b/i.test(raw)) {
			return true;
		}
		if (/\b(char|nchar)\b/i.test(raw) && !/\b(varchar|nvarchar)\b/i.test(raw)) {
			return true;
		}
		return false;
	}

	function _apply_all_strings_data_mode() {
		const yes =
			d.$wrapper.find('input[name="fmp-all-str-data"]:checked').val() === "Y";
		d.$wrapper.find(".field-type-select").each(function () {
			const $sel = $(this);
			if ($sel.prop("disabled")) {
				return;
			}
			const $tr = $sel.closest("tr");
			if ($tr.find(".skip-field-checkbox").is(":checked")) {
				return;
			}
			const prev = $sel.val();
			if (yes) {
				const db = $tr.attr("data-db-type") || "";
				if (_is_fm_text_db_type(db)) {
					$sel.val("Data");
				}
			} else {
				$sel.val($sel.data("original"));
			}
			if ($sel.val() !== prev) {
				$sel.trigger("change");
			}
		});
	}

	d.$wrapper.on("change", ".skip-field-checkbox", function () {
		refresh_skip_row_state($(this));
		_refresh_frappe_id_state();
	});
	d.$wrapper.find(".skip-field-checkbox").each(function () {
		refresh_skip_row_state($(this));
	});

	d.$wrapper.on("change", ".name-field-radio", _refresh_frappe_id_state);
	// input + change: update every row as soon as Yes/No is selected (no extra click needed)
	d.$wrapper.on("change input", 'input[name="fmp-all-str-data"]', function () {
		_apply_all_strings_data_mode();
	});
	_refresh_frappe_id_state();
	d.$wrapper.find(".field-name-input").each(function () {
		_style_fieldname_input($(this));
	});

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

	function _style_fieldname_input($inp) {
		if ($inp.prop("readonly") || $inp.prop("disabled")) {
			return;
		}
		const norm = _normalize_fmp_fieldname_input($inp.val());
		const base = _normalize_fmp_fieldname_input($inp.data("original"));
		if (norm !== base) {
			$inp.css({ "border-color": "#f0ad4e", "background-color": "#fff8e1" });
		} else {
			$inp.css({ "border-color": "", "background-color": "" });
		}
	}

	d.$wrapper.on("input blur", ".field-name-input", function () {
		const $fn = $(this);
		if ($fn.prop("readonly") || $fn.prop("disabled")) {
			return;
		}
		const norm = _normalize_fmp_fieldname_input($fn.val());
		$fn.val(norm);
		const $lab = $fn.closest("tr").find(".field-label-input");
		if (!$lab.prop("readonly") && !$lab.prop("disabled") && $fn.attr("data-label-synced") === "1") {
			$lab.val(_label_from_fmp_fieldname(norm));
		}
		_style_fieldname_input($fn);
	});

	// Highlight changed labels (with reserved-column validation); orange if not auto from field name
	d.$wrapper.on("input", ".field-label-input", function () {
		let col_name = $(this).data("column");
		let label = $(this).val().trim();
		let is_reserved = RESERVED_FIELDNAMES.includes(col_name.toLowerCase());
		const $row = $(this).closest("tr");
		const autoLabel = _label_from_fmp_fieldname($row.find(".field-name-input").val());
		if (!$(this).prop("readonly") && !$(this).prop("disabled")) {
			$row.find(".field-name-input").attr("data-label-synced", label === autoLabel ? "1" : "0");
		}

		if (is_reserved) {
			let scrubbed = _scrub_fieldname(label);
			let $hint = $(this).parent().find(".reserved-label-hint");
			if (!label || RESERVED_FIELDNAMES.includes(scrubbed)) {
				$(this).css({ "border-color": "#dc3545", "background-color": "#fff0f0" });
				$hint.show();
			} else {
				$(this).css({ "border-color": "#28a745", "background-color": "#f0fff0" });
				$hint.hide();
			}
		} else if (label && autoLabel && label !== autoLabel) {
			$(this).css("border-color", "#f0ad4e");
			$(this).css("background-color", "#fff8e1");
		} else {
			$(this).css("border-color", "");
			$(this).css("background-color", "");
		}
	});

	d.show();
	_make_preview_dialog_movable_resizable(d);
}

/** Drag header to move (fixed positioning; jQuery .offset() breaks with Bootstrap modal flex centering). */
function _make_preview_dialog_movable_resizable(dialog) {
	const $wrap = dialog.$wrapper;
	const $dlg = $wrap.find(".modal-dialog").first();
	const $content = $wrap.find(".modal-content").first();
	const $header = $wrap.find(".modal-header").first();
	if (!$dlg.length || !$content.length || !$header.length) {
		return;
	}
	$content.css({
		resize: "both",
		overflow: "auto",
		"min-width": "min(96vw, 560px)",
		"min-height": "280px",
		"max-width": "96vw",
		"max-height": "92vh",
	});
	$header.css({ cursor: "move", "user-select": "none" });
	const ns = "fmpSchemaPreviewDlg";
	$header.off(`mousedown.${ns}`).on(`mousedown.${ns}`, function (e) {
		if ($(e.target).closest("button, .close, a, input, select, label, .indicator-pill").length) {
			return;
		}
		e.preventDefault();
		const rect = $dlg[0].getBoundingClientRect();
		const startX = e.clientX;
		const startY = e.clientY;
		const baseLeft = rect.left;
		const baseTop = rect.top;
		$dlg.css({
			position: "fixed",
			margin: "0",
			left: baseLeft + "px",
			top: baseTop + "px",
			width: rect.width + "px",
			transform: "none",
		});
		function onMove(ev) {
			const dx = ev.clientX - startX;
			const dy = ev.clientY - startY;
			const pad = 8;
			let nx = baseLeft + dx;
			let ny = baseTop + dy;
			nx = Math.max(pad, Math.min(nx, window.innerWidth - rect.width - pad));
			ny = Math.max(pad, Math.min(ny, window.innerHeight - pad));
			$dlg.css({ left: nx + "px", top: ny + "px" });
		}
		function onUp() {
			$(document).off(`mousemove.${ns} mouseup.${ns}`);
			$header.css("cursor", "move");
		}
		$header.css("cursor", "grabbing");
		$(document).on(`mousemove.${ns}`, onMove);
		$(document).on(`mouseup.${ns}`, onUp);
	});
}

function show_sync_progress_dialog(frm) {
	let label = frm.doc.fmp_name || frm.doc.table_name || frm.doc.name;
	let last_log = "";
	let poll_timer = null;

	let d = new frappe.ui.Dialog({
		title: __("Sync Progress — {0}", [label]),
		size: "large",
		fields: [{ fieldtype: "HTML", fieldname: "progress_area" }],
		primary_action_label: __("Running…"),
		primary_action: function () {
			_stop_poll();
			d.hide();
			frm.reload_doc();
		},
	});

	d.get_primary_btn().prop("disabled", true);

	d.fields_dict.progress_area.$wrapper.html(`
		<div id="sync-log-box" style="
			font-family: monospace; font-size: 12px;
			background: var(--bg-color, #f8f8f8);
			border: 1px solid var(--border-color, #ddd);
			border-radius: 4px; padding: 12px;
			min-height: 120px; max-height: 360px;
			overflow-y: auto; white-space: pre-wrap; word-break: break-all;">
			<span class="text-muted">${__("Waiting for worker to start…")}</span>
		</div>
	`);

	d.show();

	function _append(text, color) {
		let $box = d.$wrapper.find("#sync-log-box");
		let ts = new Date().toLocaleTimeString();
		let style = color ? `style="color:${color};font-weight:bold;"` : "";
		$box.append(`<div ${style}>[${ts}]  ${text}</div>`);
		$box.scrollTop($box[0].scrollHeight);
	}

	function _stop_poll() {
		if (poll_timer) {
			clearInterval(poll_timer);
			poll_timer = null;
		}
	}

	function _poll() {
		frappe.db.get_value(
			"FM Tables",
			frm.doc.name,
			["last_sync_log", "last_sync_status"],
			function (data) {
				if (!data) return;
				let log = data.last_sync_log || "";
				let status = data.last_sync_status || "";

				if (log && log !== last_log) {
					last_log = log;
					_append(log);
				}

				if (status && status !== "Running") {
					_stop_poll();
					let color = status === "Success" ? "#28a745" : "#dc3545";
					_append(__("Sync finished: {0}", [status]), color);
					// Reload form immediately so badge/status updates without waiting for Close
					frm.reload_doc();
					d.set_primary_action(__("Close"), function () {
						d.hide();
					});
					d.get_primary_btn().prop("disabled", false);
				}
			},
		);
	}

	poll_timer = setInterval(_poll, 1500);
	d.$wrapper.on("hide.bs.modal", function () {
		_stop_poll();
	});
}
