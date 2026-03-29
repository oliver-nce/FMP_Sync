// Copyright (c) 2026, Oliver Reid and contributors
// For license information, please see license.txt

frappe.ui.form.on("FileMaker Connection", {
	refresh: function (frm) {
		// Test Connection button
		frm.add_custom_button(__("Test Connection"), function () {
			frappe.call({
				method: "test_connection",
				doc: frm.doc,
				freeze: true,
				freeze_message: __("Testing connection..."),
				callback: function (r) {
					frm.reload_doc();
				},
			});
		});

		// Discover Tables button
		frm.add_custom_button(__("Discover Tables"), function () {
			frappe.call({
				method: "discover_tables",
				doc: frm.doc,
				freeze: true,
				freeze_message: __("Discovering tables..."),
				callback: function (r) {
					if (r.message) {
						show_discovery_dialog(frm, r.message);
					}
				},
			});
		});

		// Mirror All Schemas button
		frm.add_custom_button(__("Mirror All Schemas"), function () {
			frappe.confirm(
				__("This will mirror all selected tables to Frappe DocTypes. Continue?"),
				function () {
					frappe.call({
						method: "mirror_all",
						doc: frm.doc,
						freeze: true,
						freeze_message: __("Mirroring schemas..."),
						callback: function (r) {
							window.location.href = "/app/tables";
						},
					});
				}
			);
		});

		// Cleanup Orphaned Shortcuts button
		frm.add_custom_button(
			__("Cleanup Workspace"),
			function () {
				frappe.call({
					method: "fmp_sync.utils.workspace_utils.cleanup_orphaned_shortcuts",
					freeze: true,
					freeze_message: __("Cleaning up workspace..."),
					callback: function (r) {
						if (r.message === 0) {
							frappe.msgprint(__("No orphaned shortcuts found"), "green");
						}
						frappe.ui.toolbar.clear_cache();
					},
				});
			},
			__("Maintenance")
		);
	},
});

function show_discovery_dialog(frm, tables_and_views) {
	// Get already selected tables
	frappe.call({
		method: "frappe.client.get_list",
		args: {
			doctype: "FM Tables",
			fields: ["table_name"],
		},
		callback: function (r) {
			let selected_tables = r.message ? r.message.map((t) => t.table_name) : [];

			// Create dialog
			let d = new frappe.ui.Dialog({
				title: __("Discover FileMaker Tables"),
				fields: [
					{
						fieldtype: "HTML",
						fieldname: "table_selector",
					},
				],
				size: "large",
			});

			// Only list base tables (FileMaker OData also exposes table occurrences)
			let base_tables_only = (tables_and_views || []).filter(
				(item) => item.table_type === "BASE TABLE"
			);

			// Build the UI
			let html = `
				<div style="display: flex; gap: 20px;">
					<div style="flex: 1;">
						<h6>${__("Available Base Tables")}</h6>
						<p class="text-muted small" style="margin-bottom: 8px;">
							${__(
								"Table occurrences are hidden. Only one row per underlying FileMaker base table."
							)}
						</p>
						<input type="text" class="form-control" id="table-search" placeholder="${__(
							"Search..."
						)}" style="margin-bottom: 10px;">
						<div id="available-tables" style="max-height: 400px; overflow-y: auto; border: 1px solid #d1d8dd; border-radius: 4px; padding: 10px;">
						</div>
					</div>
					<div style="flex: 1;">
						<h6>${__("Selected Tables")}</h6>
						<div id="selected-tables" style="max-height: 450px; overflow-y: auto; border: 1px solid #d1d8dd; border-radius: 4px; padding: 10px;">
						</div>
					</div>
				</div>
			`;

			d.fields_dict.table_selector.$wrapper.html(html);

			// Render available tables
			let available_div = d.fields_dict.table_selector.$wrapper.find("#available-tables");
			base_tables_only.forEach((item) => {
				if (!selected_tables.includes(item.table_name)) {
					let badge_class = item.table_type === "VIEW" ? "badge-info" : "badge-primary";
					let row = $(`
						<div class="table-row" data-table="${item.table_name}" data-type="${item.table_type}" style="padding: 8px; margin-bottom: 5px; border: 1px solid #e8e8e8; border-radius: 4px; cursor: pointer; background: #f9f9f9;">
							<span class="badge ${badge_class}" style="margin-right: 8px;">${item.table_type}</span>
							<span>${item.table_name}</span>
						</div>
					`);
					row.on("click", function () {
						add_table_to_fm_tables(item.table_name, item.table_type, function () {
							row.remove();
							render_selected_tables();
						});
					});
					available_div.append(row);
				}
			});

			if (base_tables_only.length === 0) {
				available_div.html(
					`<p class="text-muted" style="padding: 12px;">${__(
						"No base tables returned. Check FileMaker_Tables access or connection."
					)}</p>`
				);
			}

			// Search functionality
			d.fields_dict.table_selector.$wrapper.find("#table-search").on("input", function () {
				let search = $(this).val().toLowerCase();
				available_div.find(".table-row").each(function () {
					let text = $(this).text().toLowerCase();
					$(this).toggle(text.includes(search));
				});
			});

			// Render selected tables
			function render_selected_tables() {
				frappe.call({
					method: "frappe.client.get_list",
					args: {
						doctype: "FM Tables",
						fields: ["name", "table_name", "table_type"],
					},
					callback: function (r) {
						let selected_div =
							d.fields_dict.table_selector.$wrapper.find("#selected-tables");
						selected_div.empty();

						if (r.message && r.message.length > 0) {
							r.message.forEach((item) => {
								let badge_class =
									item.table_type === "View" ? "badge-info" : "badge-primary";
								let row = $(`
									<div class="selected-row" style="padding: 8px; margin-bottom: 5px; border: 1px solid #e8e8e8; border-radius: 4px; display: flex; justify-content: space-between; align-items: center; background: #f0f4f7;">
										<div>
											<span class="badge ${badge_class}" style="margin-right: 8px;">${item.table_type}</span>
											<span>${item.table_name}</span>
										</div>
										<button class="btn btn-xs btn-danger remove-btn" data-name="${item.name}">✕</button>
									</div>
								`);
								row.find(".remove-btn").on("click", function () {
									remove_table_from_fm_tables(item.name, function () {
										render_selected_tables();
										// Re-add to available list
										let badge_class =
											item.table_type === "View"
												? "badge-info"
												: "badge-primary";
										let new_row = $(`
											<div class="table-row" data-table="${item.table_name}" data-type="${item.table_type}" style="padding: 8px; margin-bottom: 5px; border: 1px solid #e8e8e8; border-radius: 4px; cursor: pointer; background: #f9f9f9;">
												<span class="badge ${badge_class}" style="margin-right: 8px;">${item.table_type}</span>
												<span>${item.table_name}</span>
											</div>
										`);
										new_row.on("click", function () {
											add_table_to_fm_tables(
												item.table_name,
												item.table_type,
												function () {
													new_row.remove();
													render_selected_tables();
												}
											);
										});
										available_div.append(new_row);
									});
								});
								selected_div.append(row);
							});
						} else {
							selected_div.html(
								`<p style="text-align: center; color: #888; padding: 20px;">${__(
									"No tables selected"
								)}</p>`
							);
						}
					},
				});
			}

			render_selected_tables();

			d.show();
		},
	});
}

/** Map OData discovery `table_type` to FM Tables Select options ("Table" | "View"). */
function normalize_table_type_for_fm_tables(discovered_type) {
	if (!discovered_type) return "Table";
	const t = String(discovered_type).trim();
	const upper = t.toUpperCase();
	if (upper === "VIEW" || t === "View") return "View";
	// BASE TABLE, TABLE OCCURRENCE, TABLE — all map to FileMaker-backed tables
	return "Table";
}

function add_table_to_fm_tables(table_name, table_type, callback) {
	const fm_table_type = normalize_table_type_for_fm_tables(table_type);

	// First check if it already exists
	frappe.call({
		method: "frappe.client.get_value",
		args: {
			doctype: "FM Tables",
			filters: { table_name: table_name },
			fieldname: "name",
		},
		callback: function (r) {
			console.log("Check existing result:", r);

			if (r.message && r.message.name) {
				// Already exists
				frappe.show_alert({
					message: __("Table {0} already exists", [table_name]),
					indicator: "orange",
				});
				if (callback) callback();
			} else {
				// Doesn't exist, create it
				frappe.call({
					method: "frappe.client.insert",
					args: {
						doc: {
							doctype: "FM Tables",
							table_name: table_name,
							table_type: fm_table_type,
							sync_direction: "FM to Frappe",
							mirror_status: "Pending",
						},
					},
					callback: function (r) {
						console.log("Insert result:", r);
						if (r.message) {
							console.log("Successfully created:", r.message);
							frappe.show_alert({
								message: __("Added {0}", [table_name]),
								indicator: "green",
							});
						} else {
							console.warn("Insert succeeded but no message returned");
						}
						if (callback) callback();
					},
					error: function (r) {
						console.error("Insert error:", r);
						frappe.msgprint({
							title: __("Error Adding Table"),
							message:
								r.exc ||
								r._server_messages ||
								__("Failed to add {0}. Check console for details.", [table_name]),
							indicator: "red",
						});
						if (callback) callback();
					},
				});
			}
		},
		error: function (r) {
			console.error("Check existing error:", r);
			if (callback) callback();
		},
	});
}

function remove_table_from_fm_tables(name, callback) {
	frappe.call({
		method: "frappe.client.delete",
		args: {
			doctype: "FM Tables",
			name: name,
		},
		callback: function (r) {
			if (callback) callback();
		},
	});
}
