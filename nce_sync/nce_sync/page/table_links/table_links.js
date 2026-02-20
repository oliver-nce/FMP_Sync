// Copyright (c) 2026, Oliver Reid and contributors
// For license information, please see license.txt

frappe.pages["table-links"].on_page_load = function (wrapper) {
	const page = frappe.ui.make_app_page({
		parent: wrapper,
		title: __("Table Links"),
		single_column: true,
	});

	page.main.addClass("nce-table-links-page");

	// Grid styling
	page.main.append(
		'<style>.nce-grid-diagonal{background:#f5f5f5!important;color:#999;text-align:center}.nce-grid-cell{text-align:center;vertical-align:middle}.nce-grid-row-label{background:#fafafa}.nce-links-grid td,.nce-links-grid th{padding:8px 12px}</style>'
	);

	const $grid = $('<div class="nce-table-links-grid"></div>');
	page.main.append($grid);

	page.set_primary_action(__("Refresh"), function () {
		load_and_render_grid($grid);
	});

	load_and_render_grid($grid);
};

function load_and_render_grid($container) {
	$container.html(
		'<div class="text-muted" style="padding: 2rem; text-align: center;">' +
			__("Loading...") +
			"</div>"
	);

	frappe.call({
		method: "nce_sync.api.get_table_links_grid_data",
		callback: function (r) {
			if (r.exc) {
				$container.html(
					'<div class="text-danger" style="padding: 2rem;">' +
						(r.message || __("Failed to load grid data")) +
						"</div>"
				);
				return;
			}
			render_grid($container, r.message);
		},
	});
}

function render_grid($container, data) {
	const tables = data.tables || [];
	const links = data.links || {};

	if (tables.length === 0) {
		$container.html(
			'<div class="text-muted" style="padding: 2rem; text-align: center;">' +
				__("No mirrored tables yet. Mirror some tables from WP Tables first.") +
				"</div>"
		);
		return;
	}

	let html = '<div class="nce-links-grid-wrapper" style="overflow-x: auto;">';
	html += '<table class="table table-bordered nce-links-grid">';
	html += "<thead><tr>";
	html += '<th style="min-width: 120px;">' + __("Source \\ Target") + "</th>";
	tables.forEach(function (t) {
		html += '<th style="min-width: 100px;">' + frappe.utils.escape_html(t.label) + "</th>";
	});
	html += "</tr></thead><tbody>";

	tables.forEach(function (sourceRow, rowIdx) {
		html += "<tr>";
		html +=
			'<td class="nce-grid-row-label"><strong>' +
			frappe.utils.escape_html(sourceRow.label) +
			"</strong></td>";

		tables.forEach(function (targetCol, colIdx) {
			const isDiagonal = rowIdx === colIdx;
			const sourceDt = sourceRow.doctype;
			const targetDt = targetCol.doctype;

			if (isDiagonal) {
				html += '<td class="nce-grid-cell nce-grid-diagonal">—</td>';
				return;
			}

			const cellLinks = (links[sourceDt] && links[sourceDt][targetDt]) || [];
			const count = cellLinks.length;

			if (count === 0) {
				html +=
					'<td class="nce-grid-cell nce-grid-action">' +
					'<button class="btn btn-sm btn-default nce-link-btn" ' +
					'data-source="' +
					frappe.utils.escape_html(sourceDt) +
					'" data-source-label="' +
					frappe.utils.escape_html(sourceRow.label) +
					'" data-target="' +
					frappe.utils.escape_html(targetDt) +
					'" data-target-label="' +
					frappe.utils.escape_html(targetCol.label) +
					'">' +
					__("Link") +
					"</button></td>";
			} else {
				const display = count > 1 ? "✓ " + count : "✓";
				html +=
					'<td class="nce-grid-cell nce-grid-linked">' +
					'<button class="btn btn-sm btn-success nce-link-btn" ' +
					'data-source="' +
					frappe.utils.escape_html(sourceDt) +
					'" data-source-label="' +
					frappe.utils.escape_html(sourceRow.label) +
					'" data-target="' +
					frappe.utils.escape_html(targetDt) +
					'" data-target-label="' +
					frappe.utils.escape_html(targetCol.label) +
					'" data-links="' +
					frappe.utils.escape_html(JSON.stringify(cellLinks)) +
					'">' +
					frappe.utils.escape_html(display) +
					"</button></td>";
			}
		});
		html += "</tr>";
	});

	html += "</tbody></table></div>";
	$container.html(html);

	$container.find(".nce-link-btn").on("click", function () {
		const $btn = $(this);
		show_link_dialog({
			source: $btn.data("source"),
			sourceLabel: $btn.data("source-label"),
			target: $btn.data("target"),
			targetLabel: $btn.data("target-label"),
			links: $btn.data("links") || [],
		});
	});
}

function show_link_dialog(opts) {
	const d = new frappe.ui.Dialog({
		title: __("Link: {0} → {1}", [opts.sourceLabel, opts.targetLabel]),
		size: "small",
		fields: [
			{
				fieldtype: "HTML",
				fieldname: "placeholder",
			},
		],
		primary_action_label: __("Close"),
		primary_action: function () {
			d.hide();
		},
	});

	d.fields_dict.placeholder.$wrapper.html(
		'<p class="text-muted">' +
			__("Placeholder dialog. Link configuration will be implemented here.") +
			"</p>" +
			"<p><strong>" +
			__("Source:") +
			"</strong> " +
			frappe.utils.escape_html(opts.sourceLabel) +
			"</p>" +
			"<p><strong>" +
			__("Target:") +
			"</strong> " +
			frappe.utils.escape_html(opts.targetLabel) +
			"</p>" +
			(opts.links && opts.links.length > 0
				? "<p><strong>" +
					__("Existing links:") +
					"</strong> " +
					opts.links.map((l) => l.field).join(", ") +
					"</p>"
				: "")
	);

	d.show();
}
