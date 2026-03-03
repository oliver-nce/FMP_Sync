# Copyright (c) 2026, Oliver Reid and contributors
# For license information, please see license.txt

"""
Live write-back from Frappe to WordPress.

Handles the on_update / after_insert wildcard hook for all DocTypes.
Only acts on DocTypes whose WP Tables record has write_back_mode = "SQL Direct".
"""

import json

import frappe
from frappe import _

from nce_sync.utils.data_sync import build_reverse_mapping
from nce_sync.utils.schema_mirror import get_wp_connection

CACHE_KEY = "nce_sync:sql_direct_tables"

# Frappe system fields that should never be pushed back to WP
SKIP_FIELDS = frozenset({
    "name", "owner", "creation", "modified", "modified_by",
    "docstatus", "idx", "_user_tags", "_comments", "_assign", "_liked_by",
})


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _get_sql_direct_map():
    """
    Return a dict of {frappe_doctype: wp_table_name} for all WP Tables
    with write_back_mode = "SQL Direct" and mirror_status = "Mirrored".

    Result is cached in Redis; cleared when a WP Tables record is saved.
    """
    cached = frappe.cache().get_value(CACHE_KEY)
    if cached is not None:
        return cached

    try:
        rows = frappe.get_all(
            "WP Tables",
            filters={"write_back_mode": "SQL Direct", "mirror_status": "Mirrored"},
            fields=["name", "frappe_doctype"],
        )
    except Exception:
        # Column may not exist yet during migration — treat as empty
        return {}
    mapping = {r.frappe_doctype: r.name for r in rows if r.frappe_doctype}
    frappe.cache().set_value(CACHE_KEY, mapping)
    return mapping


def clear_sql_direct_cache():
    """Remove the cached SQL-direct table map so it's rebuilt on next access."""
    frappe.cache().delete_value(CACHE_KEY)


# ---------------------------------------------------------------------------
# Hook handler
# ---------------------------------------------------------------------------

def on_record_change(doc, method):
    """
    Wildcard doc_events handler (on_update / after_insert).

    Bails early for:
    - Records being saved by the inbound sync (frappe.flags.in_sync)
    - DocTypes not in the SQL Direct map
    """
    if getattr(frappe.flags, "in_sync", False):
        return

    sql_direct_map = _get_sql_direct_map()
    if doc.doctype not in sql_direct_map:
        return

    wp_table_name = sql_direct_map[doc.doctype]

    frappe.enqueue(
        push_record_to_wp,
        wp_table_name=wp_table_name,
        doctype=doc.doctype,
        docname=doc.name,
        queue="short",
        is_async=True,
    )


# ---------------------------------------------------------------------------
# Background job
# ---------------------------------------------------------------------------

def push_record_to_wp(wp_table_name, doctype, docname):
    """
    Push a single Frappe record to the corresponding WordPress table via
    direct SQL UPDATE on db_nce_custom.

    Steps:
    1. Load the Frappe record and WP Tables metadata.
    2. Invert the column_mapping to get {frappe_fieldname: wp_column}.
    3. Build an UPDATE statement targeting the WP primary key.
    4. Execute inside a transaction and log the outcome.
    """
    try:
        frappe_doc = frappe.get_doc(doctype, docname)
    except frappe.DoesNotExistError:
        return

    wp_table_doc = frappe.get_doc("WP Tables", wp_table_name)

    column_mapping = {}
    if wp_table_doc.column_mapping:
        column_mapping = json.loads(wp_table_doc.column_mapping)

    reverse_mapping = build_reverse_mapping(column_mapping)

    pk_wp_col = wp_table_doc.name_field_column
    if not pk_wp_col:
        frappe.log_error(
            title=f"Live sync skip: {doctype}",
            message=f"No name_field_column set on WP Tables '{wp_table_name}'",
        )
        return

    pk_value = frappe_doc.name

    auto_gen_cols = set()
    if wp_table_doc.auto_generated_columns:
        auto_gen_cols = {c.strip() for c in wp_table_doc.auto_generated_columns.split(",") if c.strip()}

    valid_fields = {df.fieldname for df in frappe.get_meta(doctype).fields}

    set_clauses = []
    values = []

    for frappe_field in valid_fields:
        if frappe_field in SKIP_FIELDS:
            continue
        wp_col = reverse_mapping.get(frappe_field)
        if not wp_col:
            continue
        if wp_col in auto_gen_cols:
            continue
        if wp_col == pk_wp_col:
            continue

        val = frappe_doc.get(frappe_field)
        set_clauses.append(f"`{wp_col}` = %s")
        values.append(val)

    if not set_clauses:
        return

    values.append(pk_value)
    sql = "UPDATE `{table}` SET {sets} WHERE `{pk}` = %s".format(
        table=wp_table_doc.table_name,
        sets=", ".join(set_clauses),
        pk=pk_wp_col,
    )

    wp_conn_doc = frappe.get_single("WordPress Connection")
    conn = get_wp_connection(wp_conn_doc)
    try:
        cursor = conn.cursor()
        cursor.execute(sql, values)
        conn.commit()
        cursor.close()
    except Exception as e:
        conn.rollback()
        frappe.log_error(
            title=f"Live sync error: {doctype} {docname}",
            message=str(e),
        )
        raise
    finally:
        conn.close()
