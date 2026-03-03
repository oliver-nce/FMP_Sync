# NCE Sync — Agent Handoff Document

## Date
2026-03-03 (v2.0.2)

## What This App Does

NCE Sync is a Frappe v15 app that mirrors WordPress/MySQL tables into Frappe DocTypes and keeps them synchronized. It connects to a WordPress database (`db_nce_custom`), reads table schemas, creates matching Frappe DocTypes, and syncs data bidirectionally.

The production bench is at `~/NCE_V15` on the server (`manager.ncesoccer.com`). Development source lives at `~/Documents/_NCE_projects/NCE_Sync`.

## Architecture Overview

```
WordPress (db_nce_soccer + db_nce_custom)
    │
    │  Every 5 min: WP procedure refreshes db_nce_custom
    │
    ▼
NCE Sync (Frappe v15)
    │
    ├─ PULL: WP → Frappe  (scheduled every N minutes, or manual "Sync Now")
    │     Uses TS Compare (incremental) or Truncate & Replace (full)
    │
    ├─ PUSH: Frappe → WP  (live, on_update hook for "SQL Direct" tables)
    │     Enqueues background job → direct SQL UPDATE to db_nce_custom
    │
    └─ REVERSE SYNC: Frappe → WP  (for new records created in Frappe)
          Runs after each pull sync if sync_direction = "Both"
```

## Core DocTypes

| DocType | Type | Purpose |
|---------|------|---------|
| WordPress Connection | Single | DB host, port, user, password, timezone |
| WP Tables | List | One record per mirrored table. Stores schema mapping, sync settings, write-back mode |
| Sync Manager | Single | Global sync on/off, frequency, "Run Sync Now" button |
| Sync Log | List | Audit trail of sync runs (auto-pruned to 20 records) |

**Dynamic DocTypes:** When a user clicks "Mirror Schema" on a WP Tables record, a custom DocType is created (e.g., "Events", "Registrations") with fields matching the WordPress columns. These are **not** in the app source — they're created at runtime and stored in the database.

## Key Files

### Python (server-side)

| File | Purpose |
|------|---------|
| `nce_sync/hooks.py` | App config: doc_events, scheduler, overrides, includes |
| `nce_sync/api.py` | Whitelisted API endpoints (table links grid, link changes, Excel export) |
| `nce_sync/overrides.py` | Wraps `get_desktop_page` to inject dynamic shortcuts + version |
| `nce_sync/utils/schema_mirror.py` | Mirror WP table schemas to Frappe DocTypes. `get_wp_connection()` lives here |
| `nce_sync/utils/data_sync.py` | WP→Frappe sync engine (TS Compare, Truncate & Replace, orphan deletion) |
| `nce_sync/utils/reverse_sync.py` | Frappe→WP sync for new records (temp negative IDs → real WP IDs) |
| `nce_sync/utils/live_sync.py` | Live write-back: on_update hook → enqueue SQL UPDATE to WP |
| `nce_sync/utils/workspace_utils.py` | Dynamic workspace shortcut management |
| `nce_sync/nce_sync/doctype/wp_tables/wp_tables.py` | WP Tables controller (mirror, remap, delete, sync actions) |
| `nce_sync/nce_sync/doctype/sync_manager/sync_manager.py` | "Run Sync Now" enqueues all enabled tables |

### JavaScript (client-side)

| File | Purpose |
|------|---------|
| `nce_sync/public/js/list_auto_size.js` | Global: "Resize Columns" + "Send All to Excel" on all list views |
| `nce_sync/nce_sync/page/table_links/table_links.js` | Table Links page: Define/Visualize tabs, link dialog, Mermaid ERD |
| `nce_sync/nce_sync/doctype/wp_tables/wp_tables.js` | WP Tables form: Mirror, Remap, Sync, Delete buttons |
| `nce_sync/nce_sync/doctype/wp_tables/wp_tables_list.js` | WP Tables list: color indicators, action button |
| `nce_sync/nce_sync/doctype/sync_manager/sync_manager.js` | Sync Manager form: Load WP Tables, Run Sync Now buttons |
| `nce_sync/nce_sync/doctype/wordpress_connection/wordpress_connection.js` | WP Connection form: Test Connection, Discover Tables |

### Workspace & Config

| File | Purpose |
|------|---------|
| `nce_sync/nce_sync/workspace/Tables/Tables.json` | Workspace definition (static skeleton — dynamic shortcuts added on load) |
| `nce_sync/__init__.py` | `__version__` — shown in workspace footer, bump on each release |

## doc_events (hooks.py)

```python
doc_events = {
    "DocType": {
        "after_insert": workspace_utils.on_doctype_change,  # auto-add shortcut
        "on_trash": workspace_utils.on_doctype_change,       # auto-remove shortcut
    },
    "*": {
        "before_insert": reverse_sync.assign_temp_name,      # negative temp IDs
        "on_update": live_sync.on_record_change,             # live push to WP
        "after_insert": live_sync.on_record_change,          # live push to WP
    }
}
```

## Sync Flow (WP → Frappe)

1. **Trigger:** Scheduler (`*/5 * * * *` cron → `run_scheduled_syncs`) or manual "Sync Now"
2. **Per table:** `run_sync_for_table` → `_run_sync_with_status` → `sync_table`
3. **TS Compare method:**
   - Fetch all WP primary keys → diff against Frappe → delete orphans
   - Fetch rows modified since `last_synced - 5min buffer`
   - Also fetch rows missing from Frappe (never synced)
   - Upsert in batches of 500
   - `frappe.flags.in_sync = True` during saves (prevents live push-back loop)
4. **Truncate & Replace:** Delete all Frappe records, re-insert all WP rows
5. **Reverse sync (if direction = "Both"):** Push Frappe-created records (negative temp IDs) back to WP

## Live Write-Back (Frappe → WP)

Controlled per table via `WP Tables.write_back_mode`:

| Mode | Behavior |
|------|----------|
| **Never** (default) | Read-only from WP, no push-back |
| **SQL Direct** | `on_update` hook auto-enqueues SQL UPDATE to WP |
| **API Required** | Local edits allowed, WP push handled by external PHP functions |

**Implementation** (`utils/live_sync.py`):
- `on_record_change(doc, method)` — wildcard hook, bails if `frappe.flags.in_sync` or DocType not in SQL Direct map
- Cached map (`nce_sync:sql_direct_tables` in Redis) of `{frappe_doctype: wp_table_name}` — cleared on WP Tables save/delete
- `push_record_to_wp` — background job: inverts `column_mapping`, builds SQL UPDATE, executes against `db_nce_custom`

## Table Links (Relationships)

The **Table Links** page (`/app/table-links`) lets users define foreign-key relationships between mirrored tables:

- **Define tab:** Grid of all mirrored tables. Click a cell to open a dialog for managing Link fields between those two DocTypes.
- **Visualize tab:** Mermaid.js ERD showing 1-to-many relationships.
- **Dialog:** Shows existing links, allows adding/removing. Adding converts a field to `Link` type (preserving data). Removing reverts to `Data` type (preserving data). Changes are batched and applied on "Done".
- **Server method:** `api.apply_table_link_changes` handles the DocType field mutations with proper cache clearing.

**Important:** Re-mirroring a table (Delete Mirror → Mirror Schema) destroys the DocType and recreates it, so Link fields must be re-established manually via the Table Links page.

## Column Mapping

Each WP Tables record stores a `column_mapping` JSON field:

```json
{
  "venue_id": {"fieldname": "venue_id", "is_virtual": false},
  "id": {"fieldname": "name", "is_virtual": false, "is_name": true},
  "computed_col": {"fieldname": "computed_col", "is_virtual": true}
}
```

- Maps WP column names → Frappe fieldnames
- `is_name: true` marks the column that maps to Frappe's `name` (primary key)
- `is_virtual: true` marks computed/generated columns (skipped during reverse write)
- `name_field_column` stores which WP column is the PK (for WHERE clauses)
- `auto_generated_columns` lists auto-increment columns (skipped in reverse sync)

## Workspace Strategy

The `Tables.json` workspace contains only static system shortcuts (WP Connection, WP Tables, Sync Manager, Sync Log, Table Links). Mirrored table shortcuts are added/removed dynamically by `workspace_utils.py` when the workspace loads (via the `get_desktop_page` override).

The version number (`nce_sync.__version__`) is injected as a footer on each workspace load by `overrides._inject_version`.

## Excel Export

"Send All to Excel" button on every list view (via `list_auto_size.js`):
- Enqueues `_build_excel_file` as a background job (handles 20K+ rows without timeout)
- Builds xlsx with `openpyxl`, saves as private Frappe File
- Sends file URL via `frappe.publish_realtime("excel_export_ready", ...)` 
- Client JS listens for the event and triggers download via hidden anchor click
- Persistent "Exporting N records…" toast stays visible until download triggers

## Development Workflow

```bash
# Local development
cd ~/Documents/_NCE_projects/NCE_Sync
# Edit files, then:
git add . && git commit -m "description" && git push

# Deploy to production
ssh server
cd ~/frappe-bench
cd apps/nce_sync && git pull
cd ~/frappe-bench
bench --site all migrate
bench build --app nce_sync   # needed if JS/CSS changed
bench --site all clear-cache
bench restart
```

**Version bumps:** Update `nce_sync/__init__.py` before pushing. The version displays in the Tables workspace footer.

## Pending / Future Work

From `PLAN_APP_FINALIZATION.md`:

| Item | Status | Notes |
|------|--------|-------|
| Phase 1: Test & Stabilize | Mostly done | Some edge cases may need attention |
| Phase 2: Fix Workspace JSON | Done | Static skeleton + dynamic shortcuts |
| Phase 3: Fixtures | Not started | Bundle WP Connection, WP Tables, Sync Manager as fixtures |
| Phase 5: Polish | Ongoing | Remove console.log, clean up error messages |
| Phase 6: Table Links | Done | Define + Visualize tabs working |
| Phase 7: Related Records Portals | Planned | Phase 1 done (new tab navigation). Split-view panel is future |
| Phase 8: Google Sheets Export | Not started | |
| Live Write-Back | Done | SQL Direct auto-push + API Required flag |

### Specific pending items:
- **DocType Link auto-creation:** When a Link field is added via Table Links, auto-create a `DocType Link` entry on the target (One) DocType so the sidebar "Connections" section works
- **Sync field-type checking:** Detect type mismatches when syncing (e.g., Link field stores varchar but WP column is int — MariaDB handles this transparently, but worth logging)
- **Custom field protection:** Sync should not overwrite fields that don't exist in the WP source (user-added custom fields on mirrored DocTypes)

## Gotchas

1. **Migration crash:** The `live_sync.on_record_change` wildcard hook fires during `bench migrate`. If the `write_back_mode` column doesn't exist yet, `_get_sql_direct_map` catches the error and returns `{}` (no-op).

2. **Re-mirroring destroys links:** `delete_mirror` deletes the entire DocType. Links must be re-established manually afterward. This is by design — avoids fragile persistence logic.

3. **Frappe caching:** After modifying DocType fields programmatically, always call `frappe.clear_cache(doctype=dt)` before and after `doc.save()`, plus `frappe.db.commit()`. Client-side caches (`frappe.model.docinfo`, `frappe.boot.docs`) also need clearing for immediate UI reflection.

4. **Temp negative IDs:** New records created in Frappe get negative integer names (e.g., `-1`, `-2`). These are skipped during orphan deletion and pushed to WP during reverse sync. After push, Frappe renames them to real WP IDs.

5. **`bench build` required:** Any change to files in `nce_sync/public/` requires `bench build --app nce_sync` (or `bench build`) to take effect. Python changes only need `bench restart` (or nothing if workers auto-reload).

6. **Workspace override:** The app overrides `frappe.desk.desktop.get_desktop_page` to inject dynamic shortcuts. This runs on every Tables workspace load. If the override breaks, the entire workspace fails to render — test carefully.
