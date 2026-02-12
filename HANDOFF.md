# NCE Sync - Handoff Document

## Overview

NCE Sync is a Frappe app that mirrors WordPress database tables/views into Frappe DocTypes and syncs data between them.

**Current Status:** Phase 1 complete (WP to Frappe sync working)

---

## Architecture

```
WordPress DB (MariaDB)
        │
        ▼
┌─────────────────────┐
│  WordPress Connection │  ← Single doc with DB credentials
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│     WP Tables        │  ← One record per table/view to sync
└─────────────────────┘
        │
        ├── Mirror Schema → Creates Frappe DocType
        │
        └── Sync Now → Pulls data from WP into Frappe DocType
```

---

## Key Files

| File | Purpose |
|------|---------|
| `nce_sync/doctype/wordpress_connection/` | DB connection settings, test connection, discover tables |
| `nce_sync/doctype/wp_tables/` | Per-table sync config, mirror/sync actions |
| `nce_sync/utils/schema_mirror.py` | Introspects WP schema, creates Frappe DocTypes |
| `nce_sync/utils/data_sync.py` | Syncs data from WP to Frappe |
| `nce_sync/utils/workspace_utils.py` | Adds/removes DocTypes from NCE Sync workspace |
| `hooks.py` | Scheduler config (runs every 5 min for auto-sync) |

---

## Sync Methods

### 1. TS Compare (Timestamp Comparison)
- Incremental sync using `modified_timestamp_field` (falls back to `created_timestamp_field`)
- Only pulls rows where timestamp >= last_synced - 5 minutes (buffer for clock skew)
- Detects deleted records by comparing matching keys
- Efficient for large tables with frequent small changes

### 2. Truncate & Replace
- Deletes all Frappe records, re-inserts everything from WP
- Use when data gets out of sync or for small tables
- Simple but slower for large tables

---

## Field Mapping

WordPress columns map to Frappe fields:

| WordPress | Frappe |
|-----------|--------|
| `Order_date` | `order_date` (fieldname) / "Order Date" (label) |
| `NCE_Number` | `nce_number` (fieldname) / "NCE Number" (label) |

**Important:** Frappe always lowercases fieldnames. The `column_mapping` JSON field on WP Tables stores the original WP column names for future Frappe→WP sync.

---

## WP Tables Fields

| Field | Purpose |
|-------|---------|
| `table_name` | WordPress table/view name |
| `table_type` | "Table" or "View" |
| `nce_name` | Custom display name for the Frappe DocType |
| `modified_timestamp_field` | Column used for incremental sync |
| `created_timestamp_field` | Fallback if no modified timestamp |
| `matching_fields` | Comma-separated unique key fields (max 3) |
| `sync_direction` | "WP to Frappe" / "Frappe to WP" / "Both" |
| `sync_method` | "TS Compare" / "Truncate & Replace" |
| `sync_frequency` | Minutes between auto-syncs |
| `auto_sync_active` | "Yes" / "No" - enables scheduled sync |
| `column_mapping` | JSON: WP column name → Frappe fieldname |
| `last_synced` | Timestamp of last successful sync |
| `last_sync_status` | "Success" / "Error" / "Running" |
| `last_sync_log` | Summary or error message |
| `frappe_doctype` | Name of generated Frappe DocType |
| `mirror_status` | "Pending" / "Mirrored" / "Error" |

---

## Actions (Buttons)

| Button | When Visible | What It Does |
|--------|--------------|--------------|
| Mirror Schema | Always | Introspects WP table, shows field preview dialog, creates DocType |
| Sync Now | When mirrored | Pulls data from WP into Frappe |
| Truncate Data | When mirrored | Deletes all Frappe records (keeps DocType) |
| Regen Column Map | When mirrored | Rebuilds WP→Frappe column mapping |
| Debug Sync | When mirrored | Shows field matching diagnostics |
| Re-mirror | When mirrored | Deletes DocType, resets to Pending |
| Remove Table | When mirrored | Deletes everything (DocType + WP Tables record) |

---

## Scheduler

Configured in `hooks.py`:

```python
scheduler_events = {
    "cron": {
        "*/5 * * * *": [
            "nce_sync.utils.data_sync.run_scheduled_syncs"
        ]
    }
}
```

Every 5 minutes, checks all WP Tables where:
- `auto_sync_active == "Yes"`
- `mirror_status == "Mirrored"`
- `now - last_synced >= sync_frequency`

---

## Development Workflow

### After Code Changes

| Change Type | Action Required |
|-------------|-----------------|
| Python (.py) | Refresh browser |
| JavaScript (.js) | `bench clear-cache` + refresh |
| DocType JSON | `bench migrate` + `bench clear-cache` + refresh |
| hooks.py | Restart bench |

### AppleScript (Desktop/Start Bench.applescript)

Double-click to:
1. Kill stuck Redis/bench processes
2. Start bench
3. Wait for ready
4. Run migrate + clear-cache in second tab

### Common Issues

**"Address already in use" (Redis ports)**
```bash
killall redis-server
bench start
```

**Python changes not loading**
- Restart bench, or
- `bench clear-cache`

**New DocType fields not appearing**
```bash
bench --site dev.localhost migrate
```

---

## Bench Commands Reference

```bash
# Navigate to bench
cd ~/frappe-dev/NCE-bench
source env/bin/activate

# Start development server
bench start

# Apply DocType changes
bench --site dev.localhost migrate

# Clear all caches
bench --site dev.localhost clear-cache

# Access MariaDB directly
bench --site dev.localhost mariadb

# Check logs
tail -f logs/frappe.log
```

---

## Future Work (Phase 2+)

- [ ] Frappe to WP sync (uses `column_mapping` for original column names)
- [ ] Bidirectional sync with conflict resolution
- [ ] Bulk operations UI
- [ ] Sync history/audit log
- [ ] Field type override persistence (currently only used during mirror)

---

## File Locations

```
~/frappe-dev/NCE-bench/           # Bench directory
├── apps/
│   ├── frappe/                   # Frappe framework
│   └── nce_sync -> symlink       # Points to your app code
├── sites/
│   └── dev.localhost/            # Your development site
├── env/                          # Python virtual environment
└── logs/                         # Log files

~/Documents/_NCE_projects/Frappe_App_Builder/NCE_Sync/  # Your app code
├── nce_sync/
│   ├── nce_sync/
│   │   └── doctype/              # DocType definitions
│   ├── utils/                    # Utility modules
│   └── hooks.py                  # App hooks
└── HANDOFF.md                    # This document
```

---

*Last updated: February 11, 2026*
