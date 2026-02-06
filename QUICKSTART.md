# Phase 1 Complete — Quick Reference

## What Was Built

A complete Frappe custom app that mirrors WordPress database table schemas into Frappe DocTypes.

## Installation Commands

```bash
# Navigate to your Frappe bench directory
cd ~/frappe-bench  # or wherever your bench is

# Install the app (if not already installed)
bench get-app https://github.com/oliver-nce/NCE_Sync

# Install on your site
bench --site [your-site-name] install-app nce_sync

# Run migrations to register DocTypes
bench --site [your-site-name] migrate

# Clear cache and restart
bench --site [your-site-name] clear-cache
bench restart
```

## How to Use

### 1. Access the App
- Open Frappe Desk
- Press Cmd+K (Mac) or Ctrl+K (Windows) to open awesomebar
- Type "NCE Sync" or "WordPress Connection"
- Press Enter

### 2. Configure Connection
- Enter your WordPress database connection details:
  - Host (e.g., `my-wp-db.abc123.us-east-1.rds.amazonaws.com`)
  - Port (default: 3306)
  - Database Name
  - Username
  - Password
- Click "Save"
- Click "Test Connection" to verify

### 3. Discover Tables
- Click "Discover Tables" button
- A dialog shows two panels:
  - **Left:** Available tables/views (searchable)
  - **Right:** Selected tables
- Click any table on the left to add it
- Click "X" on the right to remove it
- Close dialog when done

### 4. Mirror Schemas
Two options:

**Option A: Mirror All**
- From WordPress Connection form
- Click "Mirror All Schemas"
- Confirms and processes all selected tables

**Option B: Mirror Individual Table**
- Go to WP Tables list (awesomebar → "WP Tables")
- Open any table record
- Click "Mirror Schema"
- Processes just that table

### 5. Use Generated DocTypes
- Type the generated DocType name in awesomebar
- Opens like any other Frappe DocType
- Can customize via Customize Form
- **Phase 1: No data sync yet — just schema**

## Key Features

✅ **PyMySQL Connection** — Direct MariaDB connection  
✅ **Auto-Discovery** — Finds all tables and views  
✅ **Type Mapping** — Intelligent MariaDB → Frappe field type conversion  
✅ **Timestamp Detection** — Auto-detects created/modified columns  
✅ **Key Replication** — Preserves primary keys, unique keys, indexes  
✅ **Source-of-Truth** — User values never overwritten by auto-detection  
✅ **Error Handling** — Clear error logs, partial success supported  
✅ **Custom DocTypes** — Generated as Frappe Custom DocTypes (stored in DB)

## Architecture

```
WordPress Connection (Single)
├── Test Connection      → utils/schema_mirror.py → get_wp_connection()
├── Discover Tables      → utils/schema_mirror.py → discover_tables_and_views()
└── Mirror All Schemas   → utils/schema_mirror.py → mirror_table_schema()
                            └── For each WP Tables record

WP Tables (Standard DocType)
├── Tracks selected tables
├── Stores metadata (timestamps, sync direction)
└── Mirror Schema button → utils/schema_mirror.py → mirror_table_schema()

utils/schema_mirror.py
├── PyMySQL connection management
├── information_schema introspection
├── Timestamp field auto-detection
├── Type mapping (MariaDB → Frappe)
└── Programmatic Custom DocType creation/update
```

## Files Structure

```
nce_sync/
├── hooks.py                           # ✅ FIXED: app_description quote
├── modules.txt                        # ✅ FIXED: "NCE Sync"
├── nce_sync/                          # Module directory
│   ├── __init__.py                    # ✅ CREATED
│   └── doctype/
│       ├── __init__.py                # ✅ CREATED
│       ├── wordpress_connection/
│       │   ├── __init__.py            # ✅ CREATED
│       │   ├── wordpress_connection.json   # ✅ CREATED (Single DocType)
│       │   ├── wordpress_connection.py     # ✅ CREATED (3 methods)
│       │   └── wordpress_connection.js     # ✅ CREATED (Discovery dialog)
│       └── wp_tables/
│           ├── __init__.py            # ✅ CREATED
│           ├── wp_tables.json         # ✅ CREATED (Standard DocType)
│           ├── wp_tables.py           # ✅ CREATED (Validation + mirror)
│           └── wp_tables.js           # ✅ CREATED (Mirror button)
└── utils/
    ├── __init__.py                    # ✅ CREATED
    └── schema_mirror.py               # ✅ CREATED (Core logic, ~650 lines)
```

## Constraints & Design Choices

### ✅ Followed
- Standard Frappe app structure
- DocTypes defined via JSON + Python controllers
- Custom DocTypes generated programmatically
- Schema introspection via information_schema
- Metadata persisted via Frappe ORM
- Partial success acceptable
- Clear error logging

### ❌ Not in Phase 1 (By Design)
- Data synchronization
- Background jobs
- Conflict resolution
- Incremental updates
- Performance optimization
- Re-discovery of schema changes

## Troubleshooting

### "Module NCE Sync not found"
```bash
bench --site [site] migrate
bench --site [site] clear-cache
bench restart
```

### "Connection failed"
- Check AWS security groups allow connection
- Verify username/password
- Test with: `mysql -h [host] -u [user] -p [database]`

### "Generated DocType not appearing"
- Check WP Tables → Mirror Status
- Check Error Log in Frappe
- Look at WP Tables → Error Log field
- Try simpler table first

### Pre-commit hooks failing
```bash
cd nce_sync
ruff format .
ruff check --fix .
```

## What's Next?

Phase 1 provides schema mirroring only. For Phase 2, consider:

1. **Data Sync** — Actual data transfer between WordPress and Frappe
2. **Scheduling** — Automated sync via Frappe scheduler
3. **Conflict Resolution** — Handle bidirectional sync conflicts
4. **Performance** — Batch operations, connection pooling
5. **Monitoring** — Sync logs, progress tracking, error alerts

## Success Criteria ✅

- [x] WordPress DB connection works
- [x] Tables and views can be discovered
- [x] Tables can be added/removed from WP_Tables
- [x] Corresponding DocTypes are generated
- [x] DocTypes structurally mirror WordPress tables
- [x] No sync logic exists (correct for Phase 1)

**Phase 1 Complete!**

---

## Quick Test Script

```python
# Run in Frappe Console (bench console)

# 1. Get WordPress Connection
wp_conn = frappe.get_single("WordPress Connection")
print(f"Host: {wp_conn.host}")
print(f"Database: {wp_conn.database}")

# 2. Test connection
wp_conn.test_connection()

# 3. Discover tables
tables = wp_conn.discover_tables()
print(f"Found {len(tables)} tables/views")

# 4. Add a table
frappe.get_doc({
    "doctype": "WP Tables",
    "table_name": "wp_posts",  # Example
    "table_type": "Table",
    "sync_direction": "WP to Frappe"
}).insert()

# 5. Mirror it
wp_table = frappe.get_doc("WP Tables", "wp_posts")
wp_table.mirror_schema()

# 6. Check result
print(f"Generated DocType: {wp_table.frappe_doctype}")
print(f"Status: {wp_table.mirror_status}")
```

---

**Documentation:** See `IMPLEMENTATION.md` for full technical details.
