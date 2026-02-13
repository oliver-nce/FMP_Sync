# NCE Sync - Feature Design Specification

## Overview

This document outlines features to be added to the NCE Sync Frappe app. Each feature should be implemented incrementally with testing between stages.

---

## Feature 1: Sync Log DocType

### Purpose
Track history of all sync operations for auditing and debugging.

### DocType: `Sync Log`
- **Type:** Regular DocType (not Single)
- **Module:** NCE Sync
- **Naming:** Auto-generated (e.g., `SYNC-{wp_table}-{#####}`)

### Fields

| Fieldname | Fieldtype | Label | Options/Notes |
|-----------|-----------|-------|---------------|
| wp_table | Link | WP Table | Options: "WP Tables", Required |
| table_name | Data | Table Name | Fetch from wp_table.table_name, Read-only |
| frappe_doctype | Data | Frappe DocType | Fetch from wp_table.frappe_doctype, Read-only |
| sync_method | Select | Sync Method | Options: TS Compare, Truncate & Replace, Manual |
| status | Select | Status | Options: Success, Failed, Partial |
| sync_started | Datetime | Sync Started | |
| sync_completed | Datetime | Sync Completed | |
| duration_seconds | Float | Duration (seconds) | Precision: 2 |
| records_synced | Int | Records Synced | |
| records_created | Int | Records Created | |
| records_updated | Int | Records Updated | |
| records_deleted | Int | Records Deleted | |
| error_message | Long Text | Error Message | |
| error_traceback | Long Text | Error Traceback | |

### List View Columns
- wp_table, table_name, sync_method, status, sync_started, records_synced

### Integration
- Modify `_run_sync_with_status()` in `data_sync.py` to create a Sync Log record after each sync
- Record timing, counts, and any errors

---

## Feature 2: Toggle Auto Sync Button in WP Tables List

### Purpose
Allow quick enable/disable of auto sync directly from the list view without opening each record.

### Implementation

1. **Change field type** in `wp_tables.json`:
   - Change `auto_sync_active` from Select (Yes/No) to Check (checkbox)
   - Add `in_list_view: 1`
   - Note: Requires data migration (convert "Yes"/"No" strings to 1/0)

2. **Create list view script** `wp_tables_list.js`:
   - Add a "Toggle Auto Sync" button that appears when rows are selected
   - Button calls an API endpoint to toggle the `auto_sync_active` field

3. **Create API endpoint** in `api.py`:
   - Whitelist function `toggle_auto_sync(table_names)`
   - Toggles the checkbox for selected tables

---

## Feature 3: Sync Manager DocType (Simplified)

### Purpose
Global settings for sync operations.

### DocType: `Sync Manager`
- **Type:** Single DocType
- **Module:** NCE Sync

### Fields

| Fieldname | Fieldtype | Label | Options/Notes |
|-----------|-----------|-------|---------------|
| syncing_active | Select | Syncing Active | Options: Yes, No. Default: No |
| sync_frequency | Select | Global Sync Frequency | Options: Every 5 Minutes, Every 15 Minutes, Every 30 Minutes, Hourly, Every 6 Hours, Daily, Weekly |
| last_run | Datetime | Last Run | Read-only |
| last_run_status | Select | Last Run Status | Options: Success, Partial, Failed. Read-only |
| next_scheduled_run | Datetime | Next Scheduled Run | Read-only |
| last_run_log | Long Text | Last Run Log | Read-only |

### Notes
- Do NOT include a child table for selecting tables (removed - too complex)
- Individual table sync settings remain on each WP Tables record
- Global frequency overrides individual settings when Sync Manager is active

---

## Feature 4: Workspace Sidebar (Frappe v16)

### Problem
Frappe v16 uses a different mechanism for workspace sidebars than v15. The sidebar is controlled by:
1. `Workspace Sidebar` DocType records in the database
2. JSON files in `app/workspace_sidebar/` folder (for standard apps)

### Current Behavior
- Frappe auto-generates sidebar items from DocTypes in the module
- It picks top 3 DocTypes by record count
- This causes mirrored tables (Families, Registrations, etc.) to appear instead of core app tables

### Requirements
- Sidebar should show: Home, Sync Manager, WordPress Connection, WP Tables, Sync Log
- Should NOT show auto-generated items from mirrored tables

### Investigation Needed
- Understand how Frappe v16 decides which sidebar items to show
- Determine if `workspace_sidebar/*.json` files are imported during migrate
- Find the correct way to override auto-generated sidebar items

### Compatibility
- Must work on both Frappe v15 and v16
- Keep existing `workspace/nce_sync/nce_sync.json` for v15 compatibility

---

## Implementation Order

1. **Sync Log DocType** - Standalone, no dependencies
2. **Toggle Auto Sync** - Requires field type change + data migration
3. **Sync Manager** - Simplified version without child table
4. **Workspace Sidebar** - Needs investigation first

---

## Data Migration Notes

### auto_sync_active Field Change
Before changing from Select to Check, run:
```sql
UPDATE `tabWP Tables` 
SET auto_sync_active = CASE 
    WHEN auto_sync_active = 'Yes' THEN 1 
    ELSE 0 
END
```

---

## Testing Checklist

For each feature:
- [ ] DocType created successfully via `bench migrate`
- [ ] Fields appear correctly in form view
- [ ] List view shows expected columns
- [ ] Functionality works as expected
- [ ] No errors in browser console
- [ ] No errors in bench logs
