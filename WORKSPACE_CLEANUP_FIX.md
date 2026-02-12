# Orphaned Workspace Shortcuts Fix

## Problem
When a DocType is deleted manually (not through the "Remove Table" button), the workspace shortcut remains, creating a broken link.

## Solution Implemented

### 1. Added `cleanup_orphaned_shortcuts()` Function
**File**: `workspace_utils.py`

This function:
- Scans the NCE Sync workspace for shortcuts
- Checks if the linked DocType still exists
- Removes shortcuts for deleted DocTypes
- Cleans both the content JSON and shortcuts child table
- Returns count of removed shortcuts

```python
@frappe.whitelist()
def cleanup_orphaned_shortcuts():
    # Finds and removes shortcuts for non-existent DocTypes
    # Returns number of shortcuts removed
```

### 2. Added "Cleanup Workspace" Button
**File**: `wordpress_connection.js`

Added a maintenance button to the WordPress Connection form:
- Located under "Maintenance" dropdown
- Runs the cleanup function
- Shows success message with count
- Refreshes the workspace

## How to Use

### Method 1: UI Button (Easiest)
1. Go to **WordPress Connection**
2. Click **Maintenance** dropdown
3. Click **Cleanup Workspace**
4. Orphaned shortcuts are automatically removed

### Method 2: Console
```python
from nce_sync.utils.workspace_utils import cleanup_orphaned_shortcuts
cleanup_orphaned_shortcuts()
```

### Method 3: Direct Removal (if you know the DocType name)
```python
from nce_sync.utils.workspace_utils import remove_from_workspace
remove_from_workspace("Registrations")
frappe.db.commit()
```

## For Your Current Issue

To remove the orphaned "Registrations" shortcut:

1. Go to **WordPress Connection** form
2. Click **Maintenance** → **Cleanup Workspace**

OR use console:
```python
from nce_sync.utils.workspace_utils import cleanup_orphaned_shortcuts
cleanup_orphaned_shortcuts()
```

This will remove the broken "Registrations" link from your workspace!

## Prevention

The best way to delete mirrored tables is to use the built-in buttons:
- **Re-mirror**: Deletes DocType and resets for fresh mirroring
- **Remove Table**: Completely removes DocType, workspace shortcut, and WP Tables record

These handle all cleanup automatically.

## Technical Details

The function:
1. Loads the NCE Sync workspace
2. Parses the content JSON
3. For each shortcut, checks if `frappe.db.exists("DocType", shortcut_name)`
4. Keeps valid shortcuts, removes invalid ones
5. Also cleans the shortcuts child table
6. Saves changes and clears cache
7. Logs removed shortcuts to Error Log
