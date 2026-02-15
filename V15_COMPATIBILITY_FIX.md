# NCE_Sync V15 Compatibility Fix — Complete

**Date:** 2026-02-13  
**Target:** Frappe Framework V15  
**Status:** Changes Applied

---

## V15 Workspace Behavior (Important!)

In V15, the navigation works like this:

### Sidebar
- Shows **only Workspace documents** (not individual DocTypes)
- Your "NCE Sync" workspace appears in the sidebar under PUBLIC
- Clicking it opens the workspace page

### Workspace Page Structure
The workspace page has two visual sections:

1. **Shortcuts (top section)**
   - Defined in `content` field (JSON string)
   - Large clickable cards at top of page
   - Used for quick access to frequently used DocTypes

2. **Card Links (below shortcuts)**
   - Defined in `links` array
   - Grouped into card sections with headers
   - More detailed navigation

### What This Means for NCE_Sync

**Core DocTymehwere is list of enacme to makepes** (WordPress Connection, WP Tables, Sync Manager):
- Appear in BOTH shortcuts AND links (defined in workspace JSON)
- Always visible on the workspace page

**Mirrored DocTypes** (dynamically created from WordPress tables):
- Added ONLY to shortcuts section
- Appear under "Mirrored Tables" header
- Keeps them visually separate from core app DocTypes

---

## Changes Made

### 1. Workspace JSON — Fixed for V15

**File:** `nce_sync/nce_sync/workspace/nce_sync/nce_sync.json`

**Removed V16-specific fields:**
- `"app": "nce_sync"` — V16 app switcher feature
- `"type": "Workspace"` — V16 extended link types

**Added V15 content structure:**
- Header: "Core" with shortcuts for core DocTypes
- Spacer
- Header: "Mirrored Tables" (placeholder for dynamically added tables)

**Simplified shortcuts array:**
- Removed V16-specific fields like `color`, `stats_filter`

### 2. workspace_utils.py — Cleaned Up

**File:** `nce_sync/utils/workspace_utils.py`

**Removed:**
- `add_to_sidebar()` function (misleading name, V15 doesn't support this)

**Kept:**
- `add_to_workspace()` — Adds mirrored tables to shortcuts section
- `remove_from_workspace()` — Removes mirrored tables
- `cleanup_orphaned_shortcuts()` — Cleans up deleted DocTypes

**Added:**
- Clear documentation explaining V15 workspace structure
- Protection for core DocTypes in cleanup function

---

## Testing Instructions

```bash
# Navigate to bench
cd ~/NCE_V15

# Remove any existing installation
bench --site ncev15.localhost uninstall-app nce_sync --yes 2>/dev/null || true
./env/bin/pip uninstall nce_sync -y 2>/dev/null || true
rm -rf apps/nce_sync

# Clean install using bench get-app
bench get-app /Users/oliver2/Documents/_NCE_projects/NCE_Sync

# Install on site
bench --site ncev15.localhost install-app nce_sync

# Run migrate
bench --site ncev15.localhost migrate

# Clear cache
bench --site ncev15.localhost clear-cache

# Start bench
bench start
```

---

## Expected Behavior After Fix

1. **Awesomebar/Magic Menu:**
   - Type "NCE" → Shows "NCE Sync" workspace
   - Core DocTypes also searchable individually

2. **Sidebar (when on workspace page):**
   - Shows "NCE Sync" under PUBLIC section

3. **NCE Sync Workspace Page:**
   - **Top section (Shortcuts):**
     - "Core" header
     - WordPress Connection, WP Tables, Sync Manager cards
     - "Mirrored Tables" header
     - (Dynamically added mirrored table cards appear here)
   
   - **Below (Card Links):**
     - "Settings" card with links to core DocTypes

4. **When mirroring a WordPress table:**
   - New shortcut card appears under "Mirrored Tables" header
   - Does NOT appear in the Settings card section

---

## Files Modified

1. `nce_sync/nce_sync/workspace/nce_sync/nce_sync.json`
   - Removed V16 fields
   - Added proper V15 content structure

2. `nce_sync/utils/workspace_utils.py`
   - Removed misleading `add_to_sidebar()` function
   - Added documentation
   - Improved cleanup function

---

## V15 vs V16 Quick Reference

| Feature | V15 | V16 |
|---------|-----|-----|
| Sidebar shows | Workspace documents only | DocTypes, Reports, Pages, URLs |
| App switcher | No | Yes |
| `app` field in workspace | No | Yes |
| `type: Workspace` field | No | Yes |
| Persistent sidebar | No (workspace pages only) | Yes (all pages) |
| `add_to_apps_screen` hook | Not needed | Required for home screen |

---

**End of V15 Compatibility Fix**
