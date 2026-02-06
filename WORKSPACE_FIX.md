# Workspace Visibility Fix — Completed

## Issues Fixed

1. **DocTypes disappearing after logout** — Fixed by creating proper Workspace with persistent shortcuts
2. **Individual DocTypes appearing in sidebar/magic menu** — Fixed by keeping `workspace.links` empty and using `content` field for shortcut cards instead
3. **`nce_name` not being used** — Already working correctly in schema_mirror.py line 367

## Changes Made

### New File: `nce_sync/utils/workspace_utils.py`
- `ensure_workspace()` — Creates or retrieves the NCE Sync workspace
- `add_to_workspace(doctype_name, label)` — Adds DocType as shortcut card to workspace page content (not links)
- `initialize_workspace_on_install()` — Called on app install to set up workspace with core DocTypes

### Modified: `nce_sync/hooks.py`
- Enabled `after_install` hook pointing to `initialize_workspace_on_install()`

### Modified: `nce_sync/utils/schema_mirror.py`
- Added import for `add_to_workspace`
- After mirroring a table, automatically adds it to workspace (line 388-389)
- Uses `nce_name` as label if set, otherwise falls back to `doctype_name`

### New File: `nce_sync/utils/__init__.py`
- Empty init file for utils package

## How It Works

**On Install:**
1. `after_install` hook runs
2. Creates "NCE Sync" workspace with icon
3. Adds "WordPress Connection" and "WP Tables" as shortcut cards to workspace content
4. Keeps `links` array empty (preventing sidebar clutter)

**After Mirroring a Table:**
1. DocType is created as Custom DocType
2. `add_to_workspace()` is called with the new DocType name
3. DocType appears as a shortcut card in the NCE Sync workspace page
4. Cache is cleared so changes are immediately visible

## Result

- Only "NCE Sync" appears in sidebar/magic menu
- Clicking "NCE Sync" opens the workspace page showing all tables as shortcut cards
- Everything persists across logout/login
- Individual DocTypes never clutter the sidebar

## Testing Required

1. **For existing installation:** Run this in bench console to initialize the workspace:
   ```python
   from nce_sync.utils.workspace_utils import initialize_workspace_on_install
   initialize_workspace_on_install()
   ```

2. **Clear cache:**
   ```bash
   bench --site apps.ncesoccer.com clear-cache
   ```

3. **Verify:**
   - Type "NCE Sync" in magic menu
   - Should open a workspace page with shortcut cards for WordPress Connection, WP Tables, and any mirrored tables
   - Individual DocTypes should NOT appear in sidebar/magic menu

## Deployment

```bash
# On your local machine (in NCE_Sync folder)
git add .
git commit -m "feat: Add workspace for proper navigation and DocType visibility"
git push origin main

# On server
cd /home/frappence/NCE/apps/nce_sync
git pull origin main
bench --site apps.ncesoccer.com migrate
bench --site apps.ncesoccer.com clear-cache
bench --site apps.ncesoccer.com console
>>> from nce_sync.utils.workspace_utils import initialize_workspace_on_install
>>> initialize_workspace_on_install()
>>> exit()
bench restart
```
