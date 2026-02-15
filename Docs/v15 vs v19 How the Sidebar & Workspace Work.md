## v15: How the Sidebar & Workspace Work

In v15, the sidebar and workspace system works like this:

**Sidebar is workspace-only and non-persistent.** The left sidebar shows a flat list of Workspace documents, split into two sections: "MY WORKSPACES" (private, per-user) and "PUBLIC" (standard, visible to all). The sidebar is only visible when you're on a workspace page — once you navigate into a list view, form, or report, it disappears. This was a major complaint (see [GitHub #27645](https://github.com/frappe/frappe/issues/27645), [#23761](https://github.com/frappe/frappe/issues/23761)).

**Workspace JSON files in your app source.** Each workspace is defined as a JSON file living in:

```
your_app/
  your_module/
    workspace/
      my_workspace/
        my_workspace.json
```

The JSON contains a `content` field — a stringified JSON array of "blocks" (headers, shortcuts, cards, spacers, charts, onboarding widgets, etc.). It also has a `links` array that defines card groups of link items, where each link has a `link_type` (DocType, Report, Page) and a `link_to` pointing at the target. These links are what populate the card sections on the workspace page body.

**Module ↔ Workspace relationship.** The workspace JSON has a `module` field tying it to a module defined in `modules.txt`. The sidebar items are essentially auto-generated from public workspaces that belong to modules your app declares. There's no concept of apps grouping workspaces — everything is a flat list under PUBLIC.

**What you can put in the sidebar:** Only other Workspace documents. You can't put a direct link to a DocType list, a Report, a Page, or an external URL in the sidebar itself. Those all go *inside* the workspace page body as card links or shortcuts.

------

## v16: What Changed

The v16 UI overhaul (tracked in [GitHub #27900](https://github.com/frappe/frappe/issues/27900)) fundamentally restructured navigation:

**1. Persistent sidebar.** The sidebar is now always visible across all desk views (list, form, report, etc.) — not just on workspace pages. It uses an "Espresso" design style and is collapsible ([PR #27499](https://github.com/frappe/frappe/pull/27499)).

**2. App Switcher.** A new app selector in the sidebar lets you filter workspaces by app ([PR #27621](https://github.com/frappe/frappe/pull/27621)). So instead of one flat PUBLIC list, the sidebar is now scoped: select "ERPNext" and you see ERPNext's workspaces, select "HRMS" and you see HRMS's. Each app gets its own sidebar context.

**3. Sidebar items can be more than just workspaces.** The Workspace DocType was extended so that a workspace can now represent a direct link to a DocType, a Page, a Report, or an external URL — not just a workspace page ([PR #27908](https://github.com/frappe/frappe/pull/27908)). This means your sidebar can now contain direct shortcuts to specific list views or reports without needing a full workspace page wrapping them.

**4. Desktop screen (home).** There's a new desktop/home screen that shows icons for all public workspaces, auto-generated from the apps. For your custom app to appear here correctly, you need the `add_to_apps_screen` hook in `hooks.py`:

python

```python
add_to_apps_screen = [
    {
        "name": "my_app",
        "logo": "/assets/my_app/images/logo.png",
        "title": "My App",
        "route": "/my_app",
    }
]
```

The migration guide specifically calls this out: for auto-generation to work for your custom app, this hook must be set correctly.

**5. Private workspaces moved.** Private workspaces now live under a virtual "My Workspaces" section accessible from the desktop screen, rather than cluttering the main sidebar ([PR #27945](https://github.com/frappe/frappe/pull/27945)).

**6. URL route change.** The desk frontend moved from `/app` to `/desk`. So workspace URLs that were `/app/my-workspace` are now `/desk/my-workspace`.

**7. Workspace customization reset warning.** If you had modified standard workspaces (like "Buying" or "Selling") in v15, those customizations get blown away on migration. You need to back them up before migrating.

------

## Practical Impact for Your v15 App Code

If you're building on v15 and thinking about forward compatibility:

**On v15**, your workspace JSON files define what shows up in the sidebar and what card links appear on the workspace page. The sidebar is just a list of workspace names. All the interesting navigation (links to DocTypes, Reports, etc.) lives *inside* the workspace page content.

**On v16**, the sidebar becomes the primary navigation surface. Your app gets its own sidebar section (via the app switcher), and sidebar items can link directly to DocTypes/Reports/Pages — not just workspace pages. The `add_to_apps_screen` hook becomes important for your app to register itself in the new desktop UI.

The workspace JSON file structure itself is largely the same (it's still a Workspace DocType serialized to JSON), but v16 adds new fields to support the extended link types and the app-scoping behavior. The migration guide doesn't list specific JSON field changes for workspaces, but the PRs show that the Workspace DocType gained the ability to store `link_type` values like DocType, Report, Page, and External URL at the workspace level (not just inside the content blocks).

------

## Workspace JSON Field Differences: V15 vs V16

When writing workspace JSON files for V15 compatibility, avoid these **V16-only fields**:

| Field | V16 Purpose | V15 Behavior |
|-------|-------------|--------------|
| `"app": "app_name"` | App switcher scoping | Ignored but may cause issues |
| `"type": "Workspace"` | Extended link types (DocType, Report, Page, URL) | Not recognized |

**V15-compatible workspace JSON example:**

```json
{
  "charts": [],
  "content": "[...]",
  "doctype": "Workspace",
  "icon": "database",
  "label": "My Workspace",
  "links": [...],
  "module": "My Module",
  "name": "My Workspace",
  "public": 1,
  "shortcuts": [...],
  "title": "My Workspace"
}
```

**V16 workspace JSON (NOT compatible with V15):**

```json
{
  "app": "my_app",           // ❌ V16-only
  "charts": [],
  "content": "[...]",
  "doctype": "Workspace",
  "icon": "database",
  "label": "My Workspace",
  "links": [...],
  "module": "My Module",
  "name": "My Workspace",
  "public": 1,
  "shortcuts": [...],
  "title": "My Workspace",
  "type": "Workspace"        // ❌ V16-only
}
```

------

## Shortcut Count Query Issue (V15)

**Problem:** Workspace shortcuts for DocTypes can trigger a `get_count` query that fails for Single DocTypes.

**Root cause:** The `shortcut_widget.js` in Frappe V15 calls `frappe.db.count()` when:
1. `type == "DocType"`
2. `doc_view != "New"`
3. `stats_filter` is truthy (including empty string `"[]"`)

Single DocTypes (like `WordPress Connection`, `System Settings`) don't have their own database table — they store data in `tabSingles`. The count query tries to query `tabWordPress Connection` which doesn't exist, causing:

```
pymysql.err.ProgrammingError: ('DocType', 'WordPress Connection')
```

**Solution:** For Single DocType shortcuts, ensure `stats_filter` is NOT set (not even to `"[]"`).

**Bad — triggers count query:**

```json
{
  "shortcuts": [
    {
      "label": "WordPress Connection",
      "link_to": "WordPress Connection",
      "type": "DocType",
      "stats_filter": "[]"    // ❌ Truthy! Triggers count query
    }
  ]
}
```

**Good — no count query:**

```json
{
  "shortcuts": [
    {
      "label": "WordPress Connection",
      "link_to": "WordPress Connection",
      "type": "DocType"
      // No stats_filter field at all ✓
    }
  ]
}
```

**For regular (non-Single) DocTypes**, you CAN use `stats_filter` and `doc_view: "List"` safely:

```json
{
  "shortcuts": [
    {
      "doc_view": "List",
      "label": "WP Tables",
      "link_to": "WP Tables",
      "type": "DocType"
      // stats_filter omitted is fine, or can be set for filtering
    }
  ]
}
```

**How to check if a DocType is Single:**
- In the DocType JSON: `"issingle": 1`
- In code: `frappe.get_meta("DocType Name").issingle`
- Single DocTypes have no list view — they open directly to the form