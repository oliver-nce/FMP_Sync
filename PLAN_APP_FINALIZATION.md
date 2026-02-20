# NCE Sync — App Finalization Plan

## Current State

The app has four core DocTypes, a global list-resize feature, and dynamic mirrored DocTypes:

| Component | Status | Permanent? |
|---|---|---|
| WordPress Connection (DocType + JS) | Working | Yes (app code) |
| WP Tables (DocType + JS + list JS) | Working | Yes (app code) |
| Sync Manager (DocType + JS) | Working | Yes (app code) |
| Sync Log (DocType) | Working | Yes (app code) |
| Resize Columns (global list feature) | Working | Yes (`app_include_js`) |
| "Tables" Workspace | Working | Yes (workspace JSON) |
| Mirrored DocTypes | Dynamic | No (created at runtime) |
| Mirrored table workspace shortcuts | Dynamic | No (added/removed by code) |

---

## Phase 1: Test & Stabilize

Full end-to-end testing of every feature before any finalization work.

### 1.1 Fresh Mirror Flow
- [ ] From WordPress Connection, Discover Tables
- [ ] Select tables, verify WP Tables records created
- [ ] Open a WP Tables record, run Mirror Schema
- [ ] Verify: DocType created, workspace shortcut appears, dialog columns correct
- [ ] Verify: "Frappe ID" column shows type "Data", auto-generated columns pre-checked
- [ ] Verify: Modified TS / Created TS radio buttons work, validation enforced
- [ ] Verify: After mirroring, stays on form view (not redirected to list)

### 1.2 Sync (WP to Frappe)
- [ ] Run "Sync Now" — progress dialog appears with live log
- [ ] Verify: records synced correctly, row counts match
- [ ] Verify: form status updates to "Synced" (green) after completion
- [ ] Verify: dialog auto-closes polling, form reloads automatically
- [ ] Test with 0 new rows (no-change sync) — should complete cleanly

### 1.3 Sync (Both Directions / Reverse Sync)
- [ ] Create a new record in Frappe on a mirrored table
- [ ] Verify: temp negative ID assigned
- [ ] Run sync — verify record pushed to WP, ID renamed to real WP ID
- [ ] Modify an existing record in Frappe, sync — verify WP updated
- [ ] Verify: auto-generated columns are NOT pushed to WP

### 1.4 Scheduled Sync
- [ ] Enable auto-sync on a table, wait for cron cycle
- [ ] Verify: sync runs, Sync Log entry created, status updates

### 1.5 Deletion Cascade
- [ ] Delete a WP Tables record
- [ ] Verify: mirrored DocType deleted (no orphaned MariaDB table)
- [ ] Verify: all soft dependencies cleaned (Reports, Charts, Scripts, etc.)
- [ ] Verify: workspace shortcut removed
- [ ] Verify: no errors in Error Log
- [ ] Repeat with a table that has a Report referencing it

### 1.6 Orphan Protection
- [ ] During WP-to-Frappe sync, verify records with temp negative IDs are NOT deleted
- [ ] Verify: only records with real WP IDs that no longer exist in source are deleted

### 1.7 Edge Cases
- [ ] Mirror a table, delete it, re-mirror the same table — should work cleanly
- [ ] Test with integer PK mapped to Frappe ID — no type errors
- [ ] Test Cleanup Workspace button on WordPress Connection form
- [ ] Resize Columns button works on all list views (core + mirrored)

---

## Phase 2: Fix Workspace JSON

**Problem:** The committed `tables.json` contains hardcoded shortcuts for instance-specific mirrored tables (Registrations, Venues, Events, etc.). On a fresh install these DocTypes won't exist.

**Fix:**
- Strip mirrored table shortcuts from `tables.json`, keeping only the 4 core shortcuts:
  - WordPress Connection
  - WP Tables
  - Sync Manager
  - Sync Log
- Mirrored table shortcuts continue to be added/removed dynamically by `add_to_workspace` / `remove_from_workspace`

---

## Phase 3: Bundle Core Table Data as Fixtures

**Goal:** On `bench install-app nce_sync` or `bench migrate`, the core DocType *records* (not just schemas) are pre-populated so the app is ready to use.

### What gets bundled:
| DocType | Bundle? | Rationale |
|---|---|---|
| WordPress Connection | Yes | Connection settings (host, DB name, credentials) |
| WP Tables | Yes | Table definitions, column mappings, sync settings |
| Sync Manager | Yes | Sync orchestration config |
| Sync Log | **No** | Transient runtime data, not app config |

### How (Frappe fixtures mechanism):

1. Add to `hooks.py`:
   ```python
   fixtures = [
       {"dt": "WordPress Connection"},
       {"dt": "WP Tables"},
       {"dt": "Sync Manager"},
   ]
   ```

2. Export current data:
   ```bash
   bench export-fixtures --app nce_sync
   ```
   This creates JSON files under `nce_sync/fixtures/` containing all records.

3. On `bench migrate` or fresh install, Frappe auto-imports these fixture files.

### Considerations:
- **Credentials:** WordPress Connection may contain DB passwords. Decide whether to:
  - Include them (convenient for same-environment deploys)
  - Exclude/redact them (safer for sharing the app)
- **Column mappings in WP Tables:** These reference the WP source schema. If the WP database structure changes, the mappings would need updating.
- **Mirrored DocTypes are NOT bundled** — they're recreated by running Mirror Schema on each WP Tables record. The WP Tables fixtures contain all the info needed to recreate them.

---

## Phase 4: Workspace Strategy (Final Architecture)

```
tables.json (permanent, committed)
├── WordPress Connection  ← shortcut (permanent)
├── WP Tables             ← shortcut (permanent)
├── Sync Manager          ← shortcut (permanent)
├── Sync Log              ← shortcut (permanent)
└── [Mirrored Tables]     ← shortcuts added/removed dynamically
    ├── Registrations     ← created by add_to_workspace()
    ├── Venues            ← created by add_to_workspace()
    └── ...               ← removed by remove_from_workspace()
```

On fresh install:
1. `bench migrate` imports workspace with 4 core shortcuts
2. `bench migrate` imports fixture data (WP Tables records exist but mirrored DocTypes don't)
3. User clicks "Mirror Schema" on each WP Tables record → DocType created → shortcut added dynamically

---

## Phase 5: Polish & Release Prep

- [ ] Remove `console.log` debug statements from JS files
- [ ] Review all `frappe.log_error` calls — ensure helpful messages
- [ ] Verify `.eslintrc` rules pass on all JS files
- [ ] Update `HANDOFF.md` with final architecture
- [ ] Final `bench build` + test in clean browser
- [ ] Git tag a release version

---

## Phase 6: Table Relationships (Link Fields)

**Goal:** Define foreign-key relationships between mirrored tables so Frappe renders them as searchable Link fields instead of raw integers.

### 6.1 Two-Tab Mirror Dialog

The existing mirror dialog gains a second tab:

| Tab | Purpose | When used |
|---|---|---|
| **Columns** | Field mapping, Frappe ID, types, auto-gen, timestamps | Initial mirror + edits |
| **Relationships** | Define Link fields between tables | After all tables are mirrored |

The dialog works in two modes:
- **Initial mirror:** Tab 1 active, Tab 2 available but likely empty (no other DocTypes yet)
- **Edit mirror (re-open):** Both tabs populated from saved config. Tab 2 now shows valid targets.

### 6.2 Relationships Tab UI

For each column that could be a foreign key (integer/ID columns):

| Source Column | Links To (DocType) | Target Column |
|---|---|---|
| `venue_id` | [Venues ▾] | [id ▾] |
| `event_id` | [Events ▾] | [id ▾] |
| `family_id` | — | — |

- **"Links To" dropdown:** Lists all mirrored DocTypes (from WP Tables where `mirror_status = "Mirrored"`)
- **"Target Column" dropdown:** Auto-populated from the selected target's Frappe ID column (usually the PK)
- Selecting a link changes the Frappe field type from `Data`/`Int` to `Link` with `options` set to the target DocType

### 6.3 Re-Open Mirror Dialog

A new button on the WP Tables form (visible only when `mirror_status = "Mirrored"`):

**"Edit Schema"** → Opens the same mirror dialog in edit mode:
- Tab 1 (Columns): Pre-filled from `column_mapping`, editable
- Tab 2 (Relationships): Pre-filled from saved link definitions
- On submit: Updates the existing DocType fields (adds/changes Link fields) instead of creating a new DocType

### 6.4 Storage

Link definitions saved in `column_mapping` JSON on the WP Tables record:

```json
{
  "venue_id": {
    "wp_column": "venue_id",
    "frappe_field": "venue_id",
    "frappe_type": "Link",
    "link_doctype": "Venues",
    "link_column": "id"
  }
}
```

### 6.5 Sync Impact

During WP-to-Frappe sync, Link field values are stored as the Frappe `name` of the linked record (which equals the WP PK mapped to Frappe ID). Since we already cast PKs to strings, this works natively — Frappe resolves `"42"` to the Venues record named `"42"`.

No special sync logic needed; the existing `_convert_row` handles it.

---

## Phase 7: Related Records Portals (Form Sections)

**Goal:** When a mirrored DocType has Link fields pointing to it from other tables, automatically render inline "portal" sections on the form showing those related records — like FileMaker portals.

### 7.1 How It Works

When viewing a record (e.g. a Venue), the form detects which other mirrored DocTypes have a Link field pointing to this DocType (e.g. Events has `venue_id → Venues`). For each, it renders an inline table section.

```
┌─────────────────────────────────────────────┐
│ Venue: Central Park Arena                   │
│ Address: 123 Main St                        │
│ Capacity: 500                               │
│                                             │
│ ┌─ Events at this Venue ──────────────────┐ │
│ │ ID  │ Event Name     │ Date       │ ... │ │
│ │ 12  │ Spring Gala    │ 2026-03-15 │     │ │
│ │ 45  │ Summer Camp    │ 2026-06-20 │     │ │
│ │ 78  │ Fall Festival  │ 2026-10-01 │     │ │
│ └─────────────────────────────────────────┘ │
│                                             │
│ ┌─ Registrations at this Venue ───────────┐ │
│ │ ID  │ Name          │ Status     │ ...  │ │
│ │ 101 │ Jane Smith    │ Confirmed  │      │ │
│ │ 102 │ John Doe      │ Pending    │      │ │
│ └─────────────────────────────────────────┘ │
└─────────────────────────────────────────────┘
```

### 7.2 Implementation

- Each mirrored DocType gets an `HTML` field at the bottom of the form (added during mirror/edit)
- A shared JS utility (`portal_sections.js`) runs on form refresh:
  1. Reads the relationship definitions from `column_mapping` (saved in Phase 6)
  2. For each "incoming" link (other tables pointing TO this DocType), calls `frappe.client.get_list`
  3. Renders the results as a styled HTML table inside the HTML field
- Features per portal section:
  - Column headers from the source table's field labels
  - Clickable rows (navigate to that record)
  - Row count badge in the section header
  - Pagination or "Show more" for large sets
  - Optional: inline "New" button to create a related record

### 7.3 Auto-Discovery vs Explicit Config

Two approaches (can support both):
- **Auto-discovery:** Scan all mirrored DocTypes for Link fields pointing to the current DocType. Zero config, always up to date.
- **Explicit config:** User picks which portals to show and which columns to display (stored in `column_mapping` or a new JSON field). More control over layout.

Start with auto-discovery, add explicit config later if needed.

### 7.4 Modular Architecture

The portal system is a **reusable component**, not tied to any specific DocType. It can render related records anywhere in the app — on any form, in any HTML field, or even in a dialog.

Core module: `portal_sections.js` included via `app_include_js`

**Public API:**

```javascript
// Render a portal section into any container
nce_sync.portal.render(container, {
    source_doctype: "Events",
    link_field: "venue_id",
    link_value: "42",
    columns: ["name", "event_name", "date"],  // optional, auto-detects if omitted
    limit: 20,
});
```

**Auto-mode:** On mirrored forms, hooks into refresh and auto-discovers portals — zero config.

**Manual mode:** Any custom JS can call `nce_sync.portal.render()` to place a related-records section anywhere.

### 7.5 "Go To Related Records"

FileMaker-style navigation to a filtered set of related records. Two variants:

**A) Go to List (filtered)**

Each portal section header includes a "View All →" link that navigates to the standard Frappe list view, pre-filtered:

```
/app/events?venue_id=42
```

Frappe's list view natively supports URL query filters, so this works out of the box. The portal just builds the URL from the link field and current record's name.

**B) Go to Report**

Each portal section can also link to a Report, if one exists for that DocType:

- A dropdown or icon menu next to "View All →" listing available reports:
  - Standard Frappe Reports (auto-discovered via `frappe.get_all("Report", {ref_doctype: ...})`)
  - Any user-defined Report Builder or Script Report
- Clicking a report navigates with the filter pre-applied:

```
/app/query-report/Events%20by%20Venue?venue_id=42
```

**Portal section header layout:**

```
┌─ Events at this Venue (3) ──── [View All →] [Reports ▾] ─┐
│ ID  │ Event Name     │ Date       │ Status                │
│ ... │ ...            │ ...        │ ...                   │
└───────────────────────────────────────────────────────────┘
```

### 7.6 Shared JS Module

A single `portal_sections.js` included via `app_include_js` (like `list_auto_size.js`):
- Hooks into every form refresh
- Checks if the DocType is a mirrored table (by querying WP Tables)
- If yes, renders portal sections automatically
- Exposes `nce_sync.portal.render()` for manual/custom placement
- No per-DocType JS needed — fully generic and reusable

---

## Phase 8: Google Sheets Export

**Goal:** Push any list view or report output to a Google Sheet — on demand or on a schedule.

### 8.1 Core Capability

Any data visible in a Frappe list or report can be exported to a Google Sheet with one click. The sheet stays linked so it can be refreshed/updated.

### 8.2 Authentication

- Google Sheets API via OAuth 2.0 or Service Account
- Credentials stored in a new **"Google Sheets Connection"** DocType (similar to WordPress Connection)
- One-time setup: paste service account JSON or complete OAuth flow
- Service Account is simpler for server-side/scheduled pushes (no user interaction needed)

### 8.3 UI: On-Demand Export

**From any list view:**
- "Push to Sheet" button in the Actions menu (injected globally via `app_include_js`, like Resize Columns)
- Exports the current filtered/sorted list with visible columns
- Creates a new Google Sheet OR updates an existing linked one
- Shows a toast with a link to the sheet when done

**From any report:**
- Same "Push to Sheet" button on the report page
- Exports the report output as-is (respects current filters)

**From a portal section (Phase 7):**
- Small export icon in the portal section header
- Pushes that related-records subset to a sheet

### 8.4 Sheet Linking & Refresh

Each export creates a **"Sheet Export"** record (child table or standalone DocType):

| Field | Example |
|---|---|
| Source DocType | Events |
| Filters | `{"venue_id": "42"}` |
| Report Name | (optional, if from a report) |
| Sheet URL | `https://docs.google.com/spreadsheets/d/...` |
| Sheet Tab Name | Events |
| Last Pushed | 2026-02-19 14:30 |
| Auto-Refresh | Every 6 hours / Daily / Manual only |

On refresh:
- Clears the sheet tab and re-writes all rows (full replace — simple, no merge conflicts)
- Or appends new rows only (optional mode for append-only logs)

### 8.5 Scheduled Push

- Auto-refresh exports run via `scheduler_events` (like sync jobs)
- Configurable per export: hourly, daily, weekly, or manual only
- Errors logged to Sync Log or a dedicated export log

### 8.6 Implementation

| Component | Technology |
|---|---|
| Google API client | `google-api-python-client` + `google-auth` (pip) |
| Sheet operations | Google Sheets API v4 (`spreadsheets.values.update`) |
| Global JS button | `app_include_js` script (like `list_auto_size.js`) |
| Server endpoint | Whitelisted Python method: `push_to_google_sheet` |
| Credentials storage | Google Sheets Connection DocType (encrypted fields) |

### 8.7 Security

- Service account credentials stored in Frappe's encrypted field type (`fieldtype: "Password"`)
- Sheet sharing controlled by Google — service account shares the sheet with specified email(s)
- No raw credentials in fixtures or git

---

## Phase 9: Visual Relationship Builder (Future)

**Optional future enhancement** — a dedicated page with a visual canvas:

- Table cards showing each mirrored DocType and its columns
- Drag lines between columns to define relationships
- Overview of all relationships across the entire schema
- Export/import the full schema definition as a single JSON file

This builds on Phase 6's data model. Phase 6 (dialog-based) delivers the core value; Phase 7 adds the visual polish.

---

## Execution Order

```
Phase 1 (Test)  →  Fix bugs found  →  Phase 2 (Workspace JSON)
    →  Phase 3 (Fixtures)  →  Phase 4 (verify strategy)
    →  Phase 5 (Polish)  →  Commit & tag v1.0

Phase 6 (Link fields via dialog tabs)     →  Test  →  tag v1.1
Phase 7 (Related records portal sections) →  Test  →  tag v1.2
Phase 8 (Google Sheets export)            →  Test  →  tag v1.3
Phase 9 (Visual relationship builder)     →  Test  →  tag v2.0
```

Each phase requires explicit approval before proceeding.
