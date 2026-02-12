# Matching Fields Feature

## Overview
Added functionality to allow users to select up to 3 fields as "matching fields" during the schema mirroring process. This is useful when WordPress tables lack proper unique keys for record matching during sync operations.

## Changes Made

### 1. WP Tables DocType (`wp_tables.json`)
- **Added field**: `matching_fields` (Small Text)
  - Stores comma-separated list of up to 3 field names
  - Positioned after timestamp fields
  - Description: "Comma-separated list of up to 3 fields to use for matching records during sync (selected during Mirror Schema)"

### 2. Field Matching Dialog UI (`wp_tables.js`)

#### Enhanced Preview Dialog
- **Added "Match" column** with checkboxes for each field
- **Auto-selection**: Fields that are primary keys or unique keys are pre-checked
- **Visual feedback**: 
  - Alert shown when trying to select more than 3 fields
  - Checkboxes limited to maximum of 3 selections
- **User guidance**: Added explanatory text about matching fields purpose

#### Dialog Structure Changes
- Table now has 6 columns:
  1. **Match** (checkbox) - NEW
  2. Column name
  3. DB Type
  4. Frappe Type (dropdown)
  5. Nullable
  6. Keys (PK/UQ/IDX badges)

#### JavaScript Logic
```javascript
// Validates max 3 selections
d.$wrapper.on("change", ".matching-field-checkbox", function () {
    let checked_count = d.$wrapper.find(".matching-field-checkbox:checked").length;
    if (checked_count > 3) {
        $(this).prop("checked", false);
        frappe.show_alert({
            message: __("Maximum 3 matching fields allowed"),
            indicator: "orange",
        });
    }
});
```

### 3. Python Backend (`wp_tables.py`)

#### Updated `mirror_schema()` method
- **New parameter**: `matching_fields` (comma-separated string)
- Stores selected matching fields in the WP Tables document before mirroring
- Passes matching fields to schema mirroring logic

```python
def mirror_schema(self, field_overrides=None, matching_fields=None):
    # Store matching fields
    if matching_fields:
        self.matching_fields = matching_fields
```

### 4. Schema Mirror Logic (`schema_mirror.py`)

#### Updated `create_custom_doctype()` function
- Reads `matching_fields` from WP Tables document
- Marks user-selected matching fields as `unique` in the generated DocType
- Fields marked as unique in either:
  - WordPress database schema (existing behavior)
  - User selection via matching fields (new behavior)

#### Updated `update_existing_doctype()` function
- Same matching fields logic applied when updating existing DocTypes
- Ensures consistency between create and update operations

```python
# Get user-selected matching fields
matching_fields = []
if wp_table_doc.matching_fields:
    matching_fields = [f.strip() for f in wp_table_doc.matching_fields.split(",") if f.strip()]

# Mark as unique if user-selected
elif col_name in matching_fields:
    field["unique"] = 1
```

## User Workflow

1. User clicks "Mirror Schema" on a WP Tables record
2. Preview dialog displays with field type review
3. User sees checkboxes in "Match" column
4. User selects up to 3 fields for record matching
   - Primary keys and unique keys are pre-selected
   - User can adjust selections as needed
5. User confirms and creates DocType
6. Selected matching fields are:
   - Stored in `WP Tables.matching_fields`
   - Marked as `unique` in the generated Frappe DocType

## Benefits

1. **Flexible Matching**: Tables without proper unique keys can still be synced
2. **User Control**: User decides which fields are most appropriate for matching
3. **Visual Feedback**: Clear indication of which fields are selected
4. **Safety**: Limited to 3 fields to prevent over-complication
5. **Auto-Detection**: Smart pre-selection of existing unique/primary keys

## Technical Notes

- Matching fields are stored as comma-separated values in a text field
- Fields marked as matching are set to `unique=1` in Frappe DocType
- The feature works for both new DocType creation and updates to existing DocTypes
- Maximum of 3 matching fields enforced at UI level and recommended for performance

## Future Enhancements

- Could add composite unique constraints (multiple fields together) rather than individual unique constraints
- Could validate that selected matching fields actually provide uniqueness by querying data
- Could auto-suggest best matching fields based on cardinality analysis
