# Field Matching Dialog Fixes

## Issues Fixed

### Issue #1: Match Column Not Showing
**Problem**: The "Match" checkbox column wasn't visible in the field preview dialog.

**Root Cause**: Browser cache - the JavaScript file needs to be reloaded.

**Solution**: 
```bash
# Clear Frappe cache
bench --site your-site-name clear-cache

# Or from browser
Ctrl + Shift + R (hard refresh)
```

The code is correct and includes:
- Match column header (line 179)
- Checkboxes for each field (lines 213-216)
- Auto-checking PK/UQ fields (line 209)
- Validation for max 3 selections

### Issue #2: Matching Fields Not Saved on Failure
**Problem**: When "Confirm & Create" was clicked, if DocType creation failed, the user's matching field selections were lost.

**Solution Implemented**: Two-step save process

#### Updated JavaScript (`wp_tables.js`)
```javascript
primary_action: function () {
    // Collect matching fields
    let matching_fields = [];
    d.$wrapper.find(".matching-field-checkbox:checked").each(function () {
        matching_fields.push($(this).data("column"));
    });

    d.hide();

    // Step 1: Save matching fields FIRST
    frm.set_value("matching_fields", matching_fields.join(","));
    frm.save("Save", function () {
        // Step 2: Then attempt mirror
        frappe.call({
            method: "mirror_schema",
            // ...
            error: function (r) {
                // Even if mirror fails, matching fields are already saved
                frappe.msgprint(
                    __("Matching fields have been saved, but DocType creation failed..."),
                    __("Partial Success")
                );
            },
        });
    });
}
```

#### Updated Python (`wp_tables.py`)
```python
def mirror_schema(self, field_overrides=None, matching_fields=None):
    # Matching fields should already be saved by JS before this is called
    # But update if provided and different (belt and suspenders)
    if matching_fields and matching_fields != self.matching_fields:
        self.matching_fields = matching_fields
        self.save()
    
    # Then proceed with mirror...
```

## New Behavior

### Before Fix
1. User selects matching fields
2. Clicks "Confirm & Create"
3. DocType creation fails
4. ❌ Matching fields are lost
5. User must reselect them

### After Fix
1. User selects matching fields
2. Clicks "Confirm & Create"
3. **Matching fields saved to WP Tables record** ✅
4. DocType creation attempted
5. If success: Great!
6. If failure: **Matching fields already saved** ✅
7. User can fix the issue and retry without reselecting

## Benefits

1. **Data Preservation**: User's work is not lost on failure
2. **Better UX**: No need to remember and reselect matching fields
3. **Debugging**: Matching fields are available for inspection even if mirror fails
4. **Resilience**: Two-step process ensures at least partial success

## Testing

To test the fix:

1. **Clear browser cache**: Ctrl + Shift + R
2. Open a WP Tables record
3. Click "Mirror Schema"
4. **Verify Match column appears** with checkboxes
5. Select some matching fields
6. Click "Confirm & Create"
7. Even if creation fails, check WP Tables record → **matching_fields should be saved**

## Cache Clearing Commands

```bash
# Method 1: Bench command
bench --site your-site-name clear-cache

# Method 2: Restart bench
bench restart

# Method 3: From browser
Hard refresh: Ctrl + Shift + R (Windows/Linux)
Hard refresh: Cmd + Shift + R (Mac)

# Method 4: Frappe UI
Settings → Clear Cache
```

## Notes

- The Match column shows checkboxes for selecting up to 3 fields
- Primary key and unique fields are pre-selected
- Validation prevents selecting more than 3 fields
- Matching fields are now saved BEFORE attempting DocType creation
- Error handler provides clear feedback about what succeeded/failed
