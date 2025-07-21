# Out of School Suspension Demographics Analysis

## Issue Summary

The demographic configuration has several mismatches with actual data:

1. **Required demographics missing from 2024 data**
2. **Unexpected demographics in 2021-2023 not configured**
3. **Year-specific configurations don't match actual data**

## Actual Demographics by Year

### 2024 (KYRC24_OVW_Student_Suspensions.csv)
**Count: 11 demographics**
- African American
- All Students
- American Indian or Alaska Native
- Asian
- English Learner
- Female
- Hispanic or Latino
- Male
- Native Hawaiian or Pacific Islander
- Two or More Races
- White (non-Hispanic)

**Missing from required list:**
- ❌ Economically Disadvantaged
- ❌ Foster Care
- ❌ Homeless
- ❌ Migrant
- ❌ Students with Disabilities (IEP)

### 2021-2023 (safe_schools_discipline files)
**Count: 19 demographics** (18 valid + "Total Events")
- All core demographics present ✓
- **Additional demographics not in config:**
  - Gifted and Talented
  - Military Dependent
- **Non-demographic entry:**
  - Total Events (should be filtered out)

## Configuration Issues

### 1. Required Demographics (config lines 191-207)
Currently lists demographics as required that are NOT present in 2024 data:
- Economically Disadvantaged
- Foster Care
- Homeless
- Migrant
- Students with Disabilities (IEP)

### 2. Year-Specific Issues

**2024 Configuration:**
- Lists "Military Dependent" as available (line 180) but it's NOT in 2024 data
- Actually appears in 2021-2023 data instead

**2021-2023 Configuration:**
- Missing "Gifted and Talented" from available_demographics
- Missing "Military Dependent" from available_demographics
- Both are present in actual data

### 3. "Total Events" Issue
Appears in 2021-2023 data but is not a demographic - should be filtered out

## Recommendations

### 1. Update Required Demographics
Move these from required to optional since they're not present in all 2024 files:
```yaml
validation:
  required_demographics:
    - "All Students"
    - "Female"
    - "Male"
    - "African American"
    - "American Indian or Alaska Native"
    - "Asian"
    - "Hispanic or Latino"
    - "Native Hawaiian or Pacific Islander"
    - "Two or More Races"
    - "White (non-Hispanic)"
    - "English Learner"  # Present in all years

  # Move to allow_missing:
  allow_missing:
    - "Economically Disadvantaged"  # Not in all 2024 files
    - "Foster Care"                 # Not in all 2024 files
    - "Homeless"                    # Not in all 2024 files
    - "Migrant"                     # Not in all 2024 files
    - "Students with Disabilities (IEP)"  # Not in all 2024 files
    # ... existing optional demographics
```

### 2. Fix Year-Specific Configurations

**2021-2023:** Add missing demographics
```yaml
"2021", "2022", "2023":
  available_demographics:
    # ... existing demographics ...
    - "Gifted and Talented"
    - "Military Dependent"
```

**2024:** Remove Military Dependent
```yaml
"2024":
  available_demographics:
    # Remove these lines:
    # - "Military Dependent"
    # - "Non-Military"
```

### 3. Add Filtering Rule
Add "Total Events" to a filter list:
```yaml
data_quality:
  # Demographics to filter out (not real demographics)
  filter_out:
    - "Total Events"
```

## Impact

These changes will:
1. Eliminate false warnings about missing required demographics
2. Properly validate year-specific demographic availability
3. Better reflect the actual data structure across years
4. Make the pipeline more robust to data variations