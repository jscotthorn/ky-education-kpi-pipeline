# Student Enrollment Indicator Clarity Improvement

## Issue Identified
The initial student enrollment ETL implementation created generic indicators like `student_enrollment_grade_1` that didn't distinguish between primary and secondary school contexts. This was problematic because:

1. Kentucky has historically maintained separate primary and secondary enrollment files
2. The same grade (like Grade 6) can appear in both primary and secondary schools
3. Users need to understand whether enrollment numbers come from primary or secondary school contexts

## Solution Implemented

### 1. Context-Aware Metric Generation
Updated the `StudentEnrollmentETL.extract_metrics()` method to determine source file context and generate appropriate metrics:

- **Primary enrollment files** → Generate `primary_enrollment_*` metrics
- **Secondary enrollment files** → Generate `middle_enrollment_*` and `secondary_enrollment_*` metrics  
- **KYRC24 unified files** → Generate `student_enrollment_*` metrics with aggregated totals

### 2. Clear Indicator Naming Convention

#### From primary_enrollment files:
- `primary_enrollment_preschool`
- `primary_enrollment_kindergarten`
- `primary_enrollment_grade_1` through `primary_enrollment_grade_5`
- `primary_enrollment_grade_6` (when applicable)
- `primary_enrollment_total`

#### From secondary_enrollment files:
- `middle_enrollment_grade_6` through `middle_enrollment_grade_8`
- `middle_enrollment_total`
- `secondary_enrollment_grade_9` through `secondary_enrollment_grade_12`
- `secondary_enrollment_grade_14`
- `secondary_enrollment_total`

#### From KYRC24 unified files:
- `student_enrollment_preschool` through `student_enrollment_grade_14`
- `student_enrollment_primary_total`
- `student_enrollment_middle_total`
- `student_enrollment_secondary_total`

### 3. Documentation Updates
Updated KPIS.md to clearly document:
- Which metrics come from which source files
- The distinction between primary, middle, and secondary school contexts
- Aggregated totals for each school level

## Benefits
1. **Clarity**: Users can immediately understand the school context of enrollment data
2. **Accuracy**: No ambiguity about whether Grade 6 enrollment is from primary or middle schools
3. **Consistency**: Maintains clear separation between historical file formats
4. **Flexibility**: Supports both historical separated files and modern unified format

## Technical Implementation
The solution uses the source filename to determine context during the `convert_to_kpi_format()` process, storing it as `self._current_source_file` for use in `extract_metrics()`. This allows the same ETL pipeline to handle multiple file formats while producing appropriately named metrics.

## Testing Impact
Unit tests will need updates to reflect the new metric naming convention, but the overall structure remains the same. The improved clarity makes test assertions more explicit about which school type is being tested.

## Reversion to Simple Approach (2025-07-26)

**Issue with Complex Implementation:**
After implementing the context-aware primary/secondary distinction, it became clear that the historical file naming convention of "primary" and "secondary" does not actually correspond to elementary vs. middle/high school divisions as initially assumed. The historical files appear to distribute schools between primary and secondary categories in ways that don't align with grade-level expectations.

**Solution - Revert to Unified Metrics:**
Reverted to the original simple approach with unified `student_enrollment_*` metrics regardless of source file:

- `student_enrollment_total` - Total enrollment across all grades
- `student_enrollment_preschool` through `student_enrollment_grade_12` - Individual grade counts
- `student_enrollment_primary` - Aggregated PreK-5 totals
- `student_enrollment_middle` - Aggregated 6-8 totals  
- `student_enrollment_secondary` - Aggregated 9-12 totals

**Benefits of Simple Approach:**
1. **Consistency**: Same metric names regardless of whether data comes from historical separate files or KYRC24 unified files
2. **Clarity**: No confusing distinctions that don't align with actual grade-level organization
3. **Simplicity**: Easier to understand and use for downstream analysis
4. **Flexibility**: Can aggregate data as needed without being constrained by source file naming conventions

The pipeline now processes all enrollment data into consistent metrics, handling both historical formats and the modern KYRC24 unified format seamlessly.