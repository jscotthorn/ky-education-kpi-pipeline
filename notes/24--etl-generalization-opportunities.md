# Journal Entry 24: ETL Generalization Opportunities Analysis

**Date:** 2025-07-20  
**Task:** Analyze four smallest ETL modules for generalization and refactoring opportunities

## Files Analyzed

1. **enrollment.py** (1,073 bytes) - Minimal template with basic config pattern
2. **postsecondary_readiness.py** (14,461 bytes) - Full ETL with demographic mapping
3. **english_learner_progress.py** (16,026 bytes) - Full ETL with demographic mapping  
4. **postsecondary_enrollment.py** (17,078 bytes) - Full ETL with demographic mapping

## Common Patterns Identified

### 1. Standard ETL Structure
All modules follow identical structural patterns:
- `normalize_column_names()` - Column mapping dictionaries
- `standardize_missing_values()` - Handle `*`, `**`, empty strings → `pd.NA`
- `add_derived_fields()` - Add source_file, data_source metadata
- `convert_to_kpi_format()` - Main transformation to 10-column format
- `transform()` - Main orchestrator with file discovery and processing

### 2. Configuration Pattern
All use identical Pydantic `Config` class:
```python
class Config(BaseModel):
    rename: Dict[str, str] = {}
    dtype: Dict[str, str] = {}
    derive: Dict[str, Union[str, int, float]] = {}
```

### 3. Column Mapping Dictionaries
Substantial overlap in `normalize_column_names()` mappings:
- School identification: `School Year`, `County Name`, `District Name`, `School Name`, etc.
- Demographics: `Demographic`, `DEMOGRAPHIC` → `demographic`
- Suppression: `Suppressed`, `SUPPRESSED` → `suppressed`

### 4. Demographic Integration
All modules integrate DemographicMapper consistently:
- Import handling (try/except for relative imports)
- Instantiation in `transform()`
- Usage in `convert_to_kpi_format()`
- Audit log generation and validation

### 5. KPI Format Generation
All produce identical 10-column output:
`district, school_id, school_name, year, student_group, metric, value, suppressed, source_file, last_updated`

### 6. File Processing Loops
Similar patterns for:
- CSV file discovery: `list(source_dir.glob("*.csv"))`
- Empty file checking
- Error handling with continue
- DataFrame concatenation
- Metadata logging

## Generalization Opportunities

### 1. **Abstract Base ETL Class** (High Priority)
Create `BaseETL` class with:
- Common column mappings as class attributes
- Standard function signatures
- Shared utility methods
- Template method pattern for `transform()`

### 2. **Column Mapping Consolidation** (Medium Priority)
- Extract common mappings to shared constants
- School identification mappings (~15 common patterns)
- Demographic mappings (~3 common patterns)
- Suppression mappings (~2 common patterns)

### 3. **KPI Row Creation Template** (Medium Priority)
Common logic for:
- School ID extraction and cleaning (remove .0 suffix)
- Year extraction from school_year (handle 8-digit formats)
- Demographic mapping integration
- Suppression handling
- Timestamp generation

### 4. **File Processing Abstraction** (Low Priority)
Shared file discovery and processing loop:
- CSV enumeration
- Error handling patterns
- Progress logging
- Metadata collection

### 5. **Validation Framework** (Low Priority)
- Common rate validation (0-100 range)
- Count validation (non-negative)
- Data type validations
- Column presence checks

## District Totals/All Schools Handling

### Inconsistent Patterns Found

**postsecondary_enrollment.py** (Lines 194-196):
```python
# Skip non-data rows 
if pd.isna(row.get('demographic')) or row.get('demographic') in ['Total Events', '---District Total---']:
    continue
```

**Other modules**: No explicit filtering of district-level aggregates

### Investigation Needed
1. **Data Source Analysis**: Check if "District Totals", "All Schools", "---District Total---" appear consistently across datasets
2. **Business Logic**: Determine if district aggregates should be:
   - Preserved as separate records with `student_group = "District Total"`
   - Filtered out consistently
   - Handled differently per metric type

**Recommendation**: Audit raw data files to catalog aggregate row patterns and establish consistent handling policy.

## Implementation Priority

### Phase 1: Base Class Creation
1. Create `etl/base_etl.py` with abstract methods
2. Extract common column mappings
3. Migrate `enrollment.py` as proof of concept

### Phase 2: Template Method Implementation  
1. Implement shared KPI row creation logic
2. Abstract file processing patterns
3. Migrate one complex module (postsecondary_readiness.py)

### Phase 3: Full Migration
1. Migrate remaining modules
2. Remove duplicated code
3. Establish district totals handling policy

## Estimated Impact
- **Code Reduction**: ~40-60% reduction in duplicated code
- **Maintainability**: Single point of change for common logic
- **Consistency**: Standardized error handling and validation
- **Testing**: Shared test utilities and patterns

## Next Steps
1. Create BaseETL class design
2. Catalog district total patterns across all data sources
3. Implement proof of concept with enrollment.py