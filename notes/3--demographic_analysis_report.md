# Demographic Labels Analysis: Kentucky Graduation Rate Data (2021-2024)

## Executive Summary

This analysis examines demographic label consistency across Kentucky graduation rate files from 2021-2024. The findings reveal significant changes in data structure, naming conventions, and demographic categorization that will impact longitudinal reporting accuracy.

## Key Findings

### 1. Data Structure Changes

**2021-2023 Files:**
- 22 columns including detailed cohort numbers and both 4-year and 5-year graduation rates
- Column 14 contains demographic information
- Files: `graduation_rate_2021.csv`, `graduation_rate_2022.csv`, `graduation_rate_2023.csv`

**2024 File:**
- 16 columns with simplified structure
- Only 4-year graduation rate included
- Column 14 contains demographic information
- File: `KYRC24_ACCT_4_Year_High_School_Graduation.csv`

### 2. Demographic Categories by Year

#### Core Demographics (Present in All Years)
- All Students
- Female
- Male
- African American
- American Indian or Alaska Native
- Asian
- Hispanic or Latino
- Native Hawaiian or Pacific Islander
- Two or More Races
- White (non-Hispanic)
- Economically Disadvantaged
- English Learner
- Foster Care
- Homeless
- Migrant
- Students with Disabilities (IEP)

#### Year-Specific Variations

**2021 Only:**
- Non-Economically Disadvantaged
- Non-English Learner
- Non-Foster Care
- Students without IEP
- English Learner including Monitored

**2022-2023:**
- Non-Economically Disadvantaged
- Non-English Learner
- Non-Foster Care
- Students without IEP
- English Learner including Monitored
- **Non-English Learner or monitored** (NEW in 2022-2023)

**2024 Only:**
- Non Economically Disadvantaged
- Non English Learner
- Non-Foster
- Non-Homeless
- Non-Migrant
- Non-Military
- Student without Disabilities (IEP)
- **Military Dependent** (NEW in 2024)

## 3. Critical Inconsistencies Identified

### A. Naming Convention Changes

| Category | 2021-2023 | 2024 | Impact |
|----------|-----------|------|---------|
| Non-Economically Disadvantaged | Non-Economically Disadvantaged | Non Economically Disadvantaged | Space/hyphen inconsistency |
| Non-English Learner | Non-English Learner | Non English Learner | Space/hyphen inconsistency |
| Non-Foster Care | Non-Foster Care | Non-Foster | Label truncation |
| Students without IEP | Students without IEP | Student without Disabilities (IEP) | Label change (singular vs plural) |

### B. New Categories in 2024
- **Military Dependent**: New demographic category not present in previous years
- **Non-Homeless**: Explicit negative category added
- **Non-Migrant**: Explicit negative category added  
- **Non-Military**: Explicit negative category added

### C. Removed Categories in 2024
- **English Learner including Monitored**: Present in 2021-2023, absent in 2024
- **Non-English Learner or monitored**: Present in 2022-2023, absent in 2024

### D. Column Header Inconsistencies
- 2021-2023: "DEMOGRAPHIC" (all caps)
- 2024: "Demographic" (title case)

## 4. Cross-Year Comparison Table

| Demographic Category | 2021 | 2022 | 2023 | 2024 | Notes |
|---------------------|------|------|------|------|-------|
| All Students | ✓ | ✓ | ✓ | ✓ | Consistent |
| Female | ✓ | ✓ | ✓ | ✓ | Consistent |
| Male | ✓ | ✓ | ✓ | ✓ | Consistent |
| African American | ✓ | ✓ | ✓ | ✓ | Consistent |
| American Indian or Alaska Native | ✓ | ✓ | ✓ | ✓ | Consistent |
| Asian | ✓ | ✓ | ✓ | ✓ | Consistent |
| Hispanic or Latino | ✓ | ✓ | ✓ | ✓ | Consistent |
| Native Hawaiian or Pacific Islander | ✓ | ✓ | ✓ | ✓ | Consistent |
| Two or More Races | ✓ | ✓ | ✓ | ✓ | Consistent |
| White (non-Hispanic) | ✓ | ✓ | ✓ | ✓ | Consistent |
| Economically Disadvantaged | ✓ | ✓ | ✓ | ✓ | Consistent |
| English Learner | ✓ | ✓ | ✓ | ✓ | Consistent |
| Foster Care | ✓ | ✓ | ✓ | ✓ | Consistent |
| Homeless | ✓ | ✓ | ✓ | ✓ | Consistent |
| Migrant | ✓ | ✓ | ✓ | ✓ | Consistent |
| Students with Disabilities (IEP) | ✓ | ✓ | ✓ | ✓ | Consistent |
| Non-Economically Disadvantaged | ✓ | ✓ | ✓ | ✗ | Name change in 2024 |
| Non-English Learner | ✓ | ✓ | ✓ | ✗ | Name change in 2024 |
| Non-Foster Care | ✓ | ✓ | ✓ | ✗ | Name change in 2024 |
| Students without IEP | ✓ | ✓ | ✓ | ✗ | Name change in 2024 |
| English Learner including Monitored | ✓ | ✓ | ✓ | ✗ | Removed in 2024 |
| Non-English Learner or monitored | ✗ | ✓ | ✓ | ✗ | Added 2022-2023 only |
| Non Economically Disadvantaged | ✗ | ✗ | ✗ | ✓ | New format in 2024 |
| Non English Learner | ✗ | ✗ | ✗ | ✓ | New format in 2024 |
| Non-Foster | ✗ | ✗ | ✗ | ✓ | New format in 2024 |
| Non-Homeless | ✗ | ✗ | ✗ | ✓ | New in 2024 |
| Non-Migrant | ✗ | ✗ | ✗ | ✓ | New in 2024 |
| Non-Military | ✗ | ✗ | ✗ | ✓ | New in 2024 |
| Student without Disabilities (IEP) | ✗ | ✗ | ✗ | ✓ | New format in 2024 |
| Military Dependent | ✗ | ✗ | ✗ | ✓ | New in 2024 |

## 5. Recommendations for Standardization

### A. Immediate Actions Required

1. **Create Demographic Mapping Table**: Develop a comprehensive mapping table to translate between different naming conventions across years.

2. **Standardize Naming Conventions**: 
   - Decide on consistent hyphenation/spacing rules
   - Establish case conventions (recommend title case)
   - Standardize singular vs plural forms

3. **Handle Missing Categories**: 
   - Determine how to handle categories that exist in some years but not others
   - Create proxy mappings where appropriate

### B. Proposed Standardization Mapping

```
2024 Format → Standardized Format
"Non Economically Disadvantaged" → "Non-Economically Disadvantaged"
"Non English Learner" → "Non-English Learner"
"Non-Foster" → "Non-Foster Care"
"Student without Disabilities (IEP)" → "Students without IEP"
```

### C. Data Quality Considerations

1. **English Learner Categories**: The disappearance of "English Learner including Monitored" and "Non-English Learner or monitored" in 2024 may indicate a change in how English learner status is tracked.

2. **Military Dependent**: This new category in 2024 represents additional demographic granularity that wasn't captured in previous years.

3. **Negative Categories**: 2024 introduces explicit negative categories (Non-Homeless, Non-Migrant, Non-Military) that weren't present in earlier years.

### D. ETL Pipeline Recommendations

1. **Pre-processing Step**: Implement a demographic label normalization step before data integration.

2. **Year-Specific Logic**: Build year-specific processing logic to handle the differences in demographic categories.

3. **Validation Rules**: Implement validation to ensure all demographic categories are properly mapped.

4. **Documentation**: Maintain detailed documentation of all demographic category changes and mapping decisions.

## 6. Impact Assessment

### High Impact Issues
- **Naming inconsistencies** will cause JOIN failures in longitudinal analysis
- **Missing categories** will result in incomplete trend analysis
- **New categories** in 2024 will have no historical comparison data

### Medium Impact Issues
- **Case sensitivity** may cause lookup failures
- **Punctuation differences** will affect string matching

### Low Impact Issues
- **Column header variations** (easily handled in ETL)

## Conclusion

The demographic label inconsistencies identified across the 2021-2024 graduation rate files pose significant challenges for longitudinal analysis. The 2024 data structure represents a major departure from previous years, with simplified column structure, new demographic categories, and altered naming conventions. 

Implementing the recommended standardization approach will ensure accurate longitudinal reporting and maintain data integrity across all years of graduation rate analysis.