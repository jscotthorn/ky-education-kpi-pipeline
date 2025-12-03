# 2018-19 Historical Data Extension

## Overview

This journal documents the process of extending KDE ETL pipelines to include 2018-19 school year data from the Kentucky Department of Education Historical Datasets.

## Background

The KDE provides historical datasets in xlsx format at:
https://www.education.ky.gov/Open-House/data/HistoricalDatasets/

These files use a different schema than the 2020+ CSV files, requiring:
1. Column name mappings (e.g., `SCH_CD` → `school_code`)
2. Demographic label mappings (e.g., `Studw/disab.` → `Students with Disabilities (IEP)`)
3. xlsx sheet handling (data typically on 'DATA' sheet)

## Summary

| Pipeline | Status | 2019 Rows | Schools | Districts |
|----------|--------|-----------|---------|-----------|
| kentucky_summative_assessment | ✅ Complete | - | - | - |
| student_enrollment | ✅ Complete | 164,810 | 1,556 | 176 |
| secondary_enrollment | ✅ Complete | 4,843 | 60 | 3 |
| graduation_rates | ✅ Complete | 29,442 | 398 | 178 |
| chronic_absenteeism | ✅ Complete | 86,355 | 1,515 | 177 |
| teacher_turnover | ✅ Complete | 4,575 | 1,525 | - |
| spending_per_student | ✅ Complete | 8,092 | 1,156 | - |
| student_teacher_ratio | ✅ Complete | 1,533 | 1,533 | - |
| district_school_list | ✅ Complete | 25,143 | 1,479 | - |
| school_courses | ✅ Complete | 29,147 | 1,361 | - |
| postsecondary_readiness | ✅ Complete | 17,864 | 406 | - |
| safe_schools_climate | ⚠️ Incompatible schema | - | - | - |
| safe_schools_discipline | ⚠️ Incompatible schema | - | - | - |

## Completed Pipelines

### 1. kentucky_summative_assessment
- **File**: ASSESSMENT_PROFICIENCY_GRADE_18-19.xlsx
- **Status**: Completed previously
- **Notes**: First pipeline extended with 2018-19 data

### 2. student_enrollment
- **File**: STUDENT_PRIMARY_ENROLLMENT_18-19.xlsx
- **Date**: December 2, 2025
- **Changes**:
  - Added column mappings to `etl/student_enrollment.py`
  - Added demographic mappings to `config/demographic_mappings.yaml`
  - Added 2019 year-specific config
- **Results**: 164,810 KPI rows, 1,556 schools, 176 districts

### 3. secondary_enrollment
- **File**: STUDENT_SECONDARY_ENROLLMENT_18-19.xlsx
- **Date**: December 2, 2025
- **Changes**: Added same column mappings as student_enrollment
- **Results**: 4,843 KPI rows, 60 schools, 3 districts

### 4. graduation_rates
- **File**: GRADUATION_RATE_18-19.xlsx
- **Date**: December 2, 2025
- **Changes**:
  - Added column mappings for GRADRATE4YR, GRADRATE5YR, GRADS4YR, etc.
  - Uses existing 3-letter demographic codes (TST, ETB, ETW, etc.)
- **Results**: 29,442 KPI rows, 398 schools, 178 districts

### 5. chronic_absenteeism
- **File**: CHRONIC_ABSENTEEISM_18-19.xlsx
- **Date**: December 2, 2025
- **Changes**:
  - Added column mappings for CHRONIC_ABSENTEE_CNT, PCT_CHR_ABSENT_CNT, etc.
  - Uses DISAGG_GROUP for demographic names
  - Added "Military Connected" demographic mapping
- **Results**: 86,355 KPI rows, 1,515 schools, 177 districts

### 6. teacher_turnover
- **File**: TEACHER_TURNOVER_18-19.xlsx
- **Date**: December 2, 2025
- **Changes**:
  - Added column mappings for TCH_TURNOVER_CNT, TCH_TOTAL, TURNOVER_PCT
- **Results**: 4,575 KPI rows, 1,525 schools

### 7. spending_per_student
- **File**: SPENDING_PER_STUDENT_18-19.xlsx
- **Date**: December 2, 2025
- **Changes**:
  - Added column mappings for PERSON_PER_STU_FED, TOTAL_PER_STU_ALLFUNDS, etc.
  - Note: File lacks SCH_CD, so STATE_SCH_ID mapped to school_code
- **Results**: 8,092 KPI rows, 1,156 schools

### 8. student_teacher_ratio
- **File**: STUDENT_TEACHER_RATIO_18-19.xlsx
- **Date**: December 2, 2025
- **Changes**:
  - Added column mapping for STDNT_TCH_RATIO
- **Results**: 1,533 KPI rows, 1,533 schools

### 9. district_school_list
- **File**: DISTRICT_SCHOOL_LIST_18-19.xlsx
- **Date**: December 2, 2025
- **Changes**:
  - Added column mappings for SCH_TYPE, TITLE1_STATUS, LOW_GRADE, HIGH_GRADE
  - Added Title I status patterns for "Title I" spelling (vs "Title 1")
- **Results**: 25,143 KPI rows, 1,479 schools

### 10. school_courses
- **File**: SCHOOL_COURSES_SUMMARY_18-19.xlsx
- **Date**: December 2, 2025
- **Changes**:
  - Added column mappings for COURSECATEGORY, STATECOURSECODE, STATECOURSENAME
  - Added grade count column mappings (P, K, G1-G14 → preschool_count, kindergarten_count, etc.)
- **Results**: 29,147 KPI rows, 1,361 schools

### 11. postsecondary_readiness
- **File**: TRANSITION_READINESS_ACCOUNTABILITY_18-19.xlsx
- **Date**: December 2, 2025
- **Changes**:
  - Added column mappings for TRANSITIONRATE, TRANRATEWBONUS, and base identification columns
  - Uses 3-letter demographic codes (TST, ETB, ETW, etc.) already in demographic_mappings.yaml
  - Fixed `convert_to_kpi_format` to handle filtered demographics returning None
- **Results**: 17,864 KPI rows (8,932 per metric), 406 schools, 21 student groups

## Incompatible Pipelines (Schema Mismatch)

### safe_schools_climate & safe_schools_discipline
- **File**: SAFE_SCHOOLS_18-19.xlsx (shared file)
- **Schema Differences**: The 2018-19 file has fundamentally different structure:
  - **TABLE column**: Multi-table file with TABLE indicating data type ('Behavior Events', 'Discipline-Resolutions', 'Legal Sanctions', etc.)
  - **Wide format demographics**: Race/gender as separate columns (`WHITE_CNT`, `BLACK_CNT`, `HISPANIC_CNT`, `MALE_CNT`, `FEMALE_CNT`) instead of a single demographic column
  - **CATEGORY column**: Sub-categorization (e.g., 'Alcohol', 'Drugs', 'Weapons', 'Total')
  - **Event counts**: `TOTAL_STUDENTS`, `TOTAL_EVENTS` columns
- **Modern pipeline structure**: Expects climate/safety survey data with index scores and question-level responses
- **Status**: Would require significant refactoring to handle wide-format demographic columns and TABLE-based filtering
- **Recommendation**: Low priority - the 2018-19 data is behavior/discipline events, not climate surveys

## Common 2018-19 Schema

### Column Mappings (shared across files)

```python
# Base identification columns
'SCH_YEAR': 'school_year',        # Format: 20182019
'CNTYNO': 'county_number',
'CNTYNAME': 'county_name',
'DIST_NUMBER': 'district_number',
'DIST_NAME': 'district_name',
'SCH_NUMBER': 'school_number',
'SCH_NAME': 'school_name',
'SCH_CD': 'school_code',
'STATE_SCH_ID': 'state_school_id',
'NCESID': 'nces_id',
'COOP': 'co_op',
'COOP_CODE': 'co_op_code',
'STUDENTGROUP': 'demographic',
'DEMOGRAPHIC': 'demographic',
'DISAGG_GROUP': 'demographic',
```

### Demographic Mappings (added to config/demographic_mappings.yaml)

```yaml
# Full text demographic names (2018-19 xlsx)
"Studw/disab.": "Students with Disabilities (IEP)"
"Free/Reduced": "Economically Disadvantaged"
"Two or More": "Two or More Races"
"Indian/Alaska": "American Indian or Alaska Native"
"Hawaiian/PI": "Native Hawaiian or Pacific Islander"
"Hispanic": "Hispanic or Latino"
"White": "White (non-Hispanic)"
"Gifted & Talented": "Gifted and Talented"
"MilitaryConnected": "Military Dependent"
"Military Connected": "Military Dependent"
"Foster": "Foster Care"

# 3-letter codes (used in graduation rates and other files)
"TST": "All Students"
"ETB": "African American"
"ETW": "White (non-Hispanic)"
# ... (full list in demographic_mappings.yaml)
```

### Additional Mappings Added

```yaml
# 2025 format variation
"Native Hawaiian or Other Pacific Islander": "Native Hawaiian or Pacific Islander"
```

## Pipelines Without 2018-19 Data

These 18 pipelines have no matching 2018-19 datasets (metrics may have been introduced later):

- advanced_coursework, career_readiness_indicators, cte_participation, cte_pathways
- dual_credit, english_learner_progress, financial_summary, gifted_talented
- kindergarten_readiness, novice_teachers, out_of_school_suspension
- postsecondary_enrollment, postsecondary_readiness
- students_taught_by_ineffective_teachers, students_taught_by_out_of_field_teachers
- teacher_certification, teacher_experience, teacher_working_conditions

**Potential matches to investigate:**
- ENGLISH_LEARNERS_18-19.xlsx → english_learner_progress?
- GIFTED_AND_TALENTED_18-19.xlsx → gifted_talented?
- FINANCE_18-19.xlsx → financial_summary?
- ADVANCED_COURSES_EXAMS_BY_COURSE_18-19.xlsx → advanced_coursework?

## Files Modified

### ETL Modules Updated
- `etl/student_enrollment.py` - Added 2018-19 column mappings
- `etl/secondary_enrollment.py` - Added 2018-19 column mappings
- `etl/graduation_rates.py` - Added 2018-19 column mappings
- `etl/chronic_absenteeism.py` - Added 2018-19 column mappings
- `etl/teacher_turnover.py` - Added 2018-19 column mappings
- `etl/spending_per_student.py` - Added 2018-19 column mappings (with STATE_SCH_ID fallback)
- `etl/student_teacher_ratio.py` - Added 2018-19 column mappings
- `etl/district_school_list.py` - Added 2018-19 column mappings and Title I patterns
- `etl/school_courses.py` - Added 2018-19 column mappings
- `etl/postsecondary_readiness.py` - Added 2018-19 column mappings and None check fix

### Configuration Files Updated
- `config/kde_sources.yaml` - Added 2018-19 xlsx files to all applicable pipelines
- `config/demographic_mappings.yaml` - Added historical demographic label mappings and 2019 year config

## Final Status

**11 KDE pipelines successfully extended with 2018-19 data:**
1. kentucky_summative_assessment
2. student_enrollment
3. secondary_enrollment
4. graduation_rates
5. chronic_absenteeism
6. teacher_turnover
7. spending_per_student
8. student_teacher_ratio
9. district_school_list
10. school_courses
11. postsecondary_readiness

**2 KDE pipelines with incompatible 2018-19 schema:**
- safe_schools_climate
- safe_schools_discipline

The SAFE_SCHOOLS_18-19.xlsx file contains behavior/discipline event data in a wide format (with race/gender as separate count columns), which is fundamentally different from the modern climate survey format. Integrating this data would require significant refactoring.

**6 additional 2018-19 files analyzed but not integrated (incompatible schemas):**
- CAREER_PATHWAYS_18-19.xlsx (pathway names, not counts)
- ADVANCED_COURSES_EXAMS_*.xlsx (different course type codes and structure)
- TRANSITION_TO_ADULT_LIFE_18-19.xlsx (adult life outcomes vs in-state college metrics)
- ENGLISH_LEARNERS_18-19.xlsx (language demographics, not proficiency scores)
- SCHOOL_EXPERIENCE_18-19.xlsx (teacher experience years, not climate survey)

## Census Data Extension

Census data (SAIPE and ACS) was also extended to cover 2018-2023 for complete historical coverage.

### Census SAIPE (County-level Income/Poverty)
- **Source**: Census Bureau API
- **Years Fetched**: 2018, 2019, 2020, 2021, 2022, 2023
- **Records**: 480 per year (4 metrics × 120 counties)
- **Metrics**: median_household_income, poverty_rate_all_ages, poverty_rate_0_17, poverty_rate_5_17
- **Files**: `data/external/census_saipe/census_saipe_*.csv`

### Census ACS 5-Year (Tract-level Demographics)
- **Source**: Census Bureau API
- **Years Fetched**: 2018, 2019, 2020, 2021, 2022, 2023
- **Records**: ~1,100-1,300 tracts per year (statewide)
- **Metrics**: income, poverty, unemployment, education, housing, broadband access
- **Files**: `data/external/census_acs/census_acs_tracts_statewide_*.csv`

**Note**: ACS 5-year data covers a 5-year period. For example, the 2018 file contains 2014-2018 data, which aligns well with the 2018-19 school year.

## Additional 2018-19 Files Investigated

The following 2018-19 xlsx files were downloaded and analyzed for potential integration with existing pipelines. Each file's schema was compared against current KPI expectations.

### Files That Can Be Integrated

#### postsecondary_readiness ← TRANSITION_READINESS_ACCOUNTABILITY_18-19.xlsx ✅ IMPLEMENTED
**Status**: Successfully integrated

**2018-19 Schema**:
```
DEMOGRAPHIC, SUPPRESSED, GRADS, ACADEMIC, CAREER, NONDUP, EL, BONUS,
TRANSITIONRATE, TRANRATEWBONUS
```

**Current KPIs Expected**:
- `postsecondary_readiness_rate` ← TRANSITIONRATE
- `postsecondary_readiness_rate_with_bonus` ← TRANRATEWBONUS

**Implementation Details**:
- Added column mappings to `etl/postsecondary_readiness.py`
- Uses 3-letter demographic codes (TST, ETB, ETW, etc.) already in demographic_mappings.yaml
- ACO/ACD accountability codes are filtered out (not student demographics)
- Fixed `convert_to_kpi_format` to handle None returns from `create_kpi_template`
- **Results**: 17,864 KPI rows (8,932 per metric), 406 schools, 21 student groups

### Files with Incompatible Schemas (Cannot Integrate)

#### 1. cte_pathways ← CAREER_PATHWAYS_18-19.xlsx ❌
**Status**: Schema mismatch - different data structure

**2018-19 Schema**:
```
PROGRAM_AREA, PATHWAYS_AVAILABLE, ACTIVE_ENROLLMENTS_CNT,
PREPARATORY_STUDENTS_CNT, PATHWAY_COMPLETERS_CNT
```

**Current KPIs Expected**:
- `num_cte_pathways` ← expects numeric count
- `cte_pathway_enrollment` ← ACTIVE_ENROLLMENTS_CNT
- `cte_concentrator_count` ← expects "Concentrator Students"
- `cte_pathway_completer_count` ← PATHWAY_COMPLETERS_CNT

**Issue**: 2018-19 PATHWAYS_AVAILABLE contains pathway **names** (e.g., "Agribusiness Systems"), not counts. Modern data has numeric pathway counts. Also uses PREPARATORY_STUDENTS_CNT instead of concentrator terminology. Different semantic meaning.

#### 2. advanced_coursework ← ADVANCED_COURSES_EXAMS files ❌
**Status**: Schema differences too significant

**2018-19 BY_STUDENT Schema**:
```
DISAGGGROUP, DEMO_ABBREV, ENROLLED_CNT, COMPLETERS_CNT,
TEST_TAKERS_CNT, EARNED_QUALIFYING_SCORE_CNT, COURSE_TYPE
```

**2018-19 BY_COURSE Schema**:
```
STATE_COURSE_CODE, STATE_COURSE_NAME, COURSE_TYPE, SUBJECT,
TOTAL_ENROLLMENT_CNT, COMPLETERS_CNT, TEST_TAKERS_CNT, EARNED_QUALIFYING_SCORE_CNT
```

**Issues**:
1. COURSE_TYPE uses abbreviations ("AP", "DC", "IB", "ALL") vs full names
2. BY_STUDENT has DISAGGGROUP demographics but BY_COURSE has none
3. Different column naming conventions throughout
4. Would require extensive mapping logic for course type determination

#### 3. postsecondary_enrollment ← TRANSITION_TO_ADULT_LIFE_18-19.xlsx ❌
**Status**: Different metric structure - completely different data

**2018-19 Schema**:
```
COLLEGE_CNT, CAREER_TECHNICAL_CNT, MILITARY_CNT, EMPLOYED_CNT,
COLLEGEANDEMPLOYED_CNT, OTHER_CNT
COLLEGE_PCT, CAREER_TECHNICAL_PCT, MILITARY_PCT, EMPLOYED_PCT,
COLLEGEANDEMPLOYED_PCT, OTHER_PCT
```

**Current KPIs Expected**:
- `postsecondary_enrollment_total_in_cohort`
- `postsecondary_enrollment_public_ky_college_count`
- `postsecondary_enrollment_private_ky_college_count`
- `postsecondary_enrollment_total_ky_college_rate`

**Mismatch**: 2018-19 tracks broad "Transition to Adult Life" outcomes (college, career tech, military, employed) while modern pipeline tracks specifically "Transition to **In-State** Postsecondary Education" with public/private KY college breakdown. **Fundamentally different metrics.**

#### 4. english_learner_progress ← ENGLISH_LEARNERS_18-19.xlsx ❌
**Status**: Completely different data type

**2018-19 Schema (169 columns)**:
```
FEMALE_CNT, MALE_CNT, HISPANIC_CNT, NATIVEAM_CNT, ASIAN_CNT, BLACK_CNT,
PI_CNT, WHITE_CNT, MULTIPLE_CNT, ALLELSTUDENTS_CNT,
[150+ language columns: SPANISH, ARABIC, CHINESE, VIETNAMESE, etc.]
```

**Current KPIs Expected**:
- `english_learner_score_0_{level}` - % at score band 0
- `english_learner_score_60_80_{level}` - % at score band 60/80
- `english_learner_score_100_{level}` - % at score band 100
- `english_learner_score_140_{level}` - % at score band 140

**Mismatch**: 2018-19 file is **EL student demographic counts by home language spoken**. Modern pipeline is **EL proficiency progress** (score bands measuring growth). **Completely different data - demographics vs assessment progress.**

#### 5. safe_schools_climate ← SCHOOL_EXPERIENCE_18-19.xlsx ❌
**Status**: Wrong data type entirely

**2018-19 Schema**:
```
TOTEXP, CNTEXP, AVGEXPERIENCEYEARS
```

This is **teacher experience years data** (total experience years, teacher count, average years), NOT school climate/safety survey data. This would belong in a `teacher_experience` pipeline if one existed.

### Summary Table - Additional Files Investigated

| 2018-19 File | Target Pipeline | Status | Reason |
|--------------|-----------------|--------|--------|
| TRANSITION_READINESS_ACCOUNTABILITY_18-19.xlsx | postsecondary_readiness | ✅ Can integrate | Direct rate mapping |
| CAREER_PATHWAYS_18-19.xlsx | cte_pathways | ❌ No | Pathway names not counts |
| ADVANCED_COURSES_EXAMS_BY_STUDENT_18-19.xlsx | advanced_coursework | ❌ No | Course type codes differ |
| ADVANCED_COURSES_EXAMS_BY_COURSE_18-19.xlsx | advanced_coursework | ❌ No | No demographics, different structure |
| TRANSITION_TO_ADULT_LIFE_18-19.xlsx | postsecondary_enrollment | ❌ No | Different metrics (adult life vs in-state college) |
| ENGLISH_LEARNERS_18-19.xlsx | english_learner_progress | ❌ No | Language demographics, not proficiency scores |
| SCHOOL_EXPERIENCE_18-19.xlsx | safe_schools_climate | ❌ No | Teacher experience, not climate survey |

### Cleanup Completed

The following files were removed from `kde_sources.yaml` since they cannot be integrated:
- `english_learner_progress`: Removed ENGLISH_LEARNERS_18-19.xlsx (language demographics, not proficiency)
- `postsecondary_enrollment`: Removed TRANSITION_TO_ADULT_LIFE_18-19.xlsx (different metrics)
- `cte_pathways`: Removed CAREER_PATHWAYS_18-19.xlsx (pathway names not counts)
- `advanced_coursework`: Removed ADVANCED_COURSES_EXAMS_*.xlsx files (incompatible schema)
- `safe_schools_climate`: Removed SCHOOL_EXPERIENCE_18-19.xlsx (teacher experience, not climate)

## Files Not Available for Download (Server Blocking)

As of December 2025, the KDE historical datasets server (education.ky.gov) is returning 403 Forbidden errors for many 2018-19 files that were previously accessible. The following files are documented in the KDE dataset list but cannot be downloaded:

### Potentially Compatible (if available)
| File | Target Pipeline | Expected Match |
|------|-----------------|----------------|
| KSCREEN_18-19.xlsx | kindergarten_readiness | BRIGANCE screening data |
| GIFTED_AND_TALENTED_18-19.xlsx | gifted_talented | G&T participation counts |
| TEACHER_QUALIFICATIONS_18-19.xlsx | teacher_certification | Emergency/provisional rates |
| NEW_TEACHER_COUNT_18-19.xlsx | novice_teachers | Novice teacher counts |
| FINANCE_18-19.xlsx | financial_summary | District financial data |
| NATIONAL_BOARD_CERTIFICATION_18-19.xlsx | teacher_certification | NBC teacher counts |

### Unknown Compatibility (would need schema review)
| File | Notes |
|------|-------|
| DROPOUT_18-19.xlsx | No current dropout pipeline |
| RETENTION_18-19.xlsx | No current retention pipeline |
| FREE_AND_REDUCED_LUNCH_18-19.xlsx | May overlap with enrollment data |
| GAP_18-19.xlsx | Achievement gap data |
| SAAR_ATTENDENCE_RATE_18-19.xlsx | Attendance rates (different from chronic absenteeism) |
| COLLEGE_ADMISSIONS_EXAM_18-19.xlsx | ACT/SAT scores (no current pipeline) |
| CTE_OPPORTUNITIES_18-19.xlsx | Could match cte_participation |
| CTE_TRANSITION_READINESS_18-19.xlsx | Could match career_readiness_indicators |
| GROWTH_18-19.xlsx | Academic growth (no current pipeline) |
| FULLTIME_EQUIVALENT_TEACHER_18-19.xlsx | Teacher FTE data |
| TELL_EQUITY_18-19.xlsx | Teacher working conditions survey |

### Files Already Downloaded and Processed
All 19 xlsx files that were successfully downloaded before the server restrictions are documented in the "Completed Pipelines" section above. These include all 11 integrated pipelines plus the 6 files with incompatible schemas
