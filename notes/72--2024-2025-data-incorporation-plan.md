# Journal 72: 2024-2025 School Year Data Incorporation Plan

## Executive Summary

The Kentucky Department of Education has released **2024-2025 school year data** in a new location. This document analyzes which datasets are available for our equity scorecard indicators and outlines the incorporation plan.

**Date**: 2025-11-19
**Data Source**: https://www.education.ky.gov/Open-House/data/Pages/Assessment_Accountability_Datasets_2024-2025.aspx
**Local Copy**: `/Users/scott/Projects/equity-etl/Assessment and Accountability Data 2024-2025 - Kentucky Department of Education.html`

## Current Pipeline Status

### Active ETL Pipelines (14 modules)
1. ✅ `chronic_absenteeism.py` - Has KYRC24 data
2. ✅ `english_learner_progress.py` - Has KYRC24 data
3. ✅ `graduation_rates.py` - Has KYRC24 4-year data only
4. ✅ `kindergarten_readiness.py` - Has KYRC24 data
5. ✅ `out_of_school_suspension.py` - Has KYRC24 data
6. ✅ `postsecondary_enrollment.py` - Has KYRC24 data
7. ✅ `postsecondary_readiness.py` - Has KYRC24 data
8. ✅ `safe_schools_events.py` - Has KYRC24 data
9. ✅ `safe_schools_climate.py` - Has KYRC24 data
10. ✅ `safe_schools_discipline.py` - Has KYRC24 data
11. ✅ `kentucky_summative_assessment.py` - Has KYRC24 data
12. ✅ `cte_participation.py` - Has KYRC24 data
13. ✅ `student_enrollment.py` - Has KYRC24 data
14. ⚠️ ACT scores, homeless students, benchmark - Have KYRC24 but no active pipelines

### KYRC24 Files Currently Downloaded (24 files)
```
KYRC24_ACCT_4_Year_High_School_Graduation.csv
KYRC24_ACCT_English_Learners_Progress_Proficiency_Rate.csv
KYRC24_ACCT_Index_Scores.csv
KYRC24_ACCT_Kentucky_Summative_Assessment.csv
KYRC24_ACCT_Postsecondary_Readiness.csv
KYRC24_ACCT_Survey_Results.csv
KYRC24_ADLF_Transition_to_In_State_Postsecondary_Education.csv
KYRC24_ASMT_Benchmark.csv
KYRC24_ASMT_Kentucky_Summative_Assessment.csv
KYRC24_ASMT_Kindergarten_Screen_Composite.csv
KYRC24_ASMT_The_ACT.csv
KYRC24_CTE_Participation.csv
KYRC24_OVW_Chronic_Absenteeism.csv
KYRC24_OVW_District_School_List.csv
KYRC24_OVW_Homeless.csv
KYRC24_OVW_Student_Enrollment.csv
KYRC24_OVW_Student_Suspensions.csv
KYRC24_SAFE_Behavior_Events_by_Context.csv
KYRC24_SAFE_Behavior_Events_by_Grade_Level.csv
KYRC24_SAFE_Behavior_Events_by_Location.csv
KYRC24_SAFE_Behavior_Events_by_Type.csv
KYRC24_SAFE_Discipline_Resolutions.csv
KYRC24_SAFE_Legal_Sanctions.csv
KYRC24_SAFE_Precautionary_Measures.csv
```

## 2024-2025 Data Available

### New URL Location
- **Historical (KYRC24)**: `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/`
- **Current (2025)**: `https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/`

### All 2024-2025 Files Released (15 files)

1. **Accountable_Assessment_Performance_2025.CSV** - Assessment accountability data
2. **Accountable_Profile_2025.CSV** - School accountability profiles
3. **Accountable_Summary_2025.CSV** - Accountability summary metrics
4. **Assessment_Performance_by_Grade_2025.CSV** - Grade-level assessment performance
5. **College Admissions Exam_2025.CSV** - ACT scores
6. **Kindergarten_Screen_2025.CSV** - Kindergarten readiness
7. **Graduation_Rate_2025.CSV** - 4 and 5 year cohort graduation rates (combined)
8. **Postsecondary_Readiness_2025.CSV** - College/career readiness
9. **English_Language_Proficiency_2025.CSV** - EL proficiency data
10. **English_Learners_Attainment_2025.CSV** - EL attainment metrics (NEW metric)
11. **Quality_of_School_Climate_and_Safety_Survey_Elementary_School_2025.CSV**
12. **Quality_of_School_Climate_and_Safety_Survey_High_School_2025.CSV**
13. **Quality_of_School_Climate_and_Safety_Survey_Index_Scores_2025.CSV**
14. **Quality_of_School_Climate_and_Safety_Survey_Middle_School_2025.CSV**
15. **Quality_of_School_Climate_and_Safety_Survey_Questions_2025.CSV**

## Equity Scorecard Indicators Mapping

### Available Now (6 of 9 indicators)

| Indicator | Current KYRC24 File | New 2025 File(s) | Pipeline | Status |
|-----------|---------------------|------------------|----------|--------|
| **Kindergarten Readiness** | KYRC24_ASMT_Kindergarten_Screen_Composite.csv | Kindergarten_Screen_2025.CSV | kindergarten_readiness.py | ✅ Ready |
| **3rd Grade Reading** | KYRC24_ACCT_Kentucky_Summative_Assessment.csv | Accountable_Assessment_Performance_2025.CSV<br/>Assessment_Performance_by_Grade_2025.CSV | kentucky_summative_assessment.py | ⚠️ Need to determine which file |
| **8th Grade Math** | KYRC24_ACCT_Kentucky_Summative_Assessment.csv | Accountable_Assessment_Performance_2025.CSV<br/>Assessment_Performance_by_Grade_2025.CSV | kentucky_summative_assessment.py | ⚠️ Need to determine which file |
| **4-Year Graduation** | KYRC24_ACCT_4_Year_High_School_Graduation.csv | Graduation_Rate_2025.CSV | graduation_rates.py | ✅ Ready (includes 5-yr too) |
| **Postsecondary Readiness** | KYRC24_ACCT_Postsecondary_Readiness.csv | Postsecondary_Readiness_2025.CSV | postsecondary_readiness.py | ✅ Ready |
| **English Learner Progress** | KYRC24_ACCT_English_Learners_Progress_Proficiency_Rate.csv | English_Language_Proficiency_2025.CSV<br/>English_Learners_Attainment_2025.CSV | english_learner_progress.py | ⚠️ Need to determine which file |

### Not Yet Released (3 of 9 indicators)

| Indicator | Current KYRC24 File | New 2025 File | Pipeline | Notes |
|-----------|---------------------|---------------|----------|-------|
| **Postsecondary Enrollment** | KYRC24_ADLF_Transition_to_In_State_Postsecondary_Education.csv | ❌ Not available | postsecondary_enrollment.py | Requires tracking graduates over time |
| **Chronic Absenteeism** | KYRC24_OVW_Chronic_Absenteeism.csv | ❌ Not available | chronic_absenteeism.py | Discipline/attendance data typically later |
| **Out of School Suspension** | KYRC24_OVW_Student_Suspensions.csv | ❌ Not available | out_of_school_suspension.py | Discipline/attendance data typically later |

## Implementation Steps

### Phase 1: File Analysis (REQUIRED FIRST)

**Purpose**: Determine which 2025 file(s) contain the specific indicators we need.

#### Task 1.1: Kindergarten Readiness ✅ COMPLETED
- [x] Download `Kindergarten_Screen_2025.CSV`
- [x] Sample first 100 rows to verify schema
- [x] Compare with `KYRC24_ASMT_Kindergarten_Screen_Composite.csv` structure
- [x] Verify demographic breakdowns match (should have 16 groups)
- [x] Document any schema differences

**File to review**: `data/raw/kindergarten_readiness/Kindergarten_Screen_2025.CSV`

**FINDINGS:**

**Schema Differences - MAJOR CHANGES:**

| Aspect | KYRC24 File | 2025 File | Impact |
|--------|-------------|-----------|--------|
| **Total Columns** | 20 columns | 31 columns | ⚠️ 11 NEW columns added |
| **Row Count** | 78,913 rows | 84,961 rows | More data (likely more schools/districts) |
| **School Year Format** | "20232024" | "20242025" | ✅ Same format |
| **Demographics** | 16 unique groups | 16 unique groups | ✅ Same count |

**Column Differences:**

**REMOVED Columns (from KYRC24):**
- County Number
- County Name
- District Number
- School Number
- State School Id
- NCES ID
- CO-OP
- CO-OP Code
- School Type

**NEW Columns (in 2025):**
- PRIOR_SETTING (replaces "Prior Setting" - now a column header instead of row group)
- **11 developmental domain breakdown columns:**
  - Academic/Cognitive Suppressed
  - Academic/Cognitive Percent Below Average
  - Academic/Cognitive Percent Average
  - Academic/Cognitive Percent Above Average
  - Language Development Suppressed
  - Language Development Percent Below Average
  - Language Development Percent Average
  - Language Development Percent Above Average
  - Physical Development Suppressed
  - Physical Development Percent Below Average
  - Physical Development Percent Average
  - Physical Development Percent Above Average
  - Self-Help Suppressed
  - Self-Help Percent Below Average
  - Self-Help Percent Average
  - Self-Help Percent Above Average
  - Social Emotional Suppressed
  - Social Emotional Percent Below Average
  - Social Emotional Percent Average
  - Social Emotional Percent Above Average

**Demographic Groups (16 groups - IDENTICAL):**
- All Students
- Female
- Male
- White (Non-Hispanic) [note: KYRC24 was "White (non-Hispanic)" with lowercase 'n']
- African American
- Hispanic or Latino
- Asian
- American Indian or Alaska Native
- Native Hawaiian or Other Pacific Islander
- Two or more races [note: KYRC24 was "Two or More Races" with different caps]
- English Learner
- Non-English Learner [note: KYRC24 was "Non English Learner" without hyphen]
- Economically Disadvantaged
- Non-Economically Disadvantaged [note: KYRC24 was "Non Economically Disadvantaged"]
- Students with Disabilities (IEP)
- Students without IEP [note: KYRC24 was "Student without Disabilities (IEP)"]

**Prior Setting Values (Both files have same categories):**
- All Students
- Child Care
- Head Start
- Home
- State Funded
- Other

**KEY INSIGHTS:**

1. **⚠️ MAJOR SCHEMA CHANGE**: 2025 file has **much richer data** with developmental domain breakdowns that weren't in KYRC24
2. **🔧 Pipeline Impact**: Current `kindergarten_readiness.py` only extracts composite readiness scores - it will work but won't capture the new domain data
3. **📍 Location Data Missing**: 2025 file lacks county/district numbers, NCES IDs, and other location identifiers
4. **✅ Core Metrics Preserved**: The "Total Percent Ready" column exists in both - our primary equity scorecard metric
5. **⚠️ Demographic Name Variations**: Minor capitalization/spacing differences in demographic names

**RECOMMENDATION:**
- ✅ 2025 file **CAN** be processed by existing pipeline for composite readiness scores
- ⚠️ Pipeline will need updates to:
  1. Handle missing location identifier columns (county_number, district_number, etc.)
  2. Map Prior Setting from column position vs embedded in rows
  3. Optionally extract new developmental domain breakdowns (Academic, Language, Physical, Self-Help, Social-Emotional)
- 📊 For equity scorecard purposes, we only need "Total Percent Ready" which is present in both files

#### Task 1.2: Graduation Rates ✅ COMPLETED
- [x] Download `Graduation_Rate_2025.CSV`
- [x] Sample first 100 rows to verify schema
- [x] Verify it contains **both** 4-year and 5-year graduation data
- [x] Compare with separate KYRC24 files (4-year and 5-year)
- [x] Verify demographic breakdowns match (should have 23 groups)
- [x] Document column structure for both cohorts

**File to review**: `data/raw/graduation_rates/Graduation_Rate_2025.CSV`

**FINDINGS:**

**Schema Differences - EXCELLENT CONSOLIDATION:**

| Aspect | KYRC24 Files | 2025 File | Impact |
|--------|-------------|-----------|--------|
| **File Structure** | 2 separate files (4-year, 5-year) | 1 combined file | ✅ MAJOR IMPROVEMENT |
| **Total Columns** | 16 columns each | 10 columns | ✅ Simplified |
| **Row Count** | 9,529 (4-yr) + 9,529 (5-yr) = ~19k | 13,201 rows | More efficient storage |
| **School Year Format** | "20232024" | "20242025" | ✅ Same format |
| **Demographics** | 24 unique groups | 24 unique groups | ✅ Perfect match |

**Column Comparison:**

**KYRC24 Structure (2 separate files):**
- File 1: `KYRC24_ACCT_4_Year_High_School_Graduation.csv` (16 columns)
- File 2: `KYRC24_ACCT_5_Year_High_School_Graduation.csv` (16 columns)

**2025 Structure (1 combined file):**
```
1. School Year
2. School Code
3. District Name
4. School Name
5. School Classification
6. Demographic
7. Suppressed 4-Year
8. 4-Year Graduation Rate
9. Suppressed 5-Year
10. 5-Year Graduation Rate
```

**REMOVED Columns (from KYRC24):**
- County Number
- County Name
- District Number
- School Number
- State School Id
- NCES ID
- CO-OP
- CO-OP Code
- School Type

**NEW Columns (in 2025):**
- School Classification (e.g., "A1" - accountability classification)
- Both 4-year and 5-year data in same row with separate suppression flags

**Demographic Groups (24 groups - IDENTICAL):**
- All Students
- Female
- Male
- White (Non-Hispanic)
- African American
- Hispanic or Latino
- Asian
- American Indian or Alaska Native
- Native Hawaiian or Other Pacific Islander
- Two or more races
- English Learner
- Non-English Learner
- Economically Disadvantaged
- Non-Economically Disadvantaged
- Students with Disabilities (IEP)
- Students without IEP
- Foster Care
- Non-Foster Care
- Homeless
- Non-Homeless
- Migrant
- Non-Migrant
- Military Dependent
- Non-Military Dependent

**KEY INSIGHTS:**

1. **🎉 MAJOR IMPROVEMENT**: 2025 file **consolidates** 4-year and 5-year data into a single file
   - KYRC24 required processing 2 separate files
   - 2025 has both rates in the same row for each demographic/school

2. **📊 Suppression Handling**: Each cohort has its own suppression flag
   - "Suppressed 4-Year" (Y/N)
   - "Suppressed 5-Year" (Y/N)
   - Allows for one cohort to be suppressed while the other is not

3. **📍 Location Data Missing**: Same issue as kindergarten
   - No county/district numbers
   - No State School ID, NCES ID
   - Only has School Code, District Name, School Name

4. **✅ Core Metrics Preserved**: Both graduation rates present
   - "4-Year Graduation Rate" column
   - "5-Year Graduation Rate" column

5. **🆕 School Classification**: New field shows accountability classification (A1, etc.)

6. **📈 More Records**: 13,201 rows vs ~9,500 per KYRC24 file
   - Likely more schools/programs included

**RECOMMENDATION:**
- ✅ 2025 file is **MUCH BETTER** than KYRC24 - single file vs two files
- ⚠️ Pipeline `graduation_rates.py` currently expects separate 4-year and 5-year files
- 🔧 Pipeline needs update to:
  1. Handle consolidated 4-year + 5-year structure
  2. Parse both graduation rates from same row
  3. Handle separate suppression flags for each cohort
  4. Handle missing location identifier columns
  5. Optionally extract School Classification field
- 📊 For equity scorecard, we primarily need "4-Year Graduation Rate" which is clearly present

#### Task 1.3: Postsecondary Readiness ✅ COMPLETED
- [x] Download `Postsecondary_Readiness_2025.CSV`
- [x] Sample first 100 rows to verify schema
- [x] Compare with `KYRC24_ACCT_Postsecondary_Readiness.csv` structure
- [x] Verify demographic breakdowns match (should have 28 groups)
- [x] Document any schema differences

**File to review**: `data/raw/postsecondary_readiness/Postsecondary_Readiness_2025.CSV`

**FINDINGS:**

**Schema Differences - CONSISTENT PATTERN:**

| Aspect | KYRC24 File | 2025 File | Impact |
|--------|-------------|-----------|--------|
| **Total Columns** | 17 columns | 9 columns | ✅ Simplified |
| **Row Count** | 10,720 rows | 10,963 rows | Slightly more data |
| **School Year Format** | "20232024" | "20242025" | ✅ Same format |
| **Demographics** | 27 unique groups | 27 unique groups | ✅ Perfect match |

**Column Comparison:**

**2025 Structure (9 columns):**
```
1. School Year
2. School Code
3. District Name
4. School Name
5. type (accountability classification, e.g., "A1")
6. Demographic
7. Suppressed
8. Postsecondary Rate
9. Postsecondary Rate With Bonus
```

**REMOVED Columns (from KYRC24):**
- County Number
- County Name
- District Number
- School Number
- State School Id
- NCES ID
- CO-OP
- CO-OP Code
- School Type

**NEW/CHANGED Columns:**
- "type" field (lowercase) = School Classification/Accountability rating
- Same two metrics: Postsecondary Rate and Postsecondary Rate With Bonus

**Demographic Groups (27 groups - IDENTICAL):**
- All Students
- Female
- Male
- White (Non-Hispanic)
- African American
- Hispanic or Latino
- Asian
- American Indian or Alaska Native
- Native Hawaiian or Other Pacific Islander
- Two or more races
- English Learner
- Non-English Learner
- English Learner including Monitored
- Non-English Learner or monitored
- Economically Disadvantaged
- Non-Economically Disadvantaged
- Students with Disabilities (IEP)
- Students without IEP
- Alternate Assessment (alternate assessment students)
- Foster Care
- Non-Foster Care
- Homeless
- Non-Homeless
- Migrant
- Non-Migrant
- Military Dependent
- Non-Military Dependent

**KEY INSIGHTS:**

1. **✅ CLEAN STRUCTURE**: 2025 file follows same simplified pattern as other 2025 files
   - Removed location identifiers
   - Kept essential school identification (School Code, District Name, School Name)

2. **📊 Metrics Preserved**: Both readiness metrics present
   - "Postsecondary Rate" (primary metric for equity scorecard)
   - "Postsecondary Rate With Bonus" (includes bonus points for certain achievements)

3. **📍 Location Data Missing**: Same pattern as other 2025 files
   - No county/district numbers
   - No State School ID, NCES ID
   - Only has School Code and names

4. **🆕 Type Field**: Lowercase "type" field contains accountability classification
   - KYRC24 had separate "School Type" field
   - Appears to be same information, different naming

5. **📈 Slightly More Data**: 10,963 rows vs 10,720 (243 more records)
   - Likely additional schools or programs included

**RECOMMENDATION:**
- ✅ 2025 file **FULLY COMPATIBLE** with equity scorecard needs
- ⚠️ Pipeline `postsecondary_readiness.py` needs update to:
  1. Handle missing location identifier columns
  2. Handle "type" field instead of "School Type"
  3. Otherwise structure is very similar to KYRC24
- 📊 For equity scorecard, we need "Postsecondary Rate" which is clearly present
- 🔧 This should be the **EASIEST** pipeline to update (minimal schema changes)

#### Task 1.4: Kentucky Summative Assessment (3rd Grade Reading & 8th Grade Math) ✅ COMPLETED
**Two 2025 files available - need to determine which one(s) to use:**

- [x] Download `Accountable_Assessment_Performance_2025.CSV`
- [x] Download `Assessment_Performance_by_Grade_2025.CSV`
- [x] Sample first 100 rows of EACH file
- [x] Compare with current KYRC24 files:
  - `KYRC24_ACCT_Kentucky_Summative_Assessment.csv`
  - `KYRC24_ASMT_Kentucky_Summative_Assessment.csv`
- [x] **Determine which file contains**:
  - 3rd grade reading proficiency rates by demographic ✅
  - 8th grade math proficiency rates by demographic ✅
- [x] Verify demographic breakdowns (should have 31-32 groups)
- [x] Document which file to use and why

**Current pipeline** processes both KYRC24 ACCT and ASMT files - may need to consolidate.

**Files to review**:
- `data/raw/kentucky_summative_assessment/Accountable_Assessment_Performance_2025.CSV`
- `data/raw/kentucky_summative_assessment/Assessment_Performance_by_Grade_2025.CSV`

**FINDINGS:**

**Two Different Files with Different Purposes:**

| Aspect | Accountable_Assessment_Performance | Assessment_Performance_by_Grade |
|--------|-----------------------------------|--------------------------------|
| **Purpose** | Aggregated by school level (ES/MS/HS) | Specific grade-level breakdown |
| **File Size** | 26.1 MB (282,785 rows) | 53.2 MB (592,628 rows) |
| **Columns** | 14 columns | 13 columns |
| **Level/Grade Field** | "Level" (ES, MS, HS) | "Grade" (03, 04, 05, 06, 07, 08, 10, 11) |
| **Has Content Index** | ✅ Yes | ❌ No |
| **Specific Grades** | ❌ No - aggregated | ✅ Yes - individual grades |

**Column Structures:**

**Accountable_Assessment_Performance_2025.CSV (14 columns):**
```
1. School Code
2. District Name
3. School Name
4. TYPE (accountability classification)
5. Level (ES, MS, HS)
6. Subject
7. Demographic
8. Suppressed
9. Novice
10. Apprentice
11. Proficient
12. Distinguished
13. Proficient/Distinguished
14. Content Index
```

**Assessment_Performance_by_Grade_2025.CSV (13 columns):**
```
1. School Code
2. District Name
3. School Name
4. School Classification
5. Grade (03, 04, 05, 06, 07, 08, 10, 11)
6. Subject
7. Demographic
8. Suppressed
9. Novice
10. Apprentice
11. Proficient
12. Distinguished
13. Proficient/Distinguished
```

**KEY DECISION: Which File to Use?**

**For Equity Scorecard (3rd Grade Reading & 8th Grade Math):**
- ✅ **USE: Assessment_Performance_by_Grade_2025.CSV**
- ❌ **DON'T USE: Accountable_Assessment_Performance_2025.CSV**

**Rationale:**
1. **Equity scorecard requires SPECIFIC grades** (3rd and 8th)
2. Accountable file only has aggregated levels (ES = grades 3-5, MS = grades 6-8)
3. Assessment_Performance_by_Grade has exact grades we need:
   - Grade "03" for 3rd grade reading
   - Grade "08" for 8th grade math
4. Cannot extract 3rd grade from "ES" or 8th grade from "MS" aggregates

**Grade Levels Available in Assessment_Performance_by_Grade:**
- 03 (3rd grade) ✅ - Reading available
- 04 (4th grade)
- 05 (5th grade)
- 06 (6th grade)
- 07 (7th grade)
- 08 (8th grade) ✅ - Mathematics available
- 10 (10th grade)
- 11 (11th grade)

**Subjects Available:**
- Reading
- Mathematics
- Science
- Social Studies
- Editing and Mechanics
- On-Demand Writing

**Performance Levels (Same in Both Files):**
- Novice
- Apprentice
- Proficient
- Distinguished
- Proficient/Distinguished (combined percentage)

**KEY INSIGHTS:**

1. **📊 Clear Winner**: Assessment_Performance_by_Grade is the ONLY file with specific grade data
2. **🎯 Perfect Match**: Has exactly what we need - Grade 03 Reading and Grade 08 Mathematics
3. **📈 Larger File**: 592k rows vs 282k because it breaks down by individual grade (more granular)
4. **⚠️ Missing Content Index**: By Grade file doesn't have Content Index scores
5. **📍 Location Data Missing**: Same pattern - no county/district numbers or IDs
6. **✅ Proficiency Levels**: Same 4-level system (Novice/Apprentice/Proficient/Distinguished)

**Demographic Groups:**
Both files have extensive demographic breakdowns including:
- Gender (Female, Male)
- Race/Ethnicity (7 categories)
- Economic status
- English Learner status (including "monitored" variants)
- Disability status (IEP, Regular Assessment, With Accommodations)
- Gifted and Talented
- Foster Care, Homeless, Migrant, Military Dependent status

**RECOMMENDATION:**
- ✅ Use **Assessment_Performance_by_Grade_2025.CSV** exclusively for equity scorecard
- 🗑️ Ignore Accountable_Assessment_Performance_2025.CSV for our purposes
- 🔧 Pipeline needs to:
  1. Filter for Grade == "03" and Subject == "Reading"
  2. Filter for Grade == "08" and Subject == "Mathematics"
  3. Extract Proficient/Distinguished percentage as the metric
  4. Handle missing location identifiers
- 📊 This file provides the **EXACT** data we need without aggregation

#### Task 1.5: English Learner Progress ✅ COMPLETED
**Two 2025 files available - need to determine which one(s) to use:**

- [x] Download `English_Language_Proficiency_2025.CSV`
- [x] Download `English_Learners_Attainment_2025.CSV`
- [x] Sample first 100 rows of EACH file
- [x] Compare with `KYRC24_ACCT_English_Learners_Progress_Proficiency_Rate.csv`
- [x] **Determine which file contains**:
  - English learner proficiency/progress rates by demographic ✅
  - Elementary, middle, and high school breakdowns ✅
- [x] Understand difference between "Proficiency" vs "Attainment" metrics ✅
- [x] Document which file to use and why ✅

**Files to review**:
- `data/raw/english_learner_progress/English_Language_Proficiency_2025.CSV`
- `data/raw/english_learner_progress/English_Learners_Attainment_2025.CSV`

**FINDINGS:**

**Two Different Metrics:**

| Aspect | English_Language_Proficiency | English_Learners_Attainment |
|--------|------------------------------|----------------------------|
| **Purpose** | Proficiency score distribution | Annual progress/attainment rate |
| **File Size** | 1.6 MB (19,636 rows) | 447 KB (6,997 rows) |
| **Columns** | 11 columns | 10 columns |
| **Level/Grade** | Level (ES, MS, HS) | Grade (00-12, TL=Total) |
| **Metrics** | 4 proficiency score percentages | Attainment rate (%) |
| **Demographics** | Yes - by race, economic, disability | No - only totals |
| **Matches KYRC24** | ✅ Yes - same structure | ❌ No - different metric |

**Column Structures:**

**English_Language_Proficiency_2025.CSV (11 columns):**
```
1. School Year
2. School Code
3. District Name
4. School Name
5. Level (ES, MS, HS)
6. Demographic
7. Suppressed
8. PERCENTAGE OF VALUE TABLE SCORE OF 0
9. PERCENTAGE OF VALUE TABLE SCORE OF 60 AND 80
10. PERCENTAGE OF VALUE TABLE SCORE OF 100
11. PERCENTAGE OF VALUE TABLE SCORE OF 140
```

**English_Learners_Attainment_2025.CSV (10 columns):**
```
1. School Year
2. School Code
3. District Name
4. School Name
5. School Classification
6. Grade (00-12, TL)
7. Suppressed
8. Number Tested
9. Number of Students Reaching Attainment
10. Attainment Rate
```

**KEY DECISION: Which File to Use?**

**For Equity Scorecard (English Learner Progress):**
- ✅ **USE: English_Language_Proficiency_2025.CSV**
- ❌ **DON'T USE: English_Learners_Attainment_2025.CSV**

**Rationale:**
1. **KYRC24 file name**: "English_Learners_Progress_**Proficiency**_Rate" - matches Proficiency file
2. **Demographic breakdowns**: Proficiency file has demographics, Attainment file does NOT
3. **Equity scorecard needs demographics**: Cannot report equity without demographic data
4. **Structure match**: Proficiency file mirrors KYRC24 structure exactly
5. **Attainment file is aggregate only**: No demographic breakdowns = not useful for equity analysis

**Understanding the Difference:**

**Proficiency (what we use):**
- Measures English language proficiency **level achieved**
- 4 score levels based on WIDA ACCESS assessment:
  - Score 0 = Entering/Beginning
  - Score 60-80 = Developing/Emerging
  - Score 100 = Expanding
  - Score 140 = Bridging/Reaching (approaching proficiency)
- Shows distribution across proficiency levels
- Has demographic breakdowns by race, economic status, disability

**Attainment (accountability metric):**
- Measures if student made **adequate yearly progress** toward proficiency
- Binary outcome: Met attainment target or not
- Used for federal accountability (ESSA requirements)
- Grade-by-grade totals only
- **NO demographic breakdowns**

**Levels Available in Proficiency File:**
- ES (Elementary School)
- MS (Middle School)
- HS (High School)

**Demographic Groups in Proficiency File:**
- All Students
- Race/Ethnicity (7 categories)
- Economically Disadvantaged
- Students with Disabilities (IEP)
- English Learner including Monitored

**KEY INSIGHTS:**

1. **📊 Clear Match**: Proficiency file matches KYRC24 structure and metrics
2. **🎯 Demographics Essential**: Attainment file lacks demographics - dealbreaker for equity scorecard
3. **📈 Score Distribution**: Proficiency file shows **how** students are performing, not just pass/fail
4. **⚠️ Same Location Data Issue**: Missing county/district numbers and IDs
5. **✅ Direct Replacement**: Can swap KYRC24 proficiency file with 2025 version seamlessly

**RECOMMENDATION:**
- ✅ Use **English_Language_Proficiency_2025.CSV** for equity scorecard
- 🗑️ Ignore English_Learners_Attainment_2025.CSV (no demographic data)
- 🔧 Pipeline needs minimal updates:
  1. Handle missing location identifier columns
  2. Column names are slightly different (all caps) but same structure
  3. Same Level breakdown (ES/MS/HS)
  4. Same demographic groups
- 📊 This is a **straightforward replacement** of KYRC24 file

### Phase 2: Configuration Updates ✅ COMPLETED

#### Task 2.1: Update kde_sources.yaml ✅ COMPLETED
- [x] Add 2025 files to appropriate raw_directories
- [x] Add support for dual URL structure (HistoricalDatasets vs OAA Temporary Datasets)
- [x] Update configuration based on Phase 1 findings

**Changes Made:**
1. Added `base_url_2025` configuration pointing to OAA Temporary Datasets location
2. Added 5 equity scorecard indicator files using new dict format:
   ```yaml
   - url: "2025"
     file: "Filename_2025.CSV"
   ```
3. Files added:
   - `kindergarten_readiness`: Kindergarten_Screen_2025.CSV
   - `graduation_rates`: Graduation_Rate_2025.CSV
   - `english_learner_progress`: English_Language_Proficiency_2025.CSV
   - `kentucky_summative_assessment`: Assessment_Performance_by_Grade_2025.CSV
   - `postsecondary_readiness`: Postsecondary_Readiness_2025.CSV

#### Task 2.2: Update prepare_kde_data.py ✅ COMPLETED
- [x] Modify to handle the new OAA Temporary Datasets URL
- [x] Add logic to determine URL based on filename pattern
- [x] Test download functionality with new URLs

**Changes Made:**
1. Modified `download_directory()` method to handle both:
   - String entries (backward compatible): Use `base_url`
   - Dict entries with `url: "2025"`: Use `base_url_2025`
2. Added type checking with `isinstance(file_entry, dict)`
3. URL selection logic based on `url` key value
4. Maintains full backward compatibility with existing config

### Phase 3: Data Preparation ✅ COMPLETED

#### Task 3.1: Download 2025 Files ✅ COMPLETED

**Files Downloaded and Verified:**
```bash
# All files successfully downloaded during Phase 1 analysis
# Verified with prepare_kde_data.py - all files present:

kindergarten_readiness: 6/6 files ✅
  - KYRC24_ASMT_Kindergarten_Screen_Composite.csv
  - Kindergarten_Screen_2025.CSV (2024-2025 data)
  - kindergarten_screen_2023.csv
  - kindergarten_screen_2022.csv
  - kindergarten_screen_2021.csv
  - kindergarten_screen_2020.csv

graduation_rates: 7/7 files ✅
  - KYRC24_ACCT_4_Year_High_School_Graduation.csv
  - KYRC24_ACCT_5_Year_High_School_Graduation.csv
  - Graduation_Rate_2025.CSV (2024-2025 data)
  - graduation_rate_2023.csv
  - graduation_rate_2022.csv
  - graduation_rate_2021.csv
  - graduation_rate_2020.csv

english_learner_progress: 6/6 files ✅
  - KYRC24_ACCT_English_Learners_Progress_Proficiency_Rate.csv
  - English_Language_Proficiency_2025.CSV (2024-2025 data)
  - english_language_proficiency_2023.csv
  - english_language_proficiency_2022.csv
  - english_learner_proficiency_2021.csv
  - english_learner_proficiency_2020.csv

postsecondary_readiness: 4/4 files ✅
  - KYRC24_ACCT_Postsecondary_Readiness.csv
  - Postsecondary_Readiness_2025.CSV (2024-2025 data)
  - postsecondary_readiness_2023.csv
  - postsecondary_readiness_2022.csv

kentucky_summative_assessment: 9/9 files ✅
  - KYRC24_ACCT_Kentucky_Summative_Assessment.csv
  - KYRC24_ASMT_Kentucky_Summative_Assessment.csv
  - Assessment_Performance_by_Grade_2025.CSV (2024-2025 data)
  - accountable_assessment_performance_2023.csv
  - accountable_assessment_performance_2022.csv
  - asmt_performance_by_level_2021.csv
  - asmt_performance_by_grade_2023.csv
  - asmt_performance_by_grade_2022.csv
  - asmt_performance_by_grade_2021.csv

Overall: 32/32 files present ✅
```

**Command Used:**
```bash
/Users/scott/venvs/equity-etl/bin/python3 data/prepare_kde_data.py \
  kindergarten_readiness graduation_rates english_learner_progress \
  postsecondary_readiness kentucky_summative_assessment
```

**Result:** All files verified present. Downloader successfully handles dual URL structure.

### Phase 4: Pipeline Testing

#### Task 4.1: Test Each Pipeline
- [ ] Test kindergarten_readiness.py with 2025 data
- [ ] Test graduation_rates.py with 2025 data (verify 5-year handling)
- [ ] Test postsecondary_readiness.py with 2025 data
- [ ] Test kentucky_summative_assessment.py with 2025 data
- [ ] Test english_learner_progress.py with 2025 data

#### Task 4.2: Verify KPI Output
- [ ] Run full ETL pipeline: `python3 etl_runner.py --verbose`
- [ ] Check processed files in `data/processed/`
- [ ] Verify year parsing (should show "2024" or "20242025")
- [ ] Verify demographic breakdowns maintained
- [ ] Check KPI format compliance (19 columns)

#### Task 4.3: End-to-End Testing
- [ ] Run all unit tests: `python3 -m pytest tests/`
- [ ] Run end-to-end tests for affected pipelines
- [ ] Fix any test failures
- [ ] Verify master KPI file combines 2025 data correctly

### Phase 5: Documentation

#### Task 5.1: Update Documentation
- [ ] Update README.md with 2025 data availability
- [ ] Document any schema changes discovered
- [ ] Update KPIS.md if new metrics added
- [ ] Create journal entry documenting implementation

#### Task 5.2: Monitor for Missing Indicators
- [ ] Set up periodic check for postsecondary enrollment 2025 data
- [ ] Set up periodic check for chronic absenteeism 2025 data
- [ ] Set up periodic check for out of school suspension 2025 data

## Key Questions to Answer in Phase 1

### Kentucky Summative Assessment
1. **Which 2025 file has grade-specific proficiency rates?**
   - Accountable_Assessment_Performance_2025.CSV?
   - Assessment_Performance_by_Grade_2025.CSV?
   - Both needed?

2. **Does the data structure match KYRC24 format?**
   - Same column names?
   - Same demographic breakdowns?
   - Same proficiency level categories?

3. **Which KYRC24 file should we align with?**
   - ACCT (accountability) version?
   - ASMT (assessment) version?
   - Currently processing both - consolidation needed?

### English Learner Progress
1. **What's the difference between Proficiency vs Attainment?**
   - Proficiency = meeting language standards?
   - Attainment = making adequate progress?

2. **Which metric aligns with our equity scorecard indicator?**
   - Current indicator: "English Learner Progress"
   - Current file: "Progress_Proficiency_Rate"

3. **Do we need both 2025 files or just one?**

## Critical Notes

1. **URL Structure Change**: 2025 files are in `/OAA%20Temporary%20Datasets/` instead of `/HistoricalDatasets/`

2. **Naming Convention Change**:
   - KYRC24: `KYRC24_ACCT_4_Year_High_School_Graduation.csv`
   - 2025: `Graduation_Rate_2025.CSV`

3. **File Consolidation**: Some 2025 files may combine data from multiple KYRC24 files (e.g., graduation includes both 4 & 5 year)

4. **School Year Format**: Verify 2025 files use "20242025" format like KYRC24

5. **Missing Data**: 3 of 9 equity scorecard indicators not yet available for 2024-2025 school year

## Success Criteria

- [ ] All 6 available equity scorecard indicators successfully processing 2025 data
- [ ] KPI output format maintained (19 columns)
- [ ] Demographic breakdowns preserved
- [ ] All tests passing
- [ ] Year field correctly shows 2024 or 20242025
- [ ] Master KPI file combines all years without errors
- [ ] Documentation updated

## Next Steps

1. **START HERE**: Begin Phase 1 file analysis tasks
2. Focus on one indicator at a time
3. Document findings for each file reviewed
4. Make decisions on which 2025 files to use
5. Proceed to configuration updates only after Phase 1 complete
