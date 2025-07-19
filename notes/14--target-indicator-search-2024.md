# Target Indicator Search - 2024 Files

## Objective
Search 2024 Kentucky Department of Education files for specific equity indicators:
1. **Postsecondary Enrollment (1 yr)**
2. **Chronic Absenteeism** 
3. **3rd-Grade Reading Proficiency**
4. **8th-Grade Math Proficiency** 
5. **9th-Grade On-Track Rate**
6. **English Learner Progress**
7. **Out-of-School Suspension Rate**

**Search Strategy**: Focus on KYRC24_ files first, then expand if needed

---

## Search Results

### ✅ FOUND - Chronic Absenteeism
**File**: `KYRC24_OVW_Chronic_Absenteeism.csv`
**Content**: Chronic absenteeism rates and counts by demographics and grade level
**Demographics**: Full demographic breakdown (race/ethnicity + special populations)
**Granularity**: District/school level, all grades + grade-specific breakdowns
**Metrics**: Rate, count of chronically absent, total enrolled 10+ days
**Status**: ✅ EXACT MATCH - Ready for ETL development

### ✅ FOUND - Postsecondary Enrollment (1 yr)
**File**: `KYRC24_ADLF_Transition_to_In_State_Postsecondary_Education.csv`
**Content**: 1-year postsecondary enrollment in Kentucky public/private colleges
**Demographics**: Full demographic breakdown
**Granularity**: District/school level
**Metrics**: Counts and percentages for public, private, and total in-state enrollment
**Status**: ✅ EXACT MATCH - This is the 1-year postsecondary metric

### ✅ FOUND - English Learner Progress  
**File**: `KYRC24_ACCT_English_Learners_Progress_Proficiency_Rate.csv`
**Content**: English learner progress measured via proficiency score distributions
**Demographics**: Race/ethnicity + economic status breakdown
**Granularity**: District/school level by education level (Elementary, Middle, High)
**Metrics**: Percentage distributions across proficiency score bands (0, 60-80, 100, 140)
**Status**: ✅ EXACT MATCH - Progress indicator for English learners

### ✅ FOUND - Out-of-School Suspension Rate
**File**: `KYRC24_OVW_Student_Suspensions.csv`
**Content**: Suspension counts by type (in-school, single out-of-school, multiple out-of-school)
**Demographics**: Full demographic breakdown
**Granularity**: District/school level
**Metrics**: Counts by suspension type and disability status
**Status**: ✅ FOUND - Contains out-of-school suspension data (can calculate rates)

### ⚠️ PARTIAL - 3rd-Grade Reading Proficiency / 8th-Grade Math Proficiency
**Files**: 
- `KYRC24_ACCT_Kentucky_Summative_Assessment.csv` (by Level: Elementary, Middle, High)
- `KYRC24_ACCT_Crosstab_Reading.csv` (detailed demographic intersections)
- `KYRC24_ACCT_Crosstab_Mathematics.csv` (detailed demographic intersections)

**Content**: Assessment proficiency by subject and level, not specific grades
**Demographics**: Full demographic breakdown
**Granularity**: District/school level by Level (Elementary covers grades 3-5, Middle covers grades 6-8)
**Metrics**: Proficiency percentages (Proficient + Distinguished)
**Status**: ⚠️ PARTIAL MATCH - Level-based rather than grade-specific
- Elementary School Reading ≈ 3rd-Grade Reading (includes grades 3-5)
- Middle School Mathematics ≈ 8th-Grade Math (includes grades 6-8)

### ❌ NOT FOUND - 9th-Grade On-Track Rate
**Search Results**: No files containing "on-track" or 9th grade specific metrics
**Potential Alternatives Searched**:
- Student retention files (grades 4-12)
- Credit accumulation data
- Promotion/advancement metrics
**Status**: ❌ NOT FOUND - May need to request this indicator specifically

---

## Summary

**✅ Exact Matches Found (4/7)**:
1. Chronic Absenteeism ✅
2. Postsecondary Enrollment (1 yr) ✅  
3. English Learner Progress ✅
4. Out-of-School Suspension Rate ✅

**⚠️ Partial Matches (2/7)**:
5. 3rd-Grade Reading Proficiency (Elementary level data available)
6. 8th-Grade Math Proficiency (Middle level data available)

**❌ Missing (1/7)**:
7. 9th-Grade On-Track Rate (not found)

**Recommendation**: Proceed with ETL development for the 6 available indicators. Request specific data for 9th-Grade On-Track Rate from Kentucky Department of Education.

---

## Historical Data Search: Chronic Absenteeism (2021-2023)

**Search Request**: Find Chronic Absenteeism data for 2021-2023

### Results:
- **2023**: ✅ `chronic_absenteeism_2023.csv` - FOUND
- **2022**: ❌ No chronic absenteeism file found
- **2021**: ❌ No chronic absenteeism file found

### 2023 File Analysis:
**File**: `chronic_absenteeism_2023.csv`
**Structure**: Wide format with grade-level columns (Preschool through Grade 14)
**Demographics**: Full demographic breakdown (race/ethnicity + special populations)
**Metrics**: 
- Chronic Absentee Count
- Enrollment Count (10+ days)
- Percent Chronically Absent
- Grade-level counts for chronically absent students

**Comparison with 2024 Data**:
- 2023: Wide format with grade-level detail
- 2024: Long format with grade-level breakdown in separate column
- Both: Same demographic categories and suppression handling

### Availability Summary:
- **2024**: KYRC24_OVW_Chronic_Absenteeism.csv ✅
- **2023**: chronic_absenteeism_2023.csv ✅  
- **2022**: Not found ❌
- **2021**: Not found ❌

**Longitudinal Analysis**: 2 years of data available (2023-2024) for chronic absenteeism trends.

---

## Historical Data Search: Postsecondary Enrollment (1 yr) (2021-2023)

**Search Request**: Find Postsecondary Enrollment (1 yr) data for 2021-2023

### Results:
- **2023**: ✅ `transition_in_state_postsecondary_education_2023.csv` - FOUND
- **2022**: ✅ `transition_in_state_postsecondary_education_2022.csv` - FOUND  
- **2021**: ✅ `transition_in_state_postsecondary_education_2021.csv` - FOUND

### File Analysis Across Years:

**Files**: 
- `transition_in_state_postsecondary_education_2021.csv` (2020-2021 school year)
- `transition_in_state_postsecondary_education_2022.csv` (2021-2022 school year)
- `transition_in_state_postsecondary_education_2023.csv` (2022-2023 school year)

**Structure**: Consistent long format across all years
**Demographics**: Full demographic breakdown (race/ethnicity + special populations)
**Metrics**: Identical across all years:
- Total in Group (graduate count)
- Public College Enrolled In State (count)
- Private College Enrolled In State (count)  
- College Enrolled In State (total count)
- Percentage Public College Enrolled In State
- Percentage Private College Enrolled In State
- Percentage College Enrolled In State

**Suppression Handling**: Consistent across years
- "*" for suppressed counts
- "**" for suppressed percentages
- "<10" notation in 2021 file for small percentages

**Comparison with 2024 Data**:
- 2021-2023: "PERCENTAGE COLLEGE ENROLLED IN STATE" (all caps)
- 2024: "Percentage College Enrolled In State Table" (different column name)
- All years: Same core metrics and demographic categories

### Availability Summary:
- **2024**: KYRC24_ADLF_Transition_to_In_State_Postsecondary_Education.csv ✅
- **2023**: transition_in_state_postsecondary_education_2023.csv ✅  
- **2022**: transition_in_state_postsecondary_education_2022.csv ✅
- **2021**: transition_in_state_postsecondary_education_2021.csv ✅

**Longitudinal Analysis**: **4 years of data available (2021-2024)** for postsecondary enrollment trends - excellent historical coverage!

---

## Historical Data Search: English Learner Progress (2021-2023)

**Search Request**: Find English Learner Progress data for 2021-2023

### Results:
- **2023**: ✅ `english_language_proficiency_2023.csv` - FOUND
- **2022**: ✅ `english_language_proficiency_2022.csv` - FOUND  
- **2021**: ❌ `english_learner_proficiency_2021.csv` - EMPTY FILE

### File Analysis:

**Primary Files**: 
- `english_language_proficiency_2023.csv` (2022-2023 school year)
- `english_language_proficiency_2022.csv` (2021-2022 school year)

**Structure**: Identical to 2024 format - long format by demographic and level
**Demographics**: Full demographic breakdown (race/ethnicity + special populations)
**Metrics**: Identical across years:
- Percentage of Value Table Score of 0 (lowest proficiency)
- Percentage of Value Table Score of 60 and 80 (intermediate)
- Percentage of Value Table Score of 100 (proficient)
- Percentage of Value Table Score of 140 (highest proficiency)

**Education Levels**: ES (Elementary School), MS (Middle School), HS (High School)

**Suppression Handling**: Consistent Y/N suppression column with empty values for suppressed percentages

**Alternative Files Found**:
- `english_learners_attainment_2021-2023.csv` (grade-level attainment rates)
- `progress_towards_state_goals_english_language_proficiency_2022-2023.csv` (longitudinal goal tracking)

**Comparison with 2024 Data**:
- 2022-2023: Column names identical to 2024
- All years: Same proficiency score bands and demographic categories
- Consistent suppression handling

### Availability Summary:
- **2024**: KYRC24_ACCT_English_Learners_Progress_Proficiency_Rate.csv ✅
- **2023**: english_language_proficiency_2023.csv ✅  
- **2022**: english_language_proficiency_2022.csv ✅
- **2021**: english_learner_proficiency_2021.csv ❌ (empty file)

**Additional Data Available**:
- English learner attainment by grade (2021-2023)
- Progress towards state goals with longitudinal tracking (2022-2023)

**Longitudinal Analysis**: **3 years of data available (2022-2024)** for English learner progress trends, plus supplementary attainment and goal-tracking data.

---

## Historical Data Search: Out-of-School Suspension Rate (2021-2023)

**Search Request**: Find Out-of-School Suspension Rate data for 2021-2023

### Results:
- **2023**: ✅ `safe_schools_discipline_2023.csv` - FOUND
- **2022**: ✅ `safe_schools_discipline_2022.csv` - FOUND  
- **2021**: ✅ `safe_schools_discipline_2021.csv` - FOUND

### File Analysis:

**Files**: 
- `safe_schools_discipline_2021.csv` (2020-2021 school year)
- `safe_schools_discipline_2022.csv` (2021-2022 school year)
- `safe_schools_discipline_2023.csv` (2022-2023 school year)

**Structure**: Identical long format across all years
**Demographics**: Full demographic breakdown (race/ethnicity + special populations)
**Key Metric**: "OUT OF SCHOOL SUSPENSION SSP3" column - exactly what we need!

**Additional Discipline Metrics Available**:
- Total Discipline Resolutions
- Expelled Receiving Services SSP1
- Expelled Not Receiving Services SSP2
- Corporal Punishment SSP5
- In-School Removal INSR
- Restraint SSP7, Seclusion SSP8
- Various IAES removal categories

**Suppression Handling**: Consistent "*" for suppressed values across all years

**Data Format**: 
- Counts provided (not rates)
- Includes "Total Events" row showing total incidents vs. "All Students" showing unique students
- Can calculate rates using enrollment data

**Comparison with 2024 Data**:
- 2021-2023: Column name "OUT OF SCHOOL SUSPENSION SSP3"
- 2024: Different structure in `KYRC24_OVW_Student_Suspensions.csv` with separate columns for single/multiple out-of-school suspensions
- All years: Same demographic categories

### Availability Summary:
- **2024**: KYRC24_OVW_Student_Suspensions.csv ✅ (different structure)
- **2023**: safe_schools_discipline_2023.csv ✅  
- **2022**: safe_schools_discipline_2022.csv ✅
- **2021**: safe_schools_discipline_2021.csv ✅

**Longitudinal Analysis**: **4 years of data available (2021-2024)** for out-of-school suspension trends. Note: 2024 format changed to separate single vs. multiple suspensions, while 2021-2023 provide total out-of-school suspension counts.

---

## Final Historical Data Summary

### Complete Longitudinal Coverage:
1. **Postsecondary Enrollment (1 yr)**: 4 years (2021-2024) ✅✅✅✅
2. **Out-of-School Suspension Rate**: 4 years (2021-2024) ✅✅✅✅
3. **English Learner Progress**: 3 years (2022-2024) ✅✅✅
4. **Chronic Absenteeism**: 2 years (2023-2024) ✅✅

### Excellent Historical Data Foundation:
- **13 total years** of historical data across the 4 available indicators
- **Consistent demographic breakdowns** across all years
- **Compatible data structures** enabling longitudinal trend analysis
- **Strong equity analysis capability** with 3-4 years of data per indicator

**ETL Development Priority**: All 4 indicators have sufficient historical data for robust longitudinal equity analysis!
