# Kentucky Department of Education File Analysis

## Overview
This journal documents the systematic analysis of CSV files downloaded from the Kentucky Department of Education website. Each file is analyzed for structure, content focus, and demographic breakdown patterns.

**Total CSV Files**: 300+ files
**Analysis Date**: 2025-07-19

---

## File-by-File Analysis

### 1. KYRC24_OVW_Student_Membership.csv
**Content**: Student enrollment counts by grade level and demographics
**Structure**: Wide format with grade columns (Preschool through Grade 14)
**Demographics**: Vertical breakdown - each row represents a demographic group
- Gender: Female, Male
- Race/Ethnicity: African American, American Indian/Alaska Native, Asian, Hispanic/Latino, Native Hawaiian/Pacific Islander, Two or More Races, White (non-Hispanic)
**Granularity**: District and school level data
**Suppression**: None visible in sample (counts shown as numbers)
**KPI Potential**: Enrollment totals by demographics - suitable for transformation to long format
**Notes**: Wide format makes this similar to existing processed files; has hierarchical geography (state > district > school)

### 2. KYRC24_OVW_Student_Enrollment.csv
**Content**: Student enrollment counts by grade level and demographics (similar to membership but different counts)
**Structure**: Wide format with grade columns (Preschool through Grade 14)
**Demographics**: Vertical breakdown with expanded categories beyond race/ethnicity:
- Gender: Female, Male
- Race/Ethnicity: Same 7 categories as membership
- Special Populations: Economically Disadvantaged, Students with Disabilities (IEP), English Learner, Foster Care, Gifted and Talented, Homeless, Migrant, Military Dependent
**Granularity**: District and school level data
**Suppression**: None visible in sample
**KPI Potential**: Multiple enrollment-related metrics by demographics
**Notes**: More comprehensive demographic breakdown than membership file; includes special populations critical for equity analysis

### 3. KYRC24_OVW_Secondary_Enrollment.csv
**Content**: Student enrollment counts focused on secondary grades (6-12, 14)
**Structure**: Wide format with secondary grade columns only
**Demographics**: Vertical breakdown similar to Student_Enrollment (race/ethnicity + special populations)
**Granularity**: District and school level data
**Suppression**: None visible in sample
**KPI Potential**: Secondary-specific enrollment metrics
**Notes**: Subset of enrollment data focused on secondary education

### 4. KYRC24_OVW_Attendance_Rate.csv
**Content**: Attendance rates by school/district
**Structure**: Long format with single attendance rate column
**Demographics**: None - aggregated data only
**Granularity**: District and individual school level
**Suppression**: Some missing values (empty cells)
**KPI Potential**: Single metric (attendance rate) - already in long format
**Notes**: Simple rate metric without demographic breakdown

### 5. KYRC24_OVW_Chronic_Absenteeism.csv
**Content**: Chronic absenteeism rates and counts by demographics
**Structure**: Long format with rate calculation components
**Demographics**: Vertical breakdown with comprehensive categories (race/ethnicity + special populations)
**Granularity**: District and school level data, with grade-level detail
**Suppression**: "Suppressed" column indicates data suppression (Yes/No)
**KPI Potential**: Rate, count, and denominator metrics - excellent for equity analysis
**Notes**: Includes both rate and underlying counts; has explicit suppression handling

### 6. KYRC24_ACCT_Kentucky_Summative_Assessment.csv
**Content**: Assessment performance levels and content index scores by subject/level/demographics
**Structure**: Long format with percentage distributions (Novice, Apprentice, Proficient, Distinguished)
**Demographics**: Vertical breakdown with comprehensive categories including "Non Economically Disadvantaged"
**Granularity**: District and school level, by subject (Mathematics, etc.) and level (Elementary, etc.)
**Suppression**: "Suppressed" column with Y/N; asterisks (*) for suppressed values
**KPI Potential**: Multiple performance metrics - proficiency rates, content index scores
**Notes**: Rich assessment data with performance level distributions; critical for academic equity analysis

### 7. KYRC24_ACCT_4_Year_High_School_Graduation.csv
**Content**: 4-year cohort graduation rates by demographics
**Structure**: Long format with single rate column
**Demographics**: Vertical breakdown with comprehensive categories
**Granularity**: District and school level
**Suppression**: "Suppressed" column with Y/N; asterisks (*) for suppressed rates
**KPI Potential**: Graduation rate metric - key equity indicator
**Notes**: Essential equity metric; similar to processed graduation_rates.py module

### 8. KYRC24_CTE_Career_Pathways.csv
**Content**: Career and Technical Education pathway enrollment and completion data
**Structure**: Long format with multiple count columns (Available, Active Enrollment, Concentrators, Completers)
**Demographics**: None - program area breakdowns instead of demographic breakdowns
**Granularity**: District and school level by program area and pathway
**Suppression**: Asterisks (*) for suppressed values
**KPI Potential**: CTE participation and completion metrics
**Notes**: Focus on program types rather than demographics; different equity lens

### 9. graduation_rate_2023.csv (historical file format)
**Content**: 4-year and 5-year graduation rates by demographics (older format)
**Structure**: Long format with separate 4-year and 5-year rate columns
**Demographics**: Vertical breakdown similar to newer files
**Granularity**: District level focus
**Suppression**: "SUPPRESSED 4 YEAR" and "SUPPRESSED 5 YEAR" columns; empty values for suppressed data
**KPI Potential**: Graduation rates with longitudinal comparison capability
**Notes**: Older file format structure; shows evolution of data formatting over time

### 10. KYRC24_FT_Financial_Summary.csv
**Content**: District financial data including fund balances and staffing counts
**Structure**: Long format with financial and staffing metrics
**Demographics**: None - district-level financial data only
**Granularity**: District level only
**Suppression**: None visible
**KPI Potential**: Financial metrics and staffing ratios
**Notes**: No demographic breakdown; focused on operational/financial data

### 11. KYRC24_ASMT_Kindergarten_Screen_Composite.csv
**Content**: Kindergarten readiness screening results by readiness levels
**Structure**: Long format with readiness percentage distributions
**Demographics**: Vertical breakdown with comprehensive categories
**Granularity**: District and school level by prior setting
**Suppression**: "Suppressed" column with Y/N
**KPI Potential**: Kindergarten readiness metrics - early equity indicator
**Notes**: Similar to existing kindergarten_readiness.py module; early childhood equity focus

---

## Patterns and Summary (Sampled 11 of 300+ files)

### File Naming Conventions
1. **KYRC24_**: 2024 Kentucky Report Card files (newest format)
2. **[topic_name]_[year].csv**: Historical files (2020-2023)
3. **Crosstab_**: Assessment cross-tabulation files
4. **College_**: College admission exam files

### Content Categories Identified
1. **OVW (Overview)**: Demographics, enrollment, attendance, discipline
2. **ACCT (Accountability)**: Assessment scores, graduation rates, school improvement
3. **ASMT (Assessment)**: Various assessment types and results
4. **CTE (Career Technical Education)**: Career pathway data
5. **EDOP (Educational Opportunities)**: Advanced courses, gifted programs, arts
6. **FT (Financial/Fiscal)**: Budget, spending, staffing financial data
7. **SAFE**: School safety and discipline data
8. **ADLF**: Adult/graduate outcomes data
9. **CRDC**: Civil Rights Data Collection

### Demographic Patterns
**Standard Demographics** (consistent across most files):
- Gender: Female, Male
- Race/Ethnicity: African American, American Indian/Alaska Native, Asian, Hispanic/Latino, Native Hawaiian/Pacific Islander, Two or More Races, White (non-Hispanic)
- Economic: Economically Disadvantaged, Non Economically Disadvantaged
- Special Populations: Students with Disabilities (IEP), English Learner, Foster Care, Gifted and Talented, Homeless, Migrant, Military Dependent

### Data Structure Patterns
1. **Wide Format**: Enrollment files with grade-level columns
2. **Long Format**: Most assessment and rate files
3. **Mixed Format**: Some files combine multiple metrics

### Suppression Handling
1. **Consistent Suppression Indicators**: "Suppressed" column (Y/N) or dedicated suppression columns
2. **Suppressed Value Markers**: Asterisks (*), empty cells, or dedicated markers
3. **Suppression Inclusion**: Suppressed records retained (not filtered out)

---

## Longitudinal File Groupings for Time Series Analysis

### 1. Graduation Rates (2020-2024)
- `graduation_rate_2020.csv` through `graduation_rate_2023.csv`
- `KYRC24_ACCT_4_Year_High_School_Graduation.csv`
- `KYRC24_ACCT_5_Year_High_School_Graduation.csv`
- `Crosstab_Cohort_4_Year_2022.csv`, `Crosstab_Cohort_4_Year_2023.csv`
- `Crosstab_Cohort_5_Year_2022.csv`, `Crosstab_Cohort_5_Year_2023.csv`

### 2. Student Enrollment/Membership (2020-2024)
- `student_membership_2020.csv` through `student_membership_2023.csv`
- `secondary_enrollment_2020.csv` through `secondary_enrollment_2023.csv`
- `primary_enrollment_2020.csv` through `primary_enrollment_2023.csv`
- `KYRC24_OVW_Student_Membership.csv`
- `KYRC24_OVW_Student_Enrollment.csv`
- `KYRC24_OVW_Secondary_Enrollment.csv`

### 3. Assessment Performance (2021-2024)
- `asmt_performance_by_grade_2021.csv` through `asmt_performance_by_grade_2023.csv`
- `asmt_performance_by_level_2021.csv`
- `accountable_assessment_performance_2022.csv`, `accountable_assessment_performance_2023.csv`
- `KYRC24_ACCT_Kentucky_Summative_Assessment.csv`
- `KYRC24_ASMT_Kentucky_Summative_Assessment.csv`
- Crosstab files by subject (Reading, Mathematics, Science, etc.) for 2022-2024

### 4. Kindergarten Readiness (2020-2024)
- `kindergarten_screen_2020.csv` through `kindergarten_screen_2023.csv`
- `KYRC24_ASMT_Kindergarten_Screen_Composite.csv`
- `KYRC24_ASMT_Kindergarten_Screen_Developmental_Domains.csv`

### 5. Attendance and Absenteeism (2020-2024)
- `attendance_rate_2023.csv`
- `chronic_absenteeism_2023.csv`
- `KYRC24_OVW_Attendance_Rate.csv`
- `KYRC24_OVW_Chronic_Absenteeism.csv`
- `KYRC24_CRDC_Chronic_Absenteeism.csv`

### 6. English Learners (2020-2024)
- `english_learners_2020.csv` through `english_learners_2023.csv`
- `english_learner_proficiency_2020.csv`, `english_learner_proficiency_2021.csv`
- `english_learners_attainment_2020.csv` through `english_learners_attainment_2023.csv`
- `english_language_proficiency_2022.csv`, `english_language_proficiency_2023.csv`
- `KYRC24_ACCT_English_Learners_Progress_Proficiency_Rate.csv`

### 7. Special Populations (2020-2024)
- `students_with_disabilities_2020.csv` through `students_with_disabilities_2023.csv`
- `economically_disadvantaged_2020.csv` through `economically_disadvantaged_2023.csv`
- `homeless_2020.csv` through `homeless_2023.csv`
- `migrant_2020.csv` through `migrant_2023.csv`
- `gifted_and_talented_2020.csv` through `gifted_and_talented_2023.csv`

### 8. Teacher Quality and Staffing (2020-2024)
- `educator_qualifications_2020.csv` through `educator_qualifications_2023.csv`
- `teacher_certifications_2020.csv` through `teacher_certifications_2023.csv`
- `teacher_turnover_2020.csv` through `teacher_turnover_2023.csv`
- `inexperienced_teachers_2020.csv` through `inexperienced_teachers_2023.csv`
- `KYRC24_OVW_Teachers_Full_Time_Equivalent_FTE.csv`
- `KYRC24_OVW_Teacher_Turnover.csv`

### 9. Financial Data (2020-2024)
- `financial_summary_2020.csv` through `financial_summary_2023.csv`
- `spending_per_student_2020.csv` through `spending_per_student_2023.csv`
- `state_funding_2020.csv` through `state_funding_2023.csv`
- `KYRC24_FT_Financial_Summary.csv`
- `KYRC24_FT_Spending_per_Student.csv`

### 10. Career and Technical Education (2020-2024)
- `career_pathways_2020.csv` through `career_pathways_2023.csv`
- `career_readiness_indicators_2022.csv`, `career_readiness_indicators_2023.csv`
- `cte_opportunities_2021.csv` through `cte_opportunities_2023.csv`
- `KYRC24_CTE_Career_Pathways.csv`
- `KYRC24_CTE_Career_Readiness_Indicators.csv`

### 11. Educational Opportunities (2020-2024)
- `advanced_courses_offered_2020.csv` through `advanced_courses_offered_2023.csv`
- `advanced_courses_participation_and_performance_2020.csv` through `advanced_courses_participation_and_performance_2023.csv`
- `visual_and_performing_arts_2020.csv` through `visual_and_performing_arts_2023.csv`
- `KYRC24_EDOP_Advanced_Courses_Offered.csv`
- `KYRC24_EDOP_Advanced_Courses_Participation_and_Performance.csv`

### 12. School Safety and Discipline (2020-2024)
- `safe_schools_discipline_2020.csv` through `safe_schools_discipline_2023.csv`
- `safe_schools_events_by_grade_2020.csv` through `safe_schools_events_by_grade_2023.csv`
- `precautionary_measures_2020.csv` through `precautionary_measures_2023.csv`
- `KYRC24_SAFE_Discipline_Resolutions.csv`
- `KYRC24_SAFE_Behavior_Events_by_Grade_Level.csv`

---

## Recommendations for ETL Development

### High Priority for Equity Analysis
1. **Graduation Rates**: Already processed - ensure compatibility with new 2024 format
2. **Kindergarten Readiness**: Already processed - validate against 2024 data
3. **Assessment Performance**: Rich demographic data, critical for academic equity
4. **Chronic Absenteeism**: Key early warning indicator with good demographic breakdown
5. **English Learner Progress**: Essential for multilingual student equity

### Medium Priority
1. **Teacher Quality Metrics**: Teacher experience/qualifications by student demographics
2. **Advanced Course Access**: Educational opportunity equity indicators
3. **Special Population Enrollment**: Comprehensive demographic tracking

### Lower Priority (Operational Focus)
1. **Financial Data**: Limited demographic breakdown
2. **CTE Programs**: Program-focused rather than demographic-focused
3. **School Safety**: Important but less directly related to academic equity

### Technical Considerations
1. **Format Evolution**: 2024 files (KYRC24_) have cleaner, more consistent formatting
2. **Suppression Handling**: Consistent across files but varies in implementation
3. **Demographic Standardization**: High compatibility with existing DemographicMapper
4. **Data Volume**: 300+ files require systematic processing approach

---
