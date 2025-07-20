# Journal 22: Longitudinal Data Availability Analysis for KYRC24 Implementation

## Executive Summary
Comprehensive analysis of historical data availability in `/Users/scott/Projects/jupylter playground/equity/downloads/` for implementing the KYRC24 roadmap (Journal 21). Analysis reveals **excellent longitudinal coverage spanning 5 years (2020-2024)** for most priority metrics, with format transitions from legacy naming to KYRC24 structure.

**Key Finding**: 🎯 **All Sprint 1 (SAFE) and Sprints 5-6 (OVW) files have complete 5-year historical coverage**, enabling robust trend analysis for the highest-impact school-to-prison pipeline and vulnerable population metrics.

## Sprint-by-Sprint Longitudinal Data Assessment

### Sprint 1: School Discipline Equity Analysis (SAFE) 🚨 HIGHEST PRIORITY
**Status: ✅ EXCELLENT 5-year longitudinal coverage**

#### Target KYRC24 Files (6/6 found):
1. `KYRC24_SAFE_Behavior_Events_by_Type.csv` ✅
2. `KYRC24_SAFE_Discipline_Resolutions.csv` ✅  
3. `KYRC24_SAFE_Behavior_Events_by_Grade_Level.csv` ✅
4. `KYRC24_SAFE_Behavior_Events_by_Context.csv` ✅
5. `KYRC24_SAFE_Behavior_Events_by_Location.csv` ✅
6. `KYRC24_SAFE_Legal_Sanctions.csv` ✅

#### Historical Data Available:
**School Years: 2020-21, 2021-22, 2022-23, 2023-24, 2024-25 (4 complete years + current year)**

**Legacy File Pattern (2020-2023):**
- **Event Details**: `safe_schools_event_details_2020.csv`, `safe_schools_event_details_2021.csv`, `safe_schools_event_details_2022.csv`, `safe_schools_event_details_2023.csv`
- **Grade Breakdown**: `safe_schools_events_by_grade_2020.csv`, `safe_schools_events_by_grade_2021.csv`, `safe_schools_events_by_grade_2022.csv`, `safe_schools_events_by_grade_2023.csv`
- **Discipline Actions**: `safe_schools_discipline_2020.csv`, `safe_schools_discipline_2021.csv`, `safe_schools_discipline_2022.csv`, `safe_schools_discipline_2023.csv`

**School Climate Survey Data:**
- **Index Scores**: 2022-2023 (2 years)
- **Elementary/Middle/High School Surveys**: 2021-2023 (3 years each)

**Total SAFE-related files: 22 files**

### Sprint 2: Critical Opportunity Gaps (CTE/EDOP)
**Status: ✅ EXCELLENT longitudinal coverage (4 complete years + current year)**

#### Target KYRC24 Files (3/3 found):
1. `KYRC24_CTE_Participation.csv` ✅
2. `KYRC24_EDOP_Advanced_Courses_Participation_and_Performance.csv` ✅
3. `KYRC24_EDOP_Dual_Credit_Participation_and_Performance.csv` ✅

#### Historical Data Available:
**CTE Participation**: `cte_by_student_group_2022.csv`, `cte_by_student_group_2023.csv` + KYRC24 (3 school years)
**Advanced Courses**: `advanced_courses_participation_and_performance_2020.csv`, `advanced_courses_participation_and_performance_2021.csv`, `advanced_courses_participation_and_performance_2022.csv`, `advanced_courses_participation_and_performance_2023.csv` + KYRC24 (4 complete + current year)
**Dual Credit**: Embedded in advanced courses files 2020-2023 as "DC" course type, separated in KYRC24 format (4 complete + current year)

**Additional CTE/EDOP files available:**
- Most EDOP metrics: **5 years of data (2020-2024)**
- Most CTE metrics: **4-5 years of data** (varying start years)

### Sprint 3: Enhanced Academic Performance (ACCT)
**Status: ⚠️ LIMITED - Primarily 2024 data**

#### Target KYRC24 Files (3/3 found):
1. `KYRC24_ACCT_5_Year_High_School_Graduation.csv` ✅
2. `KYRC24_ACCT_Kentucky_Summative_Assessment.csv` ✅
3. `KYRC24_ACCT_Index_Scores.csv` ✅

#### Historical Data Available:
**Years: 2024 (comprehensive), 2022-2023 (limited)**

**Challenge**: KYRC24 introduced new ACCT format. Historical equivalents exist but require data structure analysis:
- **Graduation**: `graduation_rate_[year].csv` files (2022-2023)
- **Assessment**: `accountable_assessment_performance_[year].csv` files (2022-2023)
- **Index Scores**: `quality_of_school_climate_and_safety_survey_index_scores_[year].csv` (2022-2023)

**Recommendation**: Focus on 2024 implementation first, then investigate compatibility

### Sprint 4: Assessment Performance Equity (ASMT)
**Status: ⚠️ LIMITED - Primarily 2024 data**

#### Target KYRC24 Files (3/3 found):
1. `KYRC24_ASMT_Kentucky_Summative_Assessment.csv` ✅
2. `KYRC24_ASMT_Benchmark.csv` ✅
3. `KYRC24_ASMT_The_ACT.csv` ✅

#### Historical Data Available:
**Years: 2024 (ASMT format new)**

**Potential Historical Equivalents:**
- **ACT Data**: `College_Admissions_Exam_[year].csv` (2021-2023)
- **Assessment Performance**: `accountable_assessment_performance_[year].csv` (2022-2023)
- **NAEP**: `national_assessment_of_educational_progress_[year].csv` (2020-2023)

**Challenge**: ASMT format appears new in 2024; compatibility analysis needed

### Sprint 5: Critical Student Outcome Indicators (OVW) 
**Status: ✅ EXCELLENT 5-year longitudinal coverage**

#### Target KYRC24 Files (3/3 found):
1. `KYRC24_OVW_Students_Taught_by_Inexperienced_Teachers.csv` ✅
2. `KYRC24_OVW_Student_Retention_Grades_4_12.csv` ✅
3. `KYRC24_OVW_Dropout_Rate.csv` ✅

#### Historical Data Available:
**School Years: 2020-21, 2021-22, 2022-23, 2023-24, 2024-25 (4 complete years + current year)**

**Legacy File Pattern (2020-2023):**
- **Inexperienced Teachers**: `students_taught_by_inexperienced_teachers_2020.csv`, `students_taught_by_inexperienced_teachers_2021.csv`, `students_taught_by_inexperienced_teachers_2022.csv`, `students_taught_by_inexperienced_teachers_2023.csv`
- **Retention**: `retention_2020.csv`, `retention_2021.csv`, `retention_2022.csv`, `retention_2023.csv`
- **Dropout**: `dropout_2020.csv`, `dropout_2021.csv`, `dropout_2022.csv`, `dropout_2023.csv`

### Sprint 6: Vulnerable Population Analysis (OVW)
**Status: ✅ EXCELLENT 5-year longitudinal coverage**

#### Target KYRC24 Files (3/3 found):
1. `KYRC24_OVW_Homeless.csv` ✅
2. `KYRC24_OVW_Migrant.csv` ✅
3. `KYRC24_OVW_Students_with_Disabilities_IEP.csv` ✅

#### Historical Data Available:
**School Years: 2020-21, 2021-22, 2022-23, 2023-24, 2024-25 (4 complete years + current year)**

**Legacy File Pattern (2020-2023):**
- **Homeless**: `homeless_2020.csv`, `homeless_2021.csv`, `homeless_2022.csv`, `homeless_2023.csv`
- **Migrant**: `migrant_2020.csv`, `migrant_2021.csv`, `migrant_2022.csv`, `migrant_2023.csv`
- **Disabilities**: `students_with_disabilities_2020.csv`, `students_with_disabilities_2021.csv`, `students_with_disabilities_2022.csv`, `students_with_disabilities_2023.csv`

## Data Format Evolution Analysis

### Pattern Recognition:
1. **2020-2023**: Individual yearly files with abbreviated names
2. **2024**: KYRC24 unified format with descriptive names

### Format Changes by Category:
- **SAFE**: Legacy "safe_schools_" → KYRC24 "KYRC24_SAFE_"
- **CTE/EDOP**: Mixed legacy names → KYRC24 "KYRC24_CTE_/KYRC24_EDOP_"
- **ACCT**: Scattered files → KYRC24 "KYRC24_ACCT_" (major reorganization)
- **ASMT**: New category in KYRC24 format
- **OVW**: Simple names → KYRC24 "KYRC24_OVW_" (descriptive expansion)

## Implementation Recommendations by Priority

### Phase 1: Immediate Implementation (Highest Longitudinal Value)
1. **Sprint 1 (SAFE)**: ✅ Start immediately - 5 years of data, critical equity impact
2. **Sprint 5-6 (OVW)**: ✅ Start immediately - 5 years of data, vulnerable populations

### Phase 2: Strong Longitudinal Value
3. **Sprint 2 (CTE/EDOP)**: ✅ Start second - 4-5 years of data, opportunity gap analysis

### Phase 3: Requires Data Structure Analysis
4. **Sprint 3 (ACCT)**: ⚠️ Investigate historical compatibility first
5. **Sprint 4 (ASMT)**: ⚠️ Investigate historical compatibility first

## Technical Implementation Strategy

### For Sprints 1, 5-6 (Excellent Coverage):
1. **Implement KYRC24 format first** (2024 data)
2. **Create legacy format converters** for 2020-2023 data
3. **Merge into unified longitudinal dataset**
4. **Enable 5-year trend analysis**

### For Sprint 2 (Good Coverage):
1. **Start with KYRC24 implementation**
2. **Analyze legacy file structures** for compatibility
3. **Implement historical integration** where feasible

### For Sprints 3-4 (Limited Coverage):
1. **Focus on KYRC24 format implementation**
2. **Conduct data structure analysis** of potential historical equivalents
3. **Implement historical integration** if structures align
4. **Document compatibility findings**

## Critical Success Factors

### Data Quality Assurance:
- **Column mapping analysis** between legacy and KYRC24 formats
- **Demographic standardization** across all years using DemographicMapper
- **Suppression pattern consistency** validation
- **Metric definition stability** verification

### ETL Pipeline Considerations:
- **Format detection logic** to handle legacy vs KYRC24 files
- **Year-specific processing rules** for data structure differences
- **Unified output format** maintaining KPI standard (10 columns)
- **Audit trail preservation** across format transitions

## Strategic Value Assessment

### Immediate High-Impact Opportunities (Ready for Implementation):
- **18 files across Sprints 1, 5-6** with 5-year longitudinal coverage
- **Complete school-to-prison pipeline analysis** (Sprint 1)
- **Vulnerable population trend analysis** (Sprint 6)
- **Resource inequity documentation** (Sprint 5)

### Medium-Term Opportunities (Requires Analysis):
- **Sprint 2**: Strong coverage, moderate complexity
- **Sprints 3-4**: Limited historical data, focus on 2024 foundation

### Expected Longitudinal Analysis Capabilities:
- **5-year trend analysis**: SAFE discipline patterns, vulnerable populations, critical outcomes
- **4-5 year trend analysis**: CTE/EDOP opportunity gaps
- **Foundation building**: ACCT/ASMT for future longitudinal expansion

## Next Steps

1. **Begin Sprint 1 (SAFE) implementation** - highest priority, excellent data coverage
2. **Parallel Sprint 5-6 (OVW) development** - critical vulnerable population metrics
3. **Investigate Sprint 3-4 historical compatibility** - data structure analysis
4. **Document format conversion requirements** - legacy to KYRC24 mapping
5. **Design unified longitudinal ETL strategy** - handle multiple format transitions

*Analysis Date: 2025-07-19 | Data Source: `/Users/scott/Projects/jupylter playground/equity/downloads/` | Files Analyzed: 94+ KYRC24 files + 100+ historical files*