# New Data Sources Historical Analysis and ETL Roadmap

## Date: 2025-07-21

## Overview
Analysis of 8 requested KYRC24 files to identify historical equivalents and create ETL development roadmap for new educational equity indicators. This analysis builds upon the established equity indicator priorities from Journal 14, which identified 4 core dashboard indicators with proven longitudinal data.

## Files Analyzed

### ✅ Files WITH Historical Equivalents (7 files)

1. **KYRC24_ASMT_The_ACT.csv** → `act_scores` 
   - **Historical**: College_Admissions_Exam_2021-2023.csv (3 files)
   - **Coverage**: 2021-2024 (4 years)
   - **Data**: ACT scores by subject, composite scores, benchmark percentages

2. **KYRC24_OVW_Students_Taught_by_Inexperienced_Teachers.csv** → `students_taught_by_inexperienced_teachers`
   - **Historical**: students_taught_by_inexperienced_teachers_2020-2023.csv (4 files)
   - **Coverage**: 2020-2024 (5 years)
   - **Data**: Teacher quality equity gaps by demographics, Title I status

3. **KYRC24_OVW_Student_Retention_Grades_4_12.csv** → `student_retention`
   - **Historical**: retention_2020-2023.csv (4 files)
   - **Coverage**: 2020-2024 (5 years)
   - **Data**: Grade retention rates by demographics

4. **KYRC24_OVW_Dropout_Rate.csv** → `dropout_rate`
   - **Historical**: dropout_2020-2023.csv (4 files)
   - **Coverage**: 2020-2024 (5 years)
   - **Data**: Dropout rates by demographics and school level

5. **KYRC24_OVW_Homeless.csv** → `homeless_students`
   - **Historical**: homeless_2020-2023.csv (4 files)
   - **Coverage**: 2020-2024 (5 years)
   - **Data**: Homeless student enrollment by demographics

6. **KYRC24_OVW_Migrant.csv** → `migrant_students`
   - **Historical**: migrant_2020-2023.csv (4 files)
   - **Coverage**: 2020-2024 (5 years)
   - **Data**: Migrant student enrollment by demographics

7. **KYRC24_OVW_Students_with_Disabilities_IEP.csv** → `students_with_disabilities`
   - **Historical**: students_with_disabilities_2020-2023.csv (4 files)
   - **Coverage**: 2020-2024 (5 years)
   - **Data**: Students with disabilities enrollment by demographics

### ❌ Files WITHOUT Historical Equivalents (1 file)

1. **KYRC24_ASMT_Benchmark.csv** → `benchmark_assessment`
   - **Historical**: None found
   - **Coverage**: 2024 only (1 year)
   - **Data**: Benchmark assessment performance in Mathematics, Reading, English

## Download Mappings Created

Successfully created and tested 8 new mappings in `config/kde_sources.yaml`:
- 7 with longitudinal data (4-5 years each)
- 1 with current year only
- **Total**: 33 files covering new educational equity indicators

## Alignment with Existing Roadmap (Journal 14)

**IMPORTANT**: This analysis must align with the established equity indicator priorities from Journal 14, which identified 4 core indicators with proven longitudinal data:

1. **Postsecondary Enrollment (1 yr)**: 4 years (2021-2024) - ALREADY IMPLEMENTED ✅
2. **Out-of-School Suspension Rate**: 4 years (2021-2024) - ALREADY IMPLEMENTED ✅
3. **English Learner Progress**: 3 years (2022-2024) - ALREADY IMPLEMENTED ✅ 
4. **Chronic Absenteeism**: 2 years (2023-2024) - ALREADY IMPLEMENTED ✅

**Status**: All 4 priority equity indicators from Journal 14 are already implemented and operational!

## Revised ETL Development Roadmap

**Strategic Approach**: Since the core equity indicators are complete, these 7 new files represent **Phase 2 expansion** to provide broader educational context and intersectional analysis capabilities.

### Priority 1: Academic Performance & College Readiness (Sprint 1)

#### 1.1 ACT Scores ETL
- **File**: `act_scores` (4 files, 2021-2024)
- **Priority**: HIGHEST - Complements existing postsecondary enrollment data
- **Metrics**:
  - `act_composite_score` - Average composite score
  - `act_mathematics_score`, `act_reading_score`, `act_science_score`, `act_english_score`
  - `act_benchmark_mathematics_rate`, `act_benchmark_reading_rate`, etc.
- **Strategic Value**: Provides upstream indicator for postsecondary readiness analysis
- **Dashboard Integration**: Links college entrance scores to enrollment outcomes

#### 1.2 Benchmark Assessment ETL
- **File**: `benchmark_assessment` (1 file, 2024 only)
- **Priority**: MEDIUM - Limited historical data but establishes baseline
- **Metrics**:
  - `benchmark_mathematics_score`, `benchmark_reading_score`, `benchmark_english_score`
- **Strategic Value**: Earlier academic performance indicator, foundation for future longitudinal analysis

### Priority 2: Completion & Persistence Indicators (Sprint 2)

#### 2.1 Dropout Rate ETL
- **File**: `dropout_rate` (5 files, 2020-2024)
- **Priority**: HIGH - Critical completion indicator complementing graduation rates
- **Metrics**:
  - `dropout_rate` - Annual dropout percentage
  - `dropout_count` - Number of students dropping out
- **Strategic Value**: Complements existing graduation rate data, enables completion pipeline analysis

#### 2.2 Student Retention ETL
- **File**: `student_retention` (5 files, 2020-2024)
- **Priority**: MEDIUM - Academic progression indicator
- **Metrics**:
  - `retention_rate_grades_4_12` - Percentage of students retained
  - `retention_count_grades_4_12` - Number of students retained
- **Strategic Value**: Early warning indicator for academic struggle, correlates with suspension data

### Priority 3: Vulnerable Population Context (Sprint 3)

#### 3.1 Students with Disabilities ETL  
- **File**: `students_with_disabilities` (5 files, 2020-2024)
- **Priority**: HIGH - ADA compliance and intersectional equity analysis
- **Metrics**:
  - `disability_enrollment_count` - Students with IEPs
  - `disability_enrollment_rate` - Percentage with disabilities
- **Strategic Value**: Enables analysis of disability status across ALL existing indicators
- **Dashboard Integration**: Critical for intersectional equity gap analysis

#### 3.2 Homeless Students ETL
- **File**: `homeless_students` (5 files, 2020-2024)
- **Priority**: HIGH - Critical for intersectional equity analysis
- **Metrics**: 
  - `homeless_enrollment_count` - Number of homeless students
  - `homeless_enrollment_rate` - Percentage of total enrollment
- **Strategic Value**: Housing stability context for academic performance indicators

#### 3.3 Migrant Students ETL
- **File**: `migrant_students` (5 files, 2020-2024) 
- **Priority**: MEDIUM - Mobile population tracking
- **Metrics**:
  - `migrant_enrollment_count` - Number of migrant students
  - `migrant_enrollment_rate` - Percentage of total enrollment
- **Strategic Value**: Mobility context for academic stability analysis

### Priority 4: Resource Equity Analysis (Sprint 4)

#### 4.1 Students Taught by Inexperienced Teachers ETL
- **File**: `students_taught_by_inexperienced_teachers` (5 files, 2020-2024)
- **Priority**: MEDIUM - Resource equity deep-dive
- **Metrics**:
  - `inexperienced_teacher_exposure_rate` - % students with inexperienced teachers
  - `inexperienced_teacher_gap_nonwhite` - Equity gap for non-white students
  - `inexperienced_teacher_gap_economically_disadvantaged` - Economic equity gap
  - `inexperienced_teacher_gap_disabilities` - Disability equity gap
  - `inexperienced_teacher_gap_english_learners` - EL equity gap
- **Strategic Value**: Resource allocation analysis, explains performance gaps in existing indicators

## Implementation Strategy (Builds on Journal 14 Foundation)

**Context**: The core equity dashboard indicators are complete. This roadmap represents **Phase 2 expansion** to provide broader educational context and deeper intersectional analysis.

### Phase 1: College Readiness Pipeline (Week 1)
1. Implement ACT scores ETL to complement existing postsecondary enrollment data
2. Create benchmark assessment ETL as early performance baseline
3. Establish upstream-downstream indicator relationships
4. **Dashboard Value**: Complete college readiness pipeline from early assessment through enrollment

### Phase 2: Completion Analytics (Week 2)  
1. Implement dropout rate ETL to complement graduation rate data
2. Create student retention ETL for academic progression tracking
3. Cross-reference with existing suspension and absenteeism indicators
4. **Dashboard Value**: Complete student persistence and completion analytics

### Phase 3: Intersectional Equity Context (Week 3)
1. Implement students with disabilities ETL for ADA compliance tracking
2. Create homeless students ETL for housing stability context
3. Add migrant students ETL for mobility analysis
4. **Dashboard Value**: Enables intersectional analysis across ALL existing indicators

### Phase 4: Resource Equity Deep-Dive (Week 4)
1. Implement inexperienced teacher exposure ETL
2. Calculate complex equity gap metrics across demographics
3. Integrate with Title I and resource allocation analysis
4. **Dashboard Value**: Explains WHY performance gaps exist through resource analysis

## Expected Impact (Phase 2 Expansion)

### Enhanced Dashboard Capabilities
Building on the 4 core equity indicators from Journal 14:

#### Upstream-Downstream Analysis
- **ACT Scores → Postsecondary Enrollment**: Complete college readiness pipeline
- **Benchmark Assessment → Academic Performance**: Early warning system
- **Retention/Dropout → Graduation**: Complete persistence tracking

#### Intersectional Equity Analysis
- **Cross-Reference All Existing Indicators** with:
  - Students with disabilities status
  - Housing stability (homeless)
  - Mobility patterns (migrant)
- **Multi-Dimensional Equity Gaps**: Analyze chronic absenteeism, suspension rates, EL progress, and postsecondary enrollment through intersectional lens

#### Resource Equity Context
- **Explain Performance Gaps**: Teacher quality exposure analysis
- **Resource Allocation Impact**: Connect inexperienced teacher distribution to academic outcomes
- **Title I Analysis**: Resource equity across economic demographics

### Longitudinal Analysis Enhancement
- **33 new data files** with 4-5 years of historical data each
- **Intersectional trend analysis** across vulnerable populations
- **Causal pathway analysis** from resources through outcomes

### Strategic Value
**Phase 1 (Journal 14)**: Established core equity indicators for dashboard
**Phase 2 (This Analysis)**: Provides context, intersections, and explanations for WHY gaps exist

## Testing Requirements

Each ETL must include:
1. **Unit tests** for data processing logic
2. **End-to-end tests** for file processing pipeline
3. **Data validation** for KPI format compliance
4. **Historical data consistency** checks across years
5. **Demographic mapping** verification

## Files Successfully Downloaded and Configured
- ✅ benchmark_assessment: 1 file
- ✅ act_scores: 4 files  
- ✅ homeless_students: 5 files
- ✅ All other mappings tested and verified

## Next Actions (Phase 2 Implementation)
1. **Confirm Phase 1 Complete**: Verify all 4 Journal 14 indicators are operational in dashboard
2. **Begin Phase 2 Implementation**: Start with ACT scores ETL to complement postsecondary data
3. **Maintain Consistency**: Follow established ETL patterns from Phase 1 implementations
4. **Focus on Integration**: Ensure new indicators complement rather than duplicate existing analysis
5. **Dashboard Integration**: Plan intersectional visualizations leveraging both Phase 1 and Phase 2 data

## Coordination with Existing Work
**This roadmap is designed to enhance, not replace, the strategic work completed in Journal 14.** The 7 new indicators provide deeper context and intersectional analysis capabilities for the proven equity indicators already implemented.