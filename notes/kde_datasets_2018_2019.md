# 2018-2019 Kentucky Education Datasets

Source: Extracted from HTML files in `/Users/scott/Projects/equity-etl/historical data/`

## Integration Status (December 2025)

### Successfully Integrated

These files have been integrated into existing pipelines and are producing 2019 KPI data:

| File | Pipeline | KPI Rows | Notes |
|------|----------|----------|-------|
| SCHOOL_EXPERIENCE_18-19.xlsx | teacher_experience | 1,580 | Average years teaching experience |
| GIFTED_AND_TALENTED_18-19.xlsx | gifted_talented | 164,485 | G&T participation by grade/demographic |
| KSCREEN_18-19.xlsx | kindergarten_readiness | 200,966 | Kindergarten readiness screening |
| FINANCE_18-19.xlsx | financial_summary | 1,892 | District financial/staffing metrics |
| GRADUATION_RATE_18-19.xlsx | graduation_rates | (previously integrated) | 4-year graduation rates |
| CHRONIC_ABSENTEEISM_18-19.xlsx | chronic_absenteeism | (previously integrated) | Chronic absence rates |
| STUDENT_PRIMARY_ENROLLMENT_18-19.xlsx | student_enrollment | (previously integrated) | Primary enrollment |
| STUDENT_SECONDARY_ENROLLMENT_18-19.xlsx | secondary_enrollment | (previously integrated) | Secondary enrollment |
| DISTRICT_SCHOOL_LIST_18-19.xlsx | district_school_list | (previously integrated) | School/district info |
| SPENDING_PER_STUDENT_18-19.xlsx | spending_per_student | (previously integrated) | Per-pupil spending |
| TEACHER_TURNOVER_18-19.xlsx | teacher_turnover | (previously integrated) | Teacher turnover rates |
| STUDENT_TEACHER_RATIO_18-19.xlsx | student_teacher_ratio | (previously integrated) | Student-teacher ratios |
| ASSESSMENT_PROFICIENCY_GRADE_18-19.xlsx | kentucky_summative_assessment | (previously integrated) | Assessment proficiency |
| TRANSITION_READINESS_ACCOUNTABILITY_18-19.xlsx | postsecondary_readiness | (previously integrated) | Postsecondary readiness |

### Not Yet Integrated - Potential Matches

These files could potentially be integrated with modifications:

| File | Potential Pipeline | Reason Not Integrated |
|------|-------------------|----------------------|
| TEACHER_QUALIFICATIONS_18-19.xlsx | teacher_certification | Different schema format |
| NEW_TEACHER_COUNT_18-19.xlsx | novice_teachers | Different schema format |
| TELL_EQUITY_18-19.xlsx | teacher_working_conditions | Survey format different |
| CTE_TRANSITION_READINESS_18-19.xlsx | career_readiness | Different structure |
| NATIONAL_BOARD_CERTIFICATION_18-19.xlsx | teacher_certification | Supplemental data |

### No Matching Pipeline

These files have no corresponding pipeline and would need new pipelines:

| File | Content | Notes |
|------|---------|-------|
| DROPOUT_18-19.xlsx | Dropout rates | Could create dropout_rate pipeline |
| RETENTION_18-19.xlsx | Student retention | Could create student_retention pipeline |
| GAP_18-19.xlsx | Achievement gaps | Complex multi-metric data |
| SAAR_ATTENDENCE_RATE_18-19.xlsx | Daily attendance rates | Different from chronic absenteeism |
| CTE_OPPORTUNITIES_18-19.xlsx | CTE course offerings | Different structure than participation |
| FULLTIME_EQUIVALENT_TEACHER_18-19.xlsx | FTE teacher counts | District-level only |
| GROWTH_18-19.xlsx | Student growth | Growth calculations |

### Different Data Structure

These files contain data that doesn't match our KPI format:

| File | Content | Notes |
|------|---------|-------|
| ENGLISH_LEARNERS_18-19.xlsx | EL demographics by language | Not proficiency progress scores |
| TRANSITION_TO_ADULT_LIFE_18-19.xlsx | Broad adult life outcomes | Not in-state postsecondary |
| SAFE_SCHOOLS_18-19.xlsx | Behavior events wide format | Incompatible with current pipeline |
| ACCOUNTABILITY_PROFICIENCY_LEVEL_18-19.xlsx | Achievement levels | Different aggregation |

---

## Full Dataset List

### Accountability Proficiency

- [ACCOUNTABILITY_PROFICIENCY_GRADE_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/ACCOUNTABILITY_PROFICIENCY_GRADE_18-19.xlsx)
- [ACCOUNTABILITY_PROFICIENCY_LEVEL_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/ACCOUNTABILITY_PROFICIENCY_LEVEL_18-19.xlsx)
- [ASSESSMENT_PROFICIENCY_GRADE_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/ASSESSMENT_PROFICIENCY_GRADE_18-19.xlsx)
- [ASSESSMENT_PROFICIENCY_LEVEL_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/ASSESSMENT_PROFICIENCY_LEVEL_18-19.xlsx)

### Advanced Coursework

- [ADVANCED_COURSES_EXAMS_BY_COURSE_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/ADVANCED_COURSES_EXAMS_BY_COURSE_18-19.xlsx)
- [ADVANCED_COURSES_EXAMS_BY_STUDENT_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/ADVANCED_COURSES_EXAMS_BY_STUDENT_18-19.xlsx)

### Assessments Proficiency

- [GAP_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/GAP_18-19.xlsx)

### Attendance

- [CHRONIC_ABSENTEEISM_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/CHRONIC_ABSENTEEISM_18-19.xlsx)
- [SAAR_ATTENDENCE_RATE_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/SAAR_ATTENDENCE_RATE_18-19.xlsx)
- [SAAR_DAILY_ATTENDANCE_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/SAAR_DAILY_ATTENDANCE_18-19.xlsx)

### Career/Technical Ed. Programs

- [CAREER_PATHWAYS_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/CAREER_PATHWAYS_18-19.xlsx)
- [CAREER_PATHWAYS_UNDUP_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/CAREER_PATHWAYS_UNDUP_18-19.xlsx)

### Certification and Qualifications

- [EMERGENCY_AND_PROVISIONAL_CERTIFICATIONS_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/EMERGENCY_AND_PROVISIONAL_CERTIFICATIONS_18-19.xlsx)
- [NATIONAL_BOARD_CERTIFICATION_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/NATIONAL_BOARD_CERTIFICATION_18-19.xlsx)
- [TEACHER_QUALIFICATIONS_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/TEACHER_QUALIFICATIONS_18-19.xlsx)

### College Admissions Exam

- [COLLEGE_ADMISSIONS_EXAM_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/COLLEGE_ADMISSIONS_EXAM_18-19.xlsx)

### Demographics

- [TEACHER_GENDER_COUNT_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/TEACHER_GENDER_COUNT_18-19.xlsx)
- [TEACHER_RACE_COUNT_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/TEACHER_RACE_COUNT_18-19.xlsx)

### District Finance

- [FINANCE_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/FINANCE_18-19.xlsx)

### Dropout Rate by School/District

- [DROPOUT_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/DROPOUT_18-19.xlsx)

### Early Postsecondary Opportunities

- [CTE_OPPORTUNITIES_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/CTE_OPPORTUNITIES_18-19.xlsx)
- [CTE_TRANSITION_READINESS_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/CTE_TRANSITION_READINESS_18-19.xlsx)

### End-of-Year Enrollment

- [STUDENT_DEMOGRAPHIC_CONTENT_LEVEL_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/STUDENT_DEMOGRAPHIC_CONTENT_LEVEL_18-19.xlsx)
- [STUDENT_DEMOGRAPHIC_RACE_GENDER_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/STUDENT_DEMOGRAPHIC_RACE_GENDER_18-19.xlsx)

### Enrollment - Secondary

- [STUDENT_SECONDARY_ENROLLMENT_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/STUDENT_SECONDARY_ENROLLMENT_18-19.xlsx)

### Enrollment - Student Enrollments (All)

- [STUDENT_PRIMARY_ENROLLMENT_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/STUDENT_PRIMARY_ENROLLMENT_18-19.xlsx)

### Experience

- [SCHOOL_EXPERIENCE_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/SCHOOL_EXPERIENCE_18-19.xlsx)

### Faculty and, Staff

- [DISPROPORTIONALITY_MEASURES_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/DISPROPORTIONALITY_MEASURES_18-19.xlsx)
- [FULLTIME_EQUIVALENT_TEACHER_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/FULLTIME_EQUIVALENT_TEACHER_18-19.xlsx)
- [NEW_TEACHER_COUNT_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/NEW_TEACHER_COUNT_18-19.xlsx)
- [TEACHER_TURNOVER_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/TEACHER_TURNOVER_18-19.xlsx)
- [TELL_EQUITY_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/TELL_EQUITY_18-19.xlsx)

### General Education Coursework

- [SCHOOL_COURSES_CERTIFICATION_DATA_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/SCHOOL_COURSES_CERTIFICATION_DATA_18-19.xlsx)
- [SCHOOL_COURSES_SUMMARY_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/SCHOOL_COURSES_SUMMARY_18-19.xlsx)

### Graduation Rate

- [GRADUATION_RATE_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/GRADUATION_RATE_18-19.xlsx)

### Growth

- [GROWTH_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/GROWTH_18-19.xlsx)

### Ky. Board of Education Members

- [KBE_BOARD_MEMBERS_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/KBE_BOARD_MEMBERS_18-19.xlsx)

### Organization Information

- [SCHOOL_BOARD_MEMBERS_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/SCHOOL_BOARD_MEMBERS_18-19.xlsx)

### Other Assessments

- [KSCREEN_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/KSCREEN_18-19.xlsx)
- [NAEP_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/NAEP_18-19.xlsx)

### Perkins

- [SRC_PERKINS_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/SRC_PERKINS_18-19.xlsx)

### Postsecondary Paths

- [TRANSITION_TO_ADULT_LIFE_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/TRANSITION_TO_ADULT_LIFE_18-19.xlsx)

### Profile

- [ACCOUNTABILITY_PROFILE_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/ACCOUNTABILITY_PROFILE_18-19.xlsx)

### Retention Counts

- [RETENTION_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/RETENTION_18-19.xlsx)

### Safety

- [SAFE_SCHOOLS_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/SAFE_SCHOOLS_18-19.xlsx)

### School Based Decision Making Council (SBDM)

- [SBDM_DATA_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/SBDM_DATA_18-19.xlsx)

### School and District Details

- [DISTRICT_SCHOOL_LIST_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/DISTRICT_SCHOOL_LIST_18-19.xlsx)

### Spending

- [SPENDING_PER_STUDENT_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/SPENDING_PER_STUDENT_18-19.xlsx)

### Student Groups

- [ENGLISH_LEARNERS_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/ENGLISH_LEARNERS_18-19.xlsx)
- [FREE_AND_REDUCED_LUNCH_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/FREE_AND_REDUCED_LUNCH_18-19.xlsx)
- [GIFTED_AND_TALENTED_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/GIFTED_AND_TALENTED_18-19.xlsx)
- [HOMELESS_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/HOMELESS_18-19.xlsx)
- [MIGRANT_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/MIGRANT_18-19.xlsx)
- [SPECIAL_EDUCATION_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/SPECIAL_EDUCATION_18-19.xlsx)

### Students-Teachers

- [STUDENT_TEACHER_RATIO_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/STUDENT_TEACHER_RATIO_18-19.xlsx)

### Summary

- [ACCOUNTABILITY_SUMMARY_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/ACCOUNTABILITY_SUMMARY_18-19.xlsx)

### Transition Readiness

- [TRANSITION_READINESS_ACCOUNTABILITY_18-19](https://www.education.ky.gov/Open-House/data/HistoricalDatasets/TRANSITION_READINESS_ACCOUNTABILITY_18-19.xlsx)
