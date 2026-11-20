# Teacher Quality Data Investigation

**Date**: 2024-11-24
**Purpose**: Document available KDE teacher quality datasets and plan ETL pipelines

## Overview

Investigated Kentucky Department of Education datasets related to teacher quality indicators. These datasets complement the existing `teacher_experience.py`, `student_teacher_ratio.py`, and `novice_teachers.py` pipelines.

## Available Data Sources

### 1. Teacher Turnover

**Description**: School-level teacher turnover rates and counts. No demographic breakdowns.

**Files**:
| Year | URL |
|------|-----|
| 2025 | `https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC25_OVW_Teacher_Turnover.csv` |
| 2024 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/KYRC24_OVW_Teacher_Turnover.csv` |
| 2023 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/teacher_turnover_2023.csv` |
| 2022 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/teacher_turnover_2022.csv` |
| 2021 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/teacher_turnover_2021.csv` |

**Schema (KYRC25)**:
```
School Year, County Number, County Name, District Number, District Name, School Number,
School Name, School Code, State School Id, NCES ID, CO-OP, CO-OP Code, School Type,
Teacher Count, Teacher Turnover Count, Turnover Percent
```

**Sample Data**:
```
20242025,001,ADAIR,001,Adair County,,All Schools,001000,,2100030,GRREC,902,,198,36,18.2
```

**Proposed KPIs**:
- `teacher_turnover_rate` - Percentage of teachers who left (Turnover Percent)
- `teacher_turnover_count` - Number of teachers who left
- `teacher_count` - Total teacher count (denominator)

---

### 2. Teacher Certification Data

**Description**: Emergency/provisional certification and National Board certification rates. No demographic breakdowns.

**Files**:
| Year | URL |
|------|-----|
| 2025 | `https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC25_OVW_Teacher_Certification_Data.csv` |
| 2024 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/KYRC24_OVW_Teacher_Certification_Data.csv` |
| 2023 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/teacher_certifications_2023.csv` |
| 2022 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/teacher_certifications_2022.csv` |
| 2021 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/teacher_certifications_2021.csv` |

**Schema (KYRC25)**:
```
School Year, County Number, County Name, District Number, District Name, School Number,
School Name, School Code, State School Id, NCES ID, CO-OP, CO-OP Code, School Type,
Teacher Count, Emergency Provisional Teacher Count, Percent Emergency Provisional Teachers,
National Board Certified Count, Percent National Board Certified Teacher
```

**Sample Data**:
```
20242025,001,ADAIR,001,Adair County,,All Schools,001000,,2100030,GRREC,902,,184,6,3.3,*,*
```

**Historical Column Variations**:
- 2021: `EMERGENCY/PROVISIONAL TEACHER COUNT`, `PERCENT EMERGENCY/PROVISIONAL TEACHERS`

**Proposed KPIs**:
- `emergency_provisional_teacher_rate` - Percent on emergency/provisional certification
- `emergency_provisional_teacher_count` - Count on emergency/provisional
- `national_board_certified_rate` - Percent with National Board certification
- `national_board_certified_count` - Count with National Board certification

---

### 3. Educator Qualifications

**Description**: Distribution of educator degree/rank levels. Data is pivoted by qualification type.

**Files**:
| Year | URL |
|------|-----|
| 2025 | `https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC25_OVW_Educator_Qualifications.csv` |
| 2024 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/KYRC24_OVW_Educator_Qualifications.csv` |
| 2023 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/educator_qualifications_2023.csv` |
| 2022 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/educator_qualifications_2022.csv` |
| 2021 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/educator_qualifications_2021.csv` |

**Schema (KYRC25)**:
```
School Year, County Number, County Name, District Number, District Name, School Number,
School Name, School Code, State School Id, NCES ID, CO-OP, CO-OP Code, School Type,
Educator Qualification, Percent Of Qualification
```

**Qualification Types** (6 per school):
- Associate Degree
- Bachelors
- Masters
- Rank I
- Specialist
- Doctorate

**Sample Data**:
```
20242025,,,999,All Districts,,All Schools,999000,,,,,,Associate Degree,0.8
20242025,,,999,All Districts,,All Schools,999000,,,,,,Bachelors,19.6
20242025,,,999,All Districts,,All Schools,999000,,,,,,Masters,27.2
```

**Historical Column Variations**:
- 2021: Values include `%` suffix (e.g., `18.80%`)

**Proposed KPIs**:
- `educator_qualification_rate_associate_degree`
- `educator_qualification_rate_bachelors`
- `educator_qualification_rate_masters`
- `educator_qualification_rate_rank_1`
- `educator_qualification_rate_specialist`
- `educator_qualification_rate_doctorate`

---

### 4. Students Taught by Ineffective Teachers

**Description**: Equity metrics showing percentage of students taught by ineffective teachers, broken down by Title I status and demographics. Structure mirrors `novice_teachers.py` equity file.

**Files**:
| Year | URL |
|------|-----|
| 2025 | `https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC25_OVW_Students_Taught_by_Ineffective_Teachers.csv` |
| 2024 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/KYRC24_OVW_Students_Taught_by_Ineffective_Teachers.csv` |
| 2023 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/students_taught_by_ineffective_teachers_2023.csv` |
| 2022 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/students_taught_by_ineffective_teachers_2022.csv` |
| 2021 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/students_taught_by_ineffective_teachers_2021.csv` |

**Schema (KYRC25)**:
```
School Year, County Number, County Name, District Number, District Name, School Number,
School Name, School Code, State School Id, NCES ID, CO-OP, CO-OP Code, School Type,
Title I Status, All Students, Non-White, White (non-Hispanic), Economically Disadvantaged,
Non Economically Disadvantaged, Students with Disabilities (IEP),
Student without Disabilities (IEP), English Learner, Non English Learner
```

**Title I Status Values**: `Title 1`, `Not Title 1` (no Equity Gap row in KYRC25)

**Historical Column Variations (2021)**:
- Includes gap calculations: `INEFFECTIVE GAP %AGE NON-WHITE`, etc.
- Column names: `% STUDENTS TAUGHT BY INEFFECTIVE TCHRS`
- Values include `%` suffix

**Proposed KPIs**:
- `students_taught_by_ineffective_teachers_rate_title_1` (with `student_group` dimension)
- `students_taught_by_ineffective_teachers_rate_not_title_1` (with `student_group` dimension)

---

### 5. Students Taught by Out-of-Field Teachers

**Description**: Equity metrics showing percentage of students taught by out-of-field teachers (teaching outside their certification area), broken down by Title I status and demographics.

**Files**:
| Year | URL |
|------|-----|
| 2025 | `https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC25_OVW_Students_Taught_by_Out_of_Field_Teachers.csv` |
| 2024 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/KYRC24_OVW_Students_Taught_by_Out_of_Field_Teachers.csv` |
| 2023 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/students_taught_by_out_of_field_teachers_2023.csv` |
| 2022 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/students_taught_by_out_of_field_teachers_2022.csv` |
| 2021 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/students_taught_by_out_of_field_teachers_2021.csv` |

**Schema (KYRC25)**:
```
School Year, County Number, County Name, District Number, District Name, School Number,
School Name, School Code, State School Id, NCES ID, CO-OP, CO-OP Code, School Type,
Title_I_Status, All Students, Non-White, White, Economically Disadvantaged,
Non-Economically Disadvantaged, Students with Disabilities (IEP),
Student without Disabilities (IEP), English Learner, Non-English Learner
```

**Title I Status Values**: `Title 1`, `Not Title 1`, `Equity Gap`

**Historical Column Variations (2021)**:
- Includes gap calculations: `OUT OF FIELD GAP % NON-WHITE`, etc.
- Column names: `% STUDENTS TAUGHT BY OUT OF FIELD TCHRS`

**Proposed KPIs**:
- `students_taught_by_out_of_field_teachers_rate_title_1` (with `student_group` dimension)
- `students_taught_by_out_of_field_teachers_rate_not_title_1` (with `student_group` dimension)
- `students_taught_by_out_of_field_teachers_rate_equity_gap` (with `student_group` dimension)

---

### 6. Teacher Working Conditions

**Description**: Survey-based index scores for teacher working conditions. Data is pivoted by impact measure type.

**Files**:
| Year | URL |
|------|-----|
| 2025 | `https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC25_OVW_Teacher_Working_Conditions.csv` |
| 2024 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/KYRC24_OVW_Teacher_Working_Conditions.csv` |
| 2023 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/teacher_working_conditions_2023.csv` |
| 2022 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/teacher_working_conditions_2022.csv` |
| 2021 | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/teacher_working_conditions_2021.csv` |

**Schema (KYRC25)**:
```
School Year, County Number, County Name, District Number, District Name, School Number,
School Name, School Code, State School Id, NCES ID, CO-OP, CO-OP Code, School Type,
Impact Measure, Impact Value
```

**Impact Measure Values** (3 per school):
- Managing Student Behavior
- School Climate
- School Leadership

**Sample Data**:
```
20242025,,,999,All Districts,,All Schools,999000,,,,,,Managing Student Behavior,67
20242025,,,999,All Districts,,All Schools,999000,,,,,,School Climate,65
20242025,,,999,All Districts,,All Schools,999000,,,,,,School Leadership,68
```

**Historical Column Variations (2021)**:
- Measure names include "Composite" suffix: `School Leadership Composite`, `Managing Student Behavior Composite`

**Proposed KPIs**:
- `teacher_working_conditions_managing_student_behavior`
- `teacher_working_conditions_school_climate`
- `teacher_working_conditions_school_leadership`

---

## Recommended Pipeline Structure

Based on file uniqueness and data structure similarity:

### Pipeline 1: `teacher_turnover.py`
- **Files**: Teacher Turnover only
- **Complexity**: Simple (like `teacher_experience.py`)
- **Demographics**: None (institutional only)

### Pipeline 2: `teacher_certification.py`
- **Files**: Teacher Certification Data only
- **Complexity**: Simple (like `teacher_experience.py`)
- **Demographics**: None (institutional only)

### Pipeline 3: `educator_qualifications.py`
- **Files**: Educator Qualifications only
- **Complexity**: Medium (pivoted data requiring unpivot)
- **Demographics**: None (institutional only)

### Pipeline 4: `students_taught_by_ineffective_teachers.py`
- **Files**: Students Taught by Ineffective Teachers only
- **Complexity**: Medium (equity metrics with Title I × demographics)
- **Demographics**: Full demographic breakdowns via `student_group` column
- **Note**: Very similar structure to `novice_teachers.py` equity handling

### Pipeline 5: `students_taught_by_out_of_field_teachers.py`
- **Files**: Students Taught by Out-of-Field Teachers only
- **Complexity**: Medium (equity metrics with Title I × demographics)
- **Demographics**: Full demographic breakdowns via `student_group` column
- **Note**: Very similar structure to `novice_teachers.py` equity handling

### Pipeline 6: `teacher_working_conditions.py`
- **Files**: Teacher Working Conditions only
- **Complexity**: Medium (pivoted data requiring unpivot)
- **Demographics**: None (institutional only)

---

## Implementation Priority

Recommended order based on complexity and value:

1. **`teacher_turnover.py`** - Simple, high-value metric for teacher quality analysis
2. **`teacher_certification.py`** - Simple, important credential quality indicator
3. **`students_taught_by_ineffective_teachers.py`** - Important equity metric, can reuse `novice_teachers.py` patterns
4. **`students_taught_by_out_of_field_teachers.py`** - Important equity metric, nearly identical to #3
5. **`educator_qualifications.py`** - Pivoted data, useful for credential analysis
6. **`teacher_working_conditions.py`** - Pivoted data, survey-based index scores

---

## Notes

- All files have been downloaded to `data/raw/` via `kde_sources.yaml` configuration
- Historical data (2021-2023) uses uppercase column names and different formatting (e.g., `%` suffixes)
- KYRC24/25 files use title case column names
- The equity files (ineffective/out-of-field teachers) follow the same pattern as the existing `novice_teachers.py` equity handling
