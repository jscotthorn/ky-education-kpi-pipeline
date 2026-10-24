# Kentucky Education Data Pipeline

Transform heterogeneous CSV exports from the Kentucky Department of Education (KDE) into standardized KPI format for multi-year educational performance reporting. Handles achievement, discipline, enrollment, readiness, and other educational metrics across districts and schools. This project has been developed with the use of [Claude Code](https://www.anthropic.com/claude-code).

**📂 Repository**: https://github.com/jscotthorn/ky-education-kpi-pipeline

## 🚀 Quick Start

```bash
# Setup environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .


# Run ETL pipeline
python3 etl_runner.py           # Standard run (warnings/errors only)
python3 etl_runner.py --verbose # Show detailed progress logs
python3 etl_runner.py --skip-etl # Only combine existing processed files

# View results
open data/processed/  # Individual source files
open data/kpi/        # Combined master dataset (CSV)
```

## 📋 Developer Workflow

### 1. Adding New Data Sources
1. **Prepare files** → Use the data preparation tool to populate raw directories:
   ```bash
   # Prepare all configured datasets
   python3 data/prepare_kde_data.py
   
   # Prepare specific datasets
   python3 data/prepare_kde_data.py chronic_absenteeism graduation_rates
   
   # List available datasets
   python3 data/prepare_kde_data.py --list
   ```
   - Files are automatically prepared in appropriate `data/raw/` directories
   - Configuration managed in `config/kde_sources.yaml`
   - Original filenames preserved for audit trails

2. **Draft run** → `python3 etl_runner.py --draft` to generate initial ETL logic
3. **Refine** → Edit generated ETL modules in `etl/` directory
4. **Test** → `python3 -m pytest tests/` to validate processing logic
5. **Production run** → Process all sources:
   ```bash
   python3 etl_runner.py           # Standard (quiet mode)
   python3 etl_runner.py --verbose # With detailed pipeline logs
   python3 etl_runner.py --skip-etl # Only combine existing files
   ```
6. **Review** → Check outputs in `data/processed/`

### 2. Dashboard Monitoring
```bash
# Generate dashboard data
python3 html/generate_dashboard_data.py

# Serve dashboard locally
python3 html/serve_dashboard.py
# Opens http://localhost:8000/equity_dashboard.html
```

## 🔍 Enhanced Pipeline Monitoring

The ETL runner now provides detailed progress tracking and logging:

### Command Options
```bash
python3 etl_runner.py              # Standard: warnings/errors only
python3 etl_runner.py --verbose    # Detailed: show all processing logs
python3 etl_runner.py --skip-etl   # Skip ETL, only combine files
python3 etl_runner.py -v --skip-etl # Verbose combine operation
```

### Progress Display
```
🚀 Starting ETL pipeline processing (8 sources)

📊 Pipeline 1/8: graduation_rates
🔄 Running graduation_rates ETL pipeline...
   Source: data/raw/graduation_rates  
   Output: data/processed
INFO: Found 3 files to process for graduation_rates
INFO: Streaming graduation_rates_2022.csv (1/3)
INFO:   → Processing row 50,000 from graduation_rates_2022.csv
✅ Completed graduation_rates pipeline

🎉 Completed all 8 ETL pipelines

📁 Combining processed files into master KPI file...
  Processing graduation_rates.csv
    Processed 100,000 rows (10 chunks)  
  Added 156,789 KPI rows from graduation_rates.csv
  Master KPI file: 2,156,789 rows

🎯 ETL pipeline completed successfully!
```

### Benefits
- **Real-time Progress**: See which pipeline is running and row counts
- **Error Isolation**: Individual pipeline failures don't stop the entire process  
- **Performance Monitoring**: Track processing speed and identify bottlenecks
- **Chunked Processing**: Memory-efficient file combination with progress indicators

## 📁 Project Structure

```
ky-education-data-pipeline/
├── data/
│   ├── raw/                    # Source CSV files (never edited)
│   │   ├── graduation_rates/   # One folder per data source
│   │   ├── kindergarten_readiness/
│   │   └── chronic_absenteeism/
│   ├── processed/              # Standardized KPI outputs
│   └── kpi/                    # Combined master KPI file
├── etl/                        # ETL transformation modules
│   ├── graduation_rates.py
│   ├── kindergarten_readiness.py
│   ├── chronic_absenteeism.py
│   └── demographic_mapper.py   # Standardizes student groups
├── html/                       # Dashboard and visualization
│   ├── equity_dashboard.html
│   ├── generate_dashboard_data.py
│   └── data/                   # Generated JSON for dashboard
├── config/                     # Configuration files
│   └── demographic_mappings.yaml   # Single source of demographic mappings used by DemographicMapper
├── tests/                      # Test suite
├── notes/                      # Documentation and analysis
└── etl_runner.py               # Main orchestration script
```

## 🎯 Data Output Format

All ETL modules produce standardized **long format** KPI data:

| Column | Description | Example |
|--------|-------------|---------|
| `year` | Academic year (4-digit) | `2024` |
| `metric` | KPI identifier | `"graduation_rate_4_year"` |
| `district` | District name | `"Jefferson County"` |
| `school_name` | School display name | `"Bryan Station High School"` |
| `student_group` | Standardized demographic | `"Hispanic or Latino"` |
| `value` | Numeric value (NaN if suppressed) | `85.2` |
| `suppressed` | Privacy suppression flag | `"Y"` or `"N"` |
| `county_number` | KDE county number | `161` |
| `county_name` | County name | `"Fayette"` |
| `district_number` | KDE district number | `175` |
| `school_id` | Unique school identifier | `"210090000123"` |
| `school_code` | KDE school code | `020` |
| `state_school_id` | State-assigned school ID | `"175020"` |
| `nces_id` | NCES ID if available | `"210090000123"` |
| `co_op` | Education cooperative | `pd.NA` |
| `co_op_code` | Cooperative code | `pd.NA` |
| `school_type` | School classification | `"High"` |
| `source_file` | Data provenance | `"graduation_rates.csv"` |
| `last_updated` | Processing timestamp | `"2025-07-19T10:30:00"` |

### KPI Metric Naming Convention
- **Rates**: `{indicator}_rate_{period}` → Percentage values (0-100)
- **Counts**: `{indicator}_count_{period}` → Number meeting criteria  
- **Totals**: `{indicator}_total_{period}` → Total number eligible

**Example**: One graduation indicator creates three KPI rows:
- `graduation_rate_4_year` = 85.2 (rate)
- `graduation_count_4_year` = 201 (students graduating)
- `graduation_total_4_year` = 236 (students in cohort)

📋 **For a complete list of all KPIs by data source, see [KPIS.md](./KPIS.md)**

## 🔧 Common Commands

```bash
# Environment setup
python3 --version                    # Should be 3.8+
source .venv/bin/activate           # Activate virtual environment

# Data preparation
python3 data/prepare_kde_data.py               # Prepare all configured datasets
python3 data/prepare_kde_data.py --list       # List available datasets
python3 data/prepare_kde_data.py chronic_absenteeism  # Prepare specific dataset

# ETL operations
python3 etl_runner.py               # Full pipeline (quiet mode)
python3 etl_runner.py --verbose     # Full pipeline with detailed logging
python3 etl_runner.py --skip-etl    # Skip ETL, only combine existing files
python3 etl_runner.py --draft       # Analysis mode only
python3 etl/graduation_rates.py     # Test single module

# Testing and validation
python3 -m pytest tests/            # Full test suite
python3 -m pytest tests/test_graduation_rates.py -v  # Specific module
python3 -m pytest --tb=short        # Concise error output

# Dashboard generation  
python3 html/generate_dashboard_data.py              # Default: Fayette County only
python3 html/generate_dashboard_data.py --all-districts  # Include all districts
python3 html/serve_dashboard.py                      # Start web server
```

## 🌐 Data Access

### Online Data Browser
Browse and download data files at: **https://education.kyopengov.org/data/**

- The data directory contains:
- **📊 KPI Master Dataset** (`/kpi/`) - Combined dataset with all metrics in standardized format (CSV, 175MB)
- **⚙️ Processed Files** (`/processed/`) - Individual metric files ready for analysis with audit logs
- **📁 Raw Data** (`/raw/`) - Original unmodified files from Kentucky Department of Education

Each directory includes detailed descriptions, file metadata, and direct download links. All datasets now follow a standardized **19-column** KPI format that captures detailed location context alongside demographic breakdowns and suppression handling.

## 📊 Available Data Sources

### Data Sources
- **Graduation Rates** - 4-year and 5-year graduation rates, counts, and totals
- **Safe Schools Events** - Behavioral event counts by type, grade, location, and context, stored as KDE published them. `All Students` is an unduplicated student count. `Total Events` is the event count. Other rows of the same metric are events, so a subgroup share is not computed against `All Students`. A `*` is value NA with suppressed Y. A blank cell is not stored. A published 0 is kept. State rows are district `All Districts` for 2024-2026 and `State` for 2020-2023. The 2026 state All Students total is 86,009 and Total Events is 251,717. `safe_incident_rate_*` is events divided by students affected. The pipeline calculates it. KDE does not publish it. This module does not emit out-of-school suspension counts
- **Kindergarten Readiness** ⭐ **ENHANCED** - BRIGANCE kindergarten-screen percents. On the KYRC composites the columns are named like counts and the cells are percents. Each rate is the published percent, and those years do not emit `_count` or `_total` metrics. In 2018-2021 the total is the published number tested and the count is the percent times that count, rounded. That count is an estimate: exact below about 1,000 tested, and about +/-24 students at state level. A 2020-21 suppressed row with a published tested count and a blank percent stores the rate and the count as NA/Y, and stores the tested count as NA. 2017-18 co-op totals 901-909 are labelled the way 2019 publishes them. State rows are district `All Districts` for 2024-2026 and `State` for 2018-2023. The 2026 state figures are 55 / 38 / 7 / 45, and the readiness rate is 45. A `*` is value NA with suppressed Y. A blank row is not stored. A published 0 is kept
- **Chronic Absenteeism** - Chronic absenteeism rates, counts, and enrollment
- **English Learner Progress** - Proficiency rates across elementary, middle, and high school
- **Postsecondary Readiness** - College and career readiness rates
- **Postsecondary Enrollment** - Post-graduation enrollment in Kentucky institutions
- **Kentucky Summative Assessment** - Published novice, apprentice, proficient, distinguished, and proficient/distinguished percents, plus the level content index, by subject. Subjects include reading, math, science, social studies, writing, combined writing, editing and mechanics, and on-demand writing. A grade row stays on that grade. Level rates come from the level file. A `*` is value NA with `suppressed` Y. A blank is not stored. A published 0 is kept. In 2021-2023 those files publish no 0, so Y can also mean fewer than 3 students. The proficient/distinguished percent is the published cell, not the sum of the two levels. State rows are district `All Districts` for 2024-2026 and `State` for 2018-2019 and 2021-2023. The 2025-26 state reading proficient/distinguished rates are 50 / 48 / 49 and combined writing is 41 / 47 / 45
- **Out-of-School Suspension** - CRDC in-school and out-of-school suspension counts by disability status. The KPI year is the report-card year: 2026 is the 2021-22 CRDC collection and 2024 is the 2020-21 collection. The output has no collected-year column. Annual SSP3 counts stay in Safe Schools Discipline
- **CTE Participation** - Career and technical education participation. 2022 publishes counts only, stored as `cte_participant_count`, `cte_high_school_enrollment_count`, `cte_eligible_completer_count_grade_12`, and `cte_completer_count_grade_12`. The rate metrics are absent in 2022. The 2023 counts stay on those count metrics from the annual file. KYRC23 stores the published 2023 participation and grade-12 completion percents, including rates above 100 and published 0.0 rates. 2023 district and state rates are on `xxx000`, `999000`, and district `All Districts`. 2023 counts are on `xxx`, `999`, and district `State`. That split is accepted until #61's shared normalization (Scott, 24 October 2026). KYRC24 stores `cte_participant_count`, `cte_completer_count_grade_12`, and the published participation and completion percents, including rates above 100. KYRC25 and KYRC26 store the published counts and do not calculate a participation rate or a completion rate. A `*` or `<10` is value NA with `suppressed` Y. A blank, `--`, or `N/A` is not stored. A published 0 is kept
- **Career Readiness Indicators** - Published graduate counts by career-readiness indicator: industry certification, CTE end-of-program assessment, TRACK apprenticeship, CTE dual credit with a C or better, work-based learning, exceptional work experience, more than one indicator, no indicator, and total graduates. One source row per school. The columns are stored as KDE published them and are not added together. There is no demographic column. A `*` is value NA with `suppressed` Y. A blank is not stored. A published 0 is kept. State rows are district `All Districts` for 2024-2026 and `State` for 2019 and 2022-2023. The 2026 state total is 49,751 graduates, with 18,286 industry certifications and 5,115 in work-based learning. In 2022 the six indicator columns add to total graduates when a blank is read as 0. In 2023 and later they overlap. The state cell does not equal the sum of the districts where every district cell is a number. CTE dual credit here is not `dual_credit_enrollment`
- **CTE Pathways** - One row per school and year for career-pathway availability. KDE publishes one row per pathway, and `Pathways Available` is the pathway name. Active enrollment is students in at least one course in that pathway. Concentrators (two to three credits) and completers (four or more credits) are different 12th-grade groups. Sums count a student in two pathways twice. A `*` on any pathway suppresses that total. A published 0 is kept. 2019 publishes zeros and has no stars. 386 all-zero school pathway rows raise the 2019 school pathway-count mean to 12.04; without those rows it is 10.75. Later years sit between 10.4 and 11.4. In 2020-2026 the smallest published count is 3, so a star can also mean 0-2. State rows are district `All Districts` for 2024-2026 and `State` for 2019-2023. The 2026 state pathway count is 113. The 2026 state enrollment total is suppressed. `cte_pathway_completer_count` sums completers on each pathway. CTE Participation's `cte_completer_count_grade_12` counts each 12th-grade completer once (2026 state 19,990). The published 2026 state pathway-completer cells sum to at least 36,436. `cte_pathway_completion_rate` is not stored. Scott Horn dropped it on 2026-10-20. The pipeline had calculated completers divided by concentrators. KDE never published that ratio. Completers and concentrators are separate groups, so it is not a share and can exceed 100%. There is no alias. `num_cte_pathways` is a count of published pathway rows, not a figure KDE publishes. Enrollment, concentrator, and completer totals are sums across pathways, so a student in two pathways counts twice. On KYRC25 and KYRC26, some state pathway cells do not equal the sum of the district cells, and the district sum is higher. Each level keeps its published value. Issue #86, flagged kde-recheck. District 606 is new in 2026
- **Gifted and Talented** - Unduplicated gifted enrollment by grade and demographic, and overlapping identification counts by talent category. KDE's 2018-19 coversheet defines the grade total as the number of gifted and talented students enrolled. A student may be identified in more than one category, so category counts are not shares of that total. State rows are district `All Districts` for 2024-2026 and `State` for 2019-2023. The 2026 state total is 90,464 and the district totals sum to 90,464. The 2025 state total is 90,490. In 2019 and 2020 the district sum sits about 1,000 above the state. School sums fall short of the district where a school cell is starred. The one year a complete school sum exceeds its district is Garrard County in 2021 (149 against 147). Grade 14 is absent in 2026. A published 0 is kept in 2019 and on the KYRC25 and KYRC26 category-only schools. The 2020-2023 files publish no 0 and no blank, so a suppressed Y in those years is not evidence the group exists. KDE did not report preschool, grade 14, or Consolidated Student Group in `gifted_and_talented_2020.csv` through `gifted_and_talented_2023.csv`. The stars on those slices are not stored. A published number is kept. A missing group row does not mean the group is empty
- **Advanced Coursework** - Unduplicated AP, IB, and Cambridge students by demographic, duplicated course enrollments by subject or course, and the CRDC advanced-coursework count by gender. The CRDC KPI year is the collection year: 2021 is the 2020-21 collection and 2022 is the 2021-22 collection. Dual credit is a separate source. Course type `ALL` is the only `advanced_*` series and includes dual-credit students. Course type `CAI` is Cambridge. State rows are district `All Districts` for 2024-2026 and `State` for 2019-2023. The 2026 state AP enrollment is 37,212. Districts sum to 37,283 and schools to 37,319, the same gap as 2020-2025. The 2026 IB tested count is 42 after 234 in 2025. The derived 2026 IB qualifying rate is not stored. A `*` is stored as value NA with suppressed Y. The 2020-2026 participation files publish no 0, so a Y in those years is not evidence the group exists. A published overview 0 is kept
- **School Courses** - Specialized courses in career and technical education, health and physical education, visual and performing arts, and world languages, stored as one row per school and year. KDE's 2018-19 coversheet defines the total as students enrolled in that course and names the four categories. There is no state row and no demographic column. `courses_offered_count` is the number of course rows. A starred total counts, and `courses_offered_suppressed_count` is how many totals are `*`. A blank total is not stored and is not counted. Category counts (`_cte`, `_hpe`, `_vpa`, `_wl`, `_other`) come from KDE's category column. 2019 spells health and PE `HPE`; 2020-2026 spell it `HEPE`. All count metrics are stored with suppressed N. The smallest published enrollment in 2020-2026 is 3, so a star in those years can also mean 0-2. 2019 publishes 1s and has no stars. The 2019, 2021, and 2023 values changed from 1 per course to these school counts. No registered file contains an Algebra 1 course
- **Dual Credit** - Unduplicated dual credit students by demographic, and duplicated course enrollments by subject. KDE defines participation as one count per student and courses offered as one count per course. A qualifying grade is a C or better. State rows are district `All Districts` for 2023-2026. The 2026 state student enrollment is 55,553 and the 2026 state course enrollment is 143,848. District course enrollment equals the state in 2024-2026. The state student total is unduplicated across districts, so a student in two districts is counted in both district rows and once at the state. District student counts sit about 200 above that state total in every year. A student in two schools is counted in both school rows and once on the district row, so school sums exceed the district in 11 to 16 districts a year even when no school cell is starred. `has_dual_credit_program` is not stored. Scott Horn dropped it on 2026-10-20. The pipeline had set it to 1 whenever enrollment was positive. No published enrollment is 0, and a school without a program has no row, so the flag was never 0. There is no alias
- **Safe Schools Climate** - Published climate and safety index scores, with the school level on the metric name (`climate_index_score_elementary`, `climate_index_score_middle`, `climate_index_score_high`, and the same three safety names). Scott decided on 2026-10-19 to keep those names and not add an unsuffixed alias. A row with no level is not stored. Scott decided on 2026-10-20 to remove `climate_index_score_calculated` and `safety_index_score_calculated`. Those averages ignored school level and kept the elementary survey file. The published per-level indexes replace them. 2021-22 is not in this series. The 2026 state All Students cells are elementary 80.0 / 74.8, middle 71.3 / 65.5, and high 68.6 / 64.0. KYRC state rows are district `All Districts` and school code `999000`. The 2022-2023 accountability profiles are district `State` and school code `999`. `KYRC23_ACCT_Index_Scores.csv` is the 2023 climate and safety index. State All Students climate / safety is elementary 79.5 / 74.0, middle 68.8 / 62.3, and high 64.3 / 59.1. 2022 stores the indicator rate as `climate_safety_indicator_rate_{level}` and 2023 stores the combined indicator rate (status plus change), not status, as `climate_safety_combined_indicator_rate_{level}`. Each accountability number is stored once. The two years do not share a metric name. State 2023 combined versus status is elementary 77.0 versus 76.8, high 62.0 versus 61.7, and middle 65.3 versus 65.6. `safety_policy_compliance_rate` is the percent of published Yes answers among the eight policy questions. 2020-2025 are 8 of 8. KYRC26 dropped classroom doors, so 2026 is 7 of 7. Scott decided on 2026-10-20 to keep `outside_doors_locked_during_school_hours` as the published yes/no answer (1 for Yes, 0 for No). It is not a rate and not a share, and it is not in that compliance rate. A `*` is value NA with `suppressed` Y. A blank is not stored. A published 0 is kept. Co-op `GREC` in 2025 and `GLEC` in 2026 are stored as published. `GRREC` stays `GRREC`
- **Safe Schools Discipline** - Discipline resolution and legal-sanction counts, stored as KDE published them. `All Students` is an unduplicated student count. Subgroup rows of the same metric are event counts, so a subgroup share is not computed against `All Students`. Female plus Male equals the published `Total Events` row, which is not stored. Scott decided on 2026-10-19 to store the published `Total` on `All Students` rows only: `safe_schools_discipline_resolutions_students_unduplicated` and `safe_schools_legal_sanctions_students_unduplicated`. Each is the unduplicated student count KDE published. A subgroup `Total` repeats the event-column sum and is not stored. Nothing is calculated from `Total`. State rows on the annual 2020-2023 discipline files are district `State`. State rows for 2024-2026, and the 2022-23 legal file, are district `All Districts`. The 2026 state out-of-school suspension count is the published 35,818. 2022-23 legal sanctions come from `KYRC23_SAFE_Legal_Sanctions.csv`. Its columns match the 2024-2026 legal files. The state All Students Total is the published 4,609, and arrests are the published 190. 2019-20 through 2021-22 have no legal-sanctions source. The KYRC23 discipline file is a republish and is not loaded. A published 0 is kept. A `*` on a row that also has a number is stored as value NA with suppressed Y. An all-star row is stored as value NA with suppressed Y, because the KYRC23 republish turns most of those 2023 rows into zeros and some into real counts. In the 2020-2026 CSVs the smallest positive number is 3. The 2018-19 workbook publishes 1s and 2s and is not loaded (issue #89)
- **Student Enrollment** - Student enrollment counts by grade level (PreK-12) with primary, middle, and secondary aggregations
- **Secondary Enrollment** - Students with a secondary (concurrent) enrollment at this school, by grade, with middle and high school aggregations
- **Teacher Experience** - Average years of teaching experience for classroom teachers at the school. KDE defines this as total teaching experience divided by the number of teachers, not years employed at that building
- **Student-Teacher Ratio** - Students per one teacher. KDE defines this as school membership divided by FTE teachers, excluding administrators, guidance counselors, media specialists, and resource teachers. KYRC26 publishes school rows only
- **District School List** - School latitude, longitude, grade span, school-type indicators, and Title I indicators. KDE's coversheet defines latitude and longitude as the school's location, school type as the A1-D1 classification, and Title I as the school's federal-grant status. There is no demographic column. District aggregate rows are not stored. The 2026 state row is district `---State---`, school id `999000`, latitude 38.184805, longitude -84.848799. Jefferson County's co-op is `GLEC` in 2026. A blank coordinate is not stored
- **Spending Per Student** - Per-pupil spending by funding source (federal, state/local, combined)
- **Benchmark Assessment** - Interim assessment performance data
- **ACT Scores** - College admission test performance
- **Students Taught by Inexperienced Teachers** - KDE defines an inexperienced teacher as a teacher with 0-3 years of experience. A first-year teacher is a New Teacher. The novice_teachers ETL stores both teacher rates (less than 1 year, and 1 to 3 years) and the share of students taught by inexperienced teachers by Title I status. State rows are district `All Districts` for 2024-2026 and `State` for 2019-2023
- **Teacher Working Conditions** - Percent favorable on the Impact Kentucky working conditions survey. KDE defines the value as the share of respondents who chose a favorable answer. The constructs are managing student behavior, school leadership, and either teaching environment (2020-2023) or school climate (2024-2026). There is no demographic column. State rows are district `All Districts` for 2024-2026 and `State` for 2020-2023
- **Teacher Turnover** - Percentage of teacher turnover experienced by the school or district. KDE defines the turnover count as the number of teaching positions which experienced turnover and the teacher count as the total number of teachers. The ETL stores the published percent. There is no demographic column. State rows are district `All Districts` for 2024-2026 and `State` for 2019-2023. 2020 is comparable to 2019 and 2021-2025. KYRC26 district and state turnover is a different measure (state 4,271, districts 5,675, schools 7,686) and is not a continuation of that series. A 0% with a teacher count of 0 is a stored placeholder, not a real zero
- **Teacher Certification** - Emergency/provisional certification and National Board certification. KDE defines emergency/provisional as the percentage of teachers granted an emergency or provisional certificate under 16 KAR 2:120 and 16 KAR 2:010. The ETL stores the published percent. There is no demographic column. State rows are district `All Districts` for 2024-2026 and `State` for 2020-2023. National Board for 2024-2026 continues (state 2.6, 2.9, 2.7). KYRC26 emergency/provisional is a different level from 2020-2025 (state 6,133 / 14.3 after 826 / 1.9). A 0% with a teacher count of 0 is a stored placeholder, not a real zero. Rank I and Rank II are not stored
- **Students Taught by Ineffective Teachers** - KDE defines an ineffective teacher as one with a local KRS 156.557 summative rating of Ineffective. The report card publishes a percent, by Title I status. In 2020-2023 and in KYRC25 and KYRC26 the group columns are shares of all students. The registered historical KYRC24 file behaves like a within-group rate. The ETL stores the published percent. State rows are district `All Districts` for 2024-2026 and `State` for 2020-2023
- **Students Taught by Out-of-Field Teachers** - KRS 161.1221 and 16 KAR 2:120 define out-of-field teaching through EPSB assignment data. The report card publishes a percent, by Title I status. The group columns are within-group rates in every registered year. The ETL stores the published percent, including a published 0. The equity gap is signed for 2021-2023 and 2026 and is the absolute value for 2024 and 2025. The 2019-20 file is not loaded. State rows are district `All Districts` for 2024-2026 and `State` for 2021-2023
- **Student Retention** - Grade retention rates (grades 4-12)
- **Dropout Rate** - Student dropout tracking
- **Homeless Students** - Homeless student population metrics
- **Migrant Students** - Migrant student population metrics
- **Students with Disabilities** - IEP student population metrics

### Dashboard Features
- **Interactive heatmaps** showing schools vs demographics  
- **Performance-based ranking** (highest performing schools at top)
- **Suppression transparency** (gaps for privacy-protected data)
- **Multi-district filtering** and data coverage metrics
- **Time series visualization** across available years

## 🛡️ Data Quality & Privacy

### Suppression Handling
- Records with `SUPPRESSED='Y'` are included with `value=NaN`
- Maintains data structure while protecting privacy (typically <10 students)
- Dashboard visualizations show gaps for suppressed data

### Demographic Standardization
All ETL pipelines use `DemographicMapper` to ensure consistent student group names:
- Handles year-to-year variations in naming
- Maps inconsistent labels to standard format
- Generates audit trails for transparency

### Validation Requirements
- Data type validation for all numeric fields
- Range validation (rates: 0-100%, counts: non-negative integers)
- Consistency checks (rate = count/total * 100)
- Required demographic coverage validation

## 🤝 Contributing

### Code Standards
- Use `python3` (not `python` - may default to Python 2.7)
- Follow existing patterns for ETL module structure
- Include comprehensive error handling and logging
- Add tests for new modules in `tests/` directory

### Documentation
- Document new data sources in numbered journal entries: `notes/#--description.md`
- Update this README for new features or data sources
- Maintain AI usage patterns in `CLAUDE.md`

### AI Assistance
See [CLAUDE.md](./CLAUDE.md) for AI assistant usage patterns and workflows.

## 📈 Recent Improvements

- **Suppression Transparency**: Includes suppressed records with explicit indicators
- **Enhanced Dashboard**: Interactive heatmaps with performance ranking
- **Data Quality Fixes**: Corrected 2024 kindergarten readiness interpretation
- **Comprehensive Testing**: Full test coverage with validation checks
- **Documentation**: Detailed journal entries tracking all changes
- **S3-based data hosting** for public access
- **Type Enforcement**: Uses `config/mappings.yaml` to enforce column dtypes

## 🎯 Future Roadmap

### Data Expansion Strategy
Based on comprehensive analysis of 94 additional Kentucky Department of Education data sources, the project roadmap includes **40+ high-value KPI opportunities** across school discipline, teacher quality, advanced coursework, vulnerable populations, and intersectional equity analysis.

**Key Implementation Priorities**:
- **School Discipline Equity** - Complete school-to-prison pipeline tracking through SAFE behavior event files
- **Teacher Quality Disparities** - Resource equity analysis (Title I schools have 66.0% vs Non-Title I 35.4% students with inexperienced teachers)
- **Vulnerable Population Analysis** - Extreme intersections (14.4% of migrant students experiencing homelessness)
- **Advanced Academic Opportunities** - College prep and career readiness equity gaps
- **Intersectional Analysis** - Multi-dimensional demographic combinations across all performance areas

### Planning Documentation
- **Journal 20** (`notes/20--kyrc24-new-data-sources-investigation.md`) - Comprehensive analysis of 94 files across 8 categories with detailed equity insights and discovery documentation
- **Journal 21** (`notes/21--kyrc24-implementation-roadmap.md`) - Strategic implementation roadmap with 12 sprints across 4 phases, prioritized by equity impact
- **Journal 22** (`notes/22--longitudinal-data-availability-analysis.md`) - Historical data availability analysis across all planned sprints, documenting 4 complete school years (2020-24) plus current year with specific legacy file naming patterns

For detailed technical implementation and AI usage guidelines, see [CLAUDE.md](./CLAUDE.md).

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

Copyright 2025 Kentucky Open Government Coalition

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.