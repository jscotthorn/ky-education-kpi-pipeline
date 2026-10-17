# KYRC25 / KYRC26 schema diff

Generated 2026-10-17 18:55 UTC by `scripts/kyrc_schema_diff.py`.

Requests used a Chrome User-Agent. A status of 403, 429, or 503 is blocked, not missing. A status of 404 is absent. No request in this run returned 403, 429, or 503.
School code, school number, and district number are compared in full in `notes/kyrc26-code-domain-diff.md`. Other coded columns still stop at 80 distinct values. Grain checks for state rows are in the audit note.

## Containers

`datasets` is the container PR #36 downloads. `datasetsuat6051276` is the container the report-card download page links (`msDownloadPath` + `DDD`).
KYRC26 names present only on datasets: 0. Only on datasetsuat6051276: 0. Same-length pairs still need a byte hash because Last-Modified and ETag differ on every object.

Byte MD5 on 99 KYRC26 CSVs present in both containers: 99 identical, 0 length mismatches, 0 same-length hash mismatches.

Same bytes on both containers. `datasets` Last-Modified is the 15 October 2026 release copy; `datasetsuat6051276` is the earlier object the download page still links.

## Header diffs (KYRC25 datasets vs KYRC26 datasets)

| Stem | Kind | Rows 25 | Rows 26 | Added | Removed | Renames / case |
| --- | --- | ---: | ---: | --- | --- | --- |
| `ACCT_4_Year_High_School_Graduation.csv` | identical | 9528 | 9528 |  |  |  |
| `ACCT_5_Year_High_School_Graduation.csv` | identical | 9528 | 9528 |  |  |  |
| `ACCT_Assessment_Participation.csv` | identical | 355096 | 353220 |  |  |  |
| `ACCT_English_Learners_Progress_Proficiency_Rate.csv` | identical | 19338 | 19316 |  |  |  |
| `ACCT_Index_Scores.csv` | identical | 47466 | 47412 |  |  |  |
| `ACCT_Kentucky_Summative_Assessment.csv` | identical | 278464 | 278848 |  |  |  |
| `ACCT_Postsecondary_Readiness.csv` | identical | 10719 | 10719 |  |  |  |
| `ACCT_School_Improvement.csv` | identical | 191 | 211 |  |  |  |
| `ACCT_Survey_Results.csv` | identical | 1281582 | 1280124 |  |  |  |
| `ADLF_Current_Year_Graduates.csv` | identical | 9666 | 9522 |  |  |  |
| `ADLF_Graduate_Outcomes.csv` | identical | 529 | 535 |  |  |  |
| `ADLF_Transition_to_In_State_Postsecondary_Education.csv` | identical | 9522 | 6512 |  |  |  |
| `ASMT_Alternate_Assessment_Participation.csv` | identical | 18770 | 18700 |  |  |  |
| `ASMT_English_Language_Proficiency.csv` | identical | 6868 | 6924 |  |  |  |
| `ASMT_Kindergarten_Screen_Composite.csv` | identical | 83808 | 82864 |  |  |  |
| `ASMT_Kindergarten_Screen_Developmental_Domains.csv` | identical | 83808 | 82864 |  |  |  |
| `ASMT_Measurements_of_Interim_Progress_English_Language_Proficiency.csv` | identical | 1778 | 1776 |  |  |  |
| `ASMT_Measurements_of_Interim_Progress_Graduation_Rate.csv` | identical | 14292 | 14292 |  |  |  |
| `ASMT_Measurements_of_Interim_Progress_Proficiency.csv` | identical | 156960 | 156852 |  |  |  |
| `ASMT_National_Assessment_of_Educational_Progress.csv` | identical | 128 | 128 |  |  |  |
| `CTE_Career_Pathways.csv` | identical | 5594 | 5662 |  |  |  |
| `CTE_Career_Readiness_Indicators.csv` | identical | 398 | 397 |  |  |  |
| `CTE_Participation.csv` | identical | 6526 | 8245 |  |  |  |
| `CTE_Perkins_Report.csv` | identical | 3396 | 3313 |  |  |  |
| `CTE_Postsecondary_Opportunities.csv` | identical | 8471 | 8146 |  |  |  |
| `CTE_Student_Objectives.csv` | identical | 474 | 479 |  |  |  |
| `EDOP_Advanced_Courses_Offered.csv` | identical | 4889 | 4909 |  |  |  |
| `EDOP_Advanced_Courses_Participation_and_Performance.csv` | identical | 6012 | 5958 |  |  |  |
| `EDOP_Dual_Credit_Participation_and_Performance.csv` | identical | 8784 | 8712 |  |  |  |
| `EDOP_Gifted_Participation_by_Category.csv` | identical | 21671 | 21593 |  |  |  |
| `EDOP_School_Courses_Summary.csv` | identical | 32793 | 33385 |  |  |  |
| `OVW_Adjusted_Average_Daily_Attendance_AADA.csv` | identical | 1669 | 1663 |  |  |  |
| `OVW_Advanced_Coursework.csv` | identical | 36612 | 4077 |  |  |  |
| `OVW_Arrests_and_Referrals_to_Law_Enforcement.csv` | identical | 14960 | 14949 |  |  |  |
| `OVW_Attendance_Rate.csv` | identical | 1669 | 1663 |  |  |  |
| `OVW_Average_Years_School_Experience.csv` | identical | 1583 | 1587 |  |  |  |
| `OVW_Board_Members_SBDM.csv` | identical | 8383 | 8318 |  |  |  |
| `OVW_Chronic_Absenteeism.csv` | identical | 433440 | 430848 |  |  |  |
| `OVW_Chronic_Absenteeism_CRDC.csv` | identical | 7526 | 7389 |  |  |  |
| `OVW_Dropout_Rate.csv` | identical | 12204 | 12204 |  |  |  |
| `OVW_Economically_Disadvantaged.csv` | identical | 28373 | 25141 |  |  |  |
| `OVW_Educator_Qualifications.csv` | identical | 9792 | 8145 |  |  |  |
| `OVW_English_Learners.csv` | identical | 28373 | 28271 |  |  |  |
| `OVW_Extra_Year_in_Primary.csv` | identical | 17289 | 16740 |  |  |  |
| `OVW_Harassment_and_Bullying.csv` | identical | 1360 | 1359 |  |  |  |
| `OVW_Homeless.csv` | identical | 28373 | 28271 |  |  |  |
| `OVW_Inexperienced_Staff.csv` | identical | 924 | 921 |  |  |  |
| `OVW_Inexperienced_Teachers.csv` | identical | 1583 | 1587 |  |  |  |
| `OVW_Migrant.csv` | identical | 28373 | 28271 |  |  |  |
| `OVW_Parental_Involvement.csv` | identical | 1143 | 1144 |  |  |  |
| `OVW_Preschool_Enrollment.csv` | identical | 13560 | 13590 |  |  |  |
| `OVW_Preschool_Suspensions_and_Expulsions.csv` | identical | 1872 | 1016 |  |  |  |
| `OVW_Secondary_Enrollment.csv` | identical | 1185 | 1235 |  |  |  |
| `OVW_Student_Enrollment.csv` | identical | 23136 | 22923 |  |  |  |
| `OVW_Student_Expulsions.csv` | identical | 14960 | 14949 |  |  |  |
| `OVW_Student_Retention_Grades_4_12.csv` | identical | 28116 | 28062 |  |  |  |
| `OVW_Student_Suspensions.csv` | identical | 14960 | 14949 |  |  |  |
| `OVW_Student_to_Teacher_Ratio.csv` | identical | 1608 | 1432 |  |  |  |
| `OVW_Students_Taught_by_Ineffective_Teachers.csv` | identical | 1692 | 1695 |  |  |  |
| `OVW_Students_Taught_by_Inexperienced_Teachers.csv` | identical | 2025 | 2019 |  |  |  |
| `OVW_Students_Taught_by_Out_of_Field_Teachers.csv` | identical | 2025 | 2019 |  |  |  |
| `OVW_Students_with_Disabilities_IEP.csv` | identical | 28233 | 28125 |  |  |  |
| `OVW_Teacher_Working_Conditions.csv` | identical | 4302 | 3282 |  |  |  |
| `OVW_Teachers_Full_Time_Equivalent_FTE.csv` | identical | 178 | 178 |  |  |  |
| `OVW_Teachers_by_Gender.csv` | identical | 3220 | 3214 |  |  |  |
| `OVW_Teachers_by_Race_Ethnicity.csv` | identical | 12880 | 12856 |  |  |  |
| `OVW_Violent_Offenses.csv` | identical | 1360 | 1359 |  |  |  |
| `SAFE_Approaches_that_are_actively_implemented_school_wide.csv` | identical | 10247 | 10042 |  |  |  |
| `SAFE_Behavior_Events_by_Context.csv` | identical | 31711 | 31597 |  |  |  |
| `SAFE_Behavior_Events_by_Grade_Level.csv` | identical | 31711 | 31597 |  |  |  |
| `SAFE_Behavior_Events_by_Location.csv` | identical | 31711 | 31597 |  |  |  |
| `SAFE_Behavior_Events_by_Type.csv` | identical | 31711 | 31597 |  |  |  |
| `SAFE_Legal_Sanctions.csv` | identical | 31711 | 31597 |  |  |  |
| `ACCT_Crosstab_Cohort_4_Year.csv` | case_or_whitespace | 109275 | 110133 |  |  | Co-Op => CO-OP; Co-Op Code => CO-OP Code |
| `ACCT_Crosstab_Cohort_5_Year.csv` | case_or_whitespace | 105437 | 108995 |  |  | Co-Op => CO-OP; Co-Op Code => CO-OP Code |
| `ACCT_Crosstab_EditingMechanics.csv` | case_or_whitespace | 379080 | 386298 |  |  | Co-Op => CO-OP; Co-Op Code => CO-OP Code |
| `ACCT_Crosstab_Mathematics.csv` | case_or_whitespace | 472112 | 475876 |  |  | Co-Op => CO-OP; Co-Op Code => CO-OP Code |
| `ACCT_Crosstab_OnDemandWriting.csv` | case_or_whitespace | 379128 | 386298 |  |  | Co-Op => CO-OP; Co-Op Code => CO-OP Code |
| `ACCT_Crosstab_Reading.csv` | case_or_whitespace | 472116 | 475876 |  |  | Co-Op => CO-OP; Co-Op Code => CO-OP Code |
| `ACCT_Crosstab_Science.csv` | case_or_whitespace | 386226 | 393623 |  |  | Co-Op => CO-OP; Co-Op Code => CO-OP Code |
| `ACCT_Crosstab_SocialStudies.csv` | case_or_whitespace | 382792 | 386326 |  |  | Co-Op => CO-OP; Co-Op Code => CO-OP Code |
| `ACCT_Profile.csv` | case_or_whitespace | 1778 | 1776 |  |  | Co-Op => CO-OP; Co-Op Code => CO-OP Code |
| `ACCT_Summary.csv` | case_or_whitespace | 17580 | 17560 |  |  | Co-Op => CO-OP; Co-Op Code => CO-OP Code |
| `ASMT_Benchmark.csv` | changed | 13032 | 13224 | Reading and Writing | Reading; English |  |
| `ASMT_CAE.csv` | changed | 13032 | 13224 | Reading and Writing; Total Score | Reading; English; Composite Score |  |
| `ASMT_Kentucky_Summative_Assessment.csv` | case_or_whitespace | 581870 | 579700 |  |  | Co-Op => CO-OP; Co-Op Code => CO-OP Code |
| `ASMT_The_ACT.csv` | no KYRC26 twin (404) | 13032 |  |  |  |  |
| `CRDC_Chronic_Absenteeism.csv` | no KYRC26 twin (404) | 7526 |  |  |  |  |
| `EDOP_Career_Studies.csv` | changed | 1490 | 1485 | School Type; Equitable Access to Instructional Space; School Budget Allocation to CS Program; Professional Development Hours Offered to CS Teachers; Instructional Minutes Agricultural Education; Instructional Minutes Business and Marketing; +11 more | Question; Classroom/instructional space for Career and Technical Education programs is equitable to other content area programs.; What percentage of the school budget is allocated to Career and Technical Education programs?; How many hours of career studies-related professional development are offered to teachers instructing in career studies courses?; Please list the number of instructional minutes students have access to the Agriculture program area weekly.; Please list the number of instructional minutes students have access to the Business program area weekly.; +15 more | Co-Op -> CO-OP; Co-Op Code -> CO-OP Code; Program Offering Agriculture -> Program Offering Agricultural Education; Program Offering Business -> Program Offering Business and Marketing; Program Offering Construction -> Program Offering Construction Technology; Program Offering Education -> Program Offering Education and Training; +5 more |
| `EDOP_Dual_Credit_Courses_Offered.csv` | changed | 6467 | 6402 |  |  | Couse Completers -> Course Completers |
| `EDOP_Gifted_Participation_by_Grade_Level.csv` | changed | 13899 | 13711 |  | Grade 14 |  |
| `EDOP_Health_Education_and_Physical_Education.csv` | changed | 1320 | 1319 | Equitable Access to Instructional Space |  | Co-Op -> CO-OP; Co-Op Code -> CO-OP Code; Instructional Minutes Physical Education -> Instructional Minutes Health Education and Physical Education; Facilities Access Health education classroom -> Facilities Access Health Education Classroom |
| `EDOP_Visual_and_Performing_Arts.csv` | changed | 1320 | 1319 | School Type | ; Program Offerings Other; Program Offering Other Program Name | Co-Op -> CO-OP; Co-Op Code -> CO-OP Code |
| `EDOP_World_Languages.csv` | changed | 1320 | 1301 |  |  | Co-Op -> CO-OP; Co-Op Code -> CO-OP Code; Language Immersion French -> WL_Language Immersion French; Language Immersion Program Not Offered -> WL_Language Immersion Program Not Offered; Language Immersion Spanish -> WL_Language Immersion Spanish |
| `FT_Facilities_Debt_Services.csv` | no KYRC26 twin (404) | 172 |  |  |  |  |
| `FT_Financial_Summary.csv` | no KYRC26 twin (404) | 172 |  |  |  |  |
| `FT_Grants.csv` | no KYRC26 twin (404) | 172 |  |  |  |  |
| `FT_Learning_Environment.csv` | no KYRC26 twin (404) | 172 |  |  |  |  |
| `FT_Operations.csv` | no KYRC26 twin (404) | 172 |  |  |  |  |
| `FT_Personnel_Spending_per_Student_Federal_Funds.csv` | no KYRC26 twin (404) | 3338 |  |  |  |  |
| `FT_Personnel_Spending_per_Student_State_Local_Funds.csv` | no KYRC26 twin (404) | 3338 |  |  |  |  |
| `FT_Spending_per_Student.csv` | no KYRC26 twin (404) | 1150 |  |  |  |  |
| `FT_State_Funding_SEEK.csv` | no KYRC26 twin (404) | 172 |  |  |  |  |
| `FT_Statewide_Funding_Sources.csv` | no KYRC26 twin (404) | 172 |  |  |  |  |
| `FT_Taxes.csv` | no KYRC26 twin (404) | 172 |  |  |  |  |
| `FT_Total_Spending_per_Student_All_Fund_Sources.csv` | no KYRC26 twin (404) | 1669 |  |  |  |  |
| `FT_Total_Spending_per_Student_Federal_Funds.csv` | no KYRC26 twin (404) | 1669 |  |  |  |  |
| `FT_Total_Spending_per_Student_State_Local_Funds.csv` | no KYRC26 twin (404) | 1669 |  |  |  |  |
| `OVW_Access_to_Technology.csv` | changed | 1417 | 1424 |  | Student to Instruction Device Ratio; Used for school level url on overpage only; should not be included in School Profile Report or in the Overview/Precautionary Data | Of the total student instructional devices available, how many are Wi-Fi enabled devices provided by the school? (To include devices such as a tablet, laptop, and/or Chromebook that connects to the Internet primarily by Wi-Fi) (Note: will not appear in public SRC) -> Of the total student instructional devices available, how many are Wi-Fi enabled devices provided by the school? (To include devices such as a tablet, laptop, and/or Chromebook that connects to the Internet primarily by Wi-Fi); Does the school have wireless able to support bring your own device or 1:1 (dense wireless, ready for every student to connect 1 or 2 devices and have a good experience)? (Note: will not appear in public SRC) -> Does the school have wireless able to support bring your own device or 1:1 (dense wireless, ready for every student to connect 1 or 2 devices and have a good experience)? |
| `OVW_District_School_List.csv` | case_or_whitespace | 1669 | 1663 |  |  | Co-Op => CO-OP; Co-Op Code => CO-OP Code |
| `OVW_Student_Membership.csv` | changed | 12977 | 12547 |  | Preschool |  |
| `OVW_Teacher_Certification_Data.csv` | changed | 1669 | 1663 | Rank I Teacher Count; Percent Rank I Teachers; Rank II Teacher Count; Percent Rank II Teachers |  |  |
| `OVW_Teacher_Turnover.csv` | changed | 1669 | 1663 | Instructional Staff Count; Instructional Staff Turnover Count; Instructional Staff Turnover Percent |  |  |
| `SAFE_Discipline_Resolutions.csv` | case_or_whitespace | 31711 | 31597 |  |  | Expelled, Not Receiving Services (Ssp2) => Expelled, Not Receiving Services (SSP2) |
| `SAFE_Precautionary_Measures.csv` | changed | 1345 | 1349 |  | All classroom doors lock from the inside |  |

## Domain changes

School code, school number, and district number are compared in full in `notes/kyrc26-code-domain-diff.md`. Other coded columns still stop at 80 distinct values. Grain checks for state rows are in the audit note.

School year rolls from `20242025` to `20252026` on 99 columns. Collected-school-year and other low-cardinality exceptions:

- `ACCT_4_Year_High_School_Graduation.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ACCT_5_Year_High_School_Graduation.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ACCT_Assessment_Participation.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ACCT_English_Learners_Progress_Proficiency_Rate.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ACCT_Index_Scores.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ACCT_Kentucky_Summative_Assessment.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ACCT_Postsecondary_Readiness.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ACCT_School_Improvement.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ACCT_Survey_Results.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ADLF_Current_Year_Graduates.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ADLF_Current_Year_Graduates.csv` `School Type`: added []; removed ['A8']
- `ADLF_Graduate_Outcomes.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ADLF_Graduate_Outcomes.csv` `School Type`: added ['D1']; removed []
- `ADLF_Transition_to_In_State_Postsecondary_Education.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ADLF_Transition_to_In_State_Postsecondary_Education.csv` `School Type`: added ['D1']; removed []
- `ASMT_Alternate_Assessment_Participation.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ASMT_Benchmark.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ASMT_CAE.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ASMT_English_Language_Proficiency.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ASMT_Kindergarten_Screen_Composite.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ASMT_Kindergarten_Screen_Developmental_Domains.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ASMT_Measurements_of_Interim_Progress_English_Language_Proficiency.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ASMT_Measurements_of_Interim_Progress_Graduation_Rate.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `ASMT_Measurements_of_Interim_Progress_Proficiency.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `CTE_Career_Pathways.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `CTE_Career_Pathways.csv` `School Type`: added ['B1']; removed []
- `CTE_Career_Readiness_Indicators.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `CTE_Participation.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `CTE_Participation.csv` `School Type`: added ['B1']; removed []
- `CTE_Perkins_Report.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `CTE_Perkins_Report.csv` `School Type`: added []; removed ['A3', 'A6']
- `CTE_Postsecondary_Opportunities.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `CTE_Student_Objectives.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `CTE_Student_Objectives.csv` `School Type`: added ['B1']; removed []
- `EDOP_Advanced_Courses_Offered.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `EDOP_Advanced_Courses_Offered.csv` `School Type`: added []; removed ['A8', 'D1']
- `EDOP_Advanced_Courses_Participation_and_Performance.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `EDOP_Advanced_Courses_Participation_and_Performance.csv` `School Type`: added []; removed ['A8', 'D1']
- `EDOP_Dual_Credit_Courses_Offered.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `EDOP_Dual_Credit_Courses_Offered.csv` `School Type`: added ['D1']; removed ['A8']
- `EDOP_Dual_Credit_Participation_and_Performance.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `EDOP_Dual_Credit_Participation_and_Performance.csv` `School Type`: added ['D1']; removed ['A8']
- `EDOP_Gifted_Participation_by_Category.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `EDOP_Gifted_Participation_by_Grade_Level.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `EDOP_Gifted_Participation_by_Grade_Level.csv` `School Type`: added []; removed ['A8']
- `EDOP_School_Courses_Summary.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Access_to_Technology.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Adjusted_Average_Daily_Attendance_AADA.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Advanced_Coursework.csv` `Collected School Year`: KYRC25 ['20202021'] -> KYRC26 ['20212022']
- `OVW_Advanced_Coursework.csv` `CO-OP`: added ['GLEC']; removed ['', 'GREC']
- `OVW_Advanced_Coursework.csv` `CO-OP Code`: added []; removed ['']
- `OVW_Advanced_Coursework.csv` `School Type`: added []; removed ['A3', 'A4', 'A8', 'D1']
- `OVW_Arrests_and_Referrals_to_Law_Enforcement.csv` `Collected School Year`: KYRC25 ['20202021'] -> KYRC26 ['20212022']
- `OVW_Arrests_and_Referrals_to_Law_Enforcement.csv` `CO-OP`: added ['', 'GLEC']; removed ['GREC']
- `OVW_Arrests_and_Referrals_to_Law_Enforcement.csv` `CO-OP Code`: added ['']; removed []
- `OVW_Attendance_Rate.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Average_Years_School_Experience.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Average_Years_School_Experience.csv` `School Type`: added ['D1']; removed []
- `OVW_Board_Members_SBDM.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Chronic_Absenteeism.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Chronic_Absenteeism.csv` `School Type`: added []; removed ['A4']
- `OVW_Chronic_Absenteeism_CRDC.csv` `Collected School Year`: KYRC25 ['20202021'] -> KYRC26 ['20242025']
- `OVW_Chronic_Absenteeism_CRDC.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Chronic_Absenteeism_CRDC.csv` `School Type`: added ['B1']; removed ['A2']
- `OVW_Dropout_Rate.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Economically_Disadvantaged.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Educator_Qualifications.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_English_Learners.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Extra_Year_in_Primary.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Extra_Year_in_Primary.csv` `School Type`: added []; removed ['A4']
- `OVW_Harassment_and_Bullying.csv` `Collected School Year`: KYRC25 ['20202021'] -> KYRC26 ['20212022']
- `OVW_Harassment_and_Bullying.csv` `CO-OP`: added ['', 'GLEC']; removed ['GREC']
- `OVW_Harassment_and_Bullying.csv` `CO-OP Code`: added ['']; removed []
- `OVW_Homeless.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Inexperienced_Staff.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Inexperienced_Teachers.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Inexperienced_Teachers.csv` `School Type`: added ['D1']; removed []
- `OVW_Migrant.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Parental_Involvement.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Preschool_Enrollment.csv` `Collected School Year`: KYRC25 ['20202021'] -> KYRC26 ['20212022']
- `OVW_Preschool_Enrollment.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Preschool_Suspensions_and_Expulsions.csv` `Collected School Year`: KYRC25 ['20202021'] -> KYRC26 ['20212022']
- `OVW_Preschool_Suspensions_and_Expulsions.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Preschool_Suspensions_and_Expulsions.csv` `School Type`: added []; removed ['A6', 'D1']
- `OVW_Secondary_Enrollment.csv` `CO-OP`: added ['NKCES']; removed []
- `OVW_Secondary_Enrollment.csv` `CO-OP Code`: added ['905']; removed []
- `OVW_Student_Enrollment.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Student_Expulsions.csv` `Collected School Year`: KYRC25 ['20202021'] -> KYRC26 ['20212022']
- `OVW_Student_Expulsions.csv` `CO-OP`: added ['', 'GLEC']; removed ['GREC']
- `OVW_Student_Expulsions.csv` `CO-OP Code`: added ['']; removed []
- `OVW_Student_Membership.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Student_Retention_Grades_4_12.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Student_Suspensions.csv` `Collected School Year`: KYRC25 ['20202021'] -> KYRC26 ['20212022']
- `OVW_Student_Suspensions.csv` `CO-OP`: added ['', 'GLEC']; removed ['GREC']
- `OVW_Student_Suspensions.csv` `CO-OP Code`: added ['']; removed []
- `OVW_Student_to_Teacher_Ratio.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Student_to_Teacher_Ratio.csv` `School Type`: added []; removed ['']
- `OVW_Students_Taught_by_Ineffective_Teachers.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Students_Taught_by_Inexperienced_Teachers.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Students_Taught_by_Out_of_Field_Teachers.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Students_with_Disabilities_IEP.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Teacher_Certification_Data.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Teacher_Turnover.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Teacher_Working_Conditions.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Teacher_Working_Conditions.csv` `School Type`: added []; removed ['B1']
- `OVW_Teachers_Full_Time_Equivalent_FTE.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Teachers_by_Gender.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Teachers_by_Race_Ethnicity.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `OVW_Violent_Offenses.csv` `Collected School Year`: KYRC25 ['20202021'] -> KYRC26 ['20212022']
- `OVW_Violent_Offenses.csv` `CO-OP`: added ['', 'GLEC']; removed ['GREC']
- `OVW_Violent_Offenses.csv` `CO-OP Code`: added ['']; removed []
- `SAFE_Approaches_that_are_actively_implemented_school_wide.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `SAFE_Approaches_that_are_actively_implemented_school_wide.csv` `School Type`: added []; removed ['A6']
- `SAFE_Behavior_Events_by_Context.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `SAFE_Behavior_Events_by_Grade_Level.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `SAFE_Behavior_Events_by_Location.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `SAFE_Behavior_Events_by_Type.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `SAFE_Discipline_Resolutions.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `SAFE_Legal_Sanctions.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `SAFE_Precautionary_Measures.csv` `CO-OP`: added ['GLEC']; removed ['GREC']
- `SAFE_Precautionary_Measures.csv` `School Type`: added []; removed ['A6']

Co-op name `GREC` is replaced by `GLEC` on the files that list a CO-OP change above. That is a label change, not a column rename.

## Grain checks added after the automated diff

These are full-file checks from the 17 October 2026 audit. Re-count them if the blobs change. Narrative is in `notes/105--kyrc26-completeness-audit.md`.

- `KYRC26_OVW_Student_to_Teacher_Ratio.csv` has 1,432 school rows and zero `School Code` `999000` or `School Name` `All Schools` rows. The KYRC25 file has the state row (`999000`, ratio `15:01`) and 173 `All Schools` district rows.
- `KYRC26_OVW_Advanced_Coursework.csv` has 4,077 rows and 257 school codes. KYRC25 has 36,612 rows and 1,356 school codes. Neither year has a `999000` row. `Collected School Year` is `20212022` on every KYRC26 row and `20202021` on every KYRC25 row. `School Year` is the release year.
- Kindergarten composite state row, `All Students` / prior setting `All Students`: `55`, `38`, `7`, `45`.
- Behavior events by grade headers: `All Grades`, `PS`, `K`, `1st` through `12th`, `14th`. No `13th`.
- Suspensions: `School Year` `20252026`, `Collected School Year` `20212022` on every row. Neither KYRC25 nor KYRC26 has a `999000` row.

## School-code and co-op checks

- `KYRC26_ACCT_Crosstab_Mathematics.csv` school codes: lengths {6: 461939, 3: 13937}, six-digit 461939/475876, other samples ['901', '901', '901', '901', '901', '901', '901', '901']
- `KYRC26_ACCT_Crosstab_Reading.csv` school codes: lengths {6: 461939, 3: 13937}, six-digit 461939/475876, other samples ['901', '901', '901', '901', '901', '901', '901', '901']
- `KYRC26_ACCT_Crosstab_Science.csv` school codes: lengths {6: 380519, 3: 13104}, six-digit 380519/393623, other samples ['901', '901', '901', '901', '901', '901', '901', '901']
- `KYRC26_ACCT_Crosstab_SocialStudies.csv` school codes: lengths {6: 373382, 3: 12944}, six-digit 373382/386326, other samples ['901', '901', '901', '901', '901', '901', '901', '901']

## Package ZIP members

- `KYRC26_ACCT_Datasets.zip` (32806282 bytes) members=19; not in the individual link list: none
- `KYRC26_ADLF_Datasets.zip` (152836 bytes) members=3; not in the individual link list: none
- `KYRC26_ASMT_Datasets.zip` (9216886 bytes) members=11; not in the individual link list: none
- `KYRC26_CTE_Datasets.zip` (283606 bytes) members=6; not in the individual link list: none
- `KYRC26_EDOP_Datasets.zip` (1345447 bytes) members=11; not in the individual link list: none
- `KYRC26_OVW_Datasets.zip` (5851445 bytes) members=41; not in the individual link list: none
- `KYRC26_SAFE_Datasets.zip` (1781433 bytes) members=8; not in the individual link list: none
- Educational coop 2026 zip members (15): Accountable_Assessment_Performance_2026.CSV, Accountable_Profile_2026.CSV, Accountable_Summary_2026.CSV, Assessment_Performance_by_Grade_2026.CSV, College Admissions Exam_2026.CSV, English_Language_Proficiency_2026.CSV, English_Learners_Attainment_2026.CSV, Graduation_Rate_2026.CSV, Kindergarten_Screen_2026.CSV, Postsecondary_Readiness_2026.CSV, Quality_of_School_Climate_and_Safety_Survey_Elementary_School_2026.CSV, Quality_of_School_Climate_and_Safety_Survey_High_School_2026.CSV, Quality_of_School_Climate_and_Safety_Survey_Index_Scores_2026.CSV, Quality_of_School_Climate_and_Safety_Survey_Middle_School_2026.CSV, Quality_of_School_Climate_and_Safety_Survey_Questions_2026.CSV

