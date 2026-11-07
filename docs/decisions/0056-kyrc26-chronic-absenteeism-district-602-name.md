# 0056 Keep the refreshed KYRC26 chronic absenteeism file as published, including district 602's name

Status: accepted

Decision source: the 7 November 2026 instruction carried out by this record. That instruction states that Scott approved adopting the refreshed KYRC26 chronic absenteeism file as published and retaining a warning about district 602. No GitHub Decision comment was found when this record was written. Repo comment: pending.

## Question

The refreshed KYRC26 chronic absenteeism overview matches the 15 October copy on every measure cell and changes District Name on district 602. Which file is stored, and is the name corrected?

## What KDE changed

`KYRC26_OVW_Chronic_Absenteeism.csv`. The 15 October datasets copy is md5 `07d218e589daaac167e0faacabecfe8e`. The UAT copy matches that md5. The refreshed datasets copy is md5 `2c317303f3f136bc102deee832c3cc95`.

The 7 November 2026 file comparison, not re-counted in this workspace:

- Both files have 19 columns and 430,848 data rows.
- No duplicate key on School Code, Grade, and Demographic.
- Sorted on every column except District Name, every row matches.
- The only cell change is District Name on district 602. That is 576 rows: school code `602000` (All Schools) and school code `602176` (Kentucky School for the Blind). The 15 October name is `Kentucky School for the Blind`. The refreshed name is `Kentucky School for the Deaf District`.
- The district-school list, old and new identical for these two districts, names 602 `Kentucky School for the Blind District` and 603 `Kentucky School for the Deaf District`.
- Eighteen other refreshed files still label 602 as the School for the Blind.
- Row order changed. Fayette (`165`, 19,296 rows) and the state rows match. No measure cell changes.

No KDE notice of this rename was found. The raw file is not in git. `data/raw/` is ignored. The parent's regen reads the staged copy.

## Options considered

Store the refreshed file and replace district 602's name with the district-school list's Blind name. Store the 15 October copy instead. Store the refreshed file as published and keep a warning on the name.

## Decision

7 November 2026. Store the refreshed file as published. Do not rewrite district 602's District Name. Keep the warning that the published name is the Deaf district's name and that 602 is the School for the Blind.

The school code stays the school id. District number 602 stays 602. The measure cells stay the published numbers.

## Who decided and how we know

The 7 November 2026 instruction for this change states that Scott approved adopting the refreshed file as published and retaining the district 602 warning. This file is the repo record of that instruction. A GitHub Decision comment was not found. Repo comment: pending.

## Why

The instruction says to preserve the published source values and to warn about district 602. [0054](0054-store-values-as-kde-publishes.md) stores what KDE published. Substituting the district-school list's name would make this file disagree with the overview KDE published. The comparison found no measure change, so the name is not a reason to keep the older copy.

## Links

- `notes/119--kyrc26-chronic-absenteeism-district-602-name.md`
- `notes/104--kyrc26-file-to-etl-mapping.md` (the 15 October md5, and the 7 November follow-up)
- `config/kde_sources.yaml` (`chronic_absenteeism`, the KYRC26 entry)
- `KPIS.md`, Chronic Absenteeism
- [kde-republishes.md](kde-republishes.md) row R27
- [0052](0052-republish-supersedes-earlier.md)
- [0054](0054-store-values-as-kde-publishes.md)

## Code check

Current code on this branch matches the decision. `ChronicAbsenteeismETL` does not override `extract_school_id` or `create_kpi_template`.

`BaseETL.extract_school_id` reads `school_code` only. It does not read District Name or District Number. A missing school code raises. `602000` and `602176` stay those school ids.

`BaseETL.create_kpi_template` copies `district_name` into `district` and copies `district_number` into its own column. There is no join to `district_school_list` and no name-to-id map in this module. `All Schools` is stored as `---District Total---`. That rewrite is the school-name convention already used for every district, including Fayette `165000`. It does not change the district label.

`convert_to_kpi_format` emits the measure columns from the same row. The district string is not a key. `etl_runner.combine_kpi_files` concatenates the processed files. It does not join on `district`.

The wrong name therefore does not move district 602's rates, counts, or enrollment onto district 603's school codes. A consumer that groups only on the district string `Kentucky School for the Deaf District` would place these 602 rows with rows other files store under that string.

`analysis/scripts/chronic_absenteeism_analysis.py` uses school id as its merge strategy and keeps years 2023-2025, so these 2026 rows do not enter that dataset. Grade span joins on `school_id` and `year`. In `BaseAnalysisDataset.merge_covariates`, teacher metrics join on `year`, `school_id`, and `district`, and financial metrics join on `year` and `district` only. A later run that included 2026 would miss teacher covariates for `602176` when the teacher file still says `Kentucky School for the Blind District`, and the financial join could attach spending stored under the Deaf name. That path is not taken by the current chronic analysis, and this record does not change it. The dashboard Fayette filter matches the district string `Fayette`. The Deaf label does not match, so district 602 stays out of that extract, as the Blind label would. The dashboard is not edited.

No numeric output of this ETL changes because of the name. This record does not repair the source and does not add a name crosswalk.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE notice that district 602's chronic-absenteeism name should be the School for the Deaf, or that a pipeline should replace a published district name with the district-school list. The district-school list is the contradicting KDE file named in the comparison. It was not re-opened here.
