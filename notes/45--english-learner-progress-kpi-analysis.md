# 40 - English Learner Progress KPI Review

## Summary
Review of `etl/english_learner_progress.py` to document how current KPIs are generated and identify potential additional metrics from the raw data.

## Current KPI Mapping
- The ETL normalizes the education level and four proficiency score columns. Key mapping lines:
```
            "Level": "level",
            "LEVEL": "level",
            "Percentage Of Value Table Score Of 0": "percentage_score_0",
            "PERCENTAGE OF VALUE TABLE SCORE OF 0": "percentage_score_0",
            "Percentage Of Value Table Score Of 60 And 80": "percentage_score_60_80",
            "PERCENTAGE OF VALUE TABLE SCORE OF 60 AND 80": "percentage_score_60_80",
            "Percentage Of Value Table Score Of 100": "percentage_score_100",
            "PERCENTAGE OF VALUE TABLE SCORE OF 100": "percentage_score_100",
            "Percentage Of Value Table Score Of 140": "percentage_score_140",
            "PERCENTAGE OF VALUE TABLE SCORE OF 140": "percentage_score_140",
```
- `extract_metrics()` maps each percentage directly to a KPI suffix by education level:
```
    score_map = {
        "english_learner_score_0": row.get("percentage_score_0", pd.NA),
        "english_learner_score_60_80": row.get("percentage_score_60_80", pd.NA),
        "english_learner_score_100": row.get("percentage_score_100", pd.NA),
        "english_learner_score_140": row.get("percentage_score_140", pd.NA),
    }
    for base_name, value in score_map.items():
        if pd.notna(value):
            metrics[f"{base_name}_{level}"] = float(value)
```
- Suppressed rows still output all four metrics with `NaN` values.

These KPIs correspond exactly to the source columns for the four score bands. A prior note explains removal of calculated progress rates in favor of these direct scores:
```
- `english_learner_score_0` (was `english_learner_beginning_rate`)
- `english_learner_score_60_80` (was `english_learner_intermediate_rate`)
- `english_learner_score_100` (was `english_learner_advanced_rate`)
- `english_learner_score_140` (was `english_learner_mastery_rate`)
- Removed artificial aggregation metrics (`*_progress_rate`, `*_proficiency_rate`)
```

## Source Data Observations
Sample raw rows used in tests include these additional fields:
```
'CO-OP', 'CO-OP Code', 'School Type'
```
Running `python3 data/prepare_kde_data.py english_learner_progress` downloaded five files for 2020–2024. The 2022–2024 files match the sample rows above and contain only the four percentage score columns. The 2021 file is empty. The 2020 file exposes additional fields:
```
'TOTAL ENROLLMENT', 'NUMBER TESTED', 'PARTICIPATION RATE',
'PER 1', 'PER 2', 'PER 3', 'PER 4', 'PER 5', 'PER 6'
```
These appear to provide counts and participation information for ACCESS testing. No totals or counts are included in the newer files.
The dataset description in the HTML index mentions "progress rates" and "proficiency rates," but those do not appear as separate columns in the available data.

## Potential Additional KPIs
- If the raw files include counts of English learner students or the number reaching each score band, those could be exposed as `{indicator}_count_{level}` metrics.
- Any state goal or longitudinal progress fields (e.g., "progress toward state goals") would also be viable metrics if present.
- Current files do not expose such values, so only the four percentage score KPIs are produced.

