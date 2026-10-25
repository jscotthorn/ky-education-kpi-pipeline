# 0013 Do not bridge Teaching Environment into School Climate

Status: accepted

Supersedes the 18 October 2026 instruction to bridge the series. That earlier comment is https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/62#issuecomment-6251855652 item 2. This record is the later choice.

## Question

KDE's KYRC23 file relabels 2023 Teaching Environment values as School Climate. Should 2020-23 Teaching Environment be copied onto the School Climate metric?

## What KDE changed

Issue #62: KYRC23 relabels 2023 Teaching Environment values, unchanged, as School Climate. The research says REL-2020 calls the 2020 constructs "School Climate, Managing Student Behavior and School Leadership," and the glossary lists "teaching environment, managing student behavior, school leadership." Impact Kentucky's site says the 2022 survey was redesigned and "aligns closely with the 2020 survey" (https://www.impactky.org/surveycontent, not re-fetched). Not found: a KDE statement that the two names are the same measure, or that they are not.

## Options considered

18 October: bridge 2020-23 Teaching Environment into School Climate. 20 October: do not join them.

## Decision

20 October 2026. Item 2: 2020/2022 `teaching environment` and 2024+ `school climate` stay separate series. They will not be joined.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/62#issuecomment-6300574763

The 25 October close comment says the bridge was declined by Scott on 20 October and documented in #115: https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/62#issuecomment-6399042643

## Why

Quoted from the 20 October comment: "KDE hasn't published them as equivalent, so they won't be joined."

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/62
- `notes/105.md`
- `notes/115--cleanup-batch1.md`
- `KPIS.md` and `README.md` (teacher working conditions)
- [0014](0014-impact-kentucky-one-wave.md)

## Code check

Matches. `notes/105.md` says Teaching Environment is not copied onto School Climate. `README.md` says the two series have no bridge. `config/kde_sources.yaml` skips `KYRC23_OVW_Teacher_Working_Conditions.csv`, which is the file that relabels the construct.

## KDE guidance check

Verdict: possible conflict. Flag only. The flag is on the stated reason, not an instruction to join the series.

REL-2020, not re-fetched, uses the name "School Climate" for the 2020 survey. KYRC23 relabels the values. The written reason "KDE hasn't published them as equivalent" is therefore only partly supported. KDE also has not said they are the same measure. The 2022 redesign is a separate fact. The decision stays as written.
