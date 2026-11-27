# DNNNN Alias KDE's "Non-Military" label to "Non-Military Dependent"

Status: accepted

Decision source: Scott, chat answer in Gremlin's chat, 27 Nov 2026, 12:29 PM ET, verified by Archivist. Repo comment: none.

## Question

KDE publishes the negative military group as "Non-Military Dependent" in 2022-23 and as "Non-Military" in 2024-26. Should every module store those labels as one `student_group`?

## What KDE changed

The 2022-23 assessment files publish `Non-Military Dependent`. The 2022-23 climate and safety index publishes `Non-Military`. The 2024-26 files that include this group publish `Non-Military`. The positive group stays `Military Dependent`. No KDE notice was found that names the two negative labels as one group or as two groups.

## Options considered

Leave the two labels as two `student_group` values, or map both to one label in the shared demographic yaml.

## Decision

27 November 2026, 12:29 PM ET. Scott's words (exact): "Approve the global Non-Military alias".

Gremlin's question said "for every module". That scope is Gremlin's framing. Scott picked the alias. The question noted that 0047 ruled out this kind of mapping for 2018-19. This record supersedes [0047](0047-workbook-demographic-labels-2018-19.md) for the Non-Military case only. 0047 stands for everything else. 2018-19 code mappings stay untouched.

The canonical stored label `Non-Military Dependent` is an implementation detail of brief F-1, not Scott's words. It matches the positive group `Military Dependent`.

Loading the republish-only KSA groups (F-2) is not decided.

## Who decided and how we know

Scott, chat answer in Gremlin's chat, 27 Nov 2026, 12:29 PM ET, verified by Archivist. The quote is Scott's words from DECISIONS-VERIFIED.md item 9. Repo comment: none.

## Why

Scott approved the alias. Reason beyond that approval: not documented. The shared map is where every module reads demographic labels, so one alias covers every module that uses it. That "every module" scope is Gremlin's framing of the question Scott answered.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/95 (still open)
- [0047](0047-workbook-demographic-labels-2018-19.md)
- [0043](0043-kyrc23-republish-only-groups.md) (the read-only audit; the load is not this record)
- `config/demographic_mappings.yaml`
- `notes/120--non-military-alias-and-kyrc23-republish-audit.md`
- DECISIONS-VERIFIED.md item 9 (Archivist, 27 Nov 2026, about 12:55 PM ET)

## Code check

Matches this record. `config/demographic_mappings.yaml` maps `Non-Military` and `non-military` to `Non-Military Dependent`, and `Non-Military` is not a separate canonical group. The 2018-19 code mappings, including `MIL`, `MilitaryConnected`, and the 2019 year-specific block, are unchanged. The alias does not add rows and does not change values. F-2 is not implemented.

`etl/advanced_coursework.py` keeps a module-local `"Military Connected": "Military Dependent"`. That label is the positive group. It does not mention `Non-Military`, so the alias does not require a change there.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE crosswalk that says `Non-Military` and `Non-Military Dependent` are the same group, or that they must stay separate.
