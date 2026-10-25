# 0047 Read 2018-19 workbook demographic labels from the workbook

Status: accepted, code differs (follow-up open)

## Question

Some 2018-19 workbooks use DEMO codes that do not match the labels in that workbook's own DGROUP sheet. Should one shared mapper keep winning?

## What KDE changed

The workbook's own coversheet or DGROUP labels disagree with the shared demographic map in at least one series. The check named in the decision is the 2019 state disability count against the 2020 count.

## Options considered

Leave `demographic_mappings.yaml` as the only map, or where the swap exists, read the workbook label before the shared mapper.

## Decision

18 October 2026. One cross-module audit. For 2018-19 workbooks, compare DEMO codes with that workbook's own DGROUP or coversheet labels. Use the 2019 state disability count against the 2020 count as the check. Where the swap exists, read the workbook label before the shared mapper, the same pattern as gifted. `demographic_mappings.yaml` stays unchanged.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/76#issuecomment-6251820847 posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comment says the workbook's own label is the source when it disagrees with the shared map. The gifted pipeline is the pattern already used. Reason the codes were swapped in the source file: not documented.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/76 (still open)
- `demographic_mappings.yaml` (unchanged by this decision)

## Code check

Does not match as an implemented audit. Issue #76 is still open. `demographic_mappings.yaml` was left unchanged on purpose. The per-workbook label read has not landed. Not resolved here.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE crosswalk that says the 2018-19 DEMO codes match, or do not match, the DGROUP labels.
