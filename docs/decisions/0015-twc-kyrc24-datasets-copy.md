# 0015 Load the datasets KYRC24 teacher working conditions file

Status: accepted

## Question

The registered historical KYRC24 teacher working conditions file starts at district 101 and differs from the datasets copy. Which copy is the 2024 source?

## What KDE changed

`notes/105.md`: the historical file lacks districts 001-095 and disagrees on some district totals. The datasets copy matches KYRC25 on shared keys. CWM-2024-12 said teacher working conditions was not populating for 2023-24 (https://content.govdelivery.com/accounts/KYDE/bulletins/3c68ec8, not re-fetched). No KDE sentence was found that names which file copy to use.

## Options considered

18 October, with #58, #64, and #66: switch the copies in one follow-up. 20 October, item 4: #105 loads the complete datasets copy, per the #95 precedent.

## Decision

20 October 2026. The datasets KYRC24 file is the 2024 source. Craft Academy rows that exist only on the historical file, and school `777135` from KYRC25, are added and stored as 2024 (`notes/105.md`).

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/62#issuecomment-6300574763

The 18 October copy decision is https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/62#issuecomment-6251855652 item 4, which pointed at #58.

## Why

The 20 October comment says "#105 loads the complete datasets copy of KYRC24 as the 2024 source, per the #95 precedent." The precedent is "load what the republish adds." A longer reason is the issue body: the historical file is missing 209 codes and has district totals that differ.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/62
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/105
- `notes/105.md`
- `config/kde_sources.yaml`

## Code check

Matches. `config/kde_sources.yaml` registers the datasets KYRC24 file (`url: kyrc25`) and saves the historical file under another name. The parser comment says it loads only the keys the datasets file does not already supply.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

No KDE page was found that says to prefer the datasets copy of this file. CWM-2024-12 says the 2023-24 working-conditions display was empty at that date. It does not choose a file.
