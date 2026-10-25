# 0043 Load only the groups that exist solely in the KYRC23 kindergarten republish

Status: accepted, code differs (follow-up open)

## Question

The KYRC23 kindergarten republish adds groups the original file does not have, and it also repeats keys the original file already supplies. Load the whole republish, or only the new groups?

## What KDE changed

The republish includes Non-Economically Disadvantaged, Non-English Learner, and Students without IEP. The decision counts 15,126 rows in that load, including 2,370 All Students rates. The research cites the 10 December 2024 commissioner's message for a variance sentence. That page was not re-fetched. It is context, not a rule for this load. https://content.govdelivery.com/accounts/KYDE/bulletins/3c68ec8

## Options considered

Skip every KYRC23 republish to avoid duplicate keys, load the full republish, or load only groups that exist solely in the republish.

## Decision

19 October 2026. Load only groups that exist solely in the KYRC23 kindergarten republish, without changing keys the original file supplies. Audit other modules that skip KYRC23 republishes.

## Who decided and how we know

The decision is in the body of issue #95, "Decision (Scott, 2026-10-19)." https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/95

The issue was opened on the `jscotthorn` account through the Cursor GitHub App. No separate confirming comment was required for this record. The body is the repo's statement of the choice.

## Why

The issue body states the choice and the row count. Reason for taking the new groups and leaving the overlapping keys: the overlapping keys would duplicate the original file. Reason beyond that: not documented.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/95 (still open)

## Code check

Does not match. Modules still skip the KYRC23 republish to avoid duplicate keys. Issue #95 is still open. Not resolved here.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE statement that says to load only the groups unique to a republish. The commissioner's variance sentence was not re-fetched and is not used as a rule.
