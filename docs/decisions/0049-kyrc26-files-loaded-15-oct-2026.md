# 0049 KYRC26 files were loaded on 15 October 2026

Status: unconfirmed

## Question

Are the KYRC26 bytes downloaded on 15 October 2026 the final public files?

## What KDE changed

`notes/103--2025-26-kyrc26-data-update-plan.md` records datasets Last-Modified of Thursday 15 October 2026, 10:00:09 through 10:00:11 GMT. The same note says the review window runs through 26 October 2026 and that both containers should be re-HEAD'd after that date. The 15 October release bulletin, re-fetched here, announces the public release. It does not, in the text this pass checked, state a 5 p.m. Eastern clock. https://content.govdelivery.com/accounts/KYDE/bulletins/42e02ae

The research cites SRC13 and SRC14 for a review close of 26 October 2026 at 5 p.m. Eastern. Those PDFs were not re-fetched. The date 26 October is in the note. The clock time is not.

## Options considered

Treat the 15 October bytes as final, or re-HEAD the registered KYRC26 files after 26 October 2026 and compare Content-MD5. The note already asks for the second option. No Scott comment was found that freezes the 15 October bytes.

## Decision

No decision found that treats the 15 October download as final. The note's instruction is to re-HEAD both containers after 26 October 2026.

## Who decided and how we know

The load date and the re-HEAD instruction are in `notes/103--2025-26-kyrc26-data-update-plan.md`. They are a record of what was downloaded and what to do next. They are not a choice to keep those bytes if KDE replaces them.

## Why

Reason not documented, because there is no choice to freeze the file. The note already gives the next check.

## Links

- `notes/103--2025-26-kyrc26-data-update-plan.md`
- REL-2026 https://content.govdelivery.com/accounts/KYDE/bulletins/42e02ae
- SRC13 https://www.education.ky.gov/districts/tech/sis/Documents/SRC13_SRC2025-2026EmbargoRelease.pdf (not re-fetched)
- SRC14 https://www.education.ky.gov/districts/tech/sis/Documents/SRC14_SRC2025-2026DataSharing.pdf (not re-fetched)
- [coverage-gaps.md](coverage-gaps.md)

## Code check

The registered KYRC26 files are the ones the note describes. This pass did not re-HEAD them. Not a code conflict.

## KDE guidance check

Verdict: possible conflict (timing). Flag only. No decision is changed.

KDE's public release is 15 October 2026. The pipeline note says the review window continues through 26 October 2026. Loading during an open review window can store a file KDE later replaces. The 5 p.m. Eastern close was not verified against SRC13 or SRC14.
