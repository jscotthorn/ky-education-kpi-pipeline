# Kentucky Summative Assessment Pipeline Implementation

## Date: 2025-07-22

Implemented a new ETL pipeline to process Kentucky Summative Assessment files. The
pipeline handles both the accountability files broken down by **school level** and
the grade level assessment files. Metrics are generated for each subject and
performance band with suffixes for the grade or school level.

### Key Features
- Subject and level normalization for historical files
- Validation of rate columns (0-100 range)
- Support for suppressed records with metric placeholders
- Unit and end-to-end tests covering grade and level inputs

