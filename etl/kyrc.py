"""Shared helpers for School Report Card (KYRC) files.

Script-specific parsers stay in their own modules. This module only holds
rules that more than one pipeline needs: which release year a filename refers
to, and the grade labels KYRC25/KYRC26 use in place of the older "Grade N"
headers.
"""

from __future__ import annotations

import re
from typing import Optional

# "file_2025.CSV" and "Graduation_Rate_2025.CSV"
_FILENAME_YEAR = re.compile(r"_(\d{4})\.")
# "KYRC26_OVW_Chronic_Absenteeism.csv" has no "_2026." suffix.
_KYRC_RELEASE = re.compile(r"KYRC(\d{2})", re.IGNORECASE)

_ORDINAL_TO_GRADE = {
    "1st": "Grade 1",
    "2nd": "Grade 2",
    "3rd": "Grade 3",
    "4th": "Grade 4",
    "5th": "Grade 5",
    "6th": "Grade 6",
    "7th": "Grade 7",
    "8th": "Grade 8",
    "9th": "Grade 9",
    "10th": "Grade 10",
    "11th": "Grade 11",
    "12th": "Grade 12",
    "13th": "Grade 13",
    "14th": "Grade 14",
}


def release_year_from_filename(source_file: str) -> Optional[str]:
    """Return the ending school year encoded in a KDE filename.

    A four-digit ``_YYYY.`` suffix wins, because the temporary accountability
    files are named that way (``Graduation_Rate_2025.CSV`` is school year
    2024-25). A ``KYRC26`` token maps to 2026. Returns None when the name
    does not encode a year.
    """
    if not source_file:
        return None

    suffix = _FILENAME_YEAR.search(source_file)
    if suffix:
        return suffix.group(1)

    kyrc = _KYRC_RELEASE.search(source_file)
    if kyrc:
        return str(2000 + int(kyrc.group(1)))

    return None


def normalize_kyrc_grade_token(value: object) -> str:
    """Map a KYRC25/KYRC26 grade label onto the historical grade token.

    Behavior-by-grade files use ``PS``, ``K``, and ``1st`` through ``14th``.
    Older files and the events parser use ``Preschool``, ``K``, and
    ``Grade 1``. Labels that are already in the historical form are returned
    unchanged. This helper is not applied inside individual ETLs; the script
    that owns the file decides whether to call it.
    """
    if value is None:
        return ""

    text = str(value).strip()
    key = text.lower()

    if key in {"ps", "preschool"}:
        return "Preschool"
    if key in {"k", "kindergarten"}:
        return "K"
    if key in {"all grades", "all_grades"}:
        return "All Grades"

    ordinal = _ORDINAL_TO_GRADE.get(key)
    if ordinal:
        return ordinal

    return text
