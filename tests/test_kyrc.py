"""Tests for shared KYRC filename and grade helpers."""

from etl.kyrc import normalize_kyrc_grade_token, release_year_from_filename


def test_release_year_prefers_four_digit_suffix():
    assert release_year_from_filename("Graduation_Rate_2025.CSV") == "2025"
    assert release_year_from_filename("KYRC26_OVW_Chronic_Absenteeism.csv") == "2026"
    assert release_year_from_filename("KYRC25_SAFE_Discipline_Resolutions.csv") == "2025"
    assert release_year_from_filename("chronic_absenteeism_2023.csv") == "2023"
    assert release_year_from_filename("no-year.csv") is None
    assert release_year_from_filename("") is None


def test_grade_tokens_match_historical_labels():
    assert normalize_kyrc_grade_token("PS") == "Preschool"
    assert normalize_kyrc_grade_token("K") == "K"
    assert normalize_kyrc_grade_token("1st") == "Grade 1"
    assert normalize_kyrc_grade_token("10th") == "Grade 10"
    assert normalize_kyrc_grade_token("14th") == "Grade 14"
    assert normalize_kyrc_grade_token("All Grades") == "All Grades"
    assert normalize_kyrc_grade_token("Grade 3") == "Grade 3"
    assert normalize_kyrc_grade_token("Preschool") == "Preschool"
