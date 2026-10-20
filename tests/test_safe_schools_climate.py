"""Unit tests for Safe Schools Climate ETL module."""
import shutil

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.safe_schools_climate import (
    SafeSchoolsClimateETL,
    _assert_unique_keys,
    calculate_policy_compliance_rate,
    clean_index_scores,
    transform,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = Path(__file__).parent / "fixtures" / "safe_schools_climate"
FIXTURE_COPIES = {
    "kyrc26_index_scores.csv": "KYRC26_ACCT_Index_Scores.csv",
    "kyrc25_index_scores.csv": "KYRC25_ACCT_Index_Scores.csv",
    "kyrc24_index_scores.csv": "KYRC24_ACCT_Index_Scores.csv",
    "kyrc26_precautionary.csv": "KYRC26_SAFE_Precautionary_Measures.csv",
    "kyrc25_precautionary.csv": "KYRC25_SAFE_Precautionary_Measures.csv",
    "accountability_profile_2022_state.csv": "accountability_profile_2022.csv",
}


class TestCalculatePolicyComplianceRate:
    """Test policy compliance rate calculation."""
    
    def test_all_yes_responses(self):
        """Test with all Yes responses."""
        row = pd.Series({
            'visitors_sign_in': 'Yes',
            'classroom_doors_lock': 'Yes',
            'classroom_phones': 'Yes'
        })
        policy_columns = ['visitors_sign_in', 'classroom_doors_lock', 'classroom_phones']
        
        result = calculate_policy_compliance_rate(row, policy_columns)
        assert result == 100.0
    
    def test_mixed_responses(self):
        """Test with mixed Yes/No responses."""
        row = pd.Series({
            'visitors_sign_in': 'Yes',
            'classroom_doors_lock': 'No',
            'classroom_phones': 'Yes',
            'annual_climate_survey': 'No'
        })
        policy_columns = ['visitors_sign_in', 'classroom_doors_lock', 
                         'classroom_phones', 'annual_climate_survey']
        
        result = calculate_policy_compliance_rate(row, policy_columns)
        assert result == 50.0
    
    def test_case_insensitive(self):
        """Test that YES, yes, Yes are all handled."""
        row = pd.Series({
            'visitors_sign_in': 'YES',
            'classroom_doors_lock': 'yes',
            'classroom_phones': 'Yes'
        })
        policy_columns = ['visitors_sign_in', 'classroom_doors_lock', 'classroom_phones']
        
        result = calculate_policy_compliance_rate(row, policy_columns)
        assert result == 100.0
    
    def test_missing_values(self):
        """Test with missing values."""
        row = pd.Series({
            'visitors_sign_in': 'Yes',
            'classroom_doors_lock': pd.NA,
            'classroom_phones': 'No'
        })
        policy_columns = ['visitors_sign_in', 'classroom_doors_lock', 'classroom_phones']
        
        result = calculate_policy_compliance_rate(row, policy_columns)
        assert result == 50.0  # Only counts non-NA values
    
    def test_all_missing(self):
        """Test with all missing values."""
        row = pd.Series({
            'visitors_sign_in': pd.NA,
            'classroom_doors_lock': pd.NA
        })
        policy_columns = ['visitors_sign_in', 'classroom_doors_lock']
        
        result = calculate_policy_compliance_rate(row, policy_columns)
        assert pd.isna(result)


class TestCleanIndexScores:
    """Test index score cleaning."""
    
    def test_valid_scores(self):
        """Test with valid scores."""
        df = pd.DataFrame({
            'climate_index': [75.5, 80.2, 65.0],
            'safety_index': [70.0, 85.5, 60.0]
        })
        
        result = clean_index_scores(df)
        assert result['climate_index'].tolist() == [75.5, 80.2, 65.0]
        assert result['safety_index'].tolist() == [70.0, 85.5, 60.0]
    
    def test_invalid_scores(self):
        """Test with scores outside 0-100 range."""
        df = pd.DataFrame({
            'climate_index': [75.5, -10, 105.0],
            'safety_index': [70.0, 85.5, 150.0]
        })
        
        result = clean_index_scores(df)
        assert result['climate_index'].tolist()[0] == 75.5
        assert pd.isna(result['climate_index'].tolist()[1])
        assert pd.isna(result['climate_index'].tolist()[2])
        assert result['safety_index'].tolist()[0] == 70.0
        assert pd.isna(result['safety_index'].tolist()[2])
    
    def test_non_numeric_values(self):
        """Test with non-numeric values."""
        df = pd.DataFrame({
            'climate_index': ['75.5', 'N/A', '80'],
            'safety_index': ['70', '', '85.5']
        })
        
        result = clean_index_scores(df)
        assert result['climate_index'].tolist()[0] == 75.5
        assert pd.isna(result['climate_index'].tolist()[1])
        assert result['climate_index'].tolist()[2] == 80.0


class TestSafeSchoolsClimateETL:
    """Test SafeSchoolsClimateETL class."""
    
    @pytest.fixture
    def etl(self):
        """Create ETL instance."""
        return SafeSchoolsClimateETL('safe_schools_climate')
    
    def test_column_mappings(self, etl):
        """Test column mappings are defined."""
        mappings = etl.module_column_mappings

        assert 'Are visitors to the building required to sign-in?' in mappings
        assert mappings['Are visitors to the building required to sign-in?'] == 'visitors_sign_in'
        assert 'Visitors required to sign-in' in mappings
        assert mappings['Visitors required to sign-in'] == 'visitors_sign_in'
        assert 'CLIMATE INDEX' in mappings
        assert mappings['CLIMATE INDEX'] == 'climate_index'
    
    def test_extract_metrics_precautionary(self, etl):
        """Test metric extraction for precautionary measures."""
        row = pd.Series({
            'visitors_sign_in': 'Yes',
            'classroom_doors_lock': 'No',
            'classroom_phones': 'Yes',
            'annual_climate_survey': 'Yes'
        })
        
        metrics = etl.extract_metrics(row)
        assert 'safety_policy_compliance_rate' in metrics
        assert metrics['safety_policy_compliance_rate'] == 75.0

    def test_extract_metrics_precautionary_synonyms(self, etl):
        """Ensure synonyms from older files are recognized."""
        df = pd.DataFrame([
            {
                'Visitors required to sign-in': 'Yes',
                'All classroom doors lock from the inside.': 'Yes',
                'All classrooms have access to telephone': 'No',
                'Student survey data collected and used': 'Yes'
            }
        ])

        norm_df = etl.normalize_column_names(df)
        metrics = etl.extract_metrics(norm_df.iloc[0])
        assert 'safety_policy_compliance_rate' in metrics
        assert metrics['safety_policy_compliance_rate'] == 75.0
    
    def test_extract_metrics_index_scores(self, etl):
        """Test metric extraction for index scores."""
        row = pd.Series({
            'climate_index': 75.5,
            'safety_index': 80.0
        })
        
        metrics = etl.extract_metrics(row)
        assert 'climate_index_score' in metrics
        assert 'safety_index_score' in metrics
        assert metrics['climate_index_score'] == 75.5
        assert metrics['safety_index_score'] == 80.0
    
    def test_extract_metrics_missing_values(self, etl):
        """Test metric extraction with missing values."""
        row = pd.Series({
            'climate_index': pd.NA,
            'safety_index': 80.0
        })
        
        metrics = etl.extract_metrics(row)
        # When climate_index is NA, it's not included in metrics
        assert 'climate_index_score' not in metrics
        assert metrics['safety_index_score'] == 80.0
    
    def test_extract_metrics_accountability_profile(self, etl):
        """Test metric extraction for accountability profile data."""
        row = pd.Series({
            'climate_safety_combined_rate': 74.2
        })
        
        metrics = etl.extract_metrics(row)
        assert 'climate_index_score' in metrics
        assert 'safety_index_score' in metrics
        assert metrics['climate_index_score'] == 74.2
        assert metrics['safety_index_score'] == 74.2
    
    def test_get_suppressed_metric_defaults(self, etl):
        """Test suppressed metric defaults based on data type."""
        # Test with index score data
        row_index = pd.Series({'climate_index': 75.0, 'safety_index': 70.0})
        defaults = etl.get_suppressed_metric_defaults(row_index)
        assert 'climate_index_score' in defaults
        assert 'safety_index_score' in defaults
        assert 'safety_policy_compliance_rate' not in defaults
        
        # Test with policy data
        row_policy = pd.Series({'visitors_sign_in': 'Yes', 'classroom_doors_lock': 'No'})
        defaults = etl.get_suppressed_metric_defaults(row_policy)
        assert 'climate_index_score' not in defaults
        assert 'safety_index_score' not in defaults
        assert 'safety_policy_compliance_rate' in defaults
        
        # Test with no relevant data
        row_empty = pd.Series({})
        defaults = etl.get_suppressed_metric_defaults(row_empty)
        assert len(defaults) == 0
    
    def test_get_files_to_process(self, etl, tmp_path):
        """Test file selection logic."""
        # Create test directory structure
        module_dir = tmp_path / 'safe_schools_climate'
        module_dir.mkdir()
        
        # Create test files - only the ones we want to process
        (module_dir / 'KYRC24_ACCT_Index_Scores.csv').touch()
        (module_dir / 'KYRC24_SAFE_Precautionary_Measures.csv').touch()
        (module_dir / 'accountability_profile_2023.csv').touch()
        (module_dir / 'precautionary_measures_2022.csv').touch()
        # Create files that should be excluded
        (module_dir / 'KYRC24_ACCT_Survey_Results.csv').touch()
        (module_dir / 'quality_of_school_climate_and_safety_survey_elementary_school_2023.csv').touch()
        
        files = etl.get_files_to_process(tmp_path)
        file_names = [f.name for f in files]
        
        # Should only include streamlined file patterns
        assert len(files) == 4
        assert 'KYRC24_ACCT_Index_Scores.csv' in file_names
        assert 'KYRC24_SAFE_Precautionary_Measures.csv' in file_names
        assert 'accountability_profile_2023.csv' in file_names
        assert 'precautionary_measures_2022.csv' in file_names
        # Should exclude survey files
        assert 'KYRC24_ACCT_Survey_Results.csv' not in file_names
        assert 'quality_of_school_climate_and_safety_survey_elementary_school_2023.csv' not in file_names


def _run(raw_root: Path, proc: Path) -> pd.DataFrame:
    proc.mkdir(parents=True, exist_ok=True)
    transform(raw_root, proc, {})
    output = proc / "safe_schools_climate.csv"
    return pd.read_csv(output, dtype={"school_id": "string", "year": "string"})


def _cell(frame: pd.DataFrame, **filters) -> pd.Series:
    matched = frame
    for column, expected in filters.items():
        matched = matched[matched[column].astype("string") == str(expected)]
    assert len(matched) == 1, (filters, len(matched))
    return matched.iloc[0]


def _absent(frame: pd.DataFrame, **filters) -> None:
    matched = frame
    for column, expected in filters.items():
        matched = matched[matched[column].astype("string") == str(expected)]
    assert matched.empty, filters


class TestKyrc26ClimateFiles:
    """KYRC26 index levels, suppression, and the KYRC25 registration."""

    def test_kyrc25_and_kyrc26_files_are_selected(self, tmp_path):
        """The filename list names KYRC25 and KYRC26 and skips the rejected names."""
        module_dir = tmp_path / "safe_schools_climate"
        module_dir.mkdir()
        selected = (
            "KYRC25_ACCT_Index_Scores.csv",
            "KYRC26_ACCT_Index_Scores.csv",
            "KYRC25_SAFE_Precautionary_Measures.csv",
            "KYRC26_SAFE_Precautionary_Measures.csv",
        )
        rejected = (
            "KYRC23_ACCT_Index_Scores.csv",
            "KYRC23_SAFE_Precautionary_Measures.csv",
            "Quality_of_School_Climate_and_Safety_Survey_Index_Scores_2025.CSV",
            "Quality_of_School_Climate_and_Safety_Survey_Index_Scores_2026.CSV",
        )
        for name in selected + rejected:
            (module_dir / name).touch()

        names = [path.name for path in SafeSchoolsClimateETL("safe_schools_climate").get_files_to_process(tmp_path)]
        for name in selected:
            assert name in names
        for name in rejected:
            assert name not in names

    def test_rejected_climate_filenames_are_not_loaded(self, tmp_path):
        """A leftover KYRC23 or temporary index file does not write a second 2023 or 2026."""
        module_dir = tmp_path / "safe_schools_climate"
        module_dir.mkdir()
        rejected = module_dir / "KYRC23_ACCT_Index_Scores.csv"
        rejected.write_text(
            "School Year,District Name,School Name,School Code,Demographic,Level,Suppressed,Climate Index,Safety Index\n"
            "20222023,All Districts,All Schools,999000,All Students,High School,N,1.1,1.1\n",
            encoding="utf-8",
        )
        temporary = module_dir / "Quality_of_School_Climate_and_Safety_Survey_Index_Scores_2026.CSV"
        temporary.write_text(
            "School Year,District Name,School Name,School Code,Demographic,Level,Suppressed,Climate Index,Safety Index\n"
            "20252026,All Districts,All Schools,999,All Students,HS,N,9.9,9.9\n",
            encoding="utf-8",
        )
        etl = SafeSchoolsClimateETL("safe_schools_climate")
        result = etl.process_files([rejected, temporary])
        assert result.empty

    def test_stringdtype_comma_star_zero_and_blank(self, tmp_path):
        """A comma survives StringDtype, a star is NA/Y, a blank is skipped, and 0 is kept."""
        module_dir = tmp_path / "raw" / "safe_schools_climate"
        module_dir.mkdir(parents=True)
        (module_dir / "KYRC26_ACCT_Index_Scores.csv").write_text(
            "School Year,District Name,School Name,School Code,Demographic,Level,Suppressed,Climate Index,Safety Index\n"
            '20252026,Test District,Comma School,001010,All Students,High School,N,"1,002.5",0\n'
            "20252026,Test District,Star School,001020,All Students,High School,Y,*,*\n"
            "20252026,Test District,Partial School,001040,All Students,Middle School,N,*,61.2\n"
            "20252026,Test District,Blank School,001030,All Students,Elementary School,N,,75.0\n",
            encoding="utf-8",
        )
        frame = _run(tmp_path / "raw", tmp_path / "processed")

        comma = _cell(frame, school_id="001010", metric="climate_index_score_high")
        assert comma["value"] == 1002.5
        assert comma["suppressed"] == "N"
        zero = _cell(frame, school_id="001010", metric="safety_index_score_high")
        assert zero["value"] == 0.0
        assert zero["suppressed"] == "N"

        for metric in ("climate_index_score_high", "safety_index_score_high"):
            starred = _cell(frame, school_id="001020", metric=metric)
            assert pd.isna(starred["value"])
            assert starred["suppressed"] == "Y"

        partial_climate = _cell(frame, school_id="001040", metric="climate_index_score_middle")
        assert pd.isna(partial_climate["value"])
        assert partial_climate["suppressed"] == "Y"
        partial_safety = _cell(frame, school_id="001040", metric="safety_index_score_middle")
        assert partial_safety["value"] == 61.2
        assert partial_safety["suppressed"] == "N"

        blank_safety = _cell(frame, school_id="001030", metric="safety_index_score_elementary")
        assert blank_safety["value"] == 75.0
        _absent(frame, school_id="001030", metric="climate_index_score_elementary")

    def test_assert_unique_keys_raises_on_a_duplicate(self):
        """A repeated year, school, student group, and metric is a hard error."""
        frame = pd.DataFrame({
            "year": ["2026", "2026"],
            "school_id": ["006010", "006010"],
            "student_group": ["All Students", "All Students"],
            "metric": ["climate_index_score_elementary", "climate_index_score_elementary"],
        })
        with pytest.raises(ValueError, match="duplicate-key"):
            _assert_unique_keys(frame)

    def test_registered_sources_include_kyrc25(self):
        """KYRC25 is the 2024-25 gap. KYRC23 and the survey stay unregistered."""
        import yaml

        config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
        names = []
        hosts = {}
        for entry in config["raw_directories"]["safe_schools_climate"]:
            if isinstance(entry, dict):
                names.append(entry["file"])
                hosts[entry["file"]] = entry.get("url")
            else:
                names.append(str(entry))
        assert hosts["KYRC25_ACCT_Index_Scores.csv"] == "kyrc25"
        assert hosts["KYRC25_SAFE_Precautionary_Measures.csv"] == "kyrc25"
        assert hosts["KYRC26_ACCT_Index_Scores.csv"] == "kyrc26"
        assert hosts["KYRC26_SAFE_Precautionary_Measures.csv"] == "kyrc26"
        assert "KYRC24_ACCT_Index_Scores.csv" in names
        assert "KYRC24_SAFE_Precautionary_Measures.csv" in names
        assert isinstance(
            next(entry for entry in config["raw_directories"]["safe_schools_climate"]
                 if entry == "KYRC24_SAFE_Precautionary_Measures.csv"),
            str,
        )
        for name in (
            "KYRC23_ACCT_Index_Scores.csv",
            "KYRC23_SAFE_Precautionary_Measures.csv",
            "Quality_of_School_Climate_and_Safety_Survey_Index_Scores_2026.CSV",
        ):
            assert name not in names

    def test_real_rows_keep_level_coop_and_outside_doors(self, tmp_path):
        """Real KDE rows: three levels, a star, GREC and GLEC, and outside doors."""
        module_dir = tmp_path / "raw" / "safe_schools_climate"
        module_dir.mkdir(parents=True)
        for source_name, dest_name in FIXTURE_COPIES.items():
            shutil.copy(FIXTURE_DIR / source_name, module_dir / dest_name)
        frame = _run(tmp_path / "raw", tmp_path / "processed")
        key = ["year", "school_id", "student_group", "metric"]
        assert not frame.duplicated(key).any()

        state_levels = {
            "elementary": (80.0, 74.8),
            "middle": (71.3, 65.5),
            "high": (68.6, 64.0),
        }
        for level, (climate, safety) in state_levels.items():
            climate_row = _cell(
                frame, year="2026", school_id="999000", student_group="All Students",
                metric=f"climate_index_score_{level}",
            )
            safety_row = _cell(
                frame, year="2026", school_id="999000", student_group="All Students",
                metric=f"safety_index_score_{level}",
            )
            assert climate_row["value"] == climate
            assert safety_row["value"] == safety
            assert climate_row["suppressed"] == "N"
            assert climate_row["district"] == "All Districts"
            assert climate_row["school_name"] == "---District Total---"

        fayette = _cell(
            frame, year="2026", school_id="165000", metric="climate_index_score_high",
            student_group="All Students",
        )
        assert fayette["value"] == 67.1
        assert fayette["district"] == "Fayette County"
        assert fayette["co_op"] == "CKEC"

        bryan = _cell(
            frame, year="2026", school_id="165170", metric="climate_index_score_high",
            student_group="All Students",
        )
        assert bryan["value"] == 67.8
        assert _cell(
            frame, year="2026", school_id="165170", metric="safety_index_score_high",
            student_group="All Students",
        )["value"] == 63.7
        suppressed = _cell(
            frame, year="2026", school_id="165170", metric="climate_index_score_high",
            student_group="American Indian or Alaska Native",
        )
        assert pd.isna(suppressed["value"])
        assert suppressed["suppressed"] == "Y"

        elementary = _cell(
            frame, year="2026", school_id="006010", metric="climate_index_score_elementary",
        )
        middle = _cell(
            frame, year="2026", school_id="006010", metric="climate_index_score_middle",
        )
        assert elementary["value"] == 80.1
        assert middle["value"] == 74.4
        assert elementary["co_op"] == "OVEC"

        grec = _cell(
            frame, year="2025", school_id="275004", metric="climate_index_score_elementary",
        )
        glec = _cell(
            frame, year="2026", school_id="275004", metric="climate_index_score_elementary",
        )
        assert grec["value"] == 77.1
        assert grec["co_op"] == "GREC"
        assert glec["value"] == 75.9
        assert glec["co_op"] == "GLEC"

        prior = {
            "2025": {"elementary": (80.0, 74.8), "middle": (70.9, 64.9), "high": (67.8, 62.9)},
            "2024": {"elementary": (79.6, 74.3), "middle": (69.5, 63.3), "high": (66.0, 61.0)},
        }
        for year, levels in prior.items():
            for level, (climate, safety) in levels.items():
                assert _cell(
                    frame, year=year, school_id="999000", metric=f"climate_index_score_{level}",
                )["value"] == climate
                assert _cell(
                    frame, year=year, school_id="999000", metric=f"safety_index_score_{level}",
                )["value"] == safety

        profile = {
            "elementary": 76.6,
            "high": 61.4,
            "middle": 65.9,
        }
        for level, rate in profile.items():
            for metric in (f"climate_index_score_{level}", f"safety_index_score_{level}"):
                row = _cell(frame, year="2022", school_id="999", metric=metric)
                assert row["value"] == rate
                assert row["district"] == "State"
                assert row["school_name"] == "---State Total---"

        assert _cell(
            frame, year="2026", school_id="165170", metric="outside_doors_locked_during_school_hours",
        )["value"] == 1.0
        assert _cell(
            frame, year="2026", school_id="165170", metric="safety_policy_compliance_rate",
        )["value"] == 100.0
        opportunity_doors = _cell(
            frame, year="2026", school_id="165899", metric="outside_doors_locked_during_school_hours",
        )
        opportunity_rate = _cell(
            frame, year="2026", school_id="165899", metric="safety_policy_compliance_rate",
        )
        assert opportunity_doors["value"] == 0.0
        assert opportunity_rate["value"] == pytest.approx(71.4)
        choices = _cell(
            frame, year="2025", school_id="476016", metric="safety_policy_compliance_rate",
        )
        assert choices["value"] == 100.0
        _absent(
            frame, year="2025", school_id="476016", metric="outside_doors_locked_during_school_hours",
        )
        bavel = _cell(
            frame, year="2025", school_id="021012", metric="outside_doors_locked_during_school_hours",
        )
        assert bavel["value"] == 0.0
        assert bavel["co_op"] == "GRREC"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])