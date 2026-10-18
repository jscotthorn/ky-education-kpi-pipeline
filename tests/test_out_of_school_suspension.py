"""Unit tests for OutOfSchoolSuspensionETL."""
import pandas as pd
import tempfile
import shutil
import yaml
from pathlib import Path
from unittest.mock import MagicMock

from etl.out_of_school_suspension import OutOfSchoolSuspensionETL, transform

ROOT = Path(__file__).resolve().parents[1]


class TestOutOfSchoolSuspensionETL:
    def setup_method(self):
        self.etl = OutOfSchoolSuspensionETL('out_of_school_suspension')

    def test_normalize_column_names(self):
        df = pd.DataFrame(columns=[
            'School Year', 'District Name', 'School Name', 'Demographic',
            'Single Out-of-School With Disabilities', 'OUT OF SCHOOL SUSPENSION SSP3'
        ])
        result = self.etl.normalize_column_names(df)
        assert 'school_year' in result.columns
        assert 'out_of_school_suspension' in result.columns
        assert 'single_out_of_school_with_disabilities' in result.columns

    def test_standardize_missing_values(self):
        df = pd.DataFrame({
            'out_of_school_suspension': ['1', '*', ''],
        })
        result = self.etl.standardize_missing_values(df)
        # The empty cell is not offered and is dropped. The star stays and
        # becomes missing. notes/8: a blank is not a suppression.
        assert len(result) == 2
        assert result['out_of_school_suspension'].isna().sum() == 1
        assert set(result['suppressed']) == {'N', 'Y'}

    def test_blank_measure_row_is_skipped(self):
        """An unsuppressed row with every count empty emits nothing."""
        self.etl.demographic_mapper = MagicMock()
        self.etl.demographic_mapper.map_demographic.return_value = 'All Students'
        blank = pd.DataFrame({
            'School Year': ['20252026'],
            'Collected School Year': ['20212022'],
            'District Name': ['Fayette County'],
            'School Code': ['165170'],
            'School Name': ['Bryan Station High School'],
            'Demographic': ['All Students'],
            'In-School With Disabilities': [''],
            'In-School Without Disabilities': [''],
            'Single Out-of-School With Disabilities': [''],
            'Single Out-of-School Without Disabilities': [''],
            'Multiple Out-of-School With Disabilities': [''],
            'Multiple Out-of-School Without Disabilities': [''],
        })
        normalized = self.etl.standardize_missing_values(self.etl.normalize_column_names(blank))
        assert normalized.empty
        kpi = self.etl.convert_to_kpi_format(normalized, 'blank.csv')
        assert kpi.empty

    def test_comma_counts_survive_string_dtype(self):
        """pandas 3 StringDtype keeps a thousands separator until it is stripped.

        The registered suspension files have no comma-formatted counts.
        ``read_csv(..., dtype=str)`` is StringDtype on pandas 3 and object
        on pandas 2.3.3. The cleaner does not check ``dtype == object``.
        """
        df = pd.DataFrame({
            'single_out_of_school_with_disabilities': pd.Series(
                ['1,024', '*', '', pd.NA], dtype='string'
            ),
            'single_out_of_school_without_disabilities': pd.Series(
                ['28', '3', '2', '0'], dtype='string'
            ),
        })
        result = self.etl._numeric_clean(df)
        assert result['single_out_of_school_with_disabilities'].iloc[0] == 1024
        assert pd.isna(result['single_out_of_school_with_disabilities'].iloc[1])
        assert pd.isna(result['single_out_of_school_with_disabilities'].iloc[2])
        assert pd.isna(result['single_out_of_school_with_disabilities'].iloc[3])
        assert result['single_out_of_school_without_disabilities'].iloc[0] == 28

    def test_convert_to_kpi_format_safe_schools(self):
        self.etl.demographic_mapper = MagicMock()
        self.etl.demographic_mapper.map_demographic.return_value = 'All Students'
        df = pd.DataFrame({
            'district_name': ['Test District'],
            'school_code': ['1234'],
            'school_name': ['Test School'],
            'school_year': ['20222023'],
            'demographic': ['All Students'],
            'out_of_school_suspension': [5],
        })
        df = self.etl.standardize_missing_values(df)
        kpi = self.etl.convert_to_kpi_format(df, 'test.csv')
        assert len(kpi) == 1
        assert kpi['metric'].iloc[0] == 'out_of_school_suspension_count'
        assert kpi['value'].iloc[0] == 5.0

    def test_negative_values_suppressed(self):
        self.etl.demographic_mapper = MagicMock()
        self.etl.demographic_mapper.map_demographic.return_value = 'All Students'
        df = pd.DataFrame({
            'district_name': ['Test'],
            'school_code': ['1'],
            'school_name': ['Test'],
            'school_year': ['20222023'],
            'demographic': ['All Students'],
            'out_of_school_suspension': [-1],
        })
        df = self.etl.standardize_missing_values(df)
        kpi = self.etl.convert_to_kpi_format(df, 'test.csv')
        assert kpi['suppressed'].iloc[0] == 'Y'
        assert pd.isna(kpi['value'].iloc[0])

    def test_extract_metrics_additional_counts(self):
        row = pd.Series({
            'single_out_of_school_with_disabilities': 1,
            'single_out_of_school_without_disabilities': 2,
            'multiple_out_of_school_with_disabilities': 0,
            'multiple_out_of_school_without_disabilities': 1,
            'in_school_with_disabilities': 3,
            'in_school_without_disabilities': 4,
            'expelled_receiving_services': 1,
            'total_discipline_resolutions': 10
        })
        metrics = self.etl.extract_metrics(row)
        assert metrics['in_school_suspension_total_count'] == 7
        assert metrics['expelled_receiving_services_count'] == 1
        assert metrics['discipline_resolution_total_count'] == 10


class TestTransform:
    def test_transform_creates_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir) / 'raw'
            proc_dir = Path(tmpdir) / 'proc'
            source_dir = raw_dir / 'out_of_school_suspension'
            source_dir.mkdir(parents=True)
            proc_dir.mkdir(parents=True)

            df = pd.DataFrame({
                'School Year': ['20232024'],
                'District Name': ['Test'],
                'School Code': ['100'],
                'School Name': ['Test School'],
                'Demographic': ['All Students'],
                'Single Out-of-School With Disabilities': [1],
                'Single Out-of-School Without Disabilities': [2],
                'Multiple Out-of-School With Disabilities': [0],
                'Multiple Out-of-School Without Disabilities': [1]
            })
            df.to_csv(source_dir / 'sample.csv', index=False)

            transform(raw_dir, proc_dir, {'derive': {}})

            out_file = proc_dir / 'out_of_school_suspension.csv'
            audit_file = proc_dir / 'out_of_school_suspension_demographic_report.md'
            assert out_file.exists()
            assert audit_file.exists()


class TestKyrc26OutOfSchoolSuspension:
    """KYRC26 opt-in. There is no temporary extract to skip."""

    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.sample_dir = self.test_dir / 'out_of_school_suspension'
        self.sample_dir.mkdir(parents=True)
        self.etl = OutOfSchoolSuspensionETL('out_of_school_suspension')

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def test_kyrc26_file_is_accepted(self):
        """The permanent KYRC files and the historical names stay in scope."""
        assert self.etl.include_kyrc26 is True
        assert self.etl.module_column_mappings['Single Out-of-School With Disabilities'] == (
            'single_out_of_school_with_disabilities'
        )
        assert self.etl.module_column_mappings['OUT OF SCHOOL SUSPENSION SSP3'] == (
            'out_of_school_suspension'
        )
        assert self.etl.module_column_mappings['Collected School Year'] == 'collected_school_year'

        accepted = [
            'KYRC26_OVW_Student_Suspensions.csv',
            'KYRC25_OVW_Student_Suspensions.csv',
            'KYRC24_OVW_Student_Suspensions.csv',
            'safe_schools_discipline_2023.csv',
            'safe_schools_discipline_2022.csv',
            'safe_schools_discipline_2021.csv',
            'safe_schools_discipline_2020.csv',
        ]
        for name in accepted:
            (self.sample_dir / name).write_text('School Year\n20252026\n')

        found = sorted(path.name for path, _sheet in self.etl._find_data_files(self.sample_dir))
        assert found == sorted(accepted)


def test_registered_sources_include_kyrc25_and_kyrc26():
    """KYRC25 was missing from the series. KYRC26 is the new report-card file."""
    config = yaml.safe_load((ROOT / 'config' / 'kde_sources.yaml').read_text())
    names = []
    for entry in config['raw_directories']['out_of_school_suspension']:
        names.append(entry['file'] if isinstance(entry, dict) else entry)

    assert names == [
        'KYRC24_OVW_Student_Suspensions.csv',
        'KYRC25_OVW_Student_Suspensions.csv',
        'KYRC26_OVW_Student_Suspensions.csv',
        'safe_schools_discipline_2023.csv',
        'safe_schools_discipline_2022.csv',
        'safe_schools_discipline_2021.csv',
        'safe_schools_discipline_2020.csv',
    ]
    assert not any('Student_Suspensions_20' in name for name in names)
