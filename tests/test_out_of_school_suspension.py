"""Unit tests for OutOfSchoolSuspensionETL."""
import pandas as pd
import tempfile
import shutil
from pathlib import Path
from unittest.mock import MagicMock

from etl.out_of_school_suspension import OutOfSchoolSuspensionETL, transform


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
        assert result['out_of_school_suspension'].isna().sum() == 2
        assert set(result['suppressed']) == {'N', 'Y'}

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
            audit_file = proc_dir / 'out_of_school_suspension_demographic_audit.csv'
            assert out_file.exists()
            assert audit_file.exists()
