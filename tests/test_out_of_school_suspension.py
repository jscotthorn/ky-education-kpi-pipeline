"""Unit tests for OutOfSchoolSuspensionETL."""
import pandas as pd
import tempfile
import shutil
import yaml
from pathlib import Path
from unittest.mock import MagicMock

from etl.out_of_school_suspension import OutOfSchoolSuspensionETL, transform

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "out_of_school_suspension" / "KYRC24_OVW_Student_Suspensions.csv"


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
        assert 'single_out_of_school_with_disabilities' in result.columns
        # The annual SSP3 header is not this module's measure.
        assert 'out_of_school_suspension' not in result.columns

    def test_standardize_missing_values(self):
        df = pd.DataFrame({
            'single_out_of_school_with_disabilities': ['1', '*', ''],
            'single_out_of_school_without_disabilities': ['2', '*', ''],
            'multiple_out_of_school_with_disabilities': ['0', '*', ''],
            'multiple_out_of_school_without_disabilities': ['1', '*', ''],
        })
        result = self.etl.standardize_missing_values(df)
        # The empty row is not offered and is dropped. The star stays and
        # becomes missing. notes/8: a blank is not a suppression.
        assert len(result) == 2
        assert result['single_out_of_school_with_disabilities'].isna().sum() == 1
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
        """This module emits nothing for an annual Safe Schools file.

        safe_schools_discipline owns ``OUT OF SCHOOL SUSPENSION SSP3``
        since e7d815a (4 Aug 2025). kpi_master concatenates without
        deduping, so emitting the count here duplicated that module.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir) / 'raw'
            proc_dir = Path(tmpdir) / 'proc'
            source_dir = raw_dir / 'out_of_school_suspension'
            source_dir.mkdir(parents=True)
            proc_dir.mkdir()
            annual = source_dir / 'safe_schools_discipline_2023.csv'
            pd.DataFrame({
                'SCHOOL YEAR': ['20222023'],
                'DISTRICT NAME': ['Fayette County'],
                'SCHOOL CODE': ['165'],
                'SCHOOL NAME': ['---District Total---'],
                'DEMOGRAPHIC': ['All Students'],
                'OUT OF SCHOOL SUSPENSION SSP3': [5],
            }).to_csv(annual, index=False)
            assert self.etl.accepts_data_file(annual) is False
            transform(raw_dir, proc_dir, {'derive': {}})
            assert not (proc_dir / 'out_of_school_suspension.csv').exists()

    def test_safe_schools_discipline_file_is_skipped(self):
        """An annual Safe Schools file emits nothing, including a negative SSP3 cell.

        safe_schools_discipline owns ``OUT OF SCHOOL SUSPENSION SSP3``
        since e7d815a (4 Aug 2025). The annual file is not read here.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir) / 'raw'
            proc_dir = Path(tmpdir) / 'proc'
            source_dir = raw_dir / 'out_of_school_suspension'
            source_dir.mkdir(parents=True)
            proc_dir.mkdir()
            annual = source_dir / 'safe_schools_discipline_2022.csv'
            pd.DataFrame({
                'SCHOOL YEAR': ['20222023'],
                'DISTRICT NAME': ['Test'],
                'SCHOOL CODE': ['1'],
                'SCHOOL NAME': ['Test'],
                'DEMOGRAPHIC': ['All Students'],
                'OUT OF SCHOOL SUSPENSION SSP3': [-1],
            }).to_csv(annual, index=False)
            assert self.etl.accepts_data_file(annual) is False
            transform(raw_dir, proc_dir, {'derive': {}})
            assert not (proc_dir / 'out_of_school_suspension.csv').exists()

    def test_extract_metrics_crdc_counts_only(self):
        """Title-case discipline aliases are not this module's metrics.

        safe_schools_discipline publishes those columns as counts.
        """
        row = pd.Series({
            'single_out_of_school_with_disabilities': 1,
            'single_out_of_school_without_disabilities': 2,
            'multiple_out_of_school_with_disabilities': 0,
            'multiple_out_of_school_without_disabilities': 1,
            'in_school_with_disabilities': 3,
            'in_school_without_disabilities': 4,
            'expelled_receiving_services': 1,
            'total_discipline_resolutions': 10,
        })
        metrics = self.etl.extract_metrics(row)
        assert metrics['in_school_suspension_total_count'] == 7
        assert metrics['out_of_school_suspension_total_count'] == 4
        assert 'expelled_receiving_services_count' not in metrics
        assert 'discipline_resolution_total_count' not in metrics
        assert 'out_of_school_suspension_count' not in metrics

    def test_starred_component_suppresses_total(self):
        """A total is suppressed when any component is starred.

        Real KYRC24 rows. Camp Taylor All Students is starred in every
        count. Summing the missing parts used to store 0 / ``N``. South
        Hopkins Middle Female stars only in-school without disabilities
        (the other in-school cell is a published 0), so that total was
        also 0 / ``N``. A sum that drops a starred part is not the
        published value. notes/8.
        """
        raw = pd.read_csv(FIXTURE, dtype=str)
        df = self.etl.standardize_missing_values(self.etl.normalize_column_names(raw))
        kpi = self.etl.convert_to_kpi_format(df, 'KYRC24_OVW_Student_Suspensions.csv')
        assert 'collected_school_year' not in kpi.columns
        assert set(kpi['year']) == {'2024'}

        camp = kpi[(kpi['school_id'] == '275004') & (kpi['student_group'] == 'All Students')]
        assert len(camp) == 10
        assert (camp['suppressed'] == 'Y').all()
        assert camp['value'].isna().all()
        assert camp['school_name'].iloc[0] == 'Camp Taylor Elementary'

        female = kpi[(kpi['school_id'] == '265185') & (kpi['student_group'] == 'Female')]
        in_without = female[female['metric'] == 'in_school_suspension_without_disabilities_count'].iloc[0]
        in_with = female[female['metric'] == 'in_school_suspension_with_disabilities_count'].iloc[0]
        in_total = female[female['metric'] == 'in_school_suspension_total_count'].iloc[0]
        assert in_without['suppressed'] == 'Y'
        assert pd.isna(in_without['value'])
        assert float(in_with['value']) == 0
        assert in_with['suppressed'] == 'N'
        assert in_total['suppressed'] == 'Y'
        assert pd.isna(in_total['value'])
        assert float(female[female['metric'] == 'out_of_school_suspension_total_count'].iloc[0]['value']) == 2
        assert female[female['metric'] == 'out_of_school_suspension_total_count'].iloc[0]['suppressed'] == 'N'

    def test_negative_crdc_count_is_suppressed(self):
        """A negative CRDC count is empty and suppressed Y.

        Bryan Station All Students is a real KYRC24 row. The published
        single-with-disabilities cell is 5. This test sets that cell to
        -1 in memory. The fixture file stays verbatim.
        """
        raw = pd.read_csv(FIXTURE, dtype=str)
        row = raw[
            (raw['School Code'] == '165170') & (raw['Demographic'] == 'All Students')
        ].copy()
        assert len(row) == 1
        assert row['Single Out-of-School With Disabilities'].iloc[0] == '5'
        row['Single Out-of-School With Disabilities'] = '-1'
        df = self.etl.standardize_missing_values(self.etl.normalize_column_names(row))
        kpi = self.etl.convert_to_kpi_format(df, 'KYRC24_OVW_Student_Suspensions.csv')
        negative = kpi[
            kpi['metric'] == 'out_of_school_suspension_single_with_disabilities_count'
        ].iloc[0]
        assert negative['suppressed'] == 'Y'
        assert pd.isna(negative['value'])
        assert negative['school_id'] == '165170'
        published = kpi[
            kpi['metric'] == 'out_of_school_suspension_single_without_disabilities_count'
        ].iloc[0]
        assert float(published['value']) == 2
        assert published['suppressed'] == 'N'
        on_disk = pd.read_csv(FIXTURE, dtype=str)
        stored = on_disk[
            (on_disk['School Code'] == '165170') & (on_disk['Demographic'] == 'All Students')
        ]
        assert stored['Single Out-of-School With Disabilities'].iloc[0] == '5'


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
    """KYRC26 opt-in. KYRC25 republishes CRDC 2020-21 and is skipped."""

    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.sample_dir = self.test_dir / 'out_of_school_suspension'
        self.sample_dir.mkdir(parents=True)
        self.etl = OutOfSchoolSuspensionETL('out_of_school_suspension')

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def test_kyrc26_file_is_accepted(self):
        """KYRC26 and KYRC24 stay in scope. KYRC25 and the annual files do not."""
        assert self.etl.include_kyrc26 is True
        assert self.etl.module_column_mappings['Single Out-of-School With Disabilities'] == (
            'single_out_of_school_with_disabilities'
        )
        assert self.etl.module_column_mappings['Collected School Year'] == 'collected_school_year'
        assert 'OUT OF SCHOOL SUSPENSION SSP3' not in self.etl.module_column_mappings
        assert 'Expelled Receiving Services SSP1' not in self.etl.module_column_mappings

        accepted = [
            'KYRC26_OVW_Student_Suspensions.csv',
            'KYRC24_OVW_Student_Suspensions.csv',
        ]
        rejected = [
            'KYRC25_OVW_Student_Suspensions.csv',
            'safe_schools_discipline_2023.csv',
            'safe_schools_discipline_2022.csv',
            'safe_schools_discipline_2021.csv',
            'safe_schools_discipline_2020.csv',
        ]
        for name in accepted + rejected:
            (self.sample_dir / name).write_text('School Year\n20252026\n')

        found = sorted(path.name for path, _sheet in self.etl._find_data_files(self.sample_dir))
        assert found == sorted(accepted)
        for name in rejected:
            assert self.etl.accepts_data_file(self.sample_dir / name) is False


def test_registered_sources_are_kyrc24_and_kyrc26():
    """KYRC25 republishes CRDC 2020-21. The annual files belong to safe_schools_discipline."""
    config = yaml.safe_load((ROOT / 'config' / 'kde_sources.yaml').read_text())
    names = []
    for entry in config['raw_directories']['out_of_school_suspension']:
        names.append(entry['file'] if isinstance(entry, dict) else entry)

    assert names == [
        'KYRC24_OVW_Student_Suspensions.csv',
        'KYRC26_OVW_Student_Suspensions.csv',
    ]
    assert not any('Student_Suspensions_20' in name for name in names)
    assert not any(name.startswith('safe_schools_discipline_') for name in names)
    assert 'KYRC25_OVW_Student_Suspensions.csv' not in names


def test_kyrc25_student_suspensions_file_is_skipped():
    """KYRC25_OVW_Student_Suspensions.csv republishes CRDC 2020-21.

    School Year is 20242025 and the collected year stays 20202021. The
    count cells match KYRC24. Accepting the file would publish 2025 as a
    second copy of report-card year 2024.
    """
    etl = OutOfSchoolSuspensionETL('out_of_school_suspension')
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / 'KYRC25_OVW_Student_Suspensions.csv'
        path.write_text('School Year\n20242025\n')
        assert etl.accepts_data_file(path) is False
        source_dir = Path(tmpdir) / 'out_of_school_suspension'
        source_dir.mkdir()
        (source_dir / path.name).write_text('School Year\n20242025\n')
        found = list(etl._find_data_files(source_dir))
        assert found == []
