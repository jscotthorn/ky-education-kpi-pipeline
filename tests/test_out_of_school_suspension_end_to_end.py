"""End-to-end tests for the OutOfSchoolSuspensionETL."""
import pandas as pd
import tempfile
from pathlib import Path
from etl.out_of_school_suspension import transform


class TestOutOfSchoolSuspensionE2E:
    def test_kyrc24_and_safe_schools(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir) / 'raw'
            proc_dir = Path(tmpdir) / 'proc'
            ky_dir = raw_dir / 'out_of_school_suspension'
            ky_dir.mkdir(parents=True)
            proc_dir.mkdir(parents=True)

            kyrc24 = pd.DataFrame({
                'School Year': ['20232024'],
                'District Name': ['Test'],
                'School Code': ['100'],
                'School Name': ['Test School'],
                'Demographic': ['All Students'],
                'Single Out-of-School With Disabilities': [1],
                'Single Out-of-School Without Disabilities': [2],
                'Multiple Out-of-School With Disabilities': [0],
                'Multiple Out-of-School Without Disabilities': [1],
                'In-School With Disabilities': [3],
                'In-School Without Disabilities': [4],
                'Expelled Receiving Services SSP1': [1],
                'Total Discipline Resolutions': [10]
            })
            kyrc24.to_csv(ky_dir / 'kyrc24.csv', index=False)

            safe = pd.DataFrame({
                'SCHOOL YEAR': ['20222023'],
                'DISTRICT NAME': ['Test'],
                'SCHOOL CODE': ['200'],
                'SCHOOL NAME': ['Test 2'],
                'DEMOGRAPHIC': ['All Students'],
                'OUT OF SCHOOL SUSPENSION SSP3': [3],
                'EXPELLED RECEIVING SERVICES SSP1': [1],
                'IN-SCHOOL REMOVAL INSR': [2]
            })
            safe.to_csv(ky_dir / 'safe.csv', index=False)

            transform(raw_dir, proc_dir, {'derive': {}})

            out_file = proc_dir / 'out_of_school_suspension.csv'
            df = pd.read_csv(out_file)
            assert len(df) == 13
            assert set(df['metric'].unique()) >= {
                'out_of_school_suspension_count',
                'out_of_school_suspension_single_with_disabilities_count'
            }
