"""End-to-end tests for Safe Schools Climate ETL pipeline."""
import pytest
import pandas as pd
import tempfile
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.safe_schools_climate import transform


@pytest.fixture
def setup_test_data(tmp_path):
    """Create test data files."""
    raw_dir = tmp_path / "raw" / "safe_schools_climate"
    raw_dir.mkdir(parents=True)
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir()
    
    # Create test precautionary measures file
    precautionary_data = pd.DataFrame({
        'School Year': ['20232024', '20232024', '20232024'],
        'County Number': ['001', '001', '001'],
        'County Name': ['TEST COUNTY', 'TEST COUNTY', 'TEST COUNTY'],
        'District Number': ['001', '001', '001'],
        'District Name': ['Test District', 'Test District', 'Test District'],
        'School Number': ['010', '020', '030'],
        'School Name': ['Test High School', 'Test Middle School', 'Test Elementary'],
        'School Code': ['001010', '001020', '001030'],
        'State School Id': ['001001010', '001001020', '001001030'],
        'NCES ID': ['210003000001', '210003000002', '210003000003'],
        'CO-OP': ['GRREC', 'GRREC', 'GRREC'],
        'CO-OP Code': ['902', '902', '902'],
        'School Type': ['A1', 'A1', 'A1'],
        'Are visitors to the building required to sign-in?': ['Yes', 'Yes', 'No'],
        'Do all classroom doors lock from the inside?': ['Yes', 'No', 'Yes'],
        'Do all classrooms have access to a telephone accessing outside lines?': ['Yes', 'Yes', 'Yes'],
        'Does your school administer a school climate survey annually?': ['Yes', 'Yes', 'Yes'],
        'Does your school collect and use student survey data?': ['Yes', 'No', 'Yes'],
        'Does your school have a full-time School Resource Officer?': ['Yes', 'Yes', 'No'],
        'Does your school have a process in place to provide mental health referrals for students?': ['Yes', 'Yes', 'Yes'],
        'Is the district discipline code distributed to parents?': ['Yes', 'Yes', 'Yes']
    })
    
    precautionary_file = raw_dir / "KYRC24_SAFE_Precautionary_Measures.csv"
    precautionary_data.to_csv(precautionary_file, index=False)

    # Create older precautionary measures file with synonym columns
    older_precautionary = pd.DataFrame({
        'SCHOOL YEAR': ['20212022'],
        'COUNTY NUMBER': ['001'],
        'COUNTY NAME': ['TEST COUNTY'],
        'DISTRICT NUMBER': ['001'],
        'DISTRICT NAME': ['Test District'],
        'SCHOOL NUMBER': ['050'],
        'SCHOOL NAME': ['Legacy School'],
        'SCHOOL CODE': ['001050'],
        'STATE SCHOOL ID': ['001001050'],
        'NCES ID': ['210003000005'],
        'CO-OP': ['GRREC'],
        'CO-OP CODE': ['902'],
        'SCHOOL TYPE': ['A1'],
        'Visitors required to sign-in': ['Yes'],
        'All classroom doors lock from the inside.': ['Yes'],
        'All classrooms have access to telephone': ['Yes'],
        'School climate survey administered annually.': ['Yes'],
        'Student survey data collected and used': ['Yes'],
        'Full-time resource officer': ['No'],
        'Mental health referral process in place': ['Yes'],
        'District discipline code distributed to parents': ['Yes']
    })

    old_file = raw_dir / "precautionary_measures_2022.csv"
    older_precautionary.to_csv(old_file, index=False)
    
    # Create test index scores file for 2023
    index_data_2023 = pd.DataFrame({
        'SCHOOL YEAR': ['20222023', '20222023', '20222023'],
        'COUNTY NUMBER': ['001', '001', '001'],
        'COUNTY NAME': ['TEST COUNTY', 'TEST COUNTY', 'TEST COUNTY'],
        'DISTRICT NUMBER': ['001', '001', '001'],
        'DISTRICT NAME': ['Test District', 'Test District', 'Test District'],
        'SCHOOL NUMBER': ['', '', ''],
        'SCHOOL NAME': ['---District Total---', '---District Total---', '---District Total---'],
        'SCHOOL CODE': ['001', '001', '001'],
        'STATE SCHOOL ID': ['', '', ''],
        'NCES ID': ['', '', ''],
        'CO-OP': ['GRREC', 'GRREC', 'GRREC'],
        'CO-OP CODE': ['902', '902', '902'],
        'SCHOOL TYPE': ['', '', ''],
        'DEMOGRAPHIC': ['All Students', 'Female', 'Male'],
        'LEVEL': ['MS', 'MS', 'MS'],
        'SUPPRESSED': ['N', 'N', 'N'],
        'CLIMATE INDEX': [75.5, 78.8, 72.2],
        'SAFETY INDEX': [68.2, 71.6, 64.8]
    })
    
    index_file_2023 = raw_dir / "quality_of_school_climate_and_safety_survey_index_scores_2023.csv"
    index_data_2023.to_csv(index_file_2023, index=False)
    
    # Create test index scores file for 2022 with some suppressed data
    index_data_2022 = pd.DataFrame({
        'SCHOOL YEAR': ['20212022', '20212022'],
        'COUNTY NUMBER': ['001', '001'],
        'COUNTY NAME': ['TEST COUNTY', 'TEST COUNTY'],
        'DISTRICT NUMBER': ['001', '001'],
        'DISTRICT NAME': ['Test District', 'Test District'],
        'SCHOOL NUMBER': ['010', '010'],
        'SCHOOL NAME': ['Test High School', 'Test High School'],
        'SCHOOL CODE': ['001010', '001010'],
        'STATE SCHOOL ID': ['001001010', '001001010'],
        'NCES ID': ['210003000001', '210003000001'],
        'CO-OP': ['GRREC', 'GRREC'],
        'CO-OP CODE': ['902', '902'],
        'SCHOOL TYPE': ['A1', 'A1'],
        'DEMOGRAPHIC': ['All Students', 'African American'],
        'LEVEL': ['HS', 'HS'],
        'SUPPRESSED': ['N', 'Y'],
        'CLIMATE INDEX': [70.0, ''],
        'SAFETY INDEX': [65.0, '']
    })
    
    index_file_2022 = raw_dir / "quality_of_school_climate_and_safety_survey_index_scores_2022.csv"
    index_data_2022.to_csv(index_file_2022, index=False)
    
    return raw_dir.parent, processed_dir


def test_safe_schools_climate_transform_creates_output(setup_test_data):
    """Test that transform creates output file."""
    raw_dir, processed_dir = setup_test_data
    
    config = {
        "derive": {
            "processing_date": "2025-07-21",
            "data_quality_flag": "reviewed"
        }
    }
    
    # Run transform
    transform(raw_dir, processed_dir, config)
    
    # Check output file exists
    output_file = processed_dir / "safe_schools_climate_kpi.csv"
    assert output_file.exists()


def test_safe_schools_climate_output_structure(setup_test_data):
    """Test output file has correct structure."""
    raw_dir, processed_dir = setup_test_data
    
    config = {}
    transform(raw_dir, processed_dir, config)
    
    # Read output
    output_file = processed_dir / "safe_schools_climate_kpi.csv"
    df = pd.read_csv(output_file)
    
    # Check required columns
    required_columns = ['district', 'school_id', 'school_name', 'year', 
                       'student_group', 'metric', 'value', 'suppressed', 
                       'source_file', 'last_updated']
    
    for col in required_columns:
        assert col in df.columns, f"Missing required column: {col}"


def test_safe_schools_climate_metrics_generated(setup_test_data):
    """Test that all three metric types are generated."""
    raw_dir, processed_dir = setup_test_data
    
    config = {}
    transform(raw_dir, processed_dir, config)
    
    # Read output
    output_file = processed_dir / "safe_schools_climate_kpi.csv"
    df = pd.read_csv(output_file)
    
    # Check metrics
    metrics = df['metric'].unique()
    assert 'safety_policy_compliance_rate' in metrics
    assert 'climate_index_score' in metrics
    assert 'safety_index_score' in metrics


def test_safe_schools_climate_policy_compliance_calculation(setup_test_data):
    """Test policy compliance rate calculations."""
    raw_dir, processed_dir = setup_test_data
    
    config = {}
    transform(raw_dir, processed_dir, config)
    
    df = pd.read_csv(processed_dir / "safe_schools_climate_kpi.csv")
    
    # Check policy compliance rates
    policy_df = df[df['metric'] == 'safety_policy_compliance_rate']
    
    # Test High School should have 100% (all Yes)
    high_school = policy_df[policy_df['school_name'] == 'Test High School']
    # Debug: print what we got
    if len(high_school) != 1:
        print(f"Expected 1 record for Test High School, got {len(high_school)}")
        print(high_school[['school_name', 'year', 'source_file', 'value']])
    assert len(high_school) == 1
    assert high_school.iloc[0]['value'] == 100.0
    
    # Test Middle School should have 75% (6 Yes out of 8)
    middle_school = policy_df[policy_df['school_name'] == 'Test Middle School']
    assert len(middle_school) == 1
    assert middle_school.iloc[0]['value'] == 75.0
    
    # Test Elementary should have 75% (6 Yes out of 8)
    elementary = policy_df[policy_df['school_name'] == 'Test Elementary']
    assert len(elementary) == 1
    assert elementary.iloc[0]['value'] == 75.0

    # Legacy School from 2022 file should have 87.5% (7 Yes out of 8)
    legacy = policy_df[policy_df['school_name'] == 'Legacy School']
    assert len(legacy) == 1
    assert legacy.iloc[0]['value'] == 87.5


def test_safe_schools_climate_index_scores(setup_test_data):
    """Test climate and safety index scores."""
    raw_dir, processed_dir = setup_test_data
    
    config = {}
    transform(raw_dir, processed_dir, config)
    
    df = pd.read_csv(processed_dir / "safe_schools_climate_kpi.csv")
    
    # Check 2023 climate scores (note: years are integers)
    climate_2023 = df[(df['metric'] == 'climate_index_score') & 
                      (df['year'] == 2023) & 
                      (df['student_group'] == 'All Students')]
    assert len(climate_2023) == 1
    assert climate_2023.iloc[0]['value'] == 75.5
    
    # Check 2023 safety scores (Female demographic)
    safety_2023 = df[(df['metric'] == 'safety_index_score') & 
                     (df['year'] == 2023) & 
                     (df['student_group'] == 'Female')]
    assert len(safety_2023) == 1
    assert safety_2023.iloc[0]['value'] == 71.6


def test_safe_schools_climate_suppressed_data(setup_test_data):
    """Test handling of suppressed data."""
    raw_dir, processed_dir = setup_test_data
    
    config = {}
    transform(raw_dir, processed_dir, config)
    
    df = pd.read_csv(processed_dir / "safe_schools_climate_kpi.csv")
    
    # Check suppressed records (note: years are integers)
    suppressed = df[(df['year'] == 2022) & 
                   (df['student_group'] == 'African American')]
    
    assert len(suppressed) == 2  # climate and safety scores
    for _, row in suppressed.iterrows():
        assert row['suppressed'] == 'Y'
        assert pd.isna(row['value'])


def test_safe_schools_climate_year_extraction(setup_test_data):
    """Test year extraction from different sources."""
    raw_dir, processed_dir = setup_test_data
    
    config = {}
    transform(raw_dir, processed_dir, config)
    
    df = pd.read_csv(processed_dir / "safe_schools_climate_kpi.csv")
    
    # Check years (note: years are stored as integers)
    years = df['year'].unique()
    assert 2022 in years  # From index file 2022
    assert 2023 in years  # From index file 2023
    assert 2024 in years  # From precautionary measures


def test_safe_schools_climate_no_duplicates(setup_test_data):
    """Test that duplicates are properly removed."""
    raw_dir, processed_dir = setup_test_data
    
    config = {}
    transform(raw_dir, processed_dir, config)
    
    df = pd.read_csv(processed_dir / "safe_schools_climate_kpi.csv")
    
    # Check for duplicates
    duplicate_check = df.duplicated(
        subset=['district', 'school_id', 'school_name', 'year', 'student_group', 'metric']
    )
    
    assert not duplicate_check.any(), "Found duplicate records in output"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])