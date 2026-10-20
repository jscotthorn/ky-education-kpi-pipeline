"""
Tests for Safe Schools Events ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
from etl.constants import KPI_COLUMNS
import tempfile
import shutil
from etl.safe_schools_events import (
    transform,
    normalize_column_names,
    clean_event_data,
    convert_to_kpi_format,
    add_derived_fields,
    files_to_process,
    rejected_event_filenames,
)


class TestSafeSchoolsEventsETL:
    
    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        
        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "safe_schools"
        self.sample_dir.mkdir(parents=True)
    
    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)
    
    def create_sample_kyrc24_events_by_type_data(self):
        """Create sample KYRC24 events by type data with three-tier structure."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County', 'Adair County'],
            'School Number': ['010', '010', '010', '010'],
            'School Name': ['Adair County High School', 'Adair County High School', 'Adair County High School', 'Adair County High School'],
            'School Code': ['001010', '001010', '001010', '001010'],
            'State School Id': ['001001010', '001001010', '001001010', '001001010'],
            'NCES ID': ['210003000001', '210003000001', '210003000001', '210003000001'],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC', 'GRREC'],
            'CO-OP Code': ['902', '902', '902', '902'],
            'School Type': ['A1', 'A1', 'A1', 'A1'],
            'Demographic': ['All Students', 'Female', 'Male', 'Total Events'],
            'Total': ['15', '8', '7', '15'],  # All Students = unique students, Total Events = sum of demographics
            'Alcohol': ['1', '0', '1', '1'],
            'Assault, 1st Degree': ['0', '0', '0', '0'],
            'Drugs': ['2', '1', '1', '2'],
            'Harassment (Includes Bullying)': ['5', '3', '2', '5'],
            'Other Assault or Violence': ['1', '1', '0', '1'],
            'Other Events Resulting in State Resolutions': ['5', '2', '3', '5'],
            'Tobacco': ['1', '1', '0', '1'],
            'Weapons': ['0', '0', '0', '0']
        })
        return data
    
    def create_sample_kyrc24_events_by_grade_data(self):
        """Create sample KYRC24 events by grade data with three-tier structure."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County', 'Adair County'],
            'School Number': ['010', '010', '010', '010'],
            'School Name': ['Adair County High School', 'Adair County High School', 'Adair County High School', 'Adair County High School'],
            'Demographic': ['All Students', 'Female', 'Male', 'Total Events'],
            'All Grades': ['15', '8', '7', '15'],
            'Preschool': ['0', '0', '0', '0'],
            'K': ['0', '0', '0', '0'],
            'Grade 1': ['0', '0', '0', '0'],
            'Grade 9': ['4', '2', '2', '4'],
            'Grade 10': ['5', '3', '2', '5'],
            'Grade 11': ['3', '2', '1', '3'],
            'Grade 12': ['3', '1', '2', '3']
        })
        return data
    
    def create_sample_historical_event_details_data(self):
        """Create sample historical event details data with three-tier structure."""
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20222023', '20222023', '20222023', '20222023'],
            'COUNTY NUMBER': ['001', '001', '001', '001'],
            'COUNTY NAME': ['ADAIR', 'ADAIR', 'ADAIR', 'ADAIR'],
            'DISTRICT NUMBER': ['001', '001', '001', '001'],
            'DISTRICT NAME': ['Adair County', 'Adair County', 'Adair County', 'Adair County'],
            'SCHOOL NUMBER': ['010', '010', '010', '010'],
            'SCHOOL NAME': ['Adair County High School', 'Adair County High School', 'Adair County High School', 'Adair County High School'],
            'DEMOGRAPHIC': ['All Students', 'Female', 'Male', 'Total Events'],
            'TOTAL EVENTS BY TYPE': ['12', '6', '6', '12'],
            'ASSAULT 1ST DEGREE': ['0', '0', '0', '0'],
            'OTHER ASSAULT OR VIOLENCE': ['1', '1', '0', '1'],
            'WEAPONS': ['0', '0', '0', '0'],
            'HARRASSMENT (INCLUDES BULLYING)': ['4', '2', '2', '4'],
            'DRUGS': ['1', '0', '1', '1'],
            'ALCOHOL': ['1', '0', '1', '1'],
            'TOBACCO': ['2', '1', '1', '2'],
            'OTHER EVENTS W_STATE RESOLUTION': ['3', '2', '1', '3'],
            'LOCATION - CLASSROOM': ['8', '4', '4', '8'],
            'LOCATION - HALLWAY/STAIRWAY': ['2', '1', '1', '2'],
            'LOCATION - CAFETERIA': ['1', '1', '0', '1'],
            'LOCATION - OTHER': ['1', '0', '1', '1'],
            'SCHOOL SPONSORED SCHOOL HOURS': ['11', '5', '6', '11'],
            'SCHOOL SPONSORED NOT SCHOOL HOURS': ['1', '1', '0', '1']
        })
        return data
    
    def test_normalize_column_names_kyrc24(self):
        """Test column name normalization for KYRC24 format."""
        df = self.create_sample_kyrc24_events_by_type_data()
        normalized_df = normalize_column_names(df)
        
        # Check key column mappings
        assert 'school_year' in normalized_df.columns
        assert 'district_name' in normalized_df.columns
        assert 'demographic' in normalized_df.columns
        assert 'total_events' in normalized_df.columns
        assert 'alcohol_events' in normalized_df.columns
        assert 'harassment_events' in normalized_df.columns
    
    def test_normalize_column_names_historical(self):
        """Test column name normalization for historical format."""
        df = self.create_sample_historical_event_details_data()
        normalized_df = normalize_column_names(df)
        
        # Check key column mappings
        assert 'school_year' in normalized_df.columns
        assert 'district_name' in normalized_df.columns
        assert 'demographic' in normalized_df.columns
        assert 'total_events' in normalized_df.columns
        assert 'drug_events' in normalized_df.columns
        assert 'harassment_events' in normalized_df.columns
        assert 'classroom' in normalized_df.columns
        assert 'school_sponsored_during' in normalized_df.columns
    
    def test_add_derived_fields(self):
        """Test addition of derived fields including data source detection."""
        # Test KYRC24 events by type
        df = self.create_sample_kyrc24_events_by_type_data()
        df = normalize_column_names(df)
        df = add_derived_fields(df, {'test_field': 'test_value'})
        
        assert 'test_field' in df.columns
        assert df['test_field'].iloc[0] == 'test_value'
        assert 'data_source' in df.columns
        assert 'kyrc24_events_by_type' in df['data_source'].iloc[0]
        
        # Test historical event details
        df_hist = self.create_sample_historical_event_details_data()
        df_hist = normalize_column_names(df_hist)
        df_hist = add_derived_fields(df_hist, {})
        
        assert 'data_source' in df_hist.columns
        assert 'historical_event_details' in df_hist['data_source'].iloc[0]
    
    def test_clean_event_data(self):
        """Test data cleaning including suppression handling."""
        # Create test data with suppression markers and invalid values
        df = pd.DataFrame({
            'school_year': ['20232024', '20232024', '20232024'],
            'total_events': ['15', '*', '-1'],
            'alcohol_events': ['1', '', '2.5'],
            'harassment_events': ['5', '3', 'invalid']
        })
        
        cleaned_df = clean_event_data(df)
        
        # Check suppression handling
        assert pd.isna(cleaned_df['total_events'].iloc[1])  # '*' should become NaN
        assert pd.isna(cleaned_df['alcohol_events'].iloc[1])  # '' should become NaN
        
        # Check invalid value handling
        assert pd.isna(cleaned_df['total_events'].iloc[2])  # negative value should become NaN
        assert pd.isna(cleaned_df['harassment_events'].iloc[2])  # 'invalid' should become NaN
        
        # Check valid values preserved
        assert cleaned_df['total_events'].iloc[0] == 15
        assert cleaned_df['alcohol_events'].iloc[0] == 1
        assert cleaned_df['alcohol_events'].iloc[2] == 2.5
    
    def test_convert_to_kpi_format_events_by_type(self):
        """Test KPI format conversion for events by type data with three-tier structure."""
        df = self.create_sample_kyrc24_events_by_type_data()
        df = normalize_column_names(df)
        df = add_derived_fields(df, {})
        df = clean_event_data(df)
        
        kpi_df = convert_to_kpi_format(df)
        
        # Check structure
        assert len(kpi_df.columns) == 19
        required_columns = KPI_COLUMNS
        for col in required_columns:
            assert col in kpi_df.columns
        
        # Check data content
        assert len(kpi_df) > 0
        assert kpi_df['year'].iloc[0] == 2024
        assert kpi_df['suppressed'].iloc[0] in ['Y', 'N']
        
        # Check three-tier structure
        actual_metrics = kpi_df['metric'].unique()
        
        # Tier 1: Students Affected (scope metrics)
        students_affected_metrics = [m for m in actual_metrics if 'safe_students_affected_' in m]
        assert len(students_affected_metrics) > 0
        assert 'safe_students_affected_total' in actual_metrics
        
        # Tier 2: Events by Demographics (equity metrics) 
        by_demo_metrics = [m for m in actual_metrics if '_by_demo' in m]
        assert len(by_demo_metrics) > 0
        assert 'safe_event_count_total_by_demo' in actual_metrics
        
        # Tier 3: Total Events (intensity metrics)
        total_event_metrics = [m for m in actual_metrics if m.startswith('safe_event_count_') and '_by_demo' not in m and 'students_affected' not in m]
        assert len(total_event_metrics) > 0
        assert 'safe_event_count_total' in actual_metrics
        
        # Check student group distinctions
        student_groups = kpi_df['student_group'].unique()
        assert 'All Students' in student_groups  # Students affected tier
        assert 'All Students - Total Events' in student_groups  # Total events tier
        assert any('Female' in sg or 'Male' in sg for sg in student_groups)  # Demographic tier
    
    def test_convert_to_kpi_format_events_by_grade(self):
        """Test KPI format conversion for events by grade data with three-tier structure."""
        df = self.create_sample_kyrc24_events_by_grade_data()
        df = normalize_column_names(df)
        df = add_derived_fields(df, {})
        df = clean_event_data(df)
        
        kpi_df = convert_to_kpi_format(df)
        
        # Check structure
        assert len(kpi_df.columns) == 19
        assert len(kpi_df) > 0
        
        # Check grade-specific metrics across all three tiers
        actual_metrics = kpi_df['metric'].unique()
        
        # Students affected grade metrics (Tier 1)
        students_affected_grade_metrics = [m for m in actual_metrics if 'safe_students_affected_grade_' in m]
        assert len(students_affected_grade_metrics) > 0
        assert 'safe_students_affected_grade_9' in actual_metrics
        assert 'safe_students_affected_all_grades' in actual_metrics
        
        # By demo grade metrics (Tier 2)
        by_demo_grade_metrics = [m for m in actual_metrics if 'grade_' in m and '_by_demo' in m]
        assert len(by_demo_grade_metrics) > 0
        assert 'safe_event_count_grade_9_by_demo' in actual_metrics
        assert 'safe_event_count_all_grades_by_demo' in actual_metrics
        
        # Total events grade metrics (Tier 3)
        total_grade_metrics = [m for m in actual_metrics if 'safe_event_count_grade_' in m and '_by_demo' not in m and 'students_affected' not in m]
        assert len(total_grade_metrics) > 0
        assert 'safe_event_count_grade_9' in actual_metrics
        assert 'safe_event_count_all_grades' in actual_metrics
    
    def test_transform_integration(self):
        """Test the full transform function with sample data."""
        # Create sample files
        kyrc24_data = self.create_sample_kyrc24_events_by_type_data()
        kyrc24_file = self.sample_dir / 'KYRC24_SAFE_Behavior_Events_by_Type.csv'
        kyrc24_data.to_csv(kyrc24_file, index=False)
        
        historical_data = self.create_sample_historical_event_details_data()
        historical_file = self.sample_dir / 'safe_schools_event_details_2022.csv'
        historical_data.to_csv(historical_file, index=False)
        
        # Test transform
        config = {'derive': {'test_field': 'test_value'}}
        result_file = transform(str(self.raw_dir), str(self.proc_dir), config)
        
        # Check output
        assert result_file != ""
        assert Path(result_file).exists()
        
        # Check output content
        output_df = pd.read_csv(result_file)
        assert len(output_df) > 0
        assert len(output_df.columns) == 19
        
        # Check audit file exists
        audit_file = self.proc_dir / 'safe_schools_events_demographic_report.md'
        assert audit_file.exists()
    
    def test_demographic_mapping(self):
        """Test that demographic mapping is properly applied."""
        df = self.create_sample_kyrc24_events_by_type_data()
        df = normalize_column_names(df)
        df = add_derived_fields(df, {})
        df = clean_event_data(df)
        
        kpi_df = convert_to_kpi_format(df)
        
        # Check that student_group is standardized
        assert 'student_group' in kpi_df.columns
        student_groups = kpi_df['student_group'].unique()
        
        # Should contain standardized demographic values
        assert len(student_groups) > 0
        # Note: Exact values depend on DemographicMapper implementation
    
    def test_suppression_handling(self):
        """Test that suppressed values are properly handled across three-tier structure."""
        # Create data with suppression markers including Total Events row
        df = pd.DataFrame({
            'school_year': ['20232024', '20232024', '20232024'],
            'district_name': ['Test District', 'Test District', 'Test District'],
            'state_school_id': ['123', '123', '123'],
            'school_name': ['Test School', 'Test School', 'Test School'],
            'demographic': ['All Students', 'Female', 'Total Events'],
            'total_events': ['15', '*', '15'],
            'alcohol_events': ['1', '', '1'],
            'data_source': ['kyrc24_events_by_type', 'kyrc24_events_by_type', 'kyrc24_events_by_type']
        })
        
        df = clean_event_data(df)
        kpi_df = convert_to_kpi_format(df)
        
        # Check suppression flags
        suppressed_rows = kpi_df[kpi_df['suppressed'] == 'Y']
        non_suppressed_rows = kpi_df[kpi_df['suppressed'] == 'N']
        
        assert len(suppressed_rows) > 0
        assert len(non_suppressed_rows) > 0
        
        # Suppressed rows should have NaN values
        assert suppressed_rows['value'].isna().all()
        
        # Non-suppressed rows should have numeric values
        assert not non_suppressed_rows['value'].isna().all()
        
        # Check that suppression is handled consistently across all three tiers
        student_groups = kpi_df['student_group'].unique()
        assert 'All Students' in student_groups  # Students affected
        assert 'All Students - Total Events' in student_groups  # Total events
        # Female should be mapped by DemographicMapper
    
    def test_three_tier_structure_validation(self):
        """Test that the three-tier structure produces mathematically consistent results."""
        df = self.create_sample_kyrc24_events_by_type_data()
        df = normalize_column_names(df)
        df = add_derived_fields(df, {})
        df = clean_event_data(df)
        
        kpi_df = convert_to_kpi_format(df)
        
        # Test for a specific metric (total_events)
        students_affected = kpi_df[
            (kpi_df['metric'] == 'safe_students_affected_total') & 
            (kpi_df['student_group'] == 'All Students')
        ]['value'].iloc[0]
        
        total_events = kpi_df[
            (kpi_df['metric'] == 'safe_event_count_total') & 
            (kpi_df['student_group'] == 'All Students - Total Events')
        ]['value'].iloc[0]
        
        # Get demographic breakdowns
        female_events = kpi_df[
            (kpi_df['metric'] == 'safe_event_count_total_by_demo') & 
            (kpi_df['student_group'] == 'Female')
        ]['value'].iloc[0]
        
        male_events = kpi_df[
            (kpi_df['metric'] == 'safe_event_count_total_by_demo') & 
            (kpi_df['student_group'] == 'Male')
        ]['value'].iloc[0]
        
        # Validate mathematical relationships
        assert students_affected == 15  # Unique students affected
        assert total_events == 15  # Total incidents (should match sum of demographics)
        assert female_events + male_events == total_events  # Demographics sum to total
        assert students_affected <= total_events  # Students affected cannot exceed total events
        
        # Check that we have records for all three tiers
        tier1_count = len(kpi_df[kpi_df['metric'].str.startswith('safe_students_affected_')])
        tier2_count = len(kpi_df[kpi_df['metric'].str.contains('_by_demo')])
        tier3_count = len(kpi_df[
            (kpi_df['metric'].str.startswith('safe_event_count_')) & 
            (~kpi_df['metric'].str.contains('_by_demo')) & 
            (~kpi_df['metric'].str.contains('students_affected'))
        ])
        
        assert tier1_count > 0, "Tier 1 (students affected) should have records"
        assert tier2_count > 0, "Tier 2 (by demographics) should have records" 
        assert tier3_count > 0, "Tier 3 (total events) should have records"
    
    def test_derived_rates_calculation(self):
        """Test that Tier 4 derived rates are calculated correctly."""
        # Import the derived rates function directly
        from etl.safe_schools_events import _calculate_derived_rates
        
        # Create sample KPI data with three tiers
        sample_kpi_data = pd.DataFrame([
            # Tier 1: Students affected
            {
                'district': 'Test District',
                'school_id': '123',
                'school_name': 'Test School',
                'year': 2023,
                'student_group': 'All Students',
                'county_number': '01',
                'county_name': 'Fayette',
                'district_number': '111',
                'school_code': 'A1',
                'state_school_id': '123',
                'nces_id': '999999123',
                'co_op': 'Central',
                'co_op_code': 'C1',
                'school_type': 'HS',
                'metric': 'safe_students_affected_total',
                'value': 10.0,
                'suppressed': 'N',
                'source_file': 'test',
                'last_updated': '2023-01-01'
            },
            
            # Tier 2: Events by demographics (we'll skip this for rate calculation)
            {
                'district': 'Test District',
                'school_id': '123',
                'school_name': 'Test School',
                'year': 2023,
                'student_group': 'Female',
                'county_number': '01',
                'county_name': 'Fayette',
                'district_number': '111',
                'school_code': 'A1',
                'state_school_id': '123',
                'nces_id': '999999123',
                'co_op': 'Central',
                'co_op_code': 'C1',
                'school_type': 'HS',
                'metric': 'safe_event_count_total_by_demo',
                'value': 15.0,
                'suppressed': 'N',
                'source_file': 'test',
                'last_updated': '2023-01-01'
            },
            
            # Tier 3: Total events 
            {
                'district': 'Test District',
                'school_id': '123',
                'school_name': 'Test School',
                'year': 2023,
                'student_group': 'All Students - Total Events',
                'county_number': '01',
                'county_name': 'Fayette',
                'district_number': '111',
                'school_code': 'A1',
                'state_school_id': '123',
                'nces_id': '999999123',
                'co_op': 'Central',
                'co_op_code': 'C1',
                'school_type': 'HS',
                'metric': 'safe_event_count_total',
                'value': 25.0,
                'suppressed': 'N',
                'source_file': 'test',
                'last_updated': '2023-01-01'
            },
        ])
        
        # Calculate derived rates
        derived_rates = _calculate_derived_rates(sample_kpi_data)
        
        # Check that rates were calculated
        assert not derived_rates.empty, "Derived rates should be generated"
        assert len(derived_rates) > 0, "Should have at least one rate calculation"
        
        # Check that the rate metric is correct
        rate_row = derived_rates[derived_rates['metric'] == 'safe_incident_rate_total']
        assert len(rate_row) == 1, "Should have exactly one total incident rate"
        
        # Verify the calculation: 25 events / 10 students = 2.5 incidents per student
        expected_rate = 25.0 / 10.0
        actual_rate = rate_row['value'].iloc[0]
        assert abs(actual_rate - expected_rate) < 0.001, f"Expected rate {expected_rate}, got {actual_rate}"
        
        # Check other fields
        assert rate_row['student_group'].iloc[0] == 'All Students'
        assert rate_row['suppressed'].iloc[0] == 'N'
        assert 'derived_rates' in rate_row['source_file'].iloc[0]

    def test_kyrc26_names_are_on_the_allow_list(self):
        """The transform reads a filename list. include_kyrc26 does not apply."""
        names = files_to_process()
        for kind in ("Type", "Grade_Level", "Location", "Context"):
            assert f"KYRC26_SAFE_Behavior_Events_by_{kind}.csv" in names
            assert f"KYRC25_SAFE_Behavior_Events_by_{kind}.csv" in names
            assert f"KYRC23_SAFE_Behavior_Events_by_{kind}.csv" not in names
        assert "KYRC23_SAFE_Behavior_Events_by_Type.csv" in rejected_event_filenames()

    def test_grade_tokens_map_before_the_column_dictionary(self):
        """PS and 1st are the KYRC25/KYRC26 grade headers."""
        df = pd.DataFrame(columns=["All Grades", "PS", "K", "1st", "12th", "14th", "Total"])
        renamed = normalize_column_names(df)
        assert "preschool" in renamed.columns
        assert "kindergarten" in renamed.columns
        assert "grade_1" in renamed.columns
        assert "grade_12" in renamed.columns
        assert "grade_14" in renamed.columns
        assert "all_grades" in renamed.columns
        assert "grade_13" not in renamed.columns

    def test_comma_counts_survive_object_and_string_dtype(self):
        """pandas 3 dtype=str is StringDtype. A comma must still parse."""
        frames = [
            pd.DataFrame({"total_events": ["86,009", "0", "*", ""]}),
            pd.DataFrame({"total_events": pd.Series(["86,009", "0", "*", ""], dtype="string")}),
        ]
        for frame in frames:
            cleaned = clean_event_data(frame.copy())
            assert cleaned["total_events"].iloc[0] == 86009
            assert cleaned["total_events"].iloc[1] == 0
            assert pd.isna(cleaned["total_events"].iloc[2])
            assert pd.isna(cleaned["total_events"].iloc[3])
            assert cleaned["_cell_total_events"].iloc[2] == "star"
            assert cleaned["_cell_total_events"].iloc[3] == "blank"

    def test_blank_cell_is_skipped_and_star_is_stored(self):
        """A blank is not a row. A star is NA with suppressed Y. A 0 is kept."""
        df = pd.DataFrame({
            "school_year": ["20252026", "20252026"],
            "district_name": ["Adair County", "Adair County"],
            "school_name": ["Adair County High School", "Adair County High School"],
            "school_code": ["001010", "001010"],
            "state_school_id": ["001001010", "001001010"],
            "demographic": ["All Students", "English Learner"],
            "total_events": ["198", "23,678"],
            "alcohol_events": ["0", ""],
            "assault_1st_degree": ["*", "*"],
            "data_source": ["kyrc24_events_by_type", "kyrc24_events_by_type"],
        })
        df = clean_event_data(df)
        kpi = convert_to_kpi_format(df)
        blank = kpi[(kpi["student_group"] == "English Learner") & (kpi["metric"] == "safe_event_count_alcohol_by_demo")]
        assert blank.empty
        star = kpi[(kpi["student_group"] == "All Students") & (kpi["metric"] == "safe_students_affected_assault_1st")]
        assert len(star) == 1
        assert star["suppressed"].iloc[0] == "Y"
        assert pd.isna(star["value"].iloc[0])
        zero = kpi[(kpi["student_group"] == "All Students") & (kpi["metric"] == "safe_students_affected_alcohol")]
        assert zero["value"].iloc[0] == 0
        assert zero["suppressed"].iloc[0] == "N"
        comma = kpi[(kpi["student_group"] == "English Learner") & (kpi["metric"] == "safe_event_count_total_by_demo")]
        assert comma["value"].iloc[0] == 23678
        assert comma["suppressed"].iloc[0] == "N"

    def test_leftover_kyrc23_file_is_not_read(self):
        """A KYRC23 behavior file republishes 2022-23 and must not add a second copy."""
        leftover = pd.DataFrame({
            "School Year": ["20222023"],
            "District Name": ["All Districts"],
            "School Name": ["All Schools"],
            "School Code": ["999000"],
            "Demographic": ["All Students"],
            "Total": ["999999"],
        })
        path = self.sample_dir / "KYRC23_SAFE_Behavior_Events_by_Type.csv"
        leftover.to_csv(path, index=False)
        result = transform(str(self.raw_dir), str(self.proc_dir), {})
        assert result == ""

    def _copy_fixtures(self):
        fixture_dir = Path(__file__).parent / "fixtures" / "safe_schools_events"
        for path in fixture_dir.glob("*.csv"):
            shutil.copy(path, self.sample_dir / path.name)

    def _lookup(self, output, **filters):
        matched = output
        for column, expected in filters.items():
            matched = matched[matched[column] == expected]
        assert len(matched) == 1, filters
        return matched.iloc[0]

    def test_real_rows_for_2026_and_prior_year_anchors(self):
        """Real KDE rows: state, district, school, a star, a blank, and prior years."""
        self._copy_fixtures()
        result = transform(str(self.raw_dir), str(self.proc_dir), {})
        output = pd.read_csv(result)

        key = ["year", "school_id", "student_group", "metric"]
        assert not output.duplicated(key).any()

        state = self._lookup(
            output,
            year=2026,
            district="All Districts",
            school_name="All Schools",
            student_group="All Students",
            metric="safe_students_affected_total",
        )
        assert state["value"] == 86009
        assert state["suppressed"] == "N"
        assert str(state["school_id"]) in {"999000", "999000.0"}

        events = self._lookup(
            output,
            year=2026,
            district="All Districts",
            school_name="All Schools",
            student_group="All Students - Total Events",
            metric="safe_event_count_total",
        )
        assert events["value"] == 251717
        rate = self._lookup(
            output,
            year=2026,
            district="All Districts",
            school_name="All Schools",
            student_group="All Students",
            metric="safe_incident_rate_total",
        )
        assert rate["value"] == round(251717 / 86009, 3)

        adair = self._lookup(
            output,
            year=2026,
            district="Adair County",
            school_name="All Schools",
            student_group="All Students",
            metric="safe_students_affected_total",
        )
        assert adair["value"] == 400
        assert str(adair["school_id"]) in {"001000", "1000", "1000.0"}

        school = self._lookup(
            output,
            year=2026,
            district="Adair County",
            school_name="Adair County High School",
            student_group="All Students",
            metric="safe_students_affected_weapons",
        )
        assert school["value"] == 0
        assert school["suppressed"] == "N"
        starred = self._lookup(
            output,
            year=2026,
            district="Adair County",
            school_name="Adair County High School",
            student_group="All Students",
            metric="safe_students_affected_harassment",
        )
        assert starred["suppressed"] == "Y"
        assert pd.isna(starred["value"])

        fayette = self._lookup(
            output,
            year=2026,
            district="Fayette County",
            school_name="All Schools",
            student_group="All Students",
            metric="safe_students_affected_total",
        )
        assert fayette["value"] == 5569
        bryan = self._lookup(
            output,
            year=2026,
            district="Fayette County",
            school_name="Bryan Station High School",
            student_group="All Students",
            metric="safe_students_affected_total",
        )
        assert bryan["value"] == 397

        learner = self._lookup(
            output,
            year=2026,
            district="All Districts",
            school_name="All Schools",
            student_group="English Learner",
            metric="safe_event_count_assault_1st_by_demo",
        )
        assert learner["suppressed"] == "Y"
        assert pd.isna(learner["value"])
        learner_total = self._lookup(
            output,
            year=2026,
            district="All Districts",
            school_name="All Schools",
            student_group="English Learner",
            metric="safe_event_count_total_by_demo",
        )
        assert learner_total["value"] == 23678
        assert learner_total["suppressed"] == "N"

        preschool = self._lookup(
            output,
            year=2026,
            district="All Districts",
            school_name="All Schools",
            student_group="All Students",
            metric="safe_students_affected_preschool",
        )
        assert preschool["value"] == 108
        grade_1 = self._lookup(
            output,
            year=2026,
            district="All Districts",
            school_name="All Schools",
            student_group="All Students",
            metric="safe_students_affected_grade_1",
        )
        assert grade_1["value"] == 2078
        grade_14 = self._lookup(
            output,
            year=2026,
            district="Adair County",
            school_name="Adair County High School",
            student_group="All Students",
            metric="safe_students_affected_grade_14",
        )
        assert grade_14["value"] == 0
        assert grade_14["suppressed"] == "N"
        skipped_ps = output[
            (output["year"] == 2026)
            & (output["school_name"] == "Adair County High School")
            & (output["student_group"] == "All Students")
            & (output["metric"] == "safe_students_affected_preschool")
        ]
        assert skipped_ps.empty

        classroom = self._lookup(
            output,
            year=2026,
            district="All Districts",
            school_name="All Schools",
            student_group="All Students",
            metric="safe_students_affected_classroom",
        )
        assert classroom["value"] == 61862
        during = self._lookup(
            output,
            year=2026,
            district="All Districts",
            school_name="All Schools",
            student_group="All Students",
            metric="safe_students_affected_school_sponsored_during",
        )
        assert during["value"] == 83342
        # The location and context files also publish Total. It is not a second metric.
        location_total = output[
            (output["year"] == 2026)
            & (output["metric"] == "safe_students_affected_total")
            & (output["district"] == "All Districts")
            & (output["student_group"] == "All Students")
        ]
        assert len(location_total) == 1

        prior_2025 = self._lookup(
            output,
            year=2025,
            district="All Districts",
            school_name="All Schools",
            student_group="All Students",
            metric="safe_students_affected_total",
        )
        assert prior_2025["value"] == 83921
        prior_2024 = self._lookup(
            output,
            year=2024,
            district="All Districts",
            school_name="All Schools",
            student_group="All Students",
            metric="safe_students_affected_total",
        )
        assert prior_2024["value"] == 89951
        prior_2023 = self._lookup(
            output,
            year=2023,
            district="State",
            school_name="---State Total---",
            student_group="All Students",
            metric="safe_students_affected_total",
        )
        assert prior_2023["value"] == 92772
        assert str(prior_2023["school_id"]) in {"999", "999.0"}

        # 2020 Adair district American Indian row is starred on every type count.
        all_star = output[
            (output["year"] == 2020)
            & (output["district"] == "Adair County")
            & (output["student_group"] == "American Indian or Alaska Native")
            & (output["metric"] == "safe_event_count_drugs_by_demo")
        ]
        assert len(all_star) == 1
        assert all_star["suppressed"].iloc[0] == "Y"
        assert pd.isna(all_star["value"].iloc[0])

        # 2020 Adair County High School preschool cell is blank and is not stored.
        blank_2020 = output[
            (output["year"] == 2020)
            & (output["school_name"] == "Adair County High School")
            & (output["student_group"] == "All Students")
            & (output["metric"] == "safe_students_affected_preschool")
        ]
        assert blank_2020.empty
        grade9 = self._lookup(
            output,
            year=2020,
            school_name="Adair County High School",
            student_group="All Students",
            metric="safe_students_affected_grade_9",
        )
        assert grade9["value"] == 65


if __name__ == "__main__":
    pytest.main([__file__, "-v"])