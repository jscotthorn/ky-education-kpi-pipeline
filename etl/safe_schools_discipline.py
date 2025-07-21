"""
Safe Schools Discipline ETL Module

Processes Kentucky safe schools discipline data including:
- KYRC24_SAFE_Discipline_Resolutions.csv
- KYRC24_SAFE_Legal_Sanctions.csv  
- Historical safe_schools_discipline_[year].csv files

Generates standardized KPIs for suspension rates, expulsion rates, 
arrest rates, and SRO involvement.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime

import sys
from pathlib import Path

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL

logger = logging.getLogger(__name__)


class SafeSchoolsDisciplineETL(BaseETL):
    """ETL processor for safe schools discipline data."""
    
    def __init__(self):
        super().__init__()
        self.source_name = "safe_schools_discipline"
        
    @property 
    def module_column_mappings(self) -> Dict[str, str]:
        """Module-specific column mappings."""
        return {
            # KYRC24 Discipline Resolutions columns
            'Corporal Punishment (SSP5)': 'corporal_punishment_count',
            'Restraint (SSP7)': 'restraint_count',
            'Seclusion (SSP8)': 'seclusion_count',
            'Expelled, Not Receiving Services (SSP2)': 'expelled_not_receiving_services_count',
            'Expelled, Receiving Services (SSP1)': 'expelled_receiving_services_count',
            'In-School Removal (INSR) Or In-District Removal (INDR) >=.5': 'in_school_removal_count',
            'Out-Of-School Suspensions (SSP3)': 'out_of_school_suspension_count',
            'Removal By Hearing Officer (IAES2)': 'removal_by_hearing_officer_count',
            'Unilateral Removal By School Personnel (IAES1)': 'unilateral_removal_count',
            
            # KYRC24 Legal Sanctions columns
            'Arrests': 'arrest_count',
            'Charges': 'charges_count', 
            'Civil Proceedings': 'civil_proceedings_count',
            'Court Designated Worker Involvement': 'court_designated_worker_count',
            'School Resource Officer Involvement': 'school_resource_officer_count',
            
            # Historical columns (uppercase versions)
            'EXPELLED RECEIVING SERVICES SSP1': 'expelled_receiving_services_count',
            'EXPELLED NOT RECEIVING SERVICES SSP2': 'expelled_not_receiving_services_count',
            'OUT OF SCHOOL SUSPENSION SSP3': 'out_of_school_suspension_count',
            'CORPORAL PUNISHMENT SSP5': 'corporal_punishment_count',
            'IN-SCHOOL REMOVAL INSR': 'in_school_removal_count',
            'RESTRAINT SSP7': 'restraint_count',
            'SECLUSION SSP8': 'seclusion_count',
            'UNILATERAL REMOVAL BY SCHOOL PERSONNEL IAES1': 'unilateral_removal_count',
            'REMOVAL BY HEARING OFFICER IAES2': 'removal_by_hearing_officer_count',
            'TOTAL DISCIPLINE RESOLUTIONS': 'total_discipline_resolutions',
            
            # Total columns
            'Total': 'total'
        }
        
    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """Extract metric values from a data row."""
        metrics = {}
        
        # Get total for rate calculations
        total = pd.to_numeric(row.get('total', 0), errors='coerce')
        if pd.isna(total) or total == 0:
            total = pd.to_numeric(row.get('total_discipline_resolutions', 0), errors='coerce')
            
        if total == 0:
            return metrics
            
        # Define metric mappings for rates
        discipline_metrics = {
            'corporal_punishment_rate': 'corporal_punishment_count',
            'restraint_rate': 'restraint_count',
            'seclusion_rate': 'seclusion_count', 
            'expelled_not_receiving_services_rate': 'expelled_not_receiving_services_count',
            'expelled_receiving_services_rate': 'expelled_receiving_services_count',
            'in_school_removal_rate': 'in_school_removal_count',
            'out_of_school_suspension_rate': 'out_of_school_suspension_count',
            'removal_by_hearing_officer_rate': 'removal_by_hearing_officer_count',
            'unilateral_removal_rate': 'unilateral_removal_count',
            'arrest_rate': 'arrest_count',
            'charges_rate': 'charges_count',
            'civil_proceedings_rate': 'civil_proceedings_count',
            'court_designated_worker_rate': 'court_designated_worker_count',
            'school_resource_officer_rate': 'school_resource_officer_count'
        }
        
        for rate_name, count_column in discipline_metrics.items():
            if count_column in row and pd.notna(row[count_column]):
                count = pd.to_numeric(row[count_column], errors='coerce')
                if pd.notna(count) and pd.notna(total) and total > 0:
                    rate = (count / total * 100)
                    if rate > 0:  # Only include non-zero rates
                        metrics[rate_name] = round(rate, 2)
                    
        return metrics
        
    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get defaults for suppressed metrics for a row."""
        return {
            'corporal_punishment_rate': pd.NA,
            'restraint_rate': pd.NA,
            'seclusion_rate': pd.NA,
            'expelled_not_receiving_services_rate': pd.NA,
            'expelled_receiving_services_rate': pd.NA,
            'in_school_removal_rate': pd.NA,
            'out_of_school_suspension_rate': pd.NA,
            'removal_by_hearing_officer_rate': pd.NA,
            'unilateral_removal_rate': pd.NA,
            'arrest_rate': pd.NA,
            'charges_rate': pd.NA,
            'civil_proceedings_rate': pd.NA,
            'court_designated_worker_rate': pd.NA,
            'school_resource_officer_rate': pd.NA
        }


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read safe schools discipline files, normalize, and convert to KPI format with demographic standardization using BaseETL."""
    # Use BaseETL for consistent processing
    etl = SafeSchoolsDisciplineETL()
    etl.transform(raw_dir, proc_dir, cfg)


def main():
    """Main execution function."""
    import sys
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        from pathlib import Path
        raw_dir = Path(__file__).parent.parent / "data" / "raw"
        proc_dir = Path(__file__).parent.parent / "data" / "processed"
        proc_dir.mkdir(exist_ok=True)
        
        
        test_config = {
            "derive": {
                "processing_date": "2025-07-20",
                "data_quality_flag": "reviewed"
            }
        }
        
        transform(raw_dir, proc_dir, test_config)
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()