"""
Dashboard Data Generator

Converts KPI CSV files to JSON format for web dashboard visualization.
Creates heatmap data showing schools vs demographics with rate values.
"""
import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Any
import argparse


def generate_heatmap_data(df: pd.DataFrame, metric: str, year: str = None) -> Dict[str, Any]:
    """
    Generate heatmap data for a specific metric.
    
    Args:
        df: KPI dataframe
        metric: The rate metric to visualize (e.g., 'graduation_rate_4_year')
        year: Optional year filter (uses latest year if None)
    
    Returns:
        Dictionary with heatmap data structure
    """
    # Filter to specified metric and non-suppressed data
    metric_data = df[
        (df['metric'] == metric) & 
        (df['suppressed'] == 'N') &
        (df['value'].notna())
    ].copy()
    
    if metric_data.empty:
        return {
            'metric': metric,
            'year': year,
            'schools': [],
            'demographics': [],
            'values': [],
            'error': 'No data available for this metric'
        }
    
    # Filter by year if specified, otherwise use latest year
    if year:
        metric_data = metric_data[metric_data['year'].astype(str) == str(year)]
    else:
        latest_year = metric_data['year'].max()
        metric_data = metric_data[metric_data['year'] == latest_year]
        year = str(latest_year)
    
    # Convert value to numeric
    metric_data['value'] = pd.to_numeric(metric_data['value'], errors='coerce')
    
    # Create pivot table: schools vs demographics
    pivot_df = metric_data.pivot_table(
        index='school_name',
        columns='student_group', 
        values='value',
        aggfunc='mean'  # Average if multiple values per school/group
    )
    
    # Sort schools by average rate (descending)
    school_averages = pivot_df.mean(axis=1, skipna=True).sort_values(ascending=False)
    sorted_schools = school_averages.index.tolist()
    
    # Sort demographics alphabetically for consistency
    sorted_demographics = sorted(pivot_df.columns.tolist())
    
    # Reorder pivot table
    pivot_df = pivot_df.reindex(index=sorted_schools, columns=sorted_demographics)
    
    # Convert to lists for JSON serialization
    schools = pivot_df.index.tolist()
    demographics = pivot_df.columns.tolist()
    
    # Create 2D array of values (schools x demographics)
    values = []
    for school in schools:
        school_values = []
        for demo in demographics:
            value = pivot_df.loc[school, demo]
            # Convert NaN to null for JSON
            school_values.append(None if pd.isna(value) else round(float(value), 1))
        values.append(school_values)
    
    return {
        'metric': metric,
        'year': year,
        'schools': schools,
        'demographics': demographics,
        'values': values,
        'stats': {
            'total_schools': len(schools),
            'total_demographics': len(demographics),
            'data_points': sum(1 for row in values for val in row if val is not None),
            'min_value': float(pivot_df.min().min()) if not pivot_df.empty else None,
            'max_value': float(pivot_df.max().max()) if not pivot_df.empty else None,
            'avg_value': float(pivot_df.mean().mean()) if not pivot_df.empty else None
        }
    }


def process_kpi_file(file_path: Path, fayette_only: bool = True) -> Dict[str, Any]:
    """
    Process a KPI CSV file and extract rate metrics.
    
    Args:
        file_path: Path to KPI CSV file
        fayette_only: Filter to Fayette County only
    
    Returns:
        Dictionary with available metrics and years
    """
    try:
        df = pd.read_csv(file_path)
        
        # Filter to Fayette County if requested
        if fayette_only:
            df = df[df['district'].str.contains('Fayette', case=False, na=False)]
        
        # Get rate and selected count metrics
        rate_metrics = df[df['metric'].str.contains('_rate', na=False)]['metric'].unique().tolist()
        
        # Add specific suspension count metrics for visualization
        suspension_metrics = df[df['metric'].str.contains('out_of_school_suspension.*_count', na=False, regex=True)]['metric'].unique().tolist()
        
        # Include total suspension counts for dashboard
        total_suspension_metrics = [m for m in suspension_metrics if 'total_count' in m]
        rate_metrics.extend(total_suspension_metrics)
        
        # Get available years
        years = sorted(df['year'].astype(str).unique().tolist())
        
        return {
            'source_file': file_path.name,
            'rate_metrics': rate_metrics,
            'years': years,
            'total_rows': len(df),
            'data_available': len(df) > 0
        }
        
    except Exception as e:
        return {
            'source_file': file_path.name,
            'error': str(e),
            'data_available': False
        }


def generate_dashboard_json(processed_dir: Path, output_dir: Path, fayette_only: bool = True):
    """
    Generate JSON files for dashboard from all KPI CSV files.
    
    Args:
        processed_dir: Directory containing processed KPI CSV files
        output_dir: Directory to save JSON files
        fayette_only: Filter to Fayette County only
    """
    output_dir.mkdir(exist_ok=True)
    
    # Find all KPI CSV files (exclude audit files)
    kpi_files = [f for f in processed_dir.glob("*.csv") if not "audit" in f.name]
    
    dashboard_config = {
        'generated_at': pd.Timestamp.now().isoformat(),
        'fayette_only': fayette_only,
        'sources': {}
    }
    
    print(f"Processing {len(kpi_files)} KPI files...")
    
    for kpi_file in kpi_files:
        print(f"\nProcessing {kpi_file.name}...")
        
        # Process file metadata
        file_info = process_kpi_file(kpi_file, fayette_only)
        source_name = kpi_file.stem  # filename without extension
        
        dashboard_config['sources'][source_name] = file_info
        
        if not file_info['data_available']:
            print(f"  Skipping {kpi_file.name}: {file_info.get('error', 'No data available')}")
            continue
        
        # Load data for heatmap generation
        df = pd.read_csv(kpi_file)
        if fayette_only:
            df = df[df['district'].str.contains('Fayette', case=False, na=False)]
        
        # Generate heatmap data for each rate metric
        for metric in file_info['rate_metrics']:
            print(f"  Generating heatmap for {metric}...")
            
            # Generate for latest year
            heatmap_data = generate_heatmap_data(df, metric)
            
            # Save individual metric JSON file
            metric_filename = f"{source_name}_{metric}.json"
            metric_path = output_dir / metric_filename
            
            with open(metric_path, 'w') as f:
                json.dump(heatmap_data, f, indent=2)
            
            # Handle both successful and error responses
            if 'error' in heatmap_data:
                print(f"    Saved {metric_filename}: No data ({heatmap_data['error']})")
            elif 'stats' in heatmap_data:
                print(f"    Saved {metric_filename}: {heatmap_data['stats']['data_points']} data points")
            else:
                print(f"    Saved {metric_filename}: Unknown status")
    
    # Save dashboard configuration
    config_path = output_dir / "dashboard_config.json"
    with open(config_path, 'w') as f:
        json.dump(dashboard_config, f, indent=2)
    
    print(f"\nDashboard data generated in {output_dir}")
    print(f"Configuration saved to {config_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate dashboard JSON data from KPI CSV files")
    parser.add_argument("--processed-dir", type=Path, default="data/processed", 
                       help="Directory containing processed KPI CSV files")
    parser.add_argument("--output-dir", type=Path, default="html/data",
                       help="Directory to save JSON files")
    parser.add_argument("--all-districts", action="store_true",
                       help="Include all districts (default: Fayette County only)")
    
    args = parser.parse_args()
    
    fayette_only = not args.all_districts
    
    print("Equity ETL Dashboard Data Generator")
    print("=" * 40)
    print(f"Input directory: {args.processed_dir}")
    print(f"Output directory: {args.output_dir}")
    print(f"Filter: {'Fayette County only' if fayette_only else 'All districts'}")
    
    generate_dashboard_json(args.processed_dir, args.output_dir, fayette_only)