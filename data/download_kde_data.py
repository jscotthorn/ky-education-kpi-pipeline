#!/usr/bin/env python3
"""
Kentucky Department of Education (KDE) Data Downloader

Downloads raw source files from KDE Historical Datasets to populate data/raw directories.
Based on configuration in config/kde_sources.json.

Usage:
    python3 data/download_kde_data.py                    # Download all configured datasets
    python3 data/download_kde_data.py chronic_absenteeism # Download specific dataset
    python3 data/download_kde_data.py graduation_rates postsecondary_readiness # Multiple datasets
"""

import argparse
import logging
import os
import requests
import sys
import time
import yaml
from pathlib import Path
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KDEDownloader:
    """Downloads files from Kentucky Department of Education Historical Datasets"""
    
    def __init__(self, config_path: str = "config/kde_sources.yaml"):
        """Initialize downloader with configuration file"""
        self.project_root = Path(__file__).parent.parent
        self.config_path = self.project_root / config_path
        self.raw_data_path = self.project_root / "data" / "raw"
        
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            self.config = yaml.safe_load(f)
    
    def download_file(self, url: str, file_path: Path, timeout: int = 30) -> bool:
        """Download a single file from KDE with retry logic"""
        try:
            logger.info(f"Downloading {file_path.name}...")
            response = requests.get(
                url, 
                headers=self.config["headers"],
                timeout=timeout,
                stream=True
            )
            response.raise_for_status()
            
            # Create parent directory if it doesn't exist
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write file in chunks to handle large files
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            file_size = file_path.stat().st_size
            logger.info(f"✓ Downloaded: {file_path.name} ({file_size:,} bytes)")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ Failed to download {file_path.name}: {e}")
            return False
        except Exception as e:
            logger.error(f"✗ Unexpected error downloading {file_path.name}: {e}")
            return False
    
    def download_directory(self, directory: str) -> Dict[str, bool]:
        """Download all files for a specific raw data directory"""
        if directory not in self.config["raw_directories"]:
            raise ValueError(f"Directory '{directory}' not found in configuration")
        
        files = self.config["raw_directories"][directory]
        target_dir = self.raw_data_path / directory
        results = {}
        
        logger.info(f"Downloading {len(files)} files for {directory}")
        
        for filename in files:
            url = f"{self.config['base_url']}{filename}"
            file_path = target_dir / filename
            
            # Skip if file already exists
            if file_path.exists():
                logger.info(f"Skipping {filename} (already exists)")
                results[filename] = True
                continue
            
            success = self.download_file(url, file_path)
            results[filename] = success
            
            # Rate limiting - small delay between requests
            time.sleep(0.5)
        
        return results
    
    def download_all(self) -> Dict[str, Dict[str, bool]]:
        """Download all configured directories"""
        all_results = {}
        
        for directory in self.config["raw_directories"].keys():
            logger.info(f"\n=== Processing directory: {directory} ===")
            all_results[directory] = self.download_directory(directory)
            
            # Longer delay between directories
            time.sleep(1.0)
        
        return all_results
    
    def list_available_directories(self) -> List[str]:
        """List all available raw data directories"""
        return list(self.config["raw_directories"].keys())

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Download Kentucky Department of Education raw data files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 data/download_kde_data.py                     # Download all datasets
  python3 data/download_kde_data.py chronic_absenteeism # Download specific dataset
  python3 data/download_kde_data.py graduation_rates postsecondary_readiness # Multiple
        """
    )
    
    parser.add_argument(
        'directories',
        nargs='*',
        help='Specific directories to download (if none provided, downloads all)'
    )
    
    parser.add_argument(
        '--list',
        action='store_true',
        help='List all available directories and exit'
    )
    
    parser.add_argument(
        '--config',
        default='config/kde_sources.yaml',
        help='Path to configuration file (default: config/kde_sources.yaml)'
    )
    
    args = parser.parse_args()
    
    try:
        downloader = KDEDownloader(args.config)
        
        if args.list:
            directories = downloader.list_available_directories()
            print("Available directories:")
            for directory in sorted(directories):
                file_count = len(downloader.config["raw_directories"][directory])
                print(f"  {directory} ({file_count} files)")
            return
        
        # Determine which directories to download
        if args.directories:
            # Validate provided directories
            available_dirs = set(downloader.list_available_directories())
            requested_dirs = set(args.directories)
            invalid_dirs = requested_dirs - available_dirs
            
            if invalid_dirs:
                logger.error(f"Invalid directories: {', '.join(invalid_dirs)}")
                logger.info(f"Available directories: {', '.join(sorted(available_dirs))}")
                sys.exit(1)
            
            # Download specific directories
            all_results = {}
            for directory in args.directories:
                logger.info(f"\n=== Processing directory: {directory} ===")
                all_results[directory] = downloader.download_directory(directory)
                time.sleep(1.0)
        else:
            # Download all directories
            all_results = downloader.download_all()
        
        # Report results
        logger.info("\n=== Download Summary ===")
        total_files = 0
        successful_files = 0
        
        for directory, results in all_results.items():
            successes = sum(1 for success in results.values() if success)
            total = len(results)
            total_files += total
            successful_files += successes
            
            logger.info(f"{directory}: {successes}/{total} files downloaded")
        
        logger.info(f"Overall: {successful_files}/{total_files} files downloaded")
        
        if successful_files < total_files:
            logger.warning(f"{total_files - successful_files} files failed to download")
            sys.exit(1)
        
        logger.info("All downloads completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("\nDownload interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()