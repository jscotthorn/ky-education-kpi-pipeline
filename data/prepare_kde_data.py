#!/usr/bin/env python3
"""
Kentucky Department of Education (KDE) Data Preparation Tool

Prepares raw source files from KDE Historical Datasets to populate data/raw directories.
Based on configuration in config/kde_sources.yaml.

Usage:
    python3 data/prepare_kde_data.py                    # Prepare all configured datasets
    python3 data/prepare_kde_data.py chronic_absenteeism # Prepare specific dataset
    python3 data/prepare_kde_data.py graduation_rates postsecondary_readiness # Multiple datasets
"""

import argparse
import logging
import os
import requests
import sys
import time
import yaml
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KDEDownloader:
    """Prepares files from Kentucky Department of Education Historical Datasets"""
    
    def __init__(self, config_path: str = "config/kde_sources.yaml"):
        """Initialize preparation tool with configuration file"""
        self.project_root = Path(__file__).parent.parent
        self.config_path = self.project_root / config_path
        self.raw_data_path = self.project_root / "data" / "raw"
        
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            self.config = yaml.safe_load(f)
    
    def _base_url_for(self, url_type: str) -> str:
        """Resolve a config url type to its download prefix."""
        if url_type == "2025":
            return self.config.get("base_url_2025", self.config["base_url"])
        if url_type == "kyrc25":
            return self.config.get("base_url_kyrc25", self.config["base_url"])
        if url_type == "kyrc26":
            return self.config.get(
                "base_url_kyrc26",
                self.config.get("base_url_kyrc25", self.config["base_url"]),
            )
        if url_type == "historical":
            return self.config.get("base_url_historical", self.config["base_url"])
        return self.config["base_url"]

    def _entry_url(self, file_entry: object) -> tuple[str, str]:
        """Return (filename, absolute url) for a string or dict config entry."""
        if isinstance(file_entry, dict):
            url_type = str(file_entry.get("url", "default"))
            filename = str(file_entry["file"])
            base_url = self._base_url_for(url_type)
        else:
            filename = str(file_entry)
            base_url = self.config["base_url"]
        return filename, f"{base_url}{filename}"

    def _meta_path(self, file_path: Path) -> Path:
        return file_path.with_name(file_path.name + ".kde-source.yaml")

    def _read_source_meta(self, file_path: Path) -> Optional[Dict[str, str]]:
        meta_path = self._meta_path(file_path)
        if not meta_path.exists():
            return None
        with open(meta_path, "r", encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle) or {}
        if not isinstance(loaded, dict):
            return None
        return {str(key): str(value) if value is not None else "" for key, value in loaded.items()}

    def _write_source_meta(self, file_path: Path, meta: Dict[str, str]) -> None:
        meta_path = self._meta_path(file_path)
        with open(meta_path, "w", encoding="utf-8") as handle:
            yaml.safe_dump(meta, handle, sort_keys=False)

    def _probe_remote(self, url: str, timeout: int = 30) -> Dict[str, str]:
        """Read Last-Modified and ETag without downloading the body.

        Open House answers HEAD with 403, so fall back to a one-byte GET.
        """
        headers = self.config["headers"]
        response = requests.head(url, headers=headers, timeout=timeout, allow_redirects=True)
        try:
            if response.status_code == 404:
                response.raise_for_status()
            needs_get = response.status_code in (403, 405) or not response.headers.get("Last-Modified")
            if needs_get:
                response.close()
                ranged = dict(headers)
                ranged["Range"] = "bytes=0-0"
                response = requests.get(url, headers=ranged, timeout=timeout, stream=True)
            response.raise_for_status()
            return {
                "url": url,
                "last_modified": response.headers.get("Last-Modified") or "",
                "etag": response.headers.get("ETag") or "",
                "content_md5": response.headers.get("Content-MD5") or "",
            }
        finally:
            response.close()

    def _probe_or_missing(self, url: str) -> Optional[Dict[str, str]]:
        """Probe url. A 404 is absence. Other request errors propagate."""
        try:
            return self._probe_remote(url)
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status == 404:
                return None
            raise

    def _kyrc_alternate_url(self, filename: str, primary_url: str) -> Optional[str]:
        """UAT container URL for a KYRC25/KYRC26 file, when configured.

        The report-card download page publishes from datasetsuat6051276.
        datasets is the later copy this tool prefers when the bytes match.
        """
        uat = self.config.get("base_url_kyrc_uat")
        if not uat:
            return None
        if not (filename.startswith("KYRC25_") or filename.startswith("KYRC26_")):
            return None
        if primary_url.startswith(str(uat)):
            return None
        if not str(uat).endswith("/"):
            uat = str(uat) + "/"
        return f"{uat}{filename}"

    @staticmethod
    def _modified_at(stamp: str):
        if not stamp:
            return None
        try:
            return parsedate_to_datetime(stamp)
        except (TypeError, ValueError, IndexError):
            return None

    def _choose_source(self, filename: str, primary_url: str) -> Dict[str, str]:
        """Pick datasets or the UAT container for one KYRC file.

        Matching or missing Content-MD5 keeps the primary URL that answered.
        Differing Content-MD5 keeps the newer Last-Modified. A 404 on the
        primary uses the alternate.
        """
        alternate_url = self._kyrc_alternate_url(filename, primary_url)
        if not alternate_url:
            return self._probe_remote(primary_url)

        primary_error: Optional[Exception] = None
        alternate_error: Optional[Exception] = None
        try:
            primary = self._probe_or_missing(primary_url)
        except requests.exceptions.RequestException as exc:
            primary = None
            primary_error = exc
        try:
            alternate = self._probe_or_missing(alternate_url)
        except requests.exceptions.RequestException as exc:
            alternate = None
            alternate_error = exc

        if primary and alternate:
            primary_md5 = primary.get("content_md5") or ""
            alternate_md5 = alternate.get("content_md5") or ""
            if not primary_md5 or not alternate_md5 or primary_md5 == alternate_md5:
                return primary
            primary_at = self._modified_at(primary.get("last_modified") or "")
            alternate_at = self._modified_at(alternate.get("last_modified") or "")
            if alternate_at and (primary_at is None or alternate_at > primary_at):
                logger.info(
                    "Content-MD5 differs for %s; using newer Last-Modified %s from %s",
                    filename,
                    alternate.get("last_modified"),
                    alternate["url"],
                )
                return alternate
            logger.info(
                "Content-MD5 differs for %s; using newer Last-Modified %s from %s",
                filename,
                primary.get("last_modified"),
                primary["url"],
            )
            return primary
        if primary:
            return primary
        if alternate:
            logger.info(
                "%s is unavailable at %s; using %s",
                filename,
                primary_url,
                alternate["url"],
            )
            return alternate
        if primary_error:
            raise primary_error
        if alternate_error:
            raise alternate_error
        raise requests.exceptions.HTTPError(f"{filename} returned 404 on {primary_url} and {alternate_url}")

    def _same_remote_version(self, local: Dict[str, str], remote: Dict[str, str]) -> bool:
        """True when the stored stamp still matches the remote object."""
        remote_modified = remote.get("last_modified") or ""
        local_modified = local.get("last_modified") or ""
        if not remote_modified or local_modified != remote_modified:
            return False
        remote_etag = remote.get("etag") or ""
        local_etag = local.get("etag") or ""
        if remote_etag and local_etag and remote_etag != local_etag:
            return False
        remote_md5 = remote.get("content_md5") or ""
        local_md5 = local.get("content_md5") or ""
        if remote_md5 and local_md5 and remote_md5 != local_md5:
            return False
        return True

    def download_file(self, url: str, file_path: Path, timeout: int = 30) -> Optional[Dict[str, str]]:
        """Download one file. Returns the source stamp, or None on failure."""
        partial = file_path.with_name(file_path.name + ".partial")
        try:
            logger.info(f"Preparing {file_path.name}...")
            response = requests.get(
                url,
                headers=self.config["headers"],
                timeout=timeout,
                stream=True,
            )
            response.raise_for_status()

            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(partial, "wb") as handle:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        handle.write(chunk)
            partial.replace(file_path)

            file_size = file_path.stat().st_size
            logger.info(f"✓ Downloaded: {file_path.name} ({file_size:,} bytes)")
            return {
                "url": url,
                "last_modified": response.headers.get("Last-Modified") or "",
                "etag": response.headers.get("ETag") or "",
                "content_md5": response.headers.get("Content-MD5") or "",
                "content_length": str(file_size),
            }
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ Failed to download {file_path.name}: {e}")
            return None
        except Exception as e:
            logger.error(f"✗ Unexpected error downloading {file_path.name}: {e}")
            return None
        finally:
            if partial.exists():
                partial.unlink()

    def _sync_file(self, url: str, file_path: Path) -> bool:
        """Download url when the local copy is missing or the remote stamp changed.

        KYRC25 and KYRC26 files also probe base_url_kyrc_uat when that key is set.
        """
        local_meta = self._read_source_meta(file_path) if file_path.exists() else None
        remote: Optional[Dict[str, str]] = None
        chosen_url = url
        needs_probe = bool(self._kyrc_alternate_url(file_path.name, url)) or (
            file_path.exists() and local_meta is not None
        )
        if needs_probe:
            try:
                remote = self._choose_source(file_path.name, url)
            except requests.exceptions.RequestException as e:
                logger.error(f"✗ Failed to check {file_path.name}: {e}")
                return False
            chosen_url = remote["url"]
            if file_path.exists() and local_meta and self._same_remote_version(local_meta, remote):
                logger.info(
                    "Skipping %s (Last-Modified unchanged: %s)",
                    file_path.name,
                    remote.get("last_modified"),
                )
                return True
            if file_path.exists() and local_meta:
                logger.info(
                    "Re-downloading %s; Last-Modified changed from %s to %s",
                    file_path.name,
                    local_meta.get("last_modified"),
                    remote.get("last_modified"),
                )
        elif file_path.exists():
            logger.info("Re-downloading %s; no source stamp is recorded", file_path.name)

        stamp = self.download_file(chosen_url, file_path)
        if stamp is None:
            return False
        if remote:
            if not stamp.get("content_md5"):
                stamp["content_md5"] = remote.get("content_md5") or ""
            if not stamp.get("last_modified"):
                stamp["last_modified"] = remote.get("last_modified") or ""
            if not stamp.get("etag"):
                stamp["etag"] = remote.get("etag") or ""
        self._write_source_meta(file_path, stamp)
        return True

    def download_directory(self, directory: str) -> Dict[str, bool]:
        """Download all files for a specific raw data directory"""
        if directory not in self.config["raw_directories"]:
            raise ValueError(f"Directory '{directory}' not found in configuration")

        files = self.config["raw_directories"][directory]
        target_dir = self.raw_data_path / directory
        results = {}

        logger.info(f"Downloading {len(files)} files for {directory}")

        for file_entry in files:
            filename, url = self._entry_url(file_entry)
            file_path = target_dir / filename
            results[filename] = self._sync_file(url, file_path)
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