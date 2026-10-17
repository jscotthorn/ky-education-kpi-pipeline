"""Tests for shared KDE download behavior.

These cover the 2025-26 prep: the kyrc26 URL, and replacing skip-if-exists
with a Last-Modified check. Requests are mocked.
"""

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
import yaml


def _load_downloader_module():
    path = Path(__file__).resolve().parents[1] / "data" / "prepare_kde_data.py"
    spec = importlib.util.spec_from_file_location("prepare_kde_data", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def downloader(tmp_path, monkeypatch):
    module = _load_downloader_module()
    monkeypatch.setattr(module.time, "sleep", lambda *_args, **_kwargs: None)
    config = {
        "base_url": "https://historical.example/",
        "base_url_2025": "https://temporary.example/",
        "base_url_kyrc25": "https://kdeschoolreportcard.blob.core.windows.net/datasets/",
        "base_url_kyrc26": "https://kdeschoolreportcard.blob.core.windows.net/datasets/",
        "headers": {"User-Agent": "test"},
        "raw_directories": {
            "chronic_absenteeism": [
                {"url": "kyrc26", "file": "KYRC26_OVW_Chronic_Absenteeism.csv"},
                {"url": "kyrc25", "file": "KYRC25_OVW_Chronic_Absenteeism.csv"},
            ]
        },
    }
    config_path = tmp_path / "kde_sources.yaml"
    config_path.write_text(yaml.safe_dump(config))
    tool = module.KDEDownloader(str(config_path))
    tool.raw_data_path = tmp_path / "raw"
    return module, tool


def _response(
    content: bytes,
    last_modified: str,
    etag: str = '"v1"',
    content_md5: str = "",
    status_code: int = 200,
) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.headers = {"Last-Modified": last_modified, "ETag": etag}
    if content_md5:
        response.headers["Content-MD5"] = content_md5
    response.iter_content = lambda chunk_size=8192: [content]
    if status_code >= 400:
        response.raise_for_status = MagicMock(
            side_effect=requests.HTTPError(response=response)
        )
    else:
        response.raise_for_status = MagicMock()
    response.close = MagicMock()
    return response


def test_kyrc26_url_uses_the_datasets_container(downloader):
    _module, tool = downloader
    filename, url = tool._entry_url({"url": "kyrc26", "file": "KYRC26_OVW_Chronic_Absenteeism.csv"})
    assert filename == "KYRC26_OVW_Chronic_Absenteeism.csv"
    assert url == (
        "https://kdeschoolreportcard.blob.core.windows.net/datasets/"
        "KYRC26_OVW_Chronic_Absenteeism.csv"
    )


def test_missing_file_is_downloaded_and_stamped(downloader, monkeypatch):
    module, tool = downloader
    stamp = "Thu, 15 Oct 2026 10:00:10 GMT"
    monkeypatch.setattr(module.requests, "get", lambda *a, **k: _response(b"h\n", stamp))

    results = tool.download_directory("chronic_absenteeism")

    assert results["KYRC26_OVW_Chronic_Absenteeism.csv"] is True
    stored = tool.raw_data_path / "chronic_absenteeism" / "KYRC26_OVW_Chronic_Absenteeism.csv"
    assert stored.read_bytes() == b"h\n"
    meta = tool._read_source_meta(stored)
    assert meta["last_modified"] == stamp
    assert meta["etag"] == '"v1"'


def test_unchanged_last_modified_skips_the_download(downloader, monkeypatch):
    module, tool = downloader
    stamp = "Thu, 15 Oct 2026 10:00:10 GMT"
    target = tool.raw_data_path / "chronic_absenteeism" / "KYRC26_OVW_Chronic_Absenteeism.csv"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"original")
    tool._write_source_meta(target, {"url": "https://example/old", "last_modified": stamp, "etag": '"v1"'})

    def fail_get(*_args, **_kwargs):
        raise AssertionError("GET should not run when Last-Modified matches")

    monkeypatch.setattr(module.requests, "head", lambda *a, **k: _response(b"", stamp))
    monkeypatch.setattr(module.requests, "get", fail_get)

    # Only the kyrc26 entry, so the second file does not also probe.
    tool.config["raw_directories"]["chronic_absenteeism"] = [
        {"url": "kyrc26", "file": "KYRC26_OVW_Chronic_Absenteeism.csv"}
    ]
    results = tool.download_directory("chronic_absenteeism")

    assert results["KYRC26_OVW_Chronic_Absenteeism.csv"] is True
    assert target.read_bytes() == b"original"


def test_changed_last_modified_redownloads(downloader, monkeypatch):
    module, tool = downloader
    target = tool.raw_data_path / "chronic_absenteeism" / "KYRC26_OVW_Chronic_Absenteeism.csv"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"original")
    tool._write_source_meta(
        target,
        {"last_modified": "Thu, 15 Oct 2026 10:00:10 GMT", "etag": '"v1"'},
    )
    tool.config["raw_directories"]["chronic_absenteeism"] = [
        {"url": "kyrc26", "file": "KYRC26_OVW_Chronic_Absenteeism.csv"}
    ]
    new_stamp = "Tue, 27 Oct 2026 12:00:00 GMT"
    monkeypatch.setattr(module.requests, "head", lambda *a, **k: _response(b"", new_stamp, '"v2"'))
    monkeypatch.setattr(module.requests, "get", lambda *a, **k: _response(b"replaced", new_stamp, '"v2"'))

    results = tool.download_directory("chronic_absenteeism")

    assert results["KYRC26_OVW_Chronic_Absenteeism.csv"] is True
    assert target.read_bytes() == b"replaced"
    assert tool._read_source_meta(target)["last_modified"] == new_stamp


def test_existing_file_without_a_stamp_is_redownloaded(downloader, monkeypatch):
    module, tool = downloader
    target = tool.raw_data_path / "chronic_absenteeism" / "KYRC26_OVW_Chronic_Absenteeism.csv"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"unstamped")
    tool.config["raw_directories"]["chronic_absenteeism"] = [
        {"url": "kyrc26", "file": "KYRC26_OVW_Chronic_Absenteeism.csv"}
    ]
    stamp = "Thu, 15 Oct 2026 10:00:10 GMT"
    monkeypatch.setattr(module.requests, "get", lambda *a, **k: _response(b"stamped", stamp))

    results = tool.download_directory("chronic_absenteeism")

    assert results["KYRC26_OVW_Chronic_Absenteeism.csv"] is True
    assert target.read_bytes() == b"stamped"
    assert tool._read_source_meta(target)["last_modified"] == stamp


def _uat_tool(downloader):
    _module, tool = downloader
    tool.config["base_url_kyrc_uat"] = (
        "https://kdeschoolreportcard.blob.core.windows.net/datasetsuat6051276/"
    )
    tool.config["raw_directories"]["chronic_absenteeism"] = [
        {"url": "kyrc26", "file": "KYRC26_OVW_Chronic_Absenteeism.csv"}
    ]
    return tool


def test_primary_404_downloads_the_uat_container(downloader, monkeypatch):
    module, tool = downloader
    tool = _uat_tool(downloader)
    primary = (
        "https://kdeschoolreportcard.blob.core.windows.net/datasets/"
        "KYRC26_OVW_Chronic_Absenteeism.csv"
    )
    uat = (
        "https://kdeschoolreportcard.blob.core.windows.net/datasetsuat6051276/"
        "KYRC26_OVW_Chronic_Absenteeism.csv"
    )
    stamp = "Wed, 14 Oct 2026 08:00:00 GMT"
    downloaded = []

    def head(url, **_kwargs):
        if url == primary:
            return _response(b"", "", status_code=404)
        return _response(b"", stamp, '"uat"', content_md5="abc=")

    def get(url, **_kwargs):
        downloaded.append(url)
        return _response(b"from-uat", stamp, '"uat"', content_md5="abc=")

    monkeypatch.setattr(module.requests, "head", head)
    monkeypatch.setattr(module.requests, "get", get)

    results = tool.download_directory("chronic_absenteeism")

    stored = tool.raw_data_path / "chronic_absenteeism" / "KYRC26_OVW_Chronic_Absenteeism.csv"
    assert results["KYRC26_OVW_Chronic_Absenteeism.csv"] is True
    assert downloaded == [uat]
    assert stored.read_bytes() == b"from-uat"
    meta = tool._read_source_meta(stored)
    assert meta["url"] == uat
    assert meta["content_md5"] == "abc="


def test_differing_md5_uses_the_newer_last_modified(downloader, monkeypatch):
    module, tool = downloader
    tool = _uat_tool(downloader)
    primary = (
        "https://kdeschoolreportcard.blob.core.windows.net/datasets/"
        "KYRC26_OVW_Chronic_Absenteeism.csv"
    )
    uat = (
        "https://kdeschoolreportcard.blob.core.windows.net/datasetsuat6051276/"
        "KYRC26_OVW_Chronic_Absenteeism.csv"
    )
    older = "Thu, 15 Oct 2026 10:00:10 GMT"
    newer = "Fri, 16 Oct 2026 10:00:10 GMT"
    downloaded = []

    def head(url, **_kwargs):
        if url == primary:
            return _response(b"", older, '"datasets"', content_md5="aaa=")
        return _response(b"", newer, '"uat"', content_md5="bbb=")

    def get(url, **_kwargs):
        downloaded.append(url)
        return _response(b"newer", newer, '"uat"', content_md5="bbb=")

    monkeypatch.setattr(module.requests, "head", head)
    monkeypatch.setattr(module.requests, "get", get)

    results = tool.download_directory("chronic_absenteeism")

    assert results["KYRC26_OVW_Chronic_Absenteeism.csv"] is True
    assert downloaded == [uat]


def test_matching_md5_keeps_the_datasets_container(downloader, monkeypatch):
    module, tool = downloader
    tool = _uat_tool(downloader)
    primary = (
        "https://kdeschoolreportcard.blob.core.windows.net/datasets/"
        "KYRC26_OVW_Chronic_Absenteeism.csv"
    )
    older = "Wed, 06 Oct 2026 10:00:10 GMT"
    newer = "Thu, 15 Oct 2026 10:00:10 GMT"
    downloaded = []

    def head(url, **_kwargs):
        if url == primary:
            return _response(b"", newer, '"datasets"', content_md5="same=")
        return _response(b"", older, '"uat"', content_md5="same=")

    def get(url, **_kwargs):
        downloaded.append(url)
        return _response(b"primary", newer, '"datasets"', content_md5="same=")

    monkeypatch.setattr(module.requests, "head", head)
    monkeypatch.setattr(module.requests, "get", get)

    results = tool.download_directory("chronic_absenteeism")

    assert results["KYRC26_OVW_Chronic_Absenteeism.csv"] is True
    assert downloaded == [primary]


def test_content_md5_mismatch_redownloads(downloader, monkeypatch):
    module, tool = downloader
    target = tool.raw_data_path / "chronic_absenteeism" / "KYRC26_OVW_Chronic_Absenteeism.csv"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"original")
    stamp = "Thu, 15 Oct 2026 10:00:10 GMT"
    tool._write_source_meta(
        target,
        {"last_modified": stamp, "etag": '"v1"', "content_md5": "old="},
    )
    tool.config["raw_directories"]["chronic_absenteeism"] = [
        {"url": "kyrc26", "file": "KYRC26_OVW_Chronic_Absenteeism.csv"}
    ]
    monkeypatch.setattr(
        module.requests,
        "head",
        lambda *a, **k: _response(b"", stamp, '"v1"', content_md5="new="),
    )
    monkeypatch.setattr(
        module.requests,
        "get",
        lambda *a, **k: _response(b"replaced", stamp, '"v1"', content_md5="new="),
    )

    results = tool.download_directory("chronic_absenteeism")

    assert results["KYRC26_OVW_Chronic_Absenteeism.csv"] is True
    assert target.read_bytes() == b"replaced"
    assert tool._read_source_meta(target)["content_md5"] == "new="
