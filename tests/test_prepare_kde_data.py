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


def test_differing_md5_keeps_datasets_when_datasets_is_newer(downloader, monkeypatch):
    """KYRC26 ineffective shape: both Content-MD5 values exist and datasets is newer."""
    module, tool = downloader
    tool = _uat_tool(downloader)
    primary = (
        "https://kdeschoolreportcard.blob.core.windows.net/datasets/"
        "KYRC26_OVW_Chronic_Absenteeism.csv"
    )
    older = "Mon, 12 Oct 2026 19:53:57 GMT"
    newer = "Wed, 04 Nov 2026 13:21:04 GMT"
    downloaded = []

    def head(url, **_kwargs):
        if url == primary:
            return _response(b"", newer, '"datasets"', content_md5="ZDwOgo98qCCpngaxpATHmA==")
        return _response(b"", older, '"uat"', content_md5="RLSb4R1BjoPE/x5wAJZb1w==")

    def get(url, **_kwargs):
        downloaded.append(url)
        return _response(b"refresh", newer, '"datasets"', content_md5="ZDwOgo98qCCpngaxpATHmA==")

    monkeypatch.setattr(module.requests, "head", head)
    monkeypatch.setattr(module.requests, "get", get)

    results = tool.download_directory("chronic_absenteeism")

    assert results["KYRC26_OVW_Chronic_Absenteeism.csv"] is True
    assert downloaded == [primary]


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


def test_save_as_downloads_the_remote_name_to_a_local_name(downloader, monkeypatch):
    """Two hosts can publish one filename. save_as keeps both local copies."""
    module, tool = downloader
    tool.config["base_url"] = "https://historical.example/"
    remote = "KYRC24_OVW_Teacher_Working_Conditions.csv"
    local = "KYRC24_OVW_Teacher_Working_Conditions_historical.csv"
    tool.config["raw_directories"]["chronic_absenteeism"] = [
        {"url": "historical", "file": remote, "save_as": local}
    ]
    stamp = "Thu, 29 May 2025 22:07:04 GMT"
    requested = []

    def get(url, **_kwargs):
        requested.append(url)
        return _response(b"historical-copy", stamp)

    monkeypatch.setattr(module.requests, "get", get)
    results = tool.download_directory("chronic_absenteeism")

    assert results[local] is True
    assert remote not in results
    stored = tool.raw_data_path / "chronic_absenteeism" / local
    assert stored.read_bytes() == b"historical-copy"
    assert not (tool.raw_data_path / "chronic_absenteeism" / remote).exists()
    assert requested == [f"https://historical.example/{remote}"]


def test_kyrc_uat_resolves_without_probing_datasets(downloader, monkeypatch):
    """A pinned kyrc_uat entry does not HEAD or GET the datasets container."""
    module, tool = downloader
    tool.config["base_url_kyrc_uat"] = (
        "https://kdeschoolreportcard.blob.core.windows.net/datasetsuat6051276/"
    )
    tool.config["base_url_kyrc_datasets"] = (
        "https://kdeschoolreportcard.blob.core.windows.net/datasets/"
    )
    name = "KYRC25_OVW_Students_Taught_by_Ineffective_Teachers.csv"
    tool.config["raw_directories"]["chronic_absenteeism"] = [
        {"url": "kyrc_uat", "file": name}
    ]
    uat = (
        "https://kdeschoolreportcard.blob.core.windows.net/datasetsuat6051276/" + name
    )
    seen = []

    def head(url, **_kwargs):
        seen.append(("head", url))
        raise AssertionError("pinned kyrc_uat must not probe")

    def get(url, **_kwargs):
        seen.append(("get", url))
        return _response(b"uat-bytes", "Mon, 12 Oct 2026 19:55:55 GMT", '"uat"')

    monkeypatch.setattr(module.requests, "head", head)
    monkeypatch.setattr(module.requests, "get", get)
    results = tool.download_directory("chronic_absenteeism")
    stored = tool.raw_data_path / "chronic_absenteeism" / name
    assert results[name] is True
    assert seen == [("get", uat)]
    assert stored.read_bytes() == b"uat-bytes"
    assert tool._read_source_meta(stored)["url"] == uat


def test_kyrc_datasets_does_not_probe_uat_for_a_kyrc25_name(downloader, monkeypatch):
    """kyrc_datasets stays on datasets even when the name would otherwise alternate."""
    module, tool = downloader
    tool.config["base_url_kyrc_uat"] = (
        "https://kdeschoolreportcard.blob.core.windows.net/datasetsuat6051276/"
    )
    tool.config["base_url_kyrc_datasets"] = (
        "https://kdeschoolreportcard.blob.core.windows.net/datasets/"
    )
    name = "KYRC25_OVW_Students_Taught_by_Ineffective_Teachers.csv"
    tool.config["raw_directories"]["chronic_absenteeism"] = [
        {"url": "kyrc_datasets", "file": name}
    ]
    datasets = "https://kdeschoolreportcard.blob.core.windows.net/datasets/" + name
    seen = []

    def head(url, **_kwargs):
        seen.append(url)
        raise AssertionError("pinned kyrc_datasets must not probe UAT")

    def get(url, **_kwargs):
        seen.append(url)
        return _response(b"datasets-bytes", "Sat, 18 Jul 2026 21:29:47 GMT")

    monkeypatch.setattr(module.requests, "head", head)
    monkeypatch.setattr(module.requests, "get", get)
    results = tool.download_directory("chronic_absenteeism")
    assert results[name] is True
    assert seen == [datasets]
    stored = tool.raw_data_path / "chronic_absenteeism" / name
    assert stored.read_bytes() == b"datasets-bytes"


def test_missing_content_md5_reports_a_body_mismatch_and_keeps_primary(downloader, monkeypatch, caplog):
    """Issue #64 item 2: missing Content-MD5 compares bodies and does not stay silent."""
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
    stamp = "Thu, 15 Oct 2026 10:00:10 GMT"

    def head(url, **_kwargs):
        return _response(b"", stamp, '"no-md5"')

    def get(url, **_kwargs):
        body = b"datasets-body" if url == primary else b"uat-body"
        return _response(body, stamp, '"no-md5"')

    monkeypatch.setattr(module.requests, "head", head)
    monkeypatch.setattr(module.requests, "get", get)
    results = tool.download_directory("chronic_absenteeism")
    stored = tool.raw_data_path / "chronic_absenteeism" / "KYRC26_OVW_Chronic_Absenteeism.csv"
    assert results["KYRC26_OVW_Chronic_Absenteeism.csv"] is True
    assert stored.read_bytes() == b"datasets-body"
    assert tool.copy_mismatches
    assert tool.copy_mismatches[0]["differ"] == "yes"
    assert tool.copy_mismatches[0]["primary_url"] == primary
    assert tool.copy_mismatches[0]["alternate_url"] == uat
    assert any("Container mismatch" in message for message in caplog.messages)


def test_missing_content_md5_matching_bodies_are_not_a_mismatch(downloader, monkeypatch):
    module, tool = downloader
    tool = _uat_tool(downloader)
    stamp = "Thu, 15 Oct 2026 10:00:10 GMT"
    monkeypatch.setattr(module.requests, "head", lambda *a, **k: _response(b"", stamp))
    monkeypatch.setattr(
        module.requests, "get", lambda *a, **k: _response(b"same-body", stamp)
    )
    results = tool.download_directory("chronic_absenteeism")
    assert results["KYRC26_OVW_Chronic_Absenteeism.csv"] is True
    assert tool.copy_mismatches == []
