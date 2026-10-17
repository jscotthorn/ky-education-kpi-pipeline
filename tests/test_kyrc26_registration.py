"""The KYRC26 filenames in kde_sources.yaml match the files probed on the blob."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "kyrc26_files.yaml"
SOURCES = ROOT / "config" / "kde_sources.yaml"


def _filename(entry):
    if isinstance(entry, dict):
        return str(entry["file"])
    return str(entry)


def test_registered_kyrc26_files_match_the_live_inventory():
    fixture = yaml.safe_load(FIXTURE.read_text())
    sources = yaml.safe_load(SOURCES.read_text())
    available = set(fixture["available_kyrc26"])
    unavailable = set(fixture["unavailable_kyrc26"])

    assert sources["base_url_kyrc25"] == fixture["container"]
    assert sources["base_url_kyrc26"] == fixture["container"]
    assert "datasetsdev" not in sources["base_url_kyrc25"]
    assert "datasetsdev" not in sources["base_url_kyrc26"]

    for directory, entries in sources["raw_directories"].items():
        present = {_filename(entry) for entry in entries}
        expected = set()
        for entry in entries:
            name = _filename(entry)
            if isinstance(entry, dict) and entry.get("url") == "kyrc26":
                assert name in available, f"{directory} registers a file that was not on the blob: {name}"
                assert name not in unavailable
            if name.startswith("KYRC24_") or name.startswith("KYRC25_"):
                twin = "KYRC26_" + name.split("_", 1)[1]
                if twin in available:
                    expected.add(twin)
        missing = expected - present
        assert not missing, f"{directory} is missing KYRC26 files that exist: {sorted(missing)}"

    configured = set()
    for entries in sources["raw_directories"].values():
        for entry in entries:
            name = _filename(entry)
            if name.startswith("KYRC26_"):
                configured.add(name)
    assert configured.isdisjoint(unavailable)
