"""Keep committed CSV fixtures small enough to review.

Full downloads stay out of git. Every CSV under tests/fixtures/ is an excerpt.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
LIMIT_BYTES = 50 * 1024


def test_every_csv_fixture_stays_under_50kb():
    """Fail if any tests/fixtures/**/*.csv exceeds 50 KB."""
    files = sorted(
        path for path in FIXTURES.rglob("*") if path.is_file() and path.suffix.lower() == ".csv"
    )
    assert files, f"No CSV fixtures under {FIXTURES}"
    oversized = [
        f"{path.relative_to(ROOT)} ({path.stat().st_size} bytes)"
        for path in files
        if path.stat().st_size > LIMIT_BYTES
    ]
    assert not oversized, "CSV fixtures must stay under 50 KB:\n" + "\n".join(oversized)
