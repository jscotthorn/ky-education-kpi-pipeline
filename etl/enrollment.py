"""
Enrollment ETL Module
"""
from pathlib import Path
import pandas as pd
from pydantic import BaseModel


class Config(BaseModel):
    rename: dict[str, str] = {}
    dtype: dict[str, str] = {}
    derive: dict[str, str | int | float] = {}


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read newest enrollment file, clean, write CSV."""
    # --- locate newest raw file ---
    source_name = Path(__file__).stem
    latest_dir = max((raw_dir / source_name).glob("*"), default=None)
    if latest_dir is None:
        print(f"No raw data for {source_name}; skipping.")
        return
    csv_path = max(latest_dir.glob("*.csv"))
    df = pd.read_csv(csv_path)

    # --- basic transforms using cfg ---
    conf = Config(**cfg)
    if conf.rename:
        df = df.rename(columns=conf.rename)
    if conf.dtype:
        df = df.astype(conf.dtype)
    for k, v in conf.derive.items():
        df[k] = v

    # --- write processed ---
    out_path = proc_dir / f"{source_name}.csv"
    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path}")