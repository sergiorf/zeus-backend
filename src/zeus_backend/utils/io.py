from datetime import datetime
from pathlib import Path
import zipfile

import pandas as pd

def echo(msg: str) -> None:
    timestamp = datetime.now().isoformat(timespec="seconds")
    print(f"[{timestamp}] {msg}", flush=True)

def unzip(src: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    echo(f"[IO] Extracting {src} to {dest}")
    with zipfile.ZipFile(src, "r") as z:
        members = z.namelist()
        z.extractall(dest)
    echo(f"[IO] Extracted {len(members)} entries from {src.name}")

def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    echo(
        f"[IO] Writing Parquet to {path} shape={df.shape[0]}x{df.shape[1]}"
    )
    df.to_parquet(path, index=False)
    echo(f"[IO] Wrote Parquet file {path.name}")
