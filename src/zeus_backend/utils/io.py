from pathlib import Path
import pandas as pd
import zipfile

def echo(msg: str):
    print(msg, flush=True)

def unzip(src: Path, dest: Path):
    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(src, 'r') as z:
        z.extractall(dest)

def write_parquet(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
