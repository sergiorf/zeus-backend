from pathlib import Path
from zeus_backend.incremental.tracker import discover_files
from zeus_backend.utils.io import echo, unzip

RAW = Path("data/raw/cvm")
BRONZE = Path("data/bronze/cvm")

def run(stage: str = "bronze"):
    BRONZE.mkdir(parents=True, exist_ok=True)
    discover_files("cvm", [RAW])
    for z in RAW.glob("*.zip"):
        unzip(z, BRONZE / z.stem)
    echo(f"[CVM] Discovered and extracted raw files → {BRONZE}")
