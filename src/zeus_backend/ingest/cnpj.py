from pathlib import Path
from zeus_backend.incremental.tracker import discover_files
from zeus_backend.utils.io import echo, unzip

RAW = Path("data/raw/cnpj")
BRONZE = Path("data/bronze/cnpj")

def run(stage: str = "bronze"):
    BRONZE.mkdir(parents=True, exist_ok=True)
    discover_files("cnpj", [RAW])
    # naive: unzip any .zip into bronze (non-recursive flattening = keep folder structure simple)
    for z in RAW.glob("*.zip"):
        unzip(z, BRONZE / z.stem)
    echo(f"[CNPJ] Discovered and extracted raw files → {BRONZE}")
