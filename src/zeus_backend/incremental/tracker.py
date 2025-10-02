from __future__ import annotations
import sqlite3
from pathlib import Path
from zeus_backend.utils.hashing import sha256sum

DB = "data/warehouse.sqlite"

def ensure_tables():
    schema = Path("src/zeus_backend/warehouse/schema.sql").read_text()
    with sqlite3.connect(DB) as con:
        con.executescript(schema)

def discover_files(source: str, roots: list[Path]):
    ensure_tables()
    with sqlite3.connect(DB) as con:
        for root in roots:
            for p in root.rglob('*'):
                if p.is_file():
                    sig = sha256sum(p)
                    con.execute(
                        """
                        INSERT OR IGNORE INTO file_ingest_log(source, path, sha256, bytes, stage)
                        VALUES (?,?,?,?,?)
                        """,
                        (source, str(p), sig, p.stat().st_size, 'raw')
                    )
        con.commit()
