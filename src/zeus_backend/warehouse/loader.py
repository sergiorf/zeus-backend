from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from zeus_backend.utils.io import echo

MODULE_DIR = Path(__file__).resolve().parent
SCHEMA_SQL = (MODULE_DIR / "schema.sql").read_text()
INDICES_SQL = (MODULE_DIR / "indices.sql").read_text() if (MODULE_DIR / "indices.sql").exists() else ""


def load_sqlite(db_path: str = "data/warehouse.sqlite") -> None:
    """Load silver-layer parquet datasets into the SQLite warehouse."""
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_file) as conn:
        _ensure_schema(conn)
        _load_cnpj(conn)
        _load_cvm(conn)

    echo(f"[LOAD] Silver datasets loaded into {db_file}")


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    if INDICES_SQL:
        conn.executescript(INDICES_SQL)


def _load_cnpj(conn: sqlite3.Connection) -> None:
    parquet_path = Path("data/silver/cnpj/cnpj_firmographics.parquet")
    if not parquet_path.exists():
        echo(f"[LOAD][CNPJ] Skipping — missing {parquet_path}")
        return

    df = pd.read_parquet(parquet_path)
    df = df.where(pd.notnull(df), None)

    conn.execute("DELETE FROM cnpj_firmographics")
    if not df.empty:
        df.to_sql("cnpj_firmographics", conn, if_exists="append", index=False)
        echo(f"[LOAD][CNPJ] Loaded {len(df)} rows")
    else:
        echo("[LOAD][CNPJ] No rows to load (empty dataframe)")


def _load_cvm(conn: sqlite3.Connection) -> None:
    parquet_path = Path("data/silver/cvm/cvm_facts.parquet")
    if not parquet_path.exists():
        echo(f"[LOAD][CVM] Skipping — missing {parquet_path}")
        return

    df = pd.read_parquet(parquet_path)
    df = df.where(pd.notnull(df), None)

    conn.execute("DELETE FROM cvm_facts")
    if not df.empty:
        df.to_sql("cvm_facts", conn, if_exists="append", index=False)
        echo(f"[LOAD][CVM] Loaded {len(df)} rows")
    else:
        echo("[LOAD][CVM] No rows to load (empty dataframe)")
