# Zeus Backend — Offline ETL Prototype (Local-Only)

This is a **zero‑infrastructure** prototype to ingest **CNPJ firmographics** and **CVM financials**
**locally**, normalize them, and load them into a single-file **SQLite** warehouse. It follows a simple,
robust data‑engineering pattern: `raw → bronze → silver → gold`.

### Why this approach?
- No cloud costs: all processing is on your machine.
- Idempotent & incremental: new files are detected by **SHA‑256**; only changed data is processed.
- Clear separation of concerns: ingest (format), normalize (clean), load (DB), and optional gold views.

## Data tiers
- **data/raw/** — original downloads (zip/csv/xbrl). *You copy files here.*
- **data/bronze/** — decompressed / 1:1 structured (CSV/Parquet) without semantic changes.
- **data/silver/** — cleaned/normalized (types, standardized column names, CNPJ14 keyed entities).
- **data/gold/** — analytics-friendly denormalized outputs (Parquet), optional.
- **data/warehouse.sqlite** — app-facing SQLite database.

## Quick start
```bash
# 1) Create virtualenv & install
make setup

# 2) Put source files in:
#    data/raw/cnpj/   (Receita CNPJ files)
#    data/raw/cvm/    (CVM ITR/DFP zips/XBRL)
# 3) Run the pipeline stages
make bronze    # discover & unpack to bronze
make silver    # clean & normalize to silver
make load      # load into SQLite

# Check the DB
sqlite3 data/warehouse.sqlite ".schema"
```

## CLI
This repo exposes a Typer CLI:
```bash
zeus ingest cnpj --to bronze
zeus ingest cvm --to bronze
zeus normalize cnpj
zeus normalize cvm
zeus load sqlite
```

## What works in this prototype?
- Project structure and commands.
- Ingestion discovery (file registry + sha256) and unzip utility.
- CNPJ normalization stubs (column mapping + CNPJ standardization).
- CVM XBRL normalization stub and fact table schema.
- SQLite schema for firmographics + cvm facts (with indexes & a sample view).

## Roadmap
1. Flesh out **CNPJ bronze→silver** mapping to your actual Receita dump format.
2. Implement **CVM XBRL parsing** for core IFRS facts:
   - Revenue, NetIncome, TotalAssets, Equity, OperatingCashFlow.
3. Materialize **gold** views (DuckDB) and add a tiny **FastAPI** for queries.
4. Add procurement data (Compras.gov.br) to enrich private firms with public payments.
5. Add alerting (cron over local files) and export features.

## Notes
- This is an MVP skeleton with safe defaults. You can grow it into production later.
- All code runs locally; no internet or external services are required.
