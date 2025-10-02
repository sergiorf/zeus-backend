from pathlib import Path
import pandas as pd
from zeus_backend.utils.mapping import normalize_cnpj14, map_cnae_desc
from zeus_backend.utils.io import write_parquet

BRONZE = Path("data/bronze/cnpj")
SILVER = Path("data/silver/cnpj")

# Adjust these to your actual Receita column names
COLUMNS = {
  "cnpj": "cnpj14",
  "razao_social": "legal_name",
  "nome_fantasia": "trade_name",
  "situacao_cadastral": "status",
  "data_abertura": "opening_date",
  "cnae_principal": "cnae",
  "natureza_juridica": "legal_nature",
  "porte": "size_bracket",
  "uf": "uf",
  "cod_municipio": "municipality_code",
  "cep": "cep",
}

def run():
    SILVER.mkdir(parents=True, exist_ok=True)
    frames = []
    for csv in BRONZE.rglob('*.csv'):
        try:
            df = pd.read_csv(csv, dtype=str)
        except Exception:
            # try semicolon delimiter typical in BR CSVs
            df = pd.read_csv(csv, dtype=str, sep=';')
        # flexible rename: only keep cols present
        cols = {k:v for k,v in COLUMNS.items() if k in df.columns}
        df = df.rename(columns=cols)
        if 'cnpj14' in df.columns:
            df['cnpj14'] = df['cnpj14'].map(normalize_cnpj14)
        if 'cnae' in df.columns:
            df['cnae_desc'] = df['cnae'].map(map_cnae_desc)
        keep = [c for c in COLUMNS.values() if c in df.columns] + (['cnae_desc'] if 'cnae' in df.columns else [])
        frames.append(df[keep])
    if frames:
        out = pd.concat(frames, ignore_index=True)
        if 'cnpj14' in out.columns:
            out = out.dropna(subset=['cnpj14']).drop_duplicates('cnpj14')
        write_parquet(out, SILVER / 'cnpj_firmographics.parquet')
