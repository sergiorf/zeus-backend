from pathlib import Path
import pandas as pd
from lxml import etree
from zeus_backend.utils.io import write_parquet
from zeus_backend.utils.mapping import normalize_cnpj14

BRONZE = Path("data/bronze/cvm")
SILVER = Path("data/silver/cvm")

# Map of our simplified concepts to likely IFRS tags (improve as needed)
IFRS_TAGS = {
    'Revenue': ['RevenueFromContractsWithCustomers', 'Revenue'],
    'NetIncome': ['ProfitLoss', 'NetIncomeLoss'],
    'TotalAssets': ['Assets'],
    'Equity': ['Equity'],
    'OperatingCashFlow': ['NetCashFlowsFromUsedInOperatingActivities']
}

def extract_facts(xbrl_path: Path) -> pd.DataFrame:
    # Minimal stub: parse XBRL file if structure is standard; otherwise return empty
    try:
        tree = etree.parse(str(xbrl_path))
        root = tree.getroot()
        ns = {k:v for k,v in root.nsmap.items() if k}
    except Exception:
        return pd.DataFrame(columns=[
            'cnpj14','company_name','period_start','period_end','fy','fq','concept','value','unit','consolidated'
        ])

    # TODO: Real context/entity parsing for MVP — placeholder empty frame
    return pd.DataFrame(columns=[
        'cnpj14','company_name','period_start','period_end','fy','fq','concept','value','unit','consolidated'
    ])

def run():
    SILVER.mkdir(parents=True, exist_ok=True)
    frames = []
    for x in BRONZE.rglob('*.xbrl'):
        df = extract_facts(x)
        if not df.empty and 'cnpj14' in df.columns:
            df['cnpj14'] = df['cnpj14'].map(normalize_cnpj14)
            frames.append(df)
    if frames:
        out = pd.concat(frames, ignore_index=True)
        write_parquet(out, SILVER / 'cvm_facts.parquet')
