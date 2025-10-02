import re

_cnae_desc = {
  # TODO: complete from official CNAE table
  '6201-5/01': 'Desenvolvimento de programas de computador sob encomenda',
}

def normalize_cnpj14(x: str | None) -> str | None:
    if not x:
        return None
    digits = re.sub(r"\D", "", x)
    return digits.zfill(14) if digits else None

def map_cnae_desc(code: str | None) -> str | None:
    return _cnae_desc.get(code)
