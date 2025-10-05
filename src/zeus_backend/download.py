from __future__ import annotations

import fnmatch
import re
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from zeus_backend.utils.io import echo

# Remote endpoints
CNPJ_BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
CVM_BASE_URL_TEMPLATE = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{doc}/DADOS/"

RAW_CNPJ_DIR = Path("data/raw/cnpj")
RAW_CVM_DIR = Path("data/raw/cvm")

DEFAULT_TIMEOUT = 60
CHUNK_SIZE = 1 << 20  # 1 MiB


def download_cnpj(
    month: str | None = None,
    patterns: Sequence[str] | None = None,
    limit: int | None = None,
    overwrite: bool = False,
) -> None:
    """Download Receita Federal CNPJ ZIP archives into data/raw/cnpj.

    When ``month`` is omitted, the most recent available directory (YYYY-MM) is used.
    """
    RAW_CNPJ_DIR.mkdir(parents=True, exist_ok=True)
    months = _list_remote_dirs(CNPJ_BASE_URL)
    if not months:
        echo("[FETCH][CNPJ] No monthly directories found at Receita endpoint")
        return

    try:
        month_key = _resolve_month(month, months)
    except ValueError as exc:
        echo(f"[FETCH][CNPJ] {exc}")
        return
    month_url = urljoin(CNPJ_BASE_URL, f"{month_key}/")
    files = _list_remote_files(month_url, suffix=".zip")
    to_fetch = _filter_by_patterns(files, patterns, limit)
    if not to_fetch:
        echo(f"[FETCH][CNPJ] No files matched the requested patterns for month {month_key}")
        return

    echo(f"[FETCH][CNPJ] Downloading {len(to_fetch)} file(s) from {month_key} to {RAW_CNPJ_DIR}")
    _download_many(month_url, to_fetch, RAW_CNPJ_DIR, overwrite)


def download_cvm(doc: str = "itr", patterns: Sequence[str] | None = None, limit: int | None = None, overwrite: bool = False) -> None:
    """Download CVM open data ZIP archives (ITR/DFP) into data/raw/cvm."""
    doc_upper = doc.strip().upper()
    base_url = CVM_BASE_URL_TEMPLATE.format(doc=doc_upper)
    RAW_CVM_DIR.mkdir(parents=True, exist_ok=True)
    files = _list_remote_files(base_url, suffix=".zip")
    to_fetch = _filter_by_patterns(files, patterns, limit)
    if not to_fetch:
        echo(f"[FETCH][CVM] No files matched the requested patterns for DOC={doc_upper}")
        return

    echo(f"[FETCH][CVM] Downloading {len(to_fetch)} file(s) from DOC={doc_upper} to {RAW_CVM_DIR}")
    _download_many(base_url, to_fetch, RAW_CVM_DIR, overwrite)


def _resolve_month(requested: str | None, available: Sequence[str]) -> str:
    cleaned = sorted({month.strip().rstrip("/") for month in available})
    if not cleaned:
        raise ValueError("No available months to resolve")

    if requested:
        key = requested.strip().rstrip("/")
        if key not in cleaned:
            raise ValueError(
                f"Month '{requested}' not found. Available options: {', '.join(cleaned)}"
            )
        return key

    return cleaned[-1]


def _list_remote_files(base_url: str, suffix: str) -> list[str]:
    resp = requests.get(base_url, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    files: set[str] = set()
    for anchor in soup.find_all("a"):
        href = anchor.get("href")
        if not href:
            continue
        if href.lower().endswith(suffix.lower()):
            files.add(href.split("?")[0])
    return sorted(files)


def _list_remote_dirs(base_url: str) -> list[str]:
    resp = requests.get(base_url, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    pattern = re.compile(r"\d{4}-\d{2}")
    dirs: set[str] = set()
    for anchor in soup.find_all("a"):
        href = anchor.get("href")
        if not href:
            continue
        if not href.endswith("/"):
            continue
        candidate = href.split("?")[0].rstrip("/")
        if pattern.fullmatch(candidate):
            dirs.add(candidate)
    return sorted(dirs)


def _filter_by_patterns(files: Sequence[str], patterns: Sequence[str] | None, limit: int | None) -> list[str]:
    if not files:
        return []
    if patterns:
        matched = []
        for pattern in patterns:
            matched.extend(f for f in files if fnmatch.fnmatch(f, pattern))
    else:
        matched = list(files)
    # Preserve original discovered order while dropping duplicates
    seen: set[str] = set()
    ordered = []
    for f in matched:
        if f not in seen:
            ordered.append(f)
            seen.add(f)
    if limit is not None:
        return ordered[:limit]
    return ordered


def _download_many(base_url: str, filenames: Iterable[str], dest_dir: Path, overwrite: bool) -> None:
    for name in filenames:
        dest = dest_dir / name
        url = urljoin(base_url, name)
        if dest.exists() and not overwrite:
            remote_size = _remote_file_size(url)
            local_size = dest.stat().st_size
            if remote_size is not None and local_size == remote_size:
                echo(f"[FETCH] Skipping existing file {dest}")
                continue
            remote_size_text = (
                f"remote size {remote_size}"
                if remote_size is not None
                else "remote size unknown"
            )
            echo(
                f"[FETCH] Re-downloading {dest} because local size {local_size} does not match {remote_size_text}"
            )
        echo(f"[FETCH] GET {url}")
        _download_file(url, dest)


def _download_file(url: str, dest: Path) -> None:
    with requests.get(url, stream=True, timeout=DEFAULT_TIMEOUT) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        with open(dest, "wb") as fh, tqdm(
            total=total if total > 0 else None,
            unit="B",
            unit_scale=True,
            desc=dest.name,
        ) as bar:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue
                fh.write(chunk)
                bar.update(len(chunk))


def _remote_file_size(url: str) -> int | None:
    """Return the remote file size from Content-Length header when available."""
    try:
        resp = requests.head(url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
    except requests.RequestException:
        return None

    length = resp.headers.get("content-length")
    try:
        size = int(length) if length is not None else None
    except (TypeError, ValueError):
        return None
    return size if size is not None and size >= 0 else None
