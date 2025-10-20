import re, pandas as pd
from typing import Any, Dict
from .schema import ENTITY_SPECS, EntitySpec
import requests

COMMON_SEPARATORS = [",", ";", "\t", "|"]
_CLEAN_RE = re.compile(r"[^a-z0-9_]+")

def _clean_header(h: str) -> str:
    return _CLEAN_RE.sub("_", str(h).strip().lower()).strip("_")

def read_file_to_df(path_or_buffer: Any) -> pd.DataFrame:
    name = getattr(path_or_buffer, "name", "uploaded")
    if str(name).lower().endswith((".xlsx", ".xls")):
        df = pd.read_excel(path_or_buffer)
    else:
        try:
            df = pd.read_csv(path_or_buffer)
        except Exception:
            if hasattr(path_or_buffer, "seek"):
                path_or_buffer.seek(0)
            last_err = None
            for sep in COMMON_SEPARATORS:
                try:
                    df = pd.read_csv(path_or_buffer, sep=sep, engine="python")
                    break
                except Exception as e:
                    last_err = e
                    if hasattr(path_or_buffer, "seek"):
                        path_or_buffer.seek(0)
            else:
                raise last_err
    df.columns = [_clean_header(c) for c in df.columns]
    return df.dropna(axis=1, how="all").dropna(axis=0, how="all")

# --- Google Sheets helpers ---

def _gsheet_to_csv_url(url: str) -> str:
    """Convert various Google Sheets URLs to a direct CSV export URL where possible."""
    # Typical forms:
    # https://docs.google.com/spreadsheets/d/<ID>/edit#gid=0 -> export?format=csv
    # https://docs.google.com/spreadsheets/d/<ID>/edit?usp=sharing -> export?format=csv
    # If it's already an export link, return as is.
    if "export?format=csv" in url:
        return url
    if "/spreadsheets/d/" in url:
        base = url.split("/edit")[0]
        return base + "/export?format=csv"
    return url  # fallback; pandas may still handle if it's a direct CSV link


def read_google_sheet(url: str) -> pd.DataFrame:
    csv_url = _gsheet_to_csv_url(url)
    try:
        df = pd.read_csv(csv_url)
    except Exception:
        # Some 'share links' require a requests.get; try manual fetch
        r = requests.get(csv_url, timeout=15)
        r.raise_for_status()
        import io
        df = pd.read_csv(io.StringIO(r.text))
    df.columns = [_clean_header(c) for c in df.columns]
    return df.dropna(axis=1, how="all").dropna(axis=0, how="all")

# --- Mapping ---

def suggest_column_mapping(df: pd.DataFrame, entity: str) -> Dict[str, str]:
    spec: EntitySpec = ENTITY_SPECS[entity]
    cand = {c.lower(): c for c in df.columns}
    mapping = {}
    # exact & synonym matches
    for field in spec.fields:
        if field.name.lower() in cand:
            mapping[field.name] = cand.pop(field.name.lower()); continue
        for syn in field.synonyms:
            if syn.lower() in cand:
                mapping[field.name] = cand.pop(syn.lower()); break
    # fuzzy contains fallback
    for field in spec.fields:
        if field.name in mapping: continue
        for key, raw in list(cand.items()):
            if field.name in key or any(s in key for s in [s.lower() for s in field.synonyms]):
                mapping[field.name] = cand.pop(key); break
    return mapping
