import pandas as pd
import numpy as np
from datetime import datetime
from .schema import ENTITY_SPECS

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans any CSV dataframe to make it safe for DuckDB and dashboard use.
    - Standardizes headers
    - Deduplicates columns
    - Detects numeric and date columns
    - Handles missing values
    - Drops fully empty rows
    """
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("[^0-9a-zA-Z_]", "", regex=True)
    )

    seen = {}
    new_cols = []
    for c in df.columns:
        if c in seen:
            seen[c] += 1
            new_cols.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            new_cols.append(c)
    df.columns = new_cols

    for col in df.columns:
        if "date" in col or "time" in col:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            except Exception:
                pass

    for col in df.columns:
        if df[col].dtype == "object":
            try:
                df[col] = pd.to_numeric(df[col], errors="ignore")
            except Exception:
                pass

    df = df.fillna("")
    df = df[df.replace("", np.nan).notna().any(axis=1)]
    return df

def transform_with_mapping(df: pd.DataFrame, entity: str, mapping: dict) -> pd.DataFrame:
    """
    Apply an entity field mapping to an input DataFrame and coerce key types.
    - mapping: dict of target_field -> source_column
    - Ensures required minimum fields exist (filled with blanks if missing)
    - Coerces common fields (dates, numbers) and derives amount if needed
    """
    spec = ENTITY_SPECS[entity]

    # Normalize source headers
    df = clean_dataframe(df.copy())

    # Build the output frame with mapped columns
    out = pd.DataFrame()
    for field in spec.fields:
        src = mapping.get(field.name)
        if src and src in df.columns:
            out[field.name] = df[src]
        else:
            out[field.name] = ""

    # Coerce dates
    date_like = [f.name for f in spec.fields if "date" in f.name or "time" in f.name]
    for c in date_like:
        if c in out.columns:
            try:
                out[c] = pd.to_datetime(out[c], errors="coerce")
            except Exception:
                pass

    # Coerce numeric fields
    for name in ("quantity", "unit_price", "amount"):
        if name in out.columns:
            try:
                out[name] = pd.to_numeric(out[name], errors="coerce")
            except Exception:
                pass

    # Derive amount if missing
    if "amount" in out.columns:
        if out["amount"].isna().all() or (out["amount"] == "").all():
            if "quantity" in out.columns and "unit_price" in out.columns:
                q = pd.to_numeric(out.get("quantity", 0), errors="coerce").fillna(0)
                p = pd.to_numeric(out.get("unit_price", 0), errors="coerce").fillna(0)
                out["amount"] = (q * p).astype(float)

    # Ensure required_min present
    for req in spec.required_min:
        if req not in out.columns:
            out[req] = ""

    # Final NA handling
    out = out.fillna("")
    return out
