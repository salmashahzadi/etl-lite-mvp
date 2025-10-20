import pandas as pd
from typing import Dict, List, Tuple
from .schema import ENTITY_SPECS, EntitySpec


def validate_dataframe(entity: str, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, str]]]:
    """
    Validate a transformed DataFrame against the entity spec.
    Returns: (possibly coerced df, list of row-level errors)
    """
    spec: EntitySpec = ENTITY_SPECS[entity]
    errors: List[Dict[str, str]] = []

    # Required minimum fields exist?
    for req in spec.required_min:
        if req not in df.columns:
            errors.append({"row": "*", "field": req, "error": "missing_required_column"})

    # Row-level checks: required fields non-empty
    for req in spec.required_min:
        if req in df.columns:
            empties = df[req].isna() | (df[req] == "")
            for i in df.index[empties]:
                errors.append({"row": str(i), "field": req, "error": "missing_required_value"})

    # Numeric sanity checks
    for col in ["amount", "quantity", "unit_price"]:
        if col in df.columns:
            try:
                num = pd.to_numeric(df[col], errors="coerce")
                neg = num < 0
                for i in df.index[neg.fillna(False)]:
                    errors.append({"row": str(i), "field": col, "error": "negative_value"})
                df[col] = num
            except Exception:
                errors.append({"row": "*", "field": col, "error": "not_numeric"})

    return df, errors


def summarize_errors(errors: List[Dict[str, str]]) -> Dict[str, int]:
    summary: Dict[str, int] = {}
    for e in errors:
        k = e.get("error", "unknown")
        summary[k] = summary.get(k, 0) + 1
    return summary


