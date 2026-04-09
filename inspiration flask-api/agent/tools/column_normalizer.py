"""
LLM-powered RCM column normalizer.

Maps arbitrary user column names to the standard columns expected by
the TOD and TOE engines.  Columns that don't match any required field
are kept as-is (pass-through).

Returns a confirmation dict so the caller can show the user exactly
which columns were mapped.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

import pandas as pd

# ── The canonical columns that TOD / TOE engines require ────────────
# Keys   = internal dataclass field names used by engines
# Values = the "display" column name the engines expect in the DataFrame
REQUIRED_COLUMNS: Dict[str, str] = {
    "process":            "Process",
    "subprocess":         "SubProcess",
    "risk_id":            "Risk Id",
    "risk_description":   "Risk Description",
    "risk_level":         "risk_level",
    "control_id":         "Control Id",
    "control_description":"Control Description",
    "nature_of_control":  "Nature of Control",
    "control_type":       "Control Type",
    "control_frequency":  "Control Frequency",
}

# Columns that are recognized and mapped if present, but NOT flagged as missing
OPTIONAL_COLUMNS: Dict[str, str] = {
    "control_objective":  "Control Objective",
    "control_owner":      "Control Owner",
    "application_system": "Application/System",
    "risk_title":         "Risk Title",
    "count_of_samples":   "count_of_samples",
    "risk_probability":   "risk_probability",
    "risk_impact":        "risk_impact",
}


def normalize_rcm_dataframe(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, Dict[str, str], List[str], List[str]]:
    """Normalize an RCM DataFrame so its columns match engine expectations.

    Delegates to the shared rcm_reader._normalize_columns which runs:
        1. Exact / alias matching (fast, no API call).
        2. LLM fuzzy matching for any remaining unresolved required columns.
        3. Rename matched columns, leave unmatched columns untouched.

    Returns:
        df            – DataFrame with renamed columns
        column_map    – {original_col: mapped_canonical_col} for all mapped columns
        passthrough   – list of original columns that were NOT mapped (kept as-is)
        still_missing – required canonical columns that couldn't be mapped at all
    """
    from engines.rcm_reader import _normalize_columns
    return _normalize_columns(df)
