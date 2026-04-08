"""
Shared utility functions reused from sox_agent.py.

Contains:
  - sanitize_for_json  (NaN / numpy safe JSON conversion)
  - normalize_rcm_columns  (column name standardisation)
  - parse_indices  (user selection parser: "1,3,5-8")
  - display_table  (ASCII table formatter)
"""

from __future__ import annotations

import math
from typing import Any, Dict, List

import numpy as np
import pandas as pd


# ── Column normalisation map ─────────────────────────────────────────────

COLUMN_NORMALIZE_MAP: Dict[str, str] = {
    "process": "Process",
    "sub process": "SubProcess",
    "subprocess": "SubProcess",
    "sub_process": "SubProcess",
    "control objective": "Control Objective",
    "control_objective": "Control Objective",
    "risk id": "Risk Id",
    "risk_id": "Risk Id",
    "riskid": "Risk Id",
    "risk title": "Risk Title",
    "risk_title": "Risk Title",
    "risk description": "Risk Description",
    "risk_description": "Risk Description",
    "control id": "Control Id",
    "control_id": "Control Id",
    "controlid": "Control Id",
    "control description": "Control Description",
    "control_description": "Control Description",
    "control owner": "Control Owner",
    "control_owner": "Control Owner",
    "nature of control": "Nature of Control",
    "nature_of_control": "Nature of Control",
    "control type": "Control Type",
    "control_type": "Control Type",
    "control frequency": "Control Frequency",
    "control_frequency": "Control Frequency",
    "application/system": "Application/System",
    "application_system": "Application/System",
    "application / system": "Application/System",
    "risk level": "risk_level",
    "risk_level": "risk_level",
    "risklevel": "risk_level",
    "count_of_samples": "count_of_samples",
    "count of samples": "count_of_samples",
    # Extended aliases from real-world RCM variants
    "mega process": "Process",
    "business process": "Process",
    "cycle": "Process",
    "sub-process description": "SubProcess",
    "sub process description": "SubProcess",
    "activity": "SubProcess",
    "risk no": "Risk Id",
    "risk #": "Risk Id",
    "risk number": "Risk Id",
    "risk name": "Risk Title",
    "risk summary": "Risk Title",
    "risk desc": "Risk Description",
    "risk narrative": "Risk Description",
    "ctrl id": "Control Id",
    "control #": "Control Id",
    "control number": "Control Id",
    "control no": "Control Id",
    "control narrative": "Control Description",
    "control activity": "Control Description",
    "performed by": "Control Owner",
    "responsible": "Control Owner",
    "preventive/detective": "Nature of Control",
    "p/d": "Nature of Control",
    "manual/automated": "Control Type",
    "automation": "Control Type",
    "frequency": "Control Frequency",
    "frequence": "Control Frequency",
    "periodicity": "Control Frequency",
    "how often": "Control Frequency",
    "system": "Application/System",
    "application": "Application/System",
    "it system": "Application/System",
    "erp": "Application/System",
    "risk rating": "risk_level",
    "inherent risk": "risk_level",
    "risk severity": "risk_level",
    "sample size": "count_of_samples",
    "# samples": "count_of_samples",
    "no. of samples": "count_of_samples",
    "assertion": "Control Objective",
}

RCM_REQUIRED_COLUMNS = [
    "Process", "SubProcess", "Risk Id",
    "Risk Description", "Control Id", "Control Description",
    "Nature of Control", "Control Type", "Control Frequency",
    "risk_level",
]

SUPPORTED_INDUSTRIES = [
    "Manufacturing", "Banking & Financial Services", "Healthcare",
    "Insurance", "Retail & Consumer", "Technology", "Telecommunications",
    "Energy & Utilities", "Pharmaceuticals", "Automotive",
    "Real Estate", "Mining & Metals", "Media & Entertainment",
    "Transportation & Logistics", "Government & Public Sector",
    "Education", "Hospitality & Tourism", "Agriculture",
    "Construction & Engineering", "Aerospace & Defense",
]


# ── Functions ─────────────────────────────────────────────────────────────

def normalize_rcm_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename DataFrame columns to standard names using COLUMN_NORMALIZE_MAP."""
    new_cols = []
    for col in df.columns:
        stripped = str(col).strip()
        lookup = stripped.lower()
        new_cols.append(COLUMN_NORMALIZE_MAP.get(lookup, stripped))
    df.columns = new_cols
    return df


def sanitize_for_json(obj: Any) -> Any:
    """Recursively replace NaN / Infinity / numpy types with JSON-safe values."""
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_json(item) for item in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return None if (np.isnan(obj) or np.isinf(obj)) else float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return sanitize_for_json(obj.tolist())
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    return obj


def parse_indices(indices_str: str, max_val: int) -> List[int]:
    """Parse ``'1,3,5-8,12'`` / ``'all'`` / ``'none'`` → sorted 1-based ints."""
    s = indices_str.strip().lower()
    if s == "all":
        return list(range(1, max_val + 1))
    if s in ("none", "0", ""):
        return []

    result: set[int] = set()
    for part in s.replace(" ", "").split(","):
        if not part:
            continue
        if "-" in part:
            lo, hi = part.split("-", 1)
            for i in range(int(lo), int(hi) + 1):
                if 1 <= i <= max_val:
                    result.add(i)
        else:
            i = int(part)
            if 1 <= i <= max_val:
                result.add(i)
    return sorted(result)


def display_table(rows: List[Dict], columns: List[str], max_col_width: int = 50) -> str:
    """Format a list of dicts as an ASCII table."""
    if not rows:
        return "(no data)"

    widths: Dict[str, int] = {}
    for col in columns:
        header_w = len(str(col))
        data_w = max((len(str(row.get(col, ""))) for row in rows), default=0)
        widths[col] = min(max(header_w, data_w), max_col_width)

    def fmt(val: Any, width: int) -> str:
        s = str(val) if val is not None else ""
        return (s[: width - 3] + "...") if len(s) > width else s.ljust(width)

    lines = [
        " | ".join(fmt(col, widths[col]) for col in columns),
        "-+-".join("-" * widths[col] for col in columns),
    ]
    for row in rows:
        lines.append(" | ".join(fmt(row.get(col, ""), widths[col]) for col in columns))
    return "\n".join(lines)
