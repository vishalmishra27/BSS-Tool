"""
Smart RCM Excel reader — single-call bulletproof pipeline.

Full pipeline in one call:
  1. Header detection   — skips logos, titles, metadata rows
  2. Merged-cell cleanup — drops Unnamed columns, empty rows
  3. Marker normalisation — ●/✓/• → Yes, ○/□ → No
  4. Column normalisation (exact alias match)
  5. LLM fuzzy column match (only for remaining unresolved required columns)

Usage:
    from engines.rcm_reader import smart_read_file

    # Full pipeline (header detect + markers + column normalisation + LLM):
    df, info = smart_read_file("path/to/rcm.xlsx", normalize_columns=True)
    print(info)  # {'header_row': 3, 'score': 28, 'column_map': {...}, ...}

    # Lightweight (no column normalisation):
    df = smart_read_file("path/to/rcm.xlsx", normalize_columns=False)

    # Legacy return style for backward compat:
    df, header_row, score = smart_read_file("path/to/rcm.xlsx", return_details=True)
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, List, Tuple, Union

import pandas as pd

logger = logging.getLogger("engines.rcm_reader")


# ═══════════════════════════════════════════════════════════════════════════
#  1. HEADER-ROW DETECTION
# ═══════════════════════════════════════════════════════════════════════════

HEADER_HINTS = {
    # Core RCM columns
    "process", "sub process", "subprocess", "sub-process",
    "control objective", "risk id", "risk title",
    "risk description", "control id", "control description",
    "control owner", "control rating", "nature of control",
    "control type", "control frequency", "application/system",
    "application / system", "risk level", "count of samples",
    # Extended columns seen in real-world RCMs
    "entity", "entity/country", "entity / country",
    "location", "mega process", "mega_process",
    "sub-process no", "sub process no", "subprocess no",
    "sub process description", "sub-process description",
    "risk no", "risk number", "risk #",
    "control no", "control number", "control #",
    "control narrative", "control activity",
    "key or non key", "key or non key control",
    "key / non key", "key/non-key", "key control",
    "significant risk", "significant risk or non significant risk",
    "risk category", "risk rating",
    # Financial Assertions
    "financial assertions", "existence or occurrence",
    "completeness", "rights and obligations",
    "valuation and allocation", "presentation and disclosure",
    "accuracy",
    # IFC Components
    "ifc components",
    "timely preparation of reliable financial information",
    "anti-fraud", "anti fraud",
    "policies and procedures", "safeguarding of assets",
    "control environment", "risk assessment",
    # Other common headers
    "performed by", "frequency", "preventive/detective",
    "p/d", "manual/automated", "automation",
    "sample size", "assertion", "coso objective",
    "coso principle", "coso point of focus", "coso points of focus",
    "info & communication", "info and communication", "monitoring",
    "control activities",
    "test of design", "test of effectiveness",
}

HEADER_PARTIAL_TOKENS = (
    "process", "risk", "control", "frequency", "owner",
    "system", "assertion", "entity", "location", "objective",
    "description", "narrative", "key", "significant",
    "completeness", "accuracy", "valuation", "occurrence",
    "safeguarding", "anti-fraud", "ifc", "coso", "monitoring",
)


def _score_header_row(values: List[Any]) -> int:
    score = 0
    for v in values:
        s = str(v).strip().lower()
        if not s or s == "nan" or s == "none":
            continue
        if s in HEADER_HINTS:
            score += 3
        elif any(tok in s for tok in HEADER_PARTIAL_TOKENS):
            score += 1
    return score


# ═══════════════════════════════════════════════════════════════════════════
#  2. MARKER / EMOJI NORMALISATION
# ═══════════════════════════════════════════════════════════════════════════

_YES_MARKERS = frozenset({
    "●", "•", "⬤", "◉", "◆",
    "✓", "✔", "☑", "☒",
    "✕", "✖", "✗", "✘",
    "■", "▪", "▸", "►",
    "y", "yes", "true", "1", "x",
})

_NO_MARKERS = frozenset({
    "○", "◯", "◇", "□", "☐",
    "n", "no", "false", "0", "-",
})

_MARKER_RE = re.compile(
    r"^[\s]*("
    r"[●•⬤◉◆✓✔☑☒✕✖✗✘■▪▸►○◯◇□☐]"
    r"|[Yy]es|[Nn]o|[Tt]rue|[Ff]alse|[Xx]"
    r")[\s]*$"
)


def _normalize_markers(df: pd.DataFrame) -> pd.DataFrame:
    def _convert_cell(val):
        if not isinstance(val, str):
            return val
        stripped = val.strip().lower()
        if not stripped:
            return val
        if not _MARKER_RE.match(val):
            return val
        if stripped in _YES_MARKERS:
            return "Yes"
        if stripped in _NO_MARKERS:
            return "No"
        return val
    return df.map(_convert_cell)


# ═══════════════════════════════════════════════════════════════════════════
#  3. COLUMN NORMALISATION — exact alias mapping
# ═══════════════════════════════════════════════════════════════════════════

# Canonical columns that TOD / TOE engines require
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

OPTIONAL_COLUMNS: Dict[str, str] = {
    "control_objective":  "Control Objective",
    "control_owner":      "Control Owner",
    "application_system": "Application/System",
    "risk_title":         "Risk Title",
    "count_of_samples":   "count_of_samples",
    "risk_probability":   "risk_probability",
    "risk_impact":        "risk_impact",
}

_ALL_COLUMNS: Dict[str, str] = {**REQUIRED_COLUMNS, **OPTIONAL_COLUMNS}

# Build exact-match lookup: lowercase → canonical display name
_KNOWN_ALIASES: Dict[str, str] = {}
for _field, _display in _ALL_COLUMNS.items():
    _KNOWN_ALIASES[_field] = _display
    _KNOWN_ALIASES[_display.lower()] = _display

_EXTRA_ALIASES: Dict[str, str] = {
    "sub process": "SubProcess",
    "sub_process": "SubProcess",
    "sub-process": "SubProcess",
    "risk id": "Risk Id",
    "riskid": "Risk Id",
    "risk_id": "Risk Id",
    "risk_title": "Risk Title",
    "risk_description": "Risk Description",
    "risk level": "risk_level",
    "risklevel": "risk_level",
    "control id": "Control Id",
    "controlid": "Control Id",
    "control_id": "Control Id",
    "control_description": "Control Description",
    "control_objective": "Control Objective",
    "control_owner": "Control Owner",
    "control_type": "Control Type",
    "control_frequency": "Control Frequency",
    "nature_of_control": "Nature of Control",
    "nature of control": "Nature of Control",
    "application/system": "Application/System",
    "application / system": "Application/System",
    "application_system": "Application/System",
    "count of samples": "count_of_samples",
    "count_of_samples": "count_of_samples",
    # Extended aliases from real-world RCM variants
    "mega process": "Process",
    "business process": "Process",
    "cycle": "Process",
    "process area": "Process",
    "sub-process description": "SubProcess",
    "sub process description": "SubProcess",
    "activity": "SubProcess",
    "process step": "SubProcess",
    # NOTE: "sub-process no" / "sub process no" are numeric IDs (e.g. 5.1),
    # NOT the subprocess name — leave them as passthrough, do NOT map to SubProcess.
    "risk no": "Risk Id",
    "risk #": "Risk Id",
    "risk ref": "Risk Id",
    "risk number": "Risk Id",
    "risk name": "Risk Title",
    "risk summary": "Risk Title",
    "risk desc": "Risk Description",
    "risk narrative": "Risk Description",
    "risk detail": "Risk Description",
    "ctrl id": "Control Id",
    "control #": "Control Id",
    "control number": "Control Id",
    "control ref": "Control Id",
    "control no": "Control Id",
    "control no.": "Control Id",
    "control activity": "Control Description",
    "control narrative": "Control Description",
    "control detail": "Control Description",
    "performed by": "Control Owner",
    "responsible": "Control Owner",
    "owner": "Control Owner",
    "preventive/detective": "Nature of Control",
    "p/d": "Nature of Control",
    "manual/automated": "Control Type",
    "manual / automated": "Control Type",
    "automation": "Control Type",
    "manual/automated/itdm": "Control Type",
    "system": "Application/System",
    "application": "Application/System",
    "it system": "Application/System",
    "erp": "Application/System",
    "risk rating": "risk_level",
    "inherent risk": "risk_level",
    "risk severity": "risk_level",
    # Risk Probability aliases
    "risk probability": "risk_probability",
    "risk_probability": "risk_probability",
    "probability": "risk_probability",
    "likelihood": "risk_probability",
    "risk likelihood": "risk_probability",
    "probability of occurrence": "risk_probability",
    "inherent likelihood": "risk_probability",
    # Risk Impact aliases
    "risk impact": "risk_impact",
    "risk_impact": "risk_impact",
    "impact": "risk_impact",
    "risk consequence": "risk_impact",
    "consequence": "risk_impact",
    "severity": "risk_impact",
    "inherent impact": "risk_impact",
    "impact rating": "risk_impact",
    "sample size": "count_of_samples",
    "# samples": "count_of_samples",
    "no. of samples": "count_of_samples",
    "assertion": "Control Objective",
    "coso objective": "Control Objective",
    "frequency of control": "Control Frequency",
}
_KNOWN_ALIASES.update(_EXTRA_ALIASES)


def _try_exact_match(
    input_columns: List[str],
) -> Tuple[Dict[str, str], List[str]]:
    """Pass 1: resolve columns via exact / case-insensitive alias match."""
    mapped: Dict[str, str] = {}
    remaining: List[str] = []
    used_canonical: set[str] = set()

    for col in input_columns:
        lookup = col.strip().lower()
        if lookup in _KNOWN_ALIASES:
            canonical = _KNOWN_ALIASES[lookup]
            if canonical not in used_canonical:
                mapped[col] = canonical
                used_canonical.add(canonical)
            else:
                remaining.append(col)
        else:
            remaining.append(col)

    return mapped, remaining


# ═══════════════════════════════════════════════════════════════════════════
#  4. COLUMN NORMALISATION — LLM fuzzy match
# ═══════════════════════════════════════════════════════════════════════════

_LLM_SYSTEM_PROMPT = (
    "You are an expert data-mapping assistant for audit RCM (Risk Control Matrix) spreadsheets.\n"
    "Given a list of INPUT column names from a user's spreadsheet and a list of REQUIRED column names "
    "expected by the audit engine, map each input column to the best matching required column.\n\n"
    "RULES:\n"
    "1. Only map when you are confident the columns represent the same concept.\n"
    "2. If an input column does not match any required column, set its value to null.\n"
    "3. Each required column can be used AT MOST once.\n"
    "4. Common synonyms to consider:\n"
    "   - 'Business Process' / 'Cycle' / 'Process Area' / 'Mega Process' → 'Process'\n"
    "   - 'Sub-Process' / 'Activity' / 'Process Step' / 'Sub Process Description' → 'SubProcess'\n"
    "   - 'Sub-Process No' / 'Sub Process No' / 'Subprocess No' are numeric IDs, NOT subprocess names → keep as passthrough (null)\n"
    "   - 'Ctrl ID' / 'Control #' / 'Control Number' / 'Control Ref' / 'Control No' → 'Control Id'\n"
    "   - 'Risk #' / 'Risk Ref' / 'Risk Number' / 'Risk No' → 'Risk Id'\n"
    "   - 'Risk Name' / 'Risk Summary' → 'Risk Title'\n"
    "   - 'Risk Desc' / 'Risk Narrative' / 'Risk Detail' → 'Risk Description'\n"
    "   - 'Control Activity' / 'Control Narrative' / 'Control Detail' → 'Control Description'\n"
    "   - 'Performed By' / 'Responsible' / 'Owner' → 'Control Owner'\n"
    "   - 'Preventive/Detective' / 'P/D' → 'Nature of Control'\n"
    "   - 'Manual/Automated/ITDM' / 'Automation' → 'Control Type'\n"
    "   - 'Frequency' / 'Periodicity' / 'How Often' → 'Control Frequency'\n"
    "   - 'System' / 'Application' / 'IT System' / 'ERP' → 'Application/System'\n"
    "   - 'Risk Rating' / 'Inherent Risk' / 'Risk Severity' / 'Significant Risk' → 'risk_level'\n"
    "   - 'Probability' / 'Likelihood' / 'Risk Likelihood' / 'Inherent Likelihood' → 'risk_probability'\n"
    "   - 'Impact' / 'Consequence' / 'Risk Consequence' / 'Severity' / 'Inherent Impact' / 'Impact Rating' → 'risk_impact'\n"
    "   - 'Sample Size' / '# Samples' / 'No. of Samples' → 'count_of_samples'\n"
    "   - 'Control Objective' / 'Assertion' / 'COSO Objective' → 'Control Objective'\n"
    "5. TYPOS & MISSPELLINGS: Always consider that column names may contain typos or non-English spelling "
    "(e.g., 'Frequence' → 'Control Frequency', 'Controle' → 'Control Id'). Match on meaning, not exact spelling.\n"
    "6. IMPORTANT: Columns ending with 'No', 'No.', or '#' are usually numeric identifiers/reference numbers, "
    "NOT descriptive text. Do NOT map them to descriptive fields like 'SubProcess', 'Risk Title', etc. "
    "Only map them to ID fields (e.g., 'Risk Id', 'Control Id') if appropriate.\n\n"
    "7. Common passthrough columns (set to null):\n"
    "   - 'Key or Non Key Control' / 'Key/Non-Key' → keep as passthrough (null)\n"
    "   - Financial assertion columns (Completeness, Accuracy, etc.) → keep as passthrough (null)\n"
    "   - IFC component columns (Anti-Fraud, Safeguarding, etc.) → keep as passthrough (null)\n\n"
    "8. Use the SAMPLE DATA provided to understand the actual content of each column "
    "(e.g., numeric IDs like '5.1' vs descriptive text like 'Accounts Payable').\n\n"
    "Return ONLY a JSON object: {\"mappings\": {\"<input_col>\": \"<required_col or null>\", ...}}"
)


def _get_llm_client():
    """Create an Azure OpenAI client using central engines/config.py credentials."""
    from engines.config import (
        AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY,
        AZURE_OPENAI_API_VERSION, AZURE_OPENAI_DEPLOYMENT,
    )

    if not AZURE_OPENAI_API_KEY:
        logger.warning("No API key found for LLM column matching — skipping LLM pass")
        return None, AZURE_OPENAI_DEPLOYMENT

    from openai import AzureOpenAI
    return AzureOpenAI(
        api_key=AZURE_OPENAI_API_KEY,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version=AZURE_OPENAI_API_VERSION,
    ), AZURE_OPENAI_DEPLOYMENT


def _llm_match(
    remaining_input: List[str],
    unmatched_targets: List[str],
    df: pd.DataFrame | None = None,
) -> Dict[str, str]:
    """Pass 2: use LLM to fuzzy-match leftover input columns to required ones."""
    if not remaining_input or not unmatched_targets:
        return {}

    client, model = _get_llm_client()
    if client is None:
        return {}

    # Build sample data snippet so the LLM can see actual values
    sample_hint = ""
    if df is not None:
        try:
            sample_rows = df[remaining_input].head(2).to_dict(orient="records")
            sample_hint = (
                f"\n\nSAMPLE DATA (first rows for the input columns above):\n"
                f"{json.dumps(sample_rows, default=str, ensure_ascii=False)}\n"
                "Use this sample data to understand what each column contains "
                "(e.g., numeric IDs vs descriptive text)."
            )
        except Exception:
            pass  # if columns don't match exactly, skip sample

    user_prompt = (
        f"INPUT columns (from user spreadsheet):\n{json.dumps(remaining_input)}\n\n"
        f"REQUIRED columns (engine expects):\n{json.dumps(unmatched_targets)}\n\n"
        "Map each input column to the best matching required column, or null if no match."
        f"{sample_hint}"
    )

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": _LLM_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            max_completion_tokens=2048,
            response_format={"type": "json_object"},
        )
        text = resp.choices[0].message.content or "{}"
        result = json.loads(text)
        raw_mappings = result.get("mappings", result)

        # Validate: only accept mappings to actual target columns
        valid_targets = set(unmatched_targets)
        used: set[str] = set()
        llm_mapped: Dict[str, str] = {}
        for inp, req in raw_mappings.items():
            if req and req in valid_targets and req not in used:
                llm_mapped[inp] = req
                used.add(req)

        logger.info("LLM column match resolved %d columns: %s", len(llm_mapped), llm_mapped)
        return llm_mapped

    except Exception as e:
        logger.warning("LLM column mapping failed: %s — continuing with exact-match only", e)
        return {}


def _normalize_columns(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, Dict[str, str], List[str], List[str]]:
    """Full column normalisation: exact alias match + LLM fuzzy match.

    Returns:
        df            – DataFrame with renamed columns
        column_map    – {original_col: canonical_col} for all mapped columns
        passthrough   – columns that were NOT mapped (kept as-is)
        still_missing – required canonical columns that couldn't be mapped
    """
    input_columns = [str(c).strip() for c in df.columns]
    all_canonical = list(_ALL_COLUMNS.values())
    required_canonical = list(REQUIRED_COLUMNS.values())

    # Pass 1: exact alias match
    exact_mapped, remaining = _try_exact_match(input_columns)
    logger.info(
        "Column normaliser — exact match: %d/%d columns resolved",
        len(exact_mapped), len(input_columns),
    )

    # Which target columns are still unresolved?
    resolved_canonical = set(exact_mapped.values())
    unmatched_all = [c for c in all_canonical if c not in resolved_canonical]

    # Pass 2: LLM fuzzy match (only if there are remaining inputs AND unresolved targets)
    llm_mapped: Dict[str, str] = {}
    if remaining and unmatched_all:
        logger.info(
            "Column normaliser — calling LLM for %d unresolved columns vs %d targets",
            len(remaining), len(unmatched_all),
        )
        llm_mapped = _llm_match(remaining, unmatched_all, df=df)

    # Build final rename map
    full_map: Dict[str, str] = {**exact_mapped, **llm_mapped}
    passthrough = [c for c in input_columns if c not in full_map]
    all_resolved = set(full_map.values())
    still_missing = [c for c in required_canonical if c not in all_resolved]

    # Apply renames
    if full_map:
        df = df.rename(columns=full_map)

    logger.info(
        "Column normaliser done: %d mapped, %d passthrough, %d still missing",
        len(full_map), len(passthrough), len(still_missing),
    )

    return df, full_map, passthrough, still_missing


# ═══════════════════════════════════════════════════════════════════════════
#  5. PUBLIC API — single-call pipeline
# ═══════════════════════════════════════════════════════════════════════════

def smart_read_excel(
    file_path: str,
    sheet_name=None,
    return_details: bool = False,
    normalize_markers: bool = True,
    normalize_columns: bool = False,
    probe_rows: int = 15,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, int, int], Tuple[pd.DataFrame, dict]]:
    """Read an Excel RCM file with full pipeline.

    Pipeline:
        1. Probe first N rows → detect the real header row (skips logos/titles)
        2. Re-read with detected header → drop Unnamed columns, empty rows
        3. Normalise marker characters (●→Yes, ○→No)
        4. Normalise column names (exact alias + LLM fuzzy match)

    Args:
        file_path:          Path to the Excel file.
        sheet_name:         Sheet to read (default: first sheet).
        return_details:     If True, return (df, header_row, score) — legacy compat.
        normalize_markers:  Convert marker chars to Yes/No (default True).
        normalize_columns:  Run full column normalisation incl. LLM (default False).
        probe_rows:         How many top rows to scan for the header.

    Returns:
        - normalize_columns=True  → (df, info_dict)
        - return_details=True     → (df, header_row_1based, score)
        - otherwise               → df
    """
    sheet = sheet_name if sheet_name is not None else 0

    # ── Step 1: probe top rows for header ──────────────────────────────
    try:
        probe = pd.read_excel(
            file_path, sheet_name=sheet, header=None,
            dtype=str, nrows=probe_rows,
        )
    except Exception:
        df = pd.read_excel(file_path, sheet_name=sheet, dtype=str)
        # Drop rows where all key ID columns are empty (trailing merged-cell rows)
        _fb_key_hints = {"control id", "control_id", "controlid", "ctrl id",
                         "control #", "control no", "control number",
                         "risk id", "risk_id", "riskid", "risk no", "risk #",
                         "control description", "control_description",
                         "control narrative", "control activity"}
        fb_key_cols = [c for c in df.columns if c.strip().lower() in _fb_key_hints]
        if fb_key_cols:
            fb_empty = df[fb_key_cols].apply(
                lambda col: col.isna() | (col.astype(str).str.strip() == "")
            ).all(axis=1)
            df = df[~fb_empty].reset_index(drop=True)
        # Forward-fill merged cells in fallback path (grouping + categorical columns)
        _fb_ffill = {
            "process", "subprocess", "sub process", "sub-process",
            "mega process", "business process", "cycle",
            "entity", "entity/country", "location",
            "risk level", "risk_level", "risklevel", "risk rating",
            "nature of control", "nature_of_control", "preventive/detective",
            "control type", "control_type", "manual/automated",
            "control frequency", "control_frequency", "frequency",
            "frequency of control",
        }
        for col in df.columns:
            if df[col].isna().any():
                col_lower = col.strip().lower()
                if col_lower not in _fb_ffill:
                    continue
                non_null_count = df[col].notna().sum()
                if non_null_count > 0 and non_null_count < len(df[col]):
                    df[col] = df[col].ffill()
        if normalize_markers:
            df = _normalize_markers(df)
        if normalize_columns:
            df, col_map, passthrough, missing = _normalize_columns(df)
            return df, {"header_row": 1, "score": 0, "column_map": col_map,
                        "passthrough": passthrough, "missing": missing}
        if return_details:
            return df, 1, 0
        return df

    if probe.empty:
        df = pd.DataFrame()
        if normalize_columns:
            return df, {"header_row": 1, "score": 0, "column_map": {},
                        "passthrough": [], "missing": list(REQUIRED_COLUMNS.values())}
        if return_details:
            return df, 1, 0
        return df

    best_idx = 0
    best_score = -1
    for idx in range(len(probe)):
        score = _score_header_row(probe.iloc[idx].tolist())
        if score > best_score:
            best_idx = idx
            best_score = score

    logger.info(
        "smart_read_excel: header at row %s (score=%s) for %s",
        best_idx + 1, best_score, file_path,
    )

    # ── Step 2: re-read with detected header, clean up ─────────────────
    df = pd.read_excel(file_path, sheet_name=sheet, header=best_idx, dtype=str)
    df = df.dropna(axis=0, how="all")

    unnamed = [c for c in df.columns if str(c).strip().lower().startswith("unnamed")]
    if unnamed:
        df = df.drop(columns=unnamed, errors="ignore")

    df.columns = [str(c).strip() for c in df.columns]

    # ── Step 2a: drop rows where all key columns are empty ──────────
    # Rows that have no Control Id, no Risk Id, and no Control Description
    # are not valid RCM rows (e.g. trailing rows from merged Process cells).
    _KEY_HINTS = {"control id", "control_id", "controlid", "ctrl id",
                  "control #", "control no", "control number", "control ref",
                  "risk id", "risk_id", "riskid", "risk no", "risk #",
                  "risk number", "risk ref",
                  "control description", "control_description",
                  "control narrative", "control activity"}
    key_cols = [c for c in df.columns if c.strip().lower() in _KEY_HINTS]
    if key_cols:
        empty_key_mask = df[key_cols].apply(
            lambda col: col.isna() | (col.astype(str).str.strip() == "")
        ).all(axis=1)
        n_dropped = empty_key_mask.sum()
        if n_dropped > 0:
            df = df[~empty_key_mask].reset_index(drop=True)
            logger.info("Dropped %d rows with all key columns empty", n_dropped)

    # ── Step 2b: forward-fill merged cells ────────────────────────────
    # Excel merged cells only store the value in the top-left cell; the
    # rest read as NaN.  Forward-fill so every row has the full data.
    # ONLY fill grouping/categorical columns that are commonly merged in
    # RCMs.  Columns with per-row unique values (IDs, descriptions,
    # owners) are NOT forward-filled — their NaNs mean genuinely empty.
    _FFILL_COLUMNS = {
        # Grouping / hierarchy columns
        "process", "subprocess", "sub process", "sub-process",
        "mega process", "business process", "cycle",
        "entity", "entity/country", "location",
        # Categorical columns often merged across controls under same risk
        "risk level", "risk_level", "risklevel", "risk rating",
        "inherent risk", "risk severity",
        "nature of control", "nature_of_control",
        "preventive/detective", "p/d",
        "control type", "control_type",
        "manual/automated", "manual / automated",
        "control frequency", "control_frequency",
        "frequency", "periodicity", "frequency of control",
    }
    for col in df.columns:
        if df[col].isna().any():
            col_lower = col.strip().lower()
            # Only forward-fill known grouping/categorical columns
            if col_lower not in _FFILL_COLUMNS:
                continue
            non_null_count = df[col].notna().sum()
            total = len(df[col])
            if non_null_count > 0 and non_null_count < total:
                df[col] = df[col].ffill()
                logger.debug("Forward-filled merged cells in column '%s'", col)

    # ── Step 3: marker normalisation ───────────────────────────────────
    if normalize_markers:
        df = _normalize_markers(df)

    # ── Step 4: column normalisation ───────────────────────────────────
    if normalize_columns:
        df, col_map, passthrough, missing = _normalize_columns(df)

        # ── Step 4a: post-normalization cleanup ───────────────────────
        # Step 2a uses original column names which may not match _KEY_HINTS
        # if the LLM renamed them. Now that columns are normalized, drop
        # rows that have no Control Id (trailing merged-cell rows).
        _norm_key_cols = [c for c in ("Control Id", "Control Description")
                         if c in df.columns]
        if _norm_key_cols:
            _empty_mask = df[_norm_key_cols].apply(
                lambda col: col.isna() | (col.astype(str).str.strip().isin(["", "nan", "none", "null"]))
            ).all(axis=1)
            _n_post_drop = _empty_mask.sum()
            if _n_post_drop > 0:
                df = df[~_empty_mask].reset_index(drop=True)
                logger.info("Post-normalization: dropped %d rows with no Control Id/Description", _n_post_drop)

        return df, {
            "header_row": best_idx + 1,
            "score": best_score,
            "column_map": col_map,
            "passthrough": passthrough,
            "missing": missing,
        }

    if return_details:
        return df, best_idx + 1, best_score
    return df


def smart_read_file(
    file_path: str,
    sheet_name=None,
    return_details: bool = False,
    normalize_markers: bool = True,
    normalize_columns: bool = False,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, int, int], Tuple[pd.DataFrame, dict]]:
    """Read an RCM file (Excel or CSV) with full pipeline.

    For CSV files, header detection is skipped (CSVs rarely have logos).
    Marker normalisation and column normalisation still apply.
    """
    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path, dtype=str)
        if normalize_markers:
            df = _normalize_markers(df)
        if normalize_columns:
            df, col_map, passthrough, missing = _normalize_columns(df)
            return df, {
                "header_row": 1, "score": 0, "column_map": col_map,
                "passthrough": passthrough, "missing": missing,
            }
        if return_details:
            return df, 1, 0
        return df

    return smart_read_excel(
        file_path,
        sheet_name=sheet_name,
        return_details=return_details,
        normalize_markers=normalize_markers,
        normalize_columns=normalize_columns,
    )
