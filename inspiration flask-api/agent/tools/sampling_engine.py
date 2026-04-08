"""
KPMG Sampling Engine tool.

Determines the number of samples required per control for the Test of
Operating Effectiveness (TOE) stage, based on control frequency and
risk level using the KPMG methodology (or a user-supplied custom table).

This tool should be run BEFORE the TOE stage. It populates the
``count_of_samples`` column in the RCM.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..utils import sanitize_for_json

logger = logging.getLogger(__name__)

# ── KPMG Default Sampling Table ──────────────────────────────────────────────
# Maps (frequency, risk_level) → required sample count.
#
# Source: KPMG Audit Sampling Methodology
# Columns: frequency | transactions | lowerRisk (Low) | higherRisk (High)

KPMG_TABLE = [
    {"frequency": "Annual",
        "transactions": "1",         "Low": 1,  "High": 1},
    {"frequency": "Quarterly (including period end, i.e. +1)",
     "transactions": "2 - 4",    "Low": 2,  "High": 2},
    {"frequency": "Monthly",
        "transactions": "5 - 12",    "Low": 2,  "High": 3},
    {"frequency": "Weekly",
        "transactions": "13 - 52",   "Low": 5,  "High": 8},
    {"frequency": "Daily",
        "transactions": "53 - 365",  "Low": 15, "High": 25},
    {"frequency": "Recurring (multiple times per day)",
     "transactions": "Above 365", "Low": 25, "High": 40},
]

# Flexible keyword mapping: user-supplied frequency → KPMG table row
# Handles variations like "Annually", "Every Month", "Per Transaction", etc.
FREQUENCY_ALIASES: Dict[str, str] = {
    # Annual
    "annual":       "Annual",
    "annually":     "Annual",
    "yearly":       "Annual",
    "once a year":  "Annual",
    "per year":     "Annual",
    "year":         "Annual",
    # Quarterly
    "quarterly":    "Quarterly (including period end, i.e. +1)",
    "quarter":      "Quarterly (including period end, i.e. +1)",
    "every quarter": "Quarterly (including period end, i.e. +1)",
    "per quarter":  "Quarterly (including period end, i.e. +1)",
    # Monthly
    "monthly":      "Monthly",
    "every month":  "Monthly",
    "per month":    "Monthly",
    "month":        "Monthly",
    # Weekly
    "weekly":       "Weekly",
    "every week":   "Weekly",
    "per week":     "Weekly",
    "week":         "Weekly",
    # Daily
    "daily":        "Daily",
    "every day":    "Daily",
    "per day":      "Daily",
    "day":          "Daily",
    # Recurring / per transaction
    "recurring":                       "Recurring (multiple times per day)",
    "multiple times per day":          "Recurring (multiple times per day)",
    "per transaction":                 "Recurring (multiple times per day)",
    "each transaction":                "Recurring (multiple times per day)",
    "per occurrence":                  "Recurring (multiple times per day)",
    "transaction":                     "Recurring (multiple times per day)",
    "transactional":                   "Recurring (multiple times per day)",
    "real-time":                       "Recurring (multiple times per day)",
    "real time":                       "Recurring (multiple times per day)",
    "continuous":                      "Recurring (multiple times per day)",
    "ongoing":                         "Recurring (multiple times per day)",
    "multiple times a day":            "Recurring (multiple times per day)",
    "recurring (multiple time per day)": "Recurring (multiple times per day)",
    "recurring (multiple times per day)": "Recurring (multiple times per day)",
    # As needed / ad hoc → Recurring (conservative)
    "as needed":                       "Recurring (multiple times per day)",
    "as-needed":                       "Recurring (multiple times per day)",
    "as required":                     "Recurring (multiple times per day)",
    "ad hoc":                          "Recurring (multiple times per day)",
    "ad-hoc":                          "Recurring (multiple times per day)",
    "on-going":                        "Recurring (multiple times per day)",
    "event based":                     "Recurring (multiple times per day)",
    "on demand":                       "Recurring (multiple times per day)",
    "on-demand":                       "Recurring (multiple times per day)",
    "event driven":                    "Recurring (multiple times per day)",
    "event-driven":                    "Recurring (multiple times per day)",
}

RISK_ALIASES: Dict[str, str] = {
    "high":     "High",
    "h":        "High",
    "higher":   "High",
    "critical": "High",
    "low":      "Low",
    "l":        "Low",
    "lower":    "Low",
    "minimal":  "Low",
    # Medium → default to High (conservative audit approach)
    "medium":   "High",
    "med":      "High",
    "moderate": "High",
    "m":        "High",
}


def _normalize_frequency(raw) -> Optional[str]:
    """Map a raw frequency string to a KPMG table key."""
    # Handle NaN/None/missing values robustly — default to most conservative
    if raw is None:
        logger.warning(
            "Missing frequency — defaulting to 'Recurring (multiple times per day)'")
        return "Recurring (multiple times per day)"
    if isinstance(raw, float) and pd.isna(raw):
        logger.warning(
            "Missing frequency — defaulting to 'Recurring (multiple times per day)'")
        return "Recurring (multiple times per day)"
    cleaned = str(raw).strip().lower()
    if not cleaned or cleaned in ("nan", "none", "null", "n/a", "na", ""):
        logger.warning(
            "Empty/null frequency — defaulting to 'Recurring (multiple times per day)'")
        return "Recurring (multiple times per day)"
    # 1. Direct alias match (exact)
    if cleaned in FREQUENCY_ALIASES:
        return FREQUENCY_ALIASES[cleaned]
    # 2. Starts-with match, longest alias first so "bi-monthly" matches before "monthly"
    for alias, canonical in sorted(FREQUENCY_ALIASES.items(), key=lambda x: -len(x[0])):
        if cleaned.startswith(alias):
            return canonical
    logger.warning("Unrecognized frequency value: '%s'", raw)
    return None


def _normalize_risk(raw) -> str:
    """Map a raw risk level string to 'High' or 'Low'."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        logger.warning("Missing risk level — defaulting to 'High'")
        return "High"
    cleaned = str(raw).strip().lower()
    if not cleaned or cleaned in ("nan", "none", "null", "n/a", "na"):
        logger.warning("Empty/null risk level — defaulting to 'High'")
        return "High"
    if cleaned in RISK_ALIASES:
        return RISK_ALIASES[cleaned]
    # Default to High (conservative) with warning
    logger.warning("Unrecognized risk level '%s' — defaulting to 'High'", raw)
    return "High"


def _lookup_kpmg(frequency: str, risk: str, table: List[Dict]) -> Optional[int]:
    """Look up required sample count from the sampling table."""
    for row in table:
        if row["frequency"] == frequency:
            return row.get(risk, row.get("High", 1))
    return None


def _parse_custom_table(file_path: str) -> List[Dict]:
    """Parse a custom sampling engine Excel/CSV into the standard format."""
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(file_path)
    else:
        df = pd.read_excel(file_path)

    # Flexible column detection
    headers = list(df.columns)
    headers_lower = [h.lower() for h in headers]

    def find_col(*keywords):
        for i, h in enumerate(headers_lower):
            for kw in keywords:
                if kw in h:
                    return headers[i]
        return None

    freq_col = find_col("frequency")
    lower_col = find_col("lower")
    higher_col = find_col("higher")

    if not freq_col:
        raise ValueError(
            "Custom sampling table must have a column containing 'frequency'. "
            f"Found columns: {headers}"
        )
    if not lower_col or not higher_col:
        raise ValueError(
            "Custom sampling table must have columns containing 'lower' and 'higher' "
            f"(for risk levels). Found columns: {headers}"
        )

    table = []
    for _, row in df.iterrows():
        raw_freq = row[freq_col]
        if raw_freq is None or (isinstance(raw_freq, float) and pd.isna(raw_freq)):
            continue
        freq = str(raw_freq).strip()
        if not freq or freq.lower() in ("nan", "none", "null", "n/a", "na"):
            continue
        try:
            low_val = int(
                float(str(row[lower_col]).replace("+", "").strip() or "0"))
        except (ValueError, TypeError):
            low_val = 1
        try:
            high_val = int(
                float(str(row[higher_col]).replace("+", "").strip() or "0"))
        except (ValueError, TypeError):
            high_val = 1
        table.append({
            "frequency": freq,
            "Low": low_val,
            "High": high_val,
        })

    if not table:
        raise ValueError("No valid rows found in the custom sampling table.")

    return table


class SamplingEngineTool(Tool):
    """Determine required sample counts per control using KPMG or custom methodology."""

    @property
    def name(self) -> str:
        return "run_sampling_engine"

    @property
    def description(self) -> str:
        return (
            "Determine the number of samples required per control for the Test of "
            "Operating Effectiveness (TOE) using the KPMG sampling methodology or a "
            "custom sampling table. Uses each control's 'Control Frequency' and "
            "'risk_level' columns to look up the required sample count, then writes "
            "the result into the 'count_of_samples' column. Run this BEFORE the TOE stage. "
            "IMPORTANT: Before calling this tool, ASK the user whether to use the "
            "standard KPMG sampling table or a custom one. Do NOT default to KPMG "
            "without asking."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.ANALYSIS

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                name="engine_type",
                type="string",
                description=(
                    "Sampling methodology to use. 'kpmg' for the standard KPMG table, "
                    "'custom' to provide your own sampling table file."
                ),
                required=True,
                enum=["kpmg", "custom"],
            ),
            ToolParameter(
                name="custom_file_path",
                type="string",
                description=(
                    "Path to a custom sampling engine Excel/CSV file. Required if "
                    "engine_type is 'custom'. Must have columns: Frequency, "
                    "Risk of Failure (Lower), Risk of Failure (Higher)."
                ),
                required=False,
            ),
            ToolParameter(
                name="frequency_overrides",
                type="object",
                description=(
                    "User-provided sample counts for unrecognized frequency values. "
                    "Format: {\"<frequency_value>\": {\"Low\": <int>, \"High\": <int>}}. "
                    "Only provide this after the tool has returned unmatched frequencies "
                    "and the user has specified the sample counts."
                ),
                required=False,
            ),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if state.rcm_df is None:
            return "No RCM loaded. Use load_rcm first."
        freq_col = None
        for col in state.rcm_df.columns:
            if col.lower().replace("_", " ").strip() in (
                "control frequency", "controlfrequency", "frequency",
                "control_frequency", "frequence", "frequency of control",
            ):
                freq_col = col
                break
        if not freq_col:
            return (
                "RCM does not have a 'Control Frequency' column. "
                f"Available columns: {list(state.rcm_df.columns)}"
            )
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        engine_type = args.get("engine_type", "kpmg").lower()
        custom_path = args.get("custom_file_path")
        frequency_overrides = args.get("frequency_overrides") or {}

        df = state.rcm_df
        assert df is not None

        # ── Drop rows without a Control Id ────────────────────────────
        # Trailing rows from merged Excel cells (e.g. Process="Commercial"
        # but no Control Id) are not real controls and must be excluded.
        _cid_col = None
        for col in df.columns:
            if col.lower().replace("_", " ").strip() in (
                "control id", "controlid", "control_id",
            ):
                _cid_col = col
                break
        if _cid_col:
            _empty_cid = df[_cid_col].isna() | (df[_cid_col].astype(
                str).str.strip().isin(["", "nan", "none", "null"]))
            n_invalid = _empty_cid.sum()
            if n_invalid > 0:
                logger.info(
                    "Dropping %d rows with no Control Id before sampling", n_invalid)
                df = df[~_empty_cid].reset_index(drop=True)
                state.rcm_df = df

        # ── Resolve the sampling table ───────────────────────────────
        if engine_type == "custom":
            if not custom_path:
                return ToolResult(
                    success=False,
                    data={},
                    error="engine_type is 'custom' but no custom_file_path provided.",
                )
            if not os.path.exists(custom_path):
                return ToolResult(
                    success=False,
                    data={},
                    error=f"Custom sampling file not found: {custom_path}",
                )
            try:
                table = _parse_custom_table(custom_path)
                table_name = f"Custom ({os.path.basename(custom_path)})"
                logger.info(f"Loaded custom sampling table: {len(table)} rows")
            except Exception as e:
                return ToolResult(
                    success=False,
                    data={},
                    error=f"Failed to parse custom sampling table: {e}",
                )
        else:
            table = KPMG_TABLE
            table_name = "KPMG Standard"
            logger.info("Using KPMG default sampling table")

        # ── Find the frequency and risk columns ──────────────────────
        freq_col = None
        for col in df.columns:
            if col.lower().replace("_", " ").strip() in (
                "control frequency", "controlfrequency", "frequency",
                "control_frequency", "frequence", "frequency of control",
            ):
                freq_col = col
                break

        risk_col = None
        for col in df.columns:
            if col.lower().replace("_", " ").strip() in (
                "risk level", "risk_level", "risklevel",
            ):
                risk_col = col
                break

        # ── Process each control ─────────────────────────────────────
        results = []
        unmatched_frequencies = []
        sample_counts = []

        # Normalize user overrides keys to lowercase for matching
        overrides_lower = {k.strip().lower(): v for k,
                           v in frequency_overrides.items()}

        for idx, row in df.iterrows():
            control_id = row.get("Control Id", row.get(
                "control_id", f"Row {idx}"))
            raw_freq = str(row.get(freq_col, "")) if freq_col else ""
            raw_risk = str(row.get(risk_col, "High")) if risk_col else "High"

            norm_freq = _normalize_frequency(raw_freq)
            norm_risk = _normalize_risk(raw_risk)

            if norm_freq is None:
                # Check if user provided an override for this frequency
                freq_lower = raw_freq.strip().lower()
                override = overrides_lower.get(freq_lower)
                if override:
                    try:
                        count = int(override.get(
                            norm_risk, override.get("High", 1)))
                    except (ValueError, TypeError):
                        count = 1
                    sample_counts.append(count)
                    results.append({
                        "control_id": str(control_id),
                        "frequency": raw_freq,
                        "risk_level": norm_risk,
                        "matched_frequency": None,
                        "sample_count": count,
                        "note": f"User-provided override for '{raw_freq}'",
                    })
                else:
                    unmatched_frequencies.append({
                        "control_id": str(control_id),
                        "raw_frequency": raw_freq,
                        "risk_level": norm_risk,
                    })
                    # Placeholder — will be replaced if we proceed, or we'll ask user
                    sample_counts.append(None)
                    results.append({
                        "control_id": str(control_id),
                        "frequency": raw_freq,
                        "risk_level": norm_risk,
                        "matched_frequency": None,
                        "sample_count": None,
                    })
                continue

            count = _lookup_kpmg(norm_freq, norm_risk, table)
            if count is None:
                # Frequency matched an alias but not in the table (custom table issue)
                freq_lower = raw_freq.strip().lower()
                override = overrides_lower.get(freq_lower)
                if override:
                    try:
                        count = int(override.get(
                            norm_risk, override.get("High", 1)))
                    except (ValueError, TypeError):
                        count = 1
                    sample_counts.append(count)
                    results.append({
                        "control_id": str(control_id),
                        "frequency": raw_freq,
                        "risk_level": norm_risk,
                        "matched_frequency": norm_freq,
                        "sample_count": count,
                        "note": f"User-provided override for '{raw_freq}'",
                    })
                else:
                    unmatched_frequencies.append({
                        "control_id": str(control_id),
                        "raw_frequency": raw_freq,
                        "risk_level": norm_risk,
                    })
                    sample_counts.append(None)
                    results.append({
                        "control_id": str(control_id),
                        "frequency": raw_freq,
                        "risk_level": norm_risk,
                        "matched_frequency": norm_freq,
                        "sample_count": None,
                        "note": f"Frequency '{norm_freq}' not found in {table_name} table",
                    })
            else:
                sample_counts.append(count)
                results.append({
                    "control_id": str(control_id),
                    "frequency": raw_freq,
                    "risk_level": norm_risk,
                    "matched_frequency": norm_freq,
                    "sample_count": count,
                })

        # ── If there are unmatched frequencies, ask the user ───────────
        if unmatched_frequencies:
            # Deduplicate by raw frequency value
            unique_unmatched = {}
            for um in unmatched_frequencies:
                freq_val = um["raw_frequency"]
                if freq_val not in unique_unmatched:
                    unique_unmatched[freq_val] = []
                unique_unmatched[freq_val].append(um["control_id"])

            unmatched_summary = []
            for freq_val, control_ids in unique_unmatched.items():
                unmatched_summary.append({
                    "frequency": freq_val,
                    "affected_controls": control_ids,
                    "count": len(control_ids),
                })

            return ToolResult(
                success=False,
                data={
                    "awaiting_input": True,
                    "unmatched_frequencies": unmatched_summary,
                    "message": (
                        "Some frequency values in the RCM could not be matched to the "
                        "sampling table. ASK the user what sample counts (Low risk and "
                        "High risk) to use for each unrecognized frequency below. "
                        "Then re-call this tool with the same parameters plus "
                        "frequency_overrides containing their answers."
                    ),
                    "example_override_format": {
                        "<frequency_value>": {"Low": 5, "High": 10},
                    },
                },
                error=(
                    f"{len(unique_unmatched)} unrecognized frequency value(s) found: "
                    f"{list(unique_unmatched.keys())}. Ask the user for sample counts."
                ),
            )

        # ── Override: Automated controls always get sample count = 1 ──
        control_type_col = None
        for col in df.columns:
            if col.lower().replace("_", " ").strip() in (
                "control type", "controltype", "control_type",
            ):
                control_type_col = col
                break

        if control_type_col:
            for i, (idx, row) in enumerate(df.iterrows()):
                raw_type = str(row.get(control_type_col, "")).strip().lower()
                if raw_type in ("automated", "automatic", "auto", "it", "it automated"):
                    if sample_counts[i] != 1:
                        logger.info(
                            "Control %s: overriding sample count %d → 1 (Automated)",
                            results[i].get(
                                "control_id", idx), sample_counts[i],
                        )
                        sample_counts[i] = 1
                        results[i]["sample_count"] = 1
                        results[i]["note"] = "Automated control — sample count set to 1"

        # ── Write count_of_samples into the RCM ─────────────────────
        df["count_of_samples"] = sample_counts
        state.rcm_df = df
        logger.info(f"Sampling complete: {len(results)} controls, "
                    f"total samples required = {sum(sample_counts)}")

        # ── Re-export normalised RCM with sampling data ──────────────
        # The normalised_rcm.xlsx was saved at load time without
        # count_of_samples. Update it so TOE and downstream tools
        # see the full data.
        if state.output_dir:
            import os
            normalised_path = os.path.join(
                state.output_dir, "normalised_rcm.xlsx")
            try:
                df.to_excel(normalised_path, index=False, engine="openpyxl")
                logger.info(
                    "Updated normalised_rcm.xlsx with count_of_samples")
            except Exception as e:
                logger.warning("Failed to update normalised_rcm.xlsx: %s", e)

        # ── Build the sampling table for display ─────────────────────
        display_table = []
        for row in table:
            display_table.append({
                "frequency": row["frequency"],
                "transactions": row.get("transactions", ""),
                "lower_risk_samples": row["Low"],
                "higher_risk_samples": row["High"],
            })

        # ── Cache results in state ───────────────────────────────────
        state.sampling_results = results

        # ── Summary ──────────────────────────────────────────────────
        total_samples = sum(sample_counts)

        return ToolResult(
            success=True,
            data=sanitize_for_json({
                "engine_type": table_name,
                "controls_processed": len(results),
                "total_samples_required": total_samples,
                "sampling_table_used": display_table,
                "per_control_results": results,
                "unmatched_frequencies": unmatched_frequencies if unmatched_frequencies else None,
                "_agent_notes": (
                    [f"INFO: {len(unmatched_frequencies)} control(s) had unrecognized frequencies "
                     f"and were defaulted to 1 sample."]
                    if unmatched_frequencies else None
                ),
            }),
            summary=f"Sampling complete: {len(results)} controls, {total_samples} total samples required",
        )
