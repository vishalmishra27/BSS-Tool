"""Chat-based tool to modify a single control's inferred frequency by control ID.

Allows the user to change the inferred frequency for a specific control via chat,
without downloading/uploading an Excel file.  Updates the pending inferences cache
so that when the user approves, the correct frequency is written to the RCM.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import Tool
from .infer_control_frequency import KPMG_FREQUENCIES, _upload_artifact
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..utils import sanitize_for_json

logger = logging.getLogger("agent.tools.modify_control_frequency")


class ModifyControlFrequencyTool(Tool):
    """Modify the inferred frequency for a single control by its Control ID."""

    @property
    def name(self) -> str:
        return "modify_control_frequency"

    @property
    def description(self) -> str:
        return (
            "Modify the inferred frequency for one or more controls by Control ID. "
            "Use this when the user wants to change a specific control's frequency "
            "via chat (e.g., 'change CC-01 to Monthly'). Accepts a control ID and "
            "the new KPMG frequency value. Valid frequencies: Annual, Quarterly, "
            "Monthly, Weekly, Daily, Recurring."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.UTILITY

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                "control_id", "string",
                "The Control ID to modify (e.g., 'CC-01', 'CTRL-005').",
                required=True,
            ),
            ToolParameter(
                "frequency", "string",
                (
                    "The new frequency value. Must be one of the KPMG sampling terms: "
                    "'Annual', 'Quarterly (including period end, i.e. +1)', 'Monthly', "
                    "'Weekly', 'Daily', 'Recurring (multiple times per day)'. "
                    "Shorthand accepted: 'Quarterly' or 'Recurring'."
                ),
                required=True,
            ),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if not state.pending_frequency_inferences:
            return (
                "No pending frequency inferences to modify. "
                "Run infer_control_frequency first."
            )
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        control_id = str(args.get("control_id", "")).strip()
        new_freq = str(args.get("frequency", "")).strip()

        if not control_id:
            return ToolResult(success=False, data={}, error="No control_id provided.")
        if not new_freq:
            return ToolResult(success=False, data={}, error="No frequency provided.")

        inferences = state.pending_frequency_inferences

        # ── Resolve Control ID (case-insensitive matching) ────────────
        matched_cid = None
        for cid in inferences:
            if cid.strip().lower() == control_id.lower():
                matched_cid = cid
                break

        if not matched_cid:
            available = sorted(inferences.keys())
            return ToolResult(
                success=False,
                data={"available_control_ids": available},
                error=(
                    f"Control ID '{control_id}' not found in pending frequency inferences. "
                    f"Available Control IDs: {available}"
                ),
            )

        # ── Resolve frequency (shorthand + case-insensitive) ──────────
        SHORTHAND_MAP = {
            "annual": "Annual",
            "quarterly": "Quarterly (including period end, i.e. +1)",
            "monthly": "Monthly",
            "weekly": "Weekly",
            "daily": "Daily",
            "recurring": "Recurring (multiple times per day)",
            "multiple times per day": "Recurring (multiple times per day)",
            "per transaction": "Recurring (multiple times per day)",
            "ad-hoc": "Recurring (multiple times per day)",
            "ad hoc": "Recurring (multiple times per day)",
            "event-driven": "Recurring (multiple times per day)",
            "event driven": "Recurring (multiple times per day)",
            "continuous": "Recurring (multiple times per day)",
        }

        resolved_freq = None
        freq_lower = new_freq.lower().strip()

        # Try exact match first
        if new_freq in KPMG_FREQUENCIES:
            resolved_freq = new_freq
        # Try case-insensitive match
        elif freq_lower in SHORTHAND_MAP:
            resolved_freq = SHORTHAND_MAP[freq_lower]
        else:
            # Try partial match
            for canonical in KPMG_FREQUENCIES:
                if canonical.lower().startswith(freq_lower):
                    resolved_freq = canonical
                    break

        if not resolved_freq:
            return ToolResult(
                success=False,
                data={
                    "provided_value": new_freq,
                    "valid_frequencies": KPMG_FREQUENCIES,
                    "shorthand_accepted": list(SHORTHAND_MAP.keys()),
                },
                error=(
                    f"'{new_freq}' is not a valid KPMG frequency. "
                    f"Valid values: {KPMG_FREQUENCIES}. "
                    "Shorthand also accepted: Annual, Quarterly, Monthly, Weekly, Daily, Recurring."
                ),
            )

        # ── Update the inference ──────────────────────────────────────
        old_freq = inferences[matched_cid]["inferred_frequency"]
        inferences[matched_cid]["inferred_frequency"] = resolved_freq
        inferences[matched_cid]["source"] = "User Override"
        inferences[matched_cid]["confidence"] = "High"

        state.pending_frequency_inferences = inferences

        # Re-export updated Excel if output dir exists
        if state.output_dir:
            excel_path = os.path.join(state.output_dir, "inferred_frequencies.xlsx")
            rows_for_excel = []
            for cid in sorted(inferences.keys()):
                inf = inferences[cid]
                rows_for_excel.append({
                    "Control ID": inf["control_id"],
                    "Control Description": inf.get("control_description", ""),
                    "Current Frequency": inf.get("current_frequency", ""),
                    "Inferred Frequency (KPMG)": inf["inferred_frequency"],
                    "Business Frequency": inf.get("business_frequency", ""),
                    "Source": inf["source"],
                    "Confidence": inf["confidence"],
                })
            try:
                export_df = pd.DataFrame(rows_for_excel)
                export_df.to_excel(excel_path, index=False, engine="openpyxl")
                state.frequency_inference_excel_path = excel_path
                _upload_artifact(excel_path, state)
            except Exception as e:
                logger.warning("Failed to re-export frequencies Excel: %s", e)

        return ToolResult(
            success=True,
            data=sanitize_for_json({
                "control_id": matched_cid,
                "old_frequency": old_freq,
                "new_frequency": resolved_freq,
                "total_pending": len(inferences),
                "requires_approval": True,
                "message": (
                    f"Updated {matched_cid}: '{old_freq}' → '{resolved_freq}'. "
                    f"{len(inferences)} controls still pending approval."
                ),
            }),
            summary=f"Updated {matched_cid} frequency: {old_freq} → {resolved_freq}",
        )
