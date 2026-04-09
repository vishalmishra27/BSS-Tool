"""Chat-based tool to override the sample count for a single control.

After the sampling engine runs, the user may want to change the number of
samples required for a specific control (e.g., "change CC-01 to 5 samples").
This tool updates both the cached sampling results and the RCM dataframe.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..utils import sanitize_for_json

logger = logging.getLogger("agent.tools.modify_sampling_output")


class ModifySamplingOutputTool(Tool):
    """Override the sample count for a single control after sampling."""

    @property
    def name(self) -> str:
        return "modify_sampling_output"

    @property
    def description(self) -> str:
        return (
            "Override the sample count for one control after the sampling engine "
            "has run. Use this when the user wants to change a specific control's "
            "required sample count via chat (e.g., 'change CC-01 to 5 samples'). "
            "Updates both the cached sampling results and the RCM."
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
                "sample_count", "string",
                "The new sample count (must be a positive integer).",
                required=True,
            ),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if state.sampling_results is None:
            return (
                "No sampling results to modify. "
                "Run run_sampling_engine first."
            )
        if state.rcm_df is None:
            return "No RCM loaded."
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        control_id = str(args.get("control_id", "")).strip()
        count_raw = str(args.get("sample_count", "")).strip()

        if not control_id:
            return ToolResult(success=False, data={}, error="No control_id provided.")

        try:
            new_count = int(float(count_raw))
            if new_count < 0:
                raise ValueError
        except (ValueError, TypeError):
            return ToolResult(
                success=False, data={},
                error=f"'{count_raw}' is not a valid sample count. Must be a positive integer.",
            )

        results = state.sampling_results
        df = state.rcm_df

        # Find in cached results (case-insensitive)
        matched_idx = None
        for i, r in enumerate(results):
            if str(r.get("control_id", "")).strip().lower() == control_id.lower():
                matched_idx = i
                break

        if matched_idx is None:
            available = sorted(set(str(r.get("control_id", "")) for r in results))
            return ToolResult(
                success=False,
                data={"available_control_ids": available},
                error=(
                    f"Control ID '{control_id}' not found in sampling results. "
                    f"Available: {available}"
                ),
            )

        old_count = results[matched_idx].get("sample_count", 0)
        matched_cid = results[matched_idx]["control_id"]

        # Update cached results
        results[matched_idx]["sample_count"] = new_count
        results[matched_idx]["note"] = f"User override: {old_count} → {new_count}"
        state.sampling_results = results

        # Update RCM dataframe
        cid_col = None
        for col in df.columns:
            if col.lower().replace("_", " ").strip() in (
                "control id", "controlid", "control_id",
            ):
                cid_col = col
                break

        if cid_col and "count_of_samples" in df.columns:
            mask = df[cid_col].astype(str).str.strip().str.lower() == matched_cid.lower()
            df.loc[mask, "count_of_samples"] = new_count
            state.rcm_df = df

            # Re-export normalised RCM
            if state.output_dir:
                normalised_path = os.path.join(state.output_dir, "normalised_rcm.xlsx")
                try:
                    df.to_excel(normalised_path, index=False, engine="openpyxl")
                except Exception as e:
                    logger.warning("Failed to update normalised_rcm.xlsx: %s", e)

        return ToolResult(
            success=True,
            data=sanitize_for_json({
                "control_id": matched_cid,
                "old_sample_count": old_count,
                "new_sample_count": new_count,
                "total_controls": len(results),
                "total_samples": sum(r.get("sample_count", 0) or 0 for r in results),
                "message": (
                    f"Updated {matched_cid}: sample count {old_count} → {new_count}."
                ),
            }),
            summary=f"Updated {matched_cid} sample count: {old_count} → {new_count}",
        )
