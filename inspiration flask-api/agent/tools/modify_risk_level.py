"""Chat-based tool to modify a single control's risk level by control ID.

Allows the user to change the risk level for a specific control via chat.
Updates the RCM dataframe in agent state so subsequent pipeline runs use the
new value.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..utils import sanitize_for_json

logger = logging.getLogger("agent.tools.modify_risk_level")

VALID_RISK_LEVELS = ["Low", "Medium", "High", "Critical"]

_SHORTHAND = {
    "low": "Low",
    "med": "Medium",
    "medium": "Medium",
    "moderate": "Medium",
    "high": "High",
    "critical": "Critical",
    "very high": "Critical",
}


class ModifyRiskLevelTool(Tool):
    """Modify the risk level for a single control by its Control ID."""

    @property
    def name(self) -> str:
        return "modify_risk_level"

    @property
    def description(self) -> str:
        return (
            "Modify the risk level for one or more controls by Control ID. "
            "Use this when the user wants to change a specific control's risk level "
            "via chat (e.g., 'change CC-01 risk level to High'). "
            f"Valid risk levels: {', '.join(VALID_RISK_LEVELS)}."
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
                "risk_level", "string",
                (
                    "The new risk level. Must be one of: "
                    "'Low', 'Medium', 'High', 'Critical'."
                ),
                required=True,
            ),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if state.rcm_df is None:
            return (
                "No RCM loaded yet. Upload or load an RCM file first."
            )
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        control_id = str(args.get("control_id", "")).strip()
        new_level = str(args.get("risk_level", "")).strip()

        if not control_id:
            return ToolResult(success=False, data={}, error="No control_id provided.")
        if not new_level:
            return ToolResult(success=False, data={}, error="No risk_level provided.")

        # ── Resolve risk level (case-insensitive) ────────────────────
        resolved = _SHORTHAND.get(new_level.lower())
        if not resolved:
            return ToolResult(
                success=False,
                data={"valid_levels": VALID_RISK_LEVELS},
                error=f"'{new_level}' is not a valid risk level. Valid: {VALID_RISK_LEVELS}.",
            )

        # ── Find the control in the RCM dataframe ────────────────────
        df = state.rcm_df
        if "control_id" not in df.columns:
            return ToolResult(
                success=False, data={},
                error="RCM dataframe has no 'control_id' column.",
            )

        mask = df["control_id"].astype(str).str.strip().str.upper() == control_id.upper()
        if not mask.any():
            available = sorted(df["control_id"].dropna().unique().tolist())
            return ToolResult(
                success=False,
                data={"available_control_ids": available[:50]},
                error=f"Control ID '{control_id}' not found in RCM. Available: {available[:20]}",
            )

        # ── Apply the change ─────────────────────────────────────────
        old_level = df.loc[mask, "risk_level"].iloc[0] if "risk_level" in df.columns else "N/A"
        df.loc[mask, "risk_level"] = resolved

        # Track override in state
        if state.risk_level_overrides is None:
            state.risk_level_overrides = {}
        matched_cid = df.loc[mask, "control_id"].iloc[0]
        state.risk_level_overrides[matched_cid] = resolved

        # Update pending risk level inferences if they exist (review flow)
        if state.pending_risk_level_inferences and matched_cid in state.pending_risk_level_inferences:
            state.pending_risk_level_inferences[matched_cid]["risk_level"] = resolved
            state.pending_risk_level_inferences[matched_cid]["source"] = "User Override"
            state.pending_risk_level_inferences[matched_cid]["confidence"] = "High"
            state.pending_risk_level_inferences[matched_cid]["flag"] = ""

        return ToolResult(
            success=True,
            data=sanitize_for_json({
                "control_id": matched_cid,
                "old_risk_level": str(old_level),
                "new_risk_level": resolved,
                "message": f"Updated {matched_cid}: risk level '{old_level}' → '{resolved}'.",
            }),
            summary=f"Updated {matched_cid} risk level: {old_level} → {resolved}",
        )
