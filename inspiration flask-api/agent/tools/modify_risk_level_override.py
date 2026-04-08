"""Chat-based tool to modify a single control's inferred risk level by control ID.

Allows the user to change the inferred risk level for a specific control via chat,
without downloading/uploading an Excel file.  Updates the pending inferences cache
so that when the user approves, the correct risk level is written to the RCM.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import Tool
from .infer_risk_level import (
    VALID_INPUTS, VALID_OUTPUTS, _normalize_risk_input,
    compute_risk_score, _upload_artifact,
)
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..utils import sanitize_for_json

logger = logging.getLogger("agent.tools.modify_risk_level_override")


class ModifyRiskLevelOverrideTool(Tool):
    """Modify the inferred risk level for a single control by its Control ID."""

    @property
    def name(self) -> str:
        return "modify_risk_level"

    @property
    def description(self) -> str:
        return (
            "Modify the inferred risk level, probability, or impact for one control "
            "by Control ID. Use this when the user wants to change a specific "
            "control's risk assessment via chat (e.g., 'change CC-01 risk level to "
            "High', 'set CC-01 probability to Medium and impact to High'). "
            "Valid risk levels: Low, Medium, High, Critical. "
            "Valid probability/impact values: Low, Medium, High."
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
                    "The new risk level. Must be one of: Low, Medium, High, Critical. "
                    "If probability and impact are also provided, risk_level is ignored "
                    "and re-computed from them."
                ),
                required=False,
            ),
            ToolParameter(
                "probability", "string",
                "The new risk probability. Must be one of: Low, Medium, High.",
                required=False,
            ),
            ToolParameter(
                "impact", "string",
                "The new risk impact. Must be one of: Low, Medium, High.",
                required=False,
            ),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if not state.pending_risk_level_inferences:
            return (
                "No pending risk level inferences to modify. "
                "Run infer_risk_level first."
            )
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        control_id = str(args.get("control_id", "")).strip()
        new_level = str(args.get("risk_level", "") or "").strip()
        new_prob = str(args.get("probability", "") or "").strip()
        new_impact = str(args.get("impact", "") or "").strip()

        if not control_id:
            return ToolResult(success=False, data={}, error="No control_id provided.")

        if not new_level and not new_prob and not new_impact:
            return ToolResult(
                success=False, data={},
                error="Provide at least one of: risk_level, probability, or impact.",
            )

        inferences = state.pending_risk_level_inferences

        # ── Resolve Control ID (case-insensitive) ────────────────────
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
                    f"Control ID '{control_id}' not found in pending risk inferences. "
                    f"Available Control IDs: {available}"
                ),
            )

        inf = inferences[matched_cid]
        old_level = inf.get("risk_level", "")
        old_prob = inf.get("probability", "")
        old_impact = inf.get("impact", "")

        # ── Validate and normalise inputs ────────────────────────────
        norm_prob = _normalize_risk_input(new_prob) if new_prob else ""
        norm_impact = _normalize_risk_input(new_impact) if new_impact else ""
        norm_level = ""

        if new_prob and not norm_prob:
            return ToolResult(
                success=False, data={},
                error=f"'{new_prob}' is not a valid probability. Use: {VALID_INPUTS}.",
            )
        if new_impact and not norm_impact:
            return ToolResult(
                success=False, data={},
                error=f"'{new_impact}' is not a valid impact. Use: {VALID_INPUTS}.",
            )

        # If P and I are both provided (or we can fill from existing), recompute
        final_prob = norm_prob or old_prob
        final_impact = norm_impact or old_impact

        _sm = getattr(state, "custom_risk_score_map", None)
        _bands = getattr(state, "custom_risk_bands", None)

        if final_prob and final_impact and (norm_prob or norm_impact):
            score, computed_level = compute_risk_score(
                final_prob, final_impact, _sm, _bands,
            )
            if score > 0:
                inf["probability"] = final_prob
                inf["impact"] = final_impact
                inf["score"] = score
                inf["risk_level"] = computed_level
                inf["source"] = "User Override"
                inf["confidence"] = "High"
                inf["flag"] = ""
            else:
                return ToolResult(
                    success=False, data={},
                    error=f"Could not compute risk score from P={final_prob}, I={final_impact}.",
                )
        elif new_level:
            norm_level = _normalize_risk_input(new_level)
            # Also accept "Critical" directly
            if not norm_level and new_level.strip().capitalize() in VALID_OUTPUTS:
                norm_level = new_level.strip().capitalize()
            if not norm_level:
                return ToolResult(
                    success=False, data={},
                    error=f"'{new_level}' is not a valid risk level. Use: {VALID_OUTPUTS}.",
                )
            inf["risk_level"] = norm_level
            inf["source"] = "User Override"
            inf["confidence"] = "High"
            inf["flag"] = ""
        else:
            # Only one of P/I provided, not enough to recompute
            if norm_prob:
                inf["probability"] = norm_prob
            if norm_impact:
                inf["impact"] = norm_impact
            inf["source"] = "User Override"
            inf["confidence"] = "High"
            inf["flag"] = ""

        state.pending_risk_level_inferences = inferences

        # Re-export updated Excel
        if state.output_dir:
            excel_path = os.path.join(state.output_dir, "inferred_risk_levels.xlsx")
            rows = []
            for cid in sorted(inferences.keys()):
                r = inferences[cid]
                rows.append({
                    "Control ID": r["control_id"],
                    "Description": r.get("description", ""),
                    "Probability": r.get("probability", ""),
                    "Impact": r.get("impact", ""),
                    "Score": r.get("score", ""),
                    "Risk Level": r["risk_level"],
                    "Source": r.get("source", ""),
                    "Confidence": r.get("confidence", ""),
                    "Flag": r.get("flag", "") or "-",
                })
            try:
                pd.DataFrame(rows).to_excel(excel_path, index=False, engine="openpyxl")
                state.risk_level_inference_excel_path = excel_path
                _upload_artifact(excel_path, state)
            except Exception as e:
                logger.warning("Failed to re-export risk levels Excel: %s", e)

        return ToolResult(
            success=True,
            data=sanitize_for_json({
                "control_id": matched_cid,
                "old_risk_level": old_level,
                "new_risk_level": inf["risk_level"],
                "probability": inf.get("probability", ""),
                "impact": inf.get("impact", ""),
                "score": inf.get("score", ""),
                "total_pending": len(inferences),
                "requires_approval": True,
                "message": (
                    f"Updated {matched_cid}: risk_level '{old_level}' → '{inf['risk_level']}'. "
                    f"{len(inferences)} controls still pending approval."
                ),
            }),
            summary=f"Updated {matched_cid} risk level: {old_level} → {inf['risk_level']}",
        )
