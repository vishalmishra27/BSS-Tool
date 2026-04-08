"""Upload a modified risk levels Excel to update pending risk level inferences.

After ``infer_risk_level`` runs, it exports an editable Excel with columns:
  Control ID | Description | Probability | Impact | Score |
  Risk Level | Source | Confidence | Flag

The user can download it, change the Probability, Impact, or Risk Level columns,
then upload it back.  This tool reads the Excel and updates the pending
inferences accordingly.
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

logger = logging.getLogger("agent.tools.upload_risk_level_overrides")


def _resolve_file_path(path: str) -> Optional[str]:
    """If path is a blob path, download to local cache. Otherwise return as-is."""
    try:
        from server.blob_store import get_blob_store, BlobStore
        if BlobStore.is_blob_path(path):
            store = get_blob_store()
            if not store.available:
                return None
            local = store.ensure_local_file(path)
            if local:
                logger.info("Downloaded blob file %s → %s", path, local)
                return local
            return None
    except Exception:
        pass
    return path


class UploadRiskLevelOverridesTool(Tool):
    """Upload a modified risk levels Excel to override pending inferences."""

    @property
    def name(self) -> str:
        return "upload_risk_level_overrides"

    @property
    def description(self) -> str:
        return (
            "Upload a modified inferred risk levels Excel file to update the pending "
            "risk level inferences. The user edits the Probability, Impact, or "
            "Risk Level columns in the exported Excel, then uploads it back. "
            "If Probability and Impact are both present, the Risk Level is "
            "auto-recomputed using the weighted scoring model. "
            "After uploading, the user should review and approve by calling "
            "infer_risk_level with apply=true."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.UTILITY

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                "file_path", "string",
                "Path to the uploaded modified risk levels Excel file.",
                required=True,
            ),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if not state.pending_risk_level_inferences:
            return (
                "No pending risk level inferences to update. "
                "Run infer_risk_level first."
            )
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        file_path = (args.get("file_path") or "").strip().strip("'\"")
        if not file_path:
            return ToolResult(success=False, data={}, error="No file path provided.")

        resolved = _resolve_file_path(file_path)
        if resolved is None:
            return ToolResult(
                success=False, data={},
                error=f"Could not access file: {file_path}",
            )
        if not os.path.exists(resolved):
            return ToolResult(
                success=False, data={"attempted_path": resolved},
                error=f"File not found at: {resolved}",
            )

        try:
            df = pd.read_excel(resolved, engine="openpyxl")
        except Exception as exc:
            return ToolResult(
                success=False, data={},
                error=f"Failed to read Excel file: {exc}",
            )

        # Normalise column names
        col_map = {}
        for col in df.columns:
            lower = str(col).strip().lower().replace("_", " ")
            if lower in ("control id", "controlid"):
                col_map[col] = "control_id"
            elif lower in ("probability", "risk probability"):
                col_map[col] = "probability"
            elif lower in ("impact", "risk impact"):
                col_map[col] = "impact"
            elif lower in ("risk level", "risklevel"):
                col_map[col] = "risk_level"

        df = df.rename(columns=col_map)

        if "control_id" not in df.columns:
            return ToolResult(
                success=False, data={"found_columns": list(df.columns)},
                error=(
                    "Missing required 'Control ID' column. "
                    "Found: " + str(list(df.columns))
                ),
            )

        has_prob_col = "probability" in df.columns
        has_impact_col = "impact" in df.columns
        has_level_col = "risk_level" in df.columns

        if not has_level_col and not (has_prob_col and has_impact_col):
            return ToolResult(
                success=False, data={"found_columns": list(df.columns)},
                error=(
                    "Need at least a 'Risk Level' column, or both 'Probability' and "
                    "'Impact' columns. Found: " + str(list(df.columns))
                ),
            )

        inferences = state.pending_risk_level_inferences
        _sm = getattr(state, "custom_risk_score_map", None)
        _bands = getattr(state, "custom_risk_bands", None)

        updated_count = 0
        invalid_values = []

        for _, row in df.iterrows():
            cid = str(row.get("control_id", "")).strip()
            if not cid or cid.lower() in ("nan", "none", "null", ""):
                continue
            if cid not in inferences:
                continue

            raw_prob = str(row.get("probability", "")).strip() if has_prob_col else ""
            raw_impact = str(row.get("impact", "")).strip() if has_impact_col else ""
            raw_level = str(row.get("risk_level", "")).strip() if has_level_col else ""

            norm_prob = _normalize_risk_input(raw_prob)
            norm_impact = _normalize_risk_input(raw_impact)

            # If both P and I are present, recompute risk level
            if norm_prob and norm_impact:
                score, level = compute_risk_score(norm_prob, norm_impact, _sm, _bands)
                if score > 0:
                    inferences[cid]["probability"] = norm_prob
                    inferences[cid]["impact"] = norm_impact
                    inferences[cid]["score"] = score
                    inferences[cid]["risk_level"] = level
                    inferences[cid]["source"] = "User Override"
                    inferences[cid]["confidence"] = "High"
                    inferences[cid]["flag"] = ""
                    updated_count += 1
                    continue

            # Else use provided risk_level directly
            if raw_level and raw_level.lower() not in ("nan", "none", "null", ""):
                norm_level = raw_level.strip()
                # Validate
                if norm_level not in VALID_OUTPUTS:
                    for vo in VALID_OUTPUTS:
                        if vo.lower() == norm_level.lower():
                            norm_level = vo
                            break
                    else:
                        invalid_values.append({"control_id": cid, "value": raw_level})
                        continue

                inferences[cid]["risk_level"] = norm_level
                if norm_prob:
                    inferences[cid]["probability"] = norm_prob
                if norm_impact:
                    inferences[cid]["impact"] = norm_impact
                inferences[cid]["source"] = "User Override"
                inferences[cid]["confidence"] = "High"
                inferences[cid]["flag"] = ""
                updated_count += 1

        state.pending_risk_level_inferences = inferences

        # Re-export updated Excel
        excel_path = None
        blob_path = None
        if state.output_dir:
            excel_path = os.path.join(state.output_dir, "inferred_risk_levels.xlsx")
            rows = []
            for cid_key in sorted(inferences.keys()):
                r = inferences[cid_key]
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
                blob_path = _upload_artifact(excel_path, state)
            except Exception as e:
                logger.warning("Failed to re-export risk levels Excel: %s", e)

        result_data = sanitize_for_json({
            "updated_count": updated_count,
            "total_pending": len(inferences),
            "requires_approval": True,
            "excel_path": excel_path,
            "excel_blob_path": blob_path,
        })

        if invalid_values:
            result_data["invalid_values"] = invalid_values
            result_data["valid_risk_levels"] = VALID_OUTPUTS
            result_data["_agent_notes"] = [
                f"WARNING: {len(invalid_values)} risk level value(s) were not recognized. "
                f"Valid values: {VALID_OUTPUTS}"
            ]

        return ToolResult(
            success=True,
            data=result_data,
            summary=(
                f"Updated {updated_count} risk level overrides from uploaded Excel. "
                f"{len(inferences)} total controls pending approval."
                + (f" {len(invalid_values)} invalid values skipped." if invalid_values else "")
            ),
        )
