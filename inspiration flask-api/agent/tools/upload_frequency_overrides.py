"""Upload a modified frequencies Excel to update pending frequency inferences.

After ``infer_control_frequency`` runs, it exports an editable Excel with columns:
  Control ID | Control Description | Current Frequency |
  Inferred Frequency (KPMG) | Business Frequency | Source | Confidence

The user can download it, change the "Inferred Frequency (KPMG)" column to any
valid KPMG frequency, then upload it back.  This tool reads the Excel and updates
the pending inferences accordingly.
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

logger = logging.getLogger("agent.tools.upload_frequency_overrides")


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


class UploadFrequencyOverridesTool(Tool):
    """Upload a modified frequencies Excel to override pending inferences."""

    @property
    def name(self) -> str:
        return "upload_frequency_overrides"

    @property
    def description(self) -> str:
        return (
            "Upload a modified inferred frequencies Excel file to update the pending "
            "frequency inferences. The user edits the 'Inferred Frequency (KPMG)' column "
            "in the exported Excel, then uploads it back. This tool reads the Excel, "
            "validates the frequency values, and updates the pending inferences. "
            "After uploading, the user should review the updated list and approve "
            "by calling infer_control_frequency with apply=true."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.UTILITY

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                "file_path", "string",
                "Path to the uploaded modified frequencies Excel file.",
                required=True,
            ),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if not state.pending_frequency_inferences:
            return (
                "No pending frequency inferences to update. "
                "Run infer_control_frequency first."
            )
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        file_path = (args.get("file_path") or "").strip().strip("'\"")
        if not file_path:
            return ToolResult(
                success=False, data={},
                error="No file path provided.",
            )

        # Resolve blob paths
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

        # Read Excel
        try:
            df = pd.read_excel(resolved, engine="openpyxl")
        except Exception as exc:
            return ToolResult(
                success=False, data={},
                error=f"Failed to read Excel file: {exc}",
            )

        # Normalize column names
        col_map = {}
        for col in df.columns:
            lower = str(col).strip().lower()
            if lower in ("control id", "control_id", "controlid"):
                col_map[col] = "control_id"
            elif "inferred" in lower and "kpmg" in lower:
                col_map[col] = "inferred_frequency"
            elif "inferred" in lower and "frequency" in lower:
                col_map[col] = "inferred_frequency"
            elif lower in ("business frequency", "business_frequency"):
                col_map[col] = "business_frequency"

        df = df.rename(columns=col_map)

        if "control_id" not in df.columns or "inferred_frequency" not in df.columns:
            return ToolResult(
                success=False, data={"found_columns": list(df.columns)},
                error=(
                    "Missing required columns. Expected: 'Control ID' and "
                    "'Inferred Frequency (KPMG)'. Found: " + str(list(df.columns))
                ),
            )

        # Process each row and update pending inferences
        inferences = state.pending_frequency_inferences
        updated_count = 0
        invalid_frequencies = []

        for _, row in df.iterrows():
            cid = str(row.get("control_id", "")).strip()
            new_freq = str(row.get("inferred_frequency", "")).strip()
            biz_freq = str(row.get("business_frequency", "")).strip() if "business_frequency" in df.columns else ""

            if not cid or cid.lower() in ("nan", "none", "null", ""):
                continue

            if cid not in inferences:
                continue

            if not new_freq or new_freq.lower() in ("nan", "none", "null", ""):
                continue

            # Validate frequency is a valid KPMG term
            if new_freq not in KPMG_FREQUENCIES:
                # Try case-insensitive matching
                matched = False
                for canonical in KPMG_FREQUENCIES:
                    if canonical.lower() == new_freq.lower():
                        new_freq = canonical
                        matched = True
                        break
                if not matched:
                    # Try partial matching
                    for canonical in KPMG_FREQUENCIES:
                        if new_freq.lower() in canonical.lower() or canonical.lower().startswith(new_freq.lower()):
                            new_freq = canonical
                            matched = True
                            break
                if not matched:
                    invalid_frequencies.append({
                        "control_id": cid,
                        "value": new_freq,
                    })
                    continue

            inferences[cid]["inferred_frequency"] = new_freq
            if biz_freq and biz_freq.lower() not in ("nan", "none", "null"):
                inferences[cid]["business_frequency"] = biz_freq
            inferences[cid]["source"] = "User Override"
            inferences[cid]["confidence"] = "High"
            updated_count += 1

        state.pending_frequency_inferences = inferences

        # Re-export updated Excel
        excel_path = None
        blob_path = None
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
                blob_path = _upload_artifact(excel_path, state)
            except Exception as e:
                logger.warning("Failed to re-export frequencies Excel: %s", e)

        # Build display
        display_results = []
        for cid in sorted(inferences.keys()):
            inf = inferences[cid]
            display_results.append({
                "control_id": inf["control_id"],
                "inferred_frequency": inf["inferred_frequency"],
                "business_frequency": inf.get("business_frequency", ""),
                "source": inf["source"],
                "confidence": inf["confidence"],
            })

        result_data = sanitize_for_json({
            "updated_count": updated_count,
            "total_pending": len(inferences),
            "inferred_frequencies": display_results,
            "requires_approval": True,
            "excel_path": excel_path,
            "excel_blob_path": blob_path,
        })

        if invalid_frequencies:
            result_data["invalid_frequencies"] = invalid_frequencies
            result_data["valid_frequencies"] = KPMG_FREQUENCIES
            result_data["_agent_notes"] = [
                f"WARNING: {len(invalid_frequencies)} frequency value(s) were not recognized "
                f"as valid KPMG terms and were skipped. Valid values are: {KPMG_FREQUENCIES}"
            ]

        return ToolResult(
            success=True,
            data=result_data,
            summary=(
                f"Updated {updated_count} frequency overrides from uploaded Excel. "
                f"{len(inferences)} controls pending approval."
            ),
        )
