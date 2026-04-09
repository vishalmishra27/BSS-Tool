"""Upload a modified sampling Excel to override sample counts per control.

After the sampling engine runs, it populates count_of_samples in the RCM
and caches results in state.sampling_results.  The user can export the
normalised RCM, edit the count_of_samples column, and upload it back.
This tool reads the Excel and updates both the cached results and the RCM.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..utils import sanitize_for_json

logger = logging.getLogger("agent.tools.upload_sampling_overrides")


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


class UploadSamplingOverridesTool(Tool):
    """Upload a modified Excel to override sample counts after sampling."""

    @property
    def name(self) -> str:
        return "upload_sampling_overrides"

    @property
    def description(self) -> str:
        return (
            "Upload a modified Excel file to override the sample counts "
            "(count_of_samples) per control after the sampling engine has run. "
            "The Excel must have 'Control ID' and 'count_of_samples' columns. "
            "Updates both the cached sampling results and the RCM."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.UTILITY

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                "file_path", "string",
                "Path to the uploaded modified sampling Excel file.",
                required=True,
            ),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if state.sampling_results is None:
            return (
                "No sampling results to update. "
                "Run run_sampling_engine first."
            )
        if state.rcm_df is None:
            return "No RCM loaded."
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
            upload_df = pd.read_excel(resolved, engine="openpyxl")
        except Exception as exc:
            return ToolResult(
                success=False, data={},
                error=f"Failed to read Excel file: {exc}",
            )

        # Normalise column names
        col_map = {}
        for col in upload_df.columns:
            lower = str(col).strip().lower().replace("_", " ")
            if lower in ("control id", "controlid"):
                col_map[col] = "control_id"
            elif lower in ("count of samples", "countofsamples", "sample count",
                           "samplecount", "samples", "count of sample"):
                col_map[col] = "count_of_samples"

        upload_df = upload_df.rename(columns=col_map)

        if "control_id" not in upload_df.columns:
            return ToolResult(
                success=False, data={"found_columns": list(upload_df.columns)},
                error=(
                    "Missing required 'Control ID' column. "
                    "Found: " + str(list(upload_df.columns))
                ),
            )
        if "count_of_samples" not in upload_df.columns:
            return ToolResult(
                success=False, data={"found_columns": list(upload_df.columns)},
                error=(
                    "Missing required 'count_of_samples' (or 'Sample Count') column. "
                    "Found: " + str(list(upload_df.columns))
                ),
            )

        results = state.sampling_results
        df = state.rcm_df

        # Build lookup from cached results
        results_lookup = {}
        for i, r in enumerate(results):
            cid = str(r.get("control_id", "")).strip().lower()
            results_lookup[cid] = i

        # Find RCM control ID column
        cid_col = None
        for col in df.columns:
            if col.lower().replace("_", " ").strip() in (
                "control id", "controlid", "control_id",
            ):
                cid_col = col
                break

        updated_count = 0
        invalid_values = []

        for _, row in upload_df.iterrows():
            cid = str(row.get("control_id", "")).strip()
            raw_count = row.get("count_of_samples")

            if not cid or cid.lower() in ("nan", "none", "null", ""):
                continue

            # Parse count
            try:
                if raw_count is None or (isinstance(raw_count, float) and pd.isna(raw_count)):
                    continue
                new_count = int(float(str(raw_count).strip()))
                if new_count < 0:
                    raise ValueError
            except (ValueError, TypeError):
                invalid_values.append({"control_id": cid, "value": str(raw_count)})
                continue

            # Update cached results
            idx = results_lookup.get(cid.lower())
            if idx is not None:
                old_count = results[idx].get("sample_count", 0)
                if old_count != new_count:
                    results[idx]["sample_count"] = new_count
                    results[idx]["note"] = f"User override: {old_count} → {new_count}"
                    updated_count += 1

            # Update RCM dataframe
            if cid_col and "count_of_samples" in df.columns:
                mask = df[cid_col].astype(str).str.strip().str.lower() == cid.lower()
                if mask.any():
                    df.loc[mask, "count_of_samples"] = new_count

        state.sampling_results = results
        state.rcm_df = df

        # Re-export normalised RCM
        if state.output_dir:
            normalised_path = os.path.join(state.output_dir, "normalised_rcm.xlsx")
            try:
                df.to_excel(normalised_path, index=False, engine="openpyxl")
            except Exception as e:
                logger.warning("Failed to update normalised_rcm.xlsx: %s", e)

        total_samples = sum(r.get("sample_count", 0) or 0 for r in results)

        result_data = sanitize_for_json({
            "updated_count": updated_count,
            "total_controls": len(results),
            "total_samples": total_samples,
            "message": (
                f"Updated {updated_count} sample counts from uploaded Excel. "
                f"Total samples: {total_samples}."
            ),
        })

        if invalid_values:
            result_data["invalid_values"] = invalid_values
            result_data["_agent_notes"] = [
                f"WARNING: {len(invalid_values)} value(s) could not be parsed as integers."
            ]

        return ToolResult(
            success=True,
            data=result_data,
            summary=(
                f"Updated {updated_count} sample count overrides from uploaded Excel. "
                f"Total samples: {total_samples}."
                + (f" {len(invalid_values)} invalid values skipped." if invalid_values else "")
            ),
        )
