"""Remove controls that failed TOD from the RCM."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult

logger = logging.getLogger("agent.tools.remove_failed_controls")


class RemoveFailedControlsTool(Tool):
    @property
    def name(self) -> str:
        return "remove_failed_tod_controls"

    @property
    def description(self) -> str:
        return (
            "Remove controls that FAILED the Test of Design (TOD) from the RCM. "
            "This creates a clean RCM containing only PASS controls, which can "
            "then be used directly for Sampling and TOE. Optionally saves the "
            "removed controls to a separate file for audit trail."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.DATA

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                "save_removed", "boolean",
                "If true, save the removed (failed) controls to a separate "
                "Excel file for audit trail. Defaults to true.",
                required=False,
            ),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if state.rcm_df is None:
            return "No RCM loaded. Use load_rcm first."
        if not state.tod_results:
            return "No TOD results available. Run run_test_of_design first."
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        import os

        save_removed = args.get("save_removed", True)
        rcm_df = state.rcm_df.copy()

        # Find the Control Id column
        control_id_col = None
        for col in rcm_df.columns:
            if col.lower().replace("_", " ").strip() in (
                "control id", "controlid", "control_id",
            ):
                control_id_col = col
                break

        if not control_id_col:
            return ToolResult(
                success=False, data={},
                error=(
                    f"Cannot find a 'Control Id' column in the RCM. "
                    f"Available columns: {list(rcm_df.columns)}"
                ),
            )

        # Extract failed control IDs from TOD results
        failed_ids = set()
        for r in state.tod_results:
            result_val = getattr(r, "result", None) or (r.get("result") if isinstance(r, dict) else None)
            control_id = getattr(r, "control_id", None) or (r.get("control_id") if isinstance(r, dict) else None)
            if result_val == "FAIL" and control_id:
                failed_ids.add(control_id.strip())

        if not failed_ids:
            return ToolResult(
                success=True,
                data={
                    "removed_count": 0,
                    "remaining_count": len(rcm_df),
                    "message": "No failed controls found in TOD results. RCM unchanged.",
                },
                summary="No failed controls to remove — all controls passed TOD.",
            )

        # Normalize for matching
        rcm_ids_normalized = rcm_df[control_id_col].astype(str).str.strip()
        failed_ids_upper = {fid.upper() for fid in failed_ids}
        mask_failed = rcm_ids_normalized.str.upper().isin(failed_ids_upper)

        removed_df = rcm_df[mask_failed].copy()
        kept_df = rcm_df[~mask_failed].reset_index(drop=True)

        removed_count = len(removed_df)
        remaining_count = len(kept_df)

        # Save removed controls for audit trail
        removed_file = None
        if save_removed and removed_count > 0:
            removed_file = os.path.join(
                state.output_dir, "TOD_Failed_Controls_Removed.xlsx"
            )
            removed_df.to_excel(removed_file, index=False, engine="openpyxl")
            logger.info("Saved %d removed controls to %s", removed_count, removed_file)
            # Upload to blob storage
            try:
                from server.blob_store import get_blob_store
                store = get_blob_store()
                if store.available:
                    session_key = os.path.basename(state.output_dir) if state.output_dir else "default"
                    blob_path = f"artifacts/{session_key}/{os.path.basename(removed_file)}"
                    result = store.upload_file(removed_file, blob_path)
                    if result:
                        removed_file = blob_path
                    else:
                        logger.warning("Blob upload failed for %s", removed_file)
            except Exception as exc:
                logger.warning("Blob upload error for %s: %s", removed_file, exc)

        # Update state with clean RCM
        state.rcm_df = kept_df

        # Build result
        removed_list = [
            {"control_id": cid, "result": "FAIL"}
            for cid in sorted(failed_ids)
            if cid.upper() in {r.upper() for r in rcm_ids_normalized[mask_failed]}
        ]

        data = {
            "removed_count": removed_count,
            "remaining_count": remaining_count,
            "removed_controls": removed_list,
            "failed_control_ids": sorted(failed_ids),
        }
        artifacts = []
        if removed_file:
            data["removed_file"] = removed_file
            artifacts.append(removed_file)

        logger.info(
            "Removed %d failed controls, %d remaining",
            removed_count, remaining_count,
        )

        return ToolResult(
            success=True,
            data=data,
            artifacts=artifacts,
            summary=(
                f"Removed {removed_count} failed controls from RCM. "
                f"{remaining_count} controls remaining (all PASS)."
            ),
        )
