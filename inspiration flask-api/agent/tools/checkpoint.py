"""Tools for saving RCM checkpoints and managing the plan scratchpad.

Checkpoints are saved to a local temp directory for engine use, then uploaded exclusively to Azure Blob Storage as the sole persistent store.
"""

from __future__ import annotations

import os
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult

logger = logging.getLogger("agent.tools.checkpoint")


def _upload_artifact(local_path: str, state: Optional["AgentState"] = None) -> str:
    """Upload a generated artifact to blob storage. Returns blob path, or local path as fallback."""
    try:
        from server.blob_store import get_blob_store
        store = get_blob_store()
        if not store.available:
            logger.warning("Blob Storage not available — artifact stays local: %s", local_path)
            return local_path
        filename = os.path.basename(local_path)
        session_key = "default"
        if state and state.output_dir:
            session_key = os.path.basename(state.output_dir)
        blob_path = f"artifacts/{session_key}/{filename}"
        result = store.upload_file(local_path, blob_path)
        if not result:
            logger.warning("Blob upload failed for %s — using local path", local_path)
            return local_path
        return result
    except Exception as exc:
        logger.warning("Blob upload error for %s: %s — using local path", local_path, exc)
        return local_path


# ═══════════════════════════════════════════════════════════════════════════
# save_excel
# ═══════════════════════════════════════════════════════════════════════════

class SaveExcelTool(Tool):
    @property
    def name(self) -> str:
        return "save_excel"

    @property
    def description(self) -> str:
        return "Save the current RCM state to an Excel checkpoint file."

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.UTILITY

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter("step_name", "string", "Checkpoint name, e.g. '1_ai_suggestions'"),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if state.rcm_df is None:
            return "No RCM loaded."
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        step_name = args["step_name"]
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        # output_dir is normally set by load_rcm, but the RCM could have been
        # created via execute_python without going through that path.
        if state.output_dir is None:
            import tempfile
            state.output_dir = tempfile.mkdtemp(prefix="sox_agent_")
            os.makedirs(state.output_dir, exist_ok=True)
            logger.warning("output_dir was None — created temp directory: %s", state.output_dir)

        # Increment version counter
        state.version_count += 1
        version_tag = f"v{state.version_count}"
        filename = f"{step_name}_{version_tag}_{ts}.xlsx"
        path = os.path.join(state.output_dir, filename)

        state.rcm_df.to_excel(path, index=False, engine="openpyxl")
        rows, cols = len(state.rcm_df), len(state.rcm_df.columns)
        logger.info("Saved checkpoint %s: %s (%d rows x %d cols)", version_tag, path, rows, cols)

        # Upload to Azure Blob Storage
        blob_path = _upload_artifact(path, state)
        logger.info("Checkpoint uploaded to blob: %s", blob_path)
        state.artifacts.append(blob_path or path)
        state.last_save_path = blob_path or path

        return ToolResult(
            success=True,
            data={
                "path": blob_path or path, "rows": rows, "columns": cols,
                "version": state.version_count,
                "version_tag": version_tag,
                "blob_path": blob_path,
            },
            artifacts=[blob_path or path],
            summary=f"Saved {version_tag}: {blob_path or path}",
        )


# ═══════════════════════════════════════════════════════════════════════════
# update_plan
# ═══════════════════════════════════════════════════════════════════════════

class UpdatePlanTool(Tool):
    """
    The plan is a free-form scratchpad the LLM maintains for itself.

    It is stored in ``state.plan_scratchpad`` and shown in the system prompt
    context.  The code never iterates through it or enforces step order.
    """

    @property
    def name(self) -> str:
        return "update_plan"

    @property
    def description(self) -> str:
        return (
            "Create or update your working plan. Use this to organise multi-step "
            "goals. Write markdown with checkboxes for tracking. "
            "The plan is YOUR scratchpad — the system will show it back to you each turn."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.UTILITY

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                "plan_text", "string",
                "Your plan as markdown text (use checkboxes: - [ ] / - [x])",
            ),
        ]

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        state.plan_scratchpad = args["plan_text"]
        logger.info("Plan scratchpad updated")
        return ToolResult(
            success=True,
            data={"plan": args["plan_text"]},
            summary="Working plan updated",
        )
