"""Wrapper around AiSuggest.py engine — gap analysis for missing risks/controls."""

from __future__ import annotations

import json
import os
import logging
import threading
from importlib import reload
from typing import Any, Dict, List, Optional

# Serialises all AiSuggest calls so module-level globals aren't clobbered
# by concurrent requests.
_ENGINE_LOCK = threading.Lock()

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..config import get_config
from ..utils import sanitize_for_json, SUPPORTED_INDUSTRIES

logger = logging.getLogger("agent.tools.ai_suggest")


class AiSuggestTool(Tool):
    @property
    def name(self) -> str:
        return "run_ai_suggestions"

    @property
    def description(self) -> str:
        return (
            "Run AI gap analysis to identify missing risks and controls in the RCM. "
            "Generates suggestions based on industry best practices. "
            "Results are cached — call merge_suggestions afterwards to apply."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.ANALYSIS

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                "industry", "string",
                "Industry context for gap analysis",
                enum=SUPPORTED_INDUSTRIES,
            ),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if state.rcm_df is None:
            return "No RCM loaded. Use load_rcm first."
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        config = get_config()
        industry = args["industry"]
        rcm_df = state.rcm_df
        output_dir = state.output_dir

        # Save current RCM to temp file for the engine
        temp_rcm = os.path.join(output_dir, "_temp_rcm_for_aisuggest.xlsx")
        rcm_df.to_excel(temp_rcm, index=False, engine="openpyxl")

        logger.info("Running AI suggestions for %s (%d rows)", industry, len(rcm_df))

        output_excel = os.path.join(output_dir, "1_AiSuggest_Combined.xlsx")
        output_json = os.path.join(output_dir, "1_AiSuggest_Response.json")
        output_text = os.path.join(output_dir, "1_AiSuggest_Report.txt")

        # Serialise engine calls — module-level globals are not thread-safe
        with _ENGINE_LOCK:
            import AiSuggest
            reload(AiSuggest)
            AiSuggest.OPENAI_API_KEY = config.openai_api_key
            AiSuggest.OPENAI_MODEL = config.openai_model
            AiSuggest.AZURE_OPENAI_ENDPOINT = config.azure_openai_endpoint
            AiSuggest.AZURE_OPENAI_API_VERSION = config.azure_openai_api_version
            AiSuggest.RCM_FILE_PATH = temp_rcm
            AiSuggest.INDUSTRY = industry
            AiSuggest.OUTPUT_EXCEL = output_excel
            AiSuggest.OUTPUT_JSON = output_json
            AiSuggest.OUTPUT_TEXT = output_text
            AiSuggest.TEST_CONNECTION_FIRST = False
            AiSuggest.main()

        # Read output JSON
        json_path = output_json
        if not os.path.exists(json_path):
            return ToolResult(
                success=False, data={},
                error="AI suggestions engine did not produce output. Check logs.",
            )

        with open(json_path, "r") as f:
            results = json.load(f)
        results = sanitize_for_json(results)

        suggestions = results.get("suggestions", [])
        state.suggestions_cache = suggestions

        # Build numbered list for display
        numbered = []
        for i, s in enumerate(suggestions, 1):
            numbered.append({
                "#": i,
                "AI_Suggestion_ID": s.get("AI_Suggestion_ID", f"RCMAI-{i:03d}"),
                "AI_Priority": s.get("AI_Priority", ""),
                "AI_Category": s.get("AI_Category", ""),
                "Risk Title": s.get("Risk Title", ""),
                "Control Description": str(s.get("Control Description", ""))[:100],
                "AI_Reason": str(s.get("AI_Reason", ""))[:100],
            })

        logger.info("Generated %d suggestions", len(suggestions))

        # Upload artifacts to blob storage
        excel_blob = output_excel
        json_blob = json_path
        try:
            from server.blob_store import get_blob_store
            store = get_blob_store()
            if store.available:
                session_key = os.path.basename(output_dir) if output_dir else "default"
                for local, name in [(output_excel, "excel"), (json_path, "json")]:
                    if local and os.path.isfile(local):
                        bp = f"artifacts/{session_key}/{os.path.basename(local)}"
                        r = store.upload_file(local, bp)
                        if r:
                            if name == "excel": excel_blob = bp
                            else: json_blob = bp
        except Exception as exc:
            logger.warning("AI suggest blob upload failed (non-fatal): %s", exc)

        return ToolResult(
            success=True,
            data={
                "suggestion_count": len(suggestions),
                "executive_summary": results.get("executive_summary", ""),
                "gap_analysis": results.get("gap_analysis", ""),
                "suggestions": numbered,
                "output_excel": excel_blob,
            },
            artifacts=[excel_blob, json_blob],
            summary=f"Generated {len(suggestions)} AI suggestions for {industry}",
        )
