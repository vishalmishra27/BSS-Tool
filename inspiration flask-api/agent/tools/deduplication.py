"""Wrapper around DeDupli.py engine — semantic duplicate detection."""

from __future__ import annotations

import json
import os
import logging
import threading
from importlib import reload
from typing import Any, Dict, List, Optional

# Serialises all DeDupli calls so module-level globals aren't clobbered
# by concurrent requests.
_ENGINE_LOCK = threading.Lock()

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..config import get_config
from ..utils import sanitize_for_json

logger = logging.getLogger("agent.tools.deduplication")


class DeduplicationTool(Tool):
    @property
    def name(self) -> str:
        return "run_deduplication"

    @property
    def description(self) -> str:
        return (
            "Analyze the RCM for semantically duplicate risks and controls. "
            "Uses AI to compare entries within each process group. "
            "Results are cached — call remove_duplicates afterwards to apply."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.ANALYSIS

    @property
    def parameters(self) -> List[ToolParameter]:
        return []  # No user-facing parameters

    def preconditions(self, state: AgentState) -> Optional[str]:
        if state.rcm_df is None:
            return "No RCM loaded. Use load_rcm first."
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        config = get_config()
        output_dir = state.output_dir
        rcm_df = state.rcm_df

        # Save RCM to temp
        temp_rcm = os.path.join(output_dir, "_temp_rcm_for_dedup.xlsx")
        rcm_df.to_excel(temp_rcm, index=False, engine="openpyxl")

        logger.info("Running deduplication on %d rows", len(rcm_df))

        # Serialise engine calls — module-level globals are not thread-safe
        with _ENGINE_LOCK:
            import DeDupli
            reload(DeDupli)
            DeDupli.OPENAI_API_KEY = config.openai_api_key
            DeDupli.OPENAI_MODEL = config.openai_model
            DeDupli.AZURE_OPENAI_ENDPOINT = config.azure_openai_endpoint
            DeDupli.AZURE_OPENAI_API_VERSION = config.azure_openai_api_version
            DeDupli.RCM_INPUT = temp_rcm
            DeDupli.INPUT_IS_FOLDER = False
            DeDupli.OUTPUT_FOLDER = output_dir
            DeDupli.OUTPUT_EXCEL_NAME = "3_Dedup_Pairs"
            DeDupli.OUTPUT_JSON_NAME = "3_Dedup_Results"
            DeDupli.main()

        # Read results
        json_path = os.path.join(output_dir, "3_Dedup_Results.json")
        if not os.path.exists(json_path):
            return ToolResult(
                success=True,
                data={"pair_count": 0, "message": "No duplicates found. RCM is clean."},
                summary="No duplicate pairs found",
            )

        with open(json_path, "r") as f:
            dedup_results = json.load(f)
        dedup_results = sanitize_for_json(dedup_results)

        # Collect all pairs with process context
        all_pairs: List[Dict] = []
        results_by_process = dedup_results.get("results_by_process", {})
        for process_name, pdata in results_by_process.items():
            for pair in pdata.get("pairs", []):
                pair["_process"] = process_name
                all_pairs.append(pair)

        # Resolve local process-group indices to absolute DataFrame indices NOW,
        # while the RCM is still in the exact state dedup was run against.
        # This prevents remove_duplicates from hitting the wrong rows if the
        # RCM is modified between dedup and removal.
        if "Process" in rcm_df.columns:
            for pair in all_pairs:
                process_name = pair.get("_process", "")
                group_indices = rcm_df[rcm_df["Process"] == process_name].index.tolist()
                local_a = pair.get("row_a")
                local_b = pair.get("row_b")
                if isinstance(local_a, int) and local_a < len(group_indices):
                    pair["global_row_a"] = group_indices[local_a]
                if isinstance(local_b, int) and local_b < len(group_indices):
                    pair["global_row_b"] = group_indices[local_b]

        state.dedup_cache = {
            "pairs": all_pairs,
            "results_by_process": results_by_process,
        }

        # Build numbered pair list for display
        numbered = []
        for i, p in enumerate(all_pairs, 1):
            numbered.append({
                "#": i,
                "process": p.get("_process", ""),
                "row_a": p.get("row_a", "?"),
                "row_a_risk": str(p.get("row_a_risk", ""))[:60],
                "row_b": p.get("row_b", "?"),
                "row_b_risk": str(p.get("row_b_risk", ""))[:60],
                "confidence": p.get("confidence", ""),
                "reasoning": str(p.get("reasoning", ""))[:100],
                "recommendation": str(p.get("recommendation", ""))[:80],
            })

        excel_path = os.path.join(output_dir, "3_Dedup_Pairs.xlsx")
        logger.info("Found %d duplicate pairs", len(all_pairs))

        # Upload artifacts to blob storage
        excel_blob = excel_path
        json_blob = json_path
        try:
            from server.blob_store import get_blob_store
            store = get_blob_store()
            if store.available:
                session_key = os.path.basename(output_dir) if output_dir else "default"
                for local, name in [(excel_path, "excel"), (json_path, "json")]:
                    if local and os.path.isfile(local):
                        bp = f"artifacts/{session_key}/{os.path.basename(local)}"
                        r = store.upload_file(local, bp)
                        if r:
                            if name == "excel": excel_blob = bp
                            else: json_blob = bp
        except Exception as exc:
            logger.warning("Dedup blob upload failed (non-fatal): %s", exc)

        return ToolResult(
            success=True,
            data={
                "pair_count": len(all_pairs),
                "pairs": numbered,
                "summary": dedup_results.get("summary", {}),
                "output_excel": excel_blob,
            },
            artifacts=[excel_blob, json_blob],
            summary=f"Found {len(all_pairs)} duplicate pairs",
        )
