"""Wrapper around ControlAssesment.py engine — OnGround Check."""

from __future__ import annotations

import json
import os
import logging
import threading
from importlib import reload
from typing import Any, Dict, List, Optional

# Serialises all ControlAssesment calls so module-level globals aren't
# clobbered by concurrent requests.
_ENGINE_LOCK = threading.Lock()

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..config import get_config
from ..utils import sanitize_for_json

logger = logging.getLogger("agent.tools.control_assessment")

SUPPORTED_POLICY_EXTS = {".pdf", ".docx", ".doc", ".txt"}
SUPPORTED_SOP_EXTS = {".pdf", ".docx", ".doc", ".txt"}


def _normalize_input_paths(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(v).strip().strip("'\"") for v in value if str(v).strip()]
    if isinstance(value, str):
        chunks = [c.strip().strip("'\"") for c in value.replace(";", "\n").splitlines()]
        return [c for c in chunks if c]
    return []


def _expand_paths(paths: List[str]) -> List[str]:
    expanded: List[str] = []
    for path in paths:
        if os.path.isdir(path):
            try:
                for name in sorted(os.listdir(path)):
                    full = os.path.join(path, name)
                    if os.path.isfile(full):
                        expanded.append(full)
            except Exception:
                continue
        else:
            expanded.append(path)
    return expanded


def _filter_existing_by_ext(paths: List[str], allowed_exts: set[str]) -> List[str]:
    valid: List[str] = []
    for p in paths:
        ext = os.path.splitext(str(p))[1].lower()
        if ext in allowed_exts and os.path.exists(p):
            valid.append(p)
    return valid


def _is_readable_file(path: str) -> bool:
    """Check that the file exists, is non-empty, and can be opened."""
    try:
        return os.path.isfile(path) and os.path.getsize(path) > 0
    except Exception:
        return False


class ControlAssessmentTool(Tool):
    @property
    def name(self) -> str:
        return "run_control_assessment"

    @property
    def description(self) -> str:
        return (
            "Run OnGround Check: validate RCM controls against policy and SOP documents. "
            "Checks whether each control is documented in the provided policies/SOPs and "
            "identifies gaps. Provide at least one policy or SOP file path."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.ANALYSIS

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                "policy_paths", "array",
                "List of absolute paths to policy PDF files",
                required=False,
                items={"type": "string"},
            ),
            ToolParameter(
                "sop_paths", "array",
                "List of absolute paths to SOP PDF files",
                required=False,
                items={"type": "string"},
            ),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if state.rcm_df is None:
            return "No RCM loaded. Use load_rcm first."
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        config = get_config()
        output_dir = state.output_dir

        raw_policy_paths = _normalize_input_paths(args.get("policy_paths"))
        raw_sop_paths = _normalize_input_paths(args.get("sop_paths"))
        policy_paths = _expand_paths(raw_policy_paths)
        sop_paths = _expand_paths(raw_sop_paths)

        # Fallback: if explicit args are missing, infer policy/SOP PDFs from known artifacts.
        # Artifacts may be blob paths — resolve them to local cache before checking existence.
        if not policy_paths and not sop_paths:
            from server.blob_store import get_blob_store, BlobStore
            _blob_store = get_blob_store()
            def _resolve(p):
                if BlobStore.is_blob_path(p) and _blob_store.available:
                    return _blob_store.ensure_local(p) or p
                return p
            inferred = [
                _resolve(p) for p in (state.artifacts or [])
                if isinstance(p, str) and (os.path.exists(p) or BlobStore.is_blob_path(p))
            ]
            for p in inferred:
                lower = os.path.basename(p).lower()
                if "policy" in lower:
                    policy_paths.append(p)
                elif "sop" in lower:
                    sop_paths.append(p)

        valid_policies = [
            p for p in _filter_existing_by_ext(policy_paths, SUPPORTED_POLICY_EXTS) if _is_readable_file(p)
        ]
        valid_sops = [p for p in _filter_existing_by_ext(sop_paths, SUPPORTED_SOP_EXTS) if _is_readable_file(p)]

        if not valid_policies and not valid_sops:
            provided = sorted(set(policy_paths + sop_paths))
            return ToolResult(
                success=False, data={},
                error=(
                    "No valid Policy/SOP files found. "
                    "Provide absolute file paths to PDF, DOCX, DOC, or TXT files (or a folder containing them). "
                    f"Received: {provided[:8]}"
                ),
            )

        # Save RCM to temp
        temp_rcm = os.path.join(output_dir, "_temp_rcm_for_ca.xlsx")
        state.rcm_df.to_excel(temp_rcm, index=False, engine="openpyxl")

        out_excel = os.path.join(output_dir, "2_ControlAssessment.xlsx")
        out_json = os.path.join(output_dir, "2_ControlAssessment.json")

        logger.info("Running control assessment: %d policies, %d SOPs",
                     len(valid_policies), len(valid_sops))

        # Serialise engine calls — module-level globals are not thread-safe
        with _ENGINE_LOCK:
            import ControlAssesment
            reload(ControlAssesment)
            ControlAssesment.Config.OPENAI_API_KEY = config.openai_api_key
            ControlAssesment.Config.OPENAI_MODEL = config.openai_model
            ControlAssesment.Config.AZURE_OPENAI_ENDPOINT = config.azure_openai_endpoint
            ControlAssesment.Config.AZURE_OPENAI_API_VERSION = config.azure_openai_api_version
            checker = ControlAssesment.OnGroundCheck(
                rcm_path=temp_rcm,
                policy_paths=valid_policies if valid_policies else None,
                sop_paths=valid_sops if valid_sops else None,
                out_excel=out_excel,
                out_json=out_json,
            )
            checker.run()

        # Read results
        if not os.path.exists(out_json):
            # Upload excel to blob even in this early-return path
            _early_blob = out_excel
            try:
                from server.blob_store import get_blob_store
                _s = get_blob_store()
                if _s.available and os.path.isfile(out_excel):
                    _sk = os.path.basename(state.output_dir) if state.output_dir else "default"
                    _r = _s.upload_file(out_excel, f"artifacts/{_sk}/{os.path.basename(out_excel)}")
                    if _r: _early_blob = _r
            except Exception:
                pass
            return ToolResult(
                success=True,
                data={"message": "Assessment complete but no JSON output. Check Excel.",
                      "output_excel": _early_blob},
                artifacts=[_early_blob],
                summary="Control assessment complete (no JSON output)",
            )

        with open(out_json, "r") as f:
            ca_results = json.load(f)
        ca_results = sanitize_for_json(ca_results)

        controls = ca_results.get("controls", [])
        summary = []
        for c in controls:
            summary.append({
                "control_id": c.get("control_id", ""),
                "policy_documented": c.get("policy_check", {}).get("documented", "N/A"),
                "match_pct": f"{c.get('match_pct', 0):.0f}%",
                "gaps": c.get("gaps", []),
            })

        logger.info("Assessed %d controls", len(controls))

        # Upload artifacts to blob storage
        excel_blob = out_excel
        json_blob = out_json
        try:
            from server.blob_store import get_blob_store
            store = get_blob_store()
            if store.available:
                session_key = os.path.basename(state.output_dir) if state.output_dir else "default"
                for local, name in [(out_excel, "excel"), (out_json, "json")]:
                    if local and os.path.isfile(local):
                        bp = f"artifacts/{session_key}/{os.path.basename(local)}"
                        r = store.upload_file(local, bp)
                        if r:
                            if name == "excel": excel_blob = bp
                            else: json_blob = bp
        except Exception as exc:
            logger.warning("Control assessment blob upload failed (non-fatal): %s", exc)

        return ToolResult(
            success=True,
            data={
                "controls_assessed": len(controls),
                "results": summary,
                "output_excel": excel_blob,
                "policies_used": len(valid_policies),
                "sops_used": len(valid_sops),
            },
            artifacts=[excel_blob, json_blob],
            summary=f"Assessed {len(controls)} controls against {len(valid_policies)} policies, {len(valid_sops)} SOPs",
        )
