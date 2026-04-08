"""Wrapper around TOD_Engine.py — Test of Design (TOD).

Uses the same engine architecture as TOE (same schema generation, evaluation
pipeline, and self-correction loop). The only difference: TOD uses 1 sample
per control. Schemas generated during TOD are cached in AgentState so TOE
can reuse them later.

Three-phase execution:
  Phase 1 (no evidence folder): Generate Required Documents list from approved
           attributes → return the list and ask user for evidence folder.
  Phase 2 (evidence folder provided): Validate evidence against Required Documents
           using embeddings; return match report if documents missing.
  Phase 3 (all matched or skip_validation=true): Run the actual TOD engine.

Supports Azure Blob Storage paths for evidence folders and uploads
generated workpapers back to blob.
"""

from __future__ import annotations

import logging
import sys
import os
from importlib import reload
from typing import Any, Dict, List, Optional

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..config import get_config
from ..core.progress import update_tool_progress, clear_tool_progress

logger = logging.getLogger("agent.tools.test_of_design")


def _resolve_evidence_folder(path: str) -> Optional[str]:
    """If path is a blob path, download the evidence folder to local cache."""
    from server.blob_store import get_blob_store, BlobStore
    if BlobStore.is_blob_path(path):
        store = get_blob_store()
        if not store.available:
            logger.error("Azure Blob Storage is not available")
            return None
        local = store.ensure_local_directory(path)
        if local:
            logger.info("Downloaded blob evidence folder %s → %s", path, local)
            return local
        return None
    return path


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
        if state and getattr(state, "output_dir", None):
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

_TOOL_NAME = "run_test_of_design"


class TestOfDesignTool(Tool):
    @property
    def name(self) -> str:
        return "run_test_of_design"

    @property
    def description(self) -> str:
        return (
            "Run Test of Design (TOD) — this is NOT Test of Effectiveness (TOE). "
            "TOD evaluates whether controls are adequately DESIGNED to mitigate risks "
            "(1 sample per control, PASS/FAIL). Use this when the user says 'TOD', "
            "'test of design', or 'design test'. Do NOT use run_test_of_effectiveness "
            "or preview_toe_attributes for TOD. "
            "When called WITHOUT an evidence folder, it auto-generates the Required "
            "Documents list and asks the user for the evidence folder path. "
            "When called WITH an evidence folder, it runs the full TOD test."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.ANALYSIS

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter("evidence_folder", "string",
                          "Absolute path to the evidence folder. "
                          "If you do not have the path, call this tool WITHOUT "
                          "this parameter — it will generate the Required Documents "
                          "list first, then ask the user for the evidence folder.",
                          required=False),
            ToolParameter("skip_validation", "boolean",
                          "Set to true to skip evidence validation and proceed directly "
                          "to the TOD test. Only use when the user explicitly says to "
                          "skip or continue despite missing documents.",
                          required=False, default=False),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if state.rcm_df is None:
            return "No RCM loaded. Use load_rcm first."
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        config = get_config()
        evidence_folder = (args.get("evidence_folder") or "").strip().strip("'\"")
        # Fallback to cached evidence folder from Phase 2 (e.g. when user
        # calls with skip_validation=true without re-providing the path)
        if not evidence_folder and state.pending_tod_evidence_folder:
            evidence_folder = state.pending_tod_evidence_folder
            logger.info("Using cached evidence folder from Phase 2: %s", evidence_folder)
        output_dir = state.output_dir

        # ── Guard: require attribute preview/approval ──────────────────
        if not state.pending_tod_schemas:
            return ToolResult(
                success=False,
                data={
                    "needs_preview": True,
                    "evidence_folder": evidence_folder or None,
                    "message": (
                        "TOD attributes have not been previewed yet. "
                        "Run preview_tod_attributes first so the user can review "
                        "and approve the testing attributes before running TOD."
                    ),
                },
                error=(
                    "Attributes not previewed. Call preview_tod_attributes first, "
                    "wait for user approval, then call run_test_of_design."
                ),
            )

        # ── Phase 1: No evidence folder → generate doc list, ask for evidence ──
        if not evidence_folder:
            # Ensure output directory
            if output_dir is None:
                import tempfile
                state.output_dir = tempfile.mkdtemp(prefix="sox_agent_")
                os.makedirs(state.output_dir, exist_ok=True)
                output_dir = state.output_dir

            doc_list_result = None
            try:
                from .document_list import generate_document_list_excel
                logger.info(
                    "Phase 1: Generating Required Documents list for %d controls...",
                    len(state.pending_tod_schemas),
                )
                doc_list_result = generate_document_list_excel(
                    schemas=state.pending_tod_schemas,
                    rcm_df=state.rcm_df,
                    output_dir=output_dir,
                    phase="TOD",
                    state=state,
                )
                # Cache rows for evidence validation in Phase 2
                if doc_list_result and doc_list_result.get("rows"):
                    state.document_list_rows = doc_list_result["rows"]
                    state.evidence_validated = False
                logger.info(
                    "Document list generated: %d controls, %d documents → %s",
                    doc_list_result.get("controls_processed", 0),
                    doc_list_result.get("total_documents", 0),
                    doc_list_result.get("output_excel", ""),
                )
            except Exception as exc:
                logger.warning("Document list generation failed (non-fatal): %s", exc)

            return ToolResult(
                success=True,
                data={
                    "phase": "document_list",
                    "document_list": doc_list_result,
                    "awaiting_evidence_folder": True,
                    "message": (
                        "Required Documents list has been generated. "
                        "Now ASK the user for the TOD evidence folder path. "
                        "The folder should contain subfolders named by Control ID "
                        "(e.g. C-P2P-001/) with evidence files inside. "
                        "Do NOT guess or invent a path — WAIT for the user."
                    ),
                },
                artifacts=(
                    [doc_list_result["output_excel"]]
                    if doc_list_result and doc_list_result.get("output_excel")
                    else []
                ),
                summary=(
                    f"Required Documents list generated"
                    + (
                        f" ({doc_list_result['total_documents']} documents "
                        f"for {doc_list_result['controls_processed']} controls)"
                        if doc_list_result
                        else ""
                    )
                    + ". Awaiting evidence folder path from user."
                ),
            )

        # ── Evidence folder provided ──────────────────────────────────────
        skip_validation = bool(args.get("skip_validation", False))

        # Resolve blob paths to local cache
        evidence_folder = _resolve_evidence_folder(evidence_folder)
        if evidence_folder is None:
            return ToolResult(success=False, data={},
                              error="Failed to download evidence folder from blob storage.")

        if not os.path.exists(evidence_folder):
            return ToolResult(success=False, data={
                "ask_for_path": True,
                "attempted_path": evidence_folder,
            },
                              error=(
                                  f"Evidence folder not found at: {evidence_folder}. "
                                  "Please ask the user to provide the correct path to their "
                                  "TOD evidence folder. The folder should contain subfolders "
                                  "named by Control ID (e.g. C-P2P-001/) with evidence files inside."
                              ))

        # ── Phase 2: Evidence validation (compare evidence vs Required Documents) ──
        # If the user provided a DIFFERENT evidence folder than what was validated,
        # reset the validation flag so Phase 2 re-runs on the new folder.
        if (state.evidence_validated
                and state.pending_tod_evidence_folder
                and evidence_folder != state.pending_tod_evidence_folder):
            logger.info(
                "Evidence folder changed (%s → %s) — resetting validation",
                state.pending_tod_evidence_folder, evidence_folder,
            )
            state.evidence_validated = False

        if not skip_validation and not state.evidence_validated and state.document_list_rows:
            try:
                from .evidence_validator import validate_evidence_against_documents
                logger.info("Phase 2: Validating evidence against Required Documents list...")

                control_ids = list(state.pending_tod_schemas.keys()) if state.pending_tod_schemas else []
                validation = validate_evidence_against_documents(
                    evidence_folder=evidence_folder,
                    document_list_rows=state.document_list_rows,
                    control_ids=control_ids,
                    api_key=config.openai_api_key,
                    endpoint=config.azure_openai_endpoint,
                    api_version=config.azure_openai_api_version,
                    embedding_model=config.azure_openai_embedding_deployment,
                    llm_model=config.openai_model,
                )

                total_missing = validation.get("total_missing", 0)
                total_required = validation.get("total_required", 0)
                total_matched = validation.get("total_matched", 0)

                logger.info(
                    "Evidence validation: %d/%d matched, %d missing",
                    total_matched, total_required, total_missing,
                )

                # Cache the evidence folder so the user doesn't have to provide it again
                state.pending_tod_evidence_folder = evidence_folder

                # Cache extraction results so Phase 3 skips re-extraction via
                # Document Intelligence — embeddings/content extracted ONCE here.
                if validation.get("extract_cache"):
                    state.evidence_extract_cache = validation["extract_cache"]
                    logger.info(
                        "Cached %d file extractions for Phase 3 reuse",
                        len(state.evidence_extract_cache),
                    )

                # Export validation report as Excel
                validation_excel = None
                try:
                    from .evidence_validator import export_validation_excel
                    validation_excel = export_validation_excel(
                        validation_controls=validation.get("controls", []),
                        output_dir=output_dir or state.output_dir,
                        phase="TOD",
                        state=state,
                    )
                except Exception as _vex:
                    logger.warning("Validation Excel export failed (non-fatal): %s", _vex)

                if total_missing > 0:
                    # Return validation report — user must decide to re-upload or skip
                    result_data = {
                        "phase": "evidence_validation",
                        "validation": validation.get("controls", []),
                        "total_required": total_required,
                        "total_matched": total_matched,
                        "total_missing": total_missing,
                        "evidence_folder": evidence_folder,
                        "message": (
                            f"Evidence validation found {total_matched}/{total_required} "
                            f"required documents matched. {total_missing} document(s) are "
                            "missing. The user can:\n"
                            "1. Re-upload the evidence folder with the missing documents "
                            "and call run_test_of_design again with the new path.\n"
                            "2. Skip validation and continue with the TOD test by calling "
                            "run_test_of_design with skip_validation=true."
                        ),
                    }
                    if validation_excel:
                        result_data["validation_excel"] = validation_excel
                    return ToolResult(
                        success=True,
                        data=result_data,
                        artifacts=[validation_excel] if validation_excel else [],
                        summary=(
                            f"Evidence validation: {total_matched}/{total_required} documents matched, "
                            f"{total_missing} missing. Awaiting user decision."
                        ),
                    )
                else:
                    # All documents matched — proceed automatically
                    state.evidence_validated = True
                    logger.info("All required documents matched — proceeding to TOD test")

            except Exception as exc:
                logger.warning("Evidence validation failed (non-fatal, proceeding): %s", exc)
                state.evidence_validated = True
        else:
            if skip_validation:
                logger.info("Evidence validation skipped by user request")
            state.evidence_validated = True

        # ── Phase 3: Run the actual TOD test ──────────────────────────────

        rcm_df = state.rcm_df.copy()

        # Auto-add missing required columns
        if "risk_level" not in rcm_df.columns:
            rcm_df["risk_level"] = "High"
            logger.info("Auto-added 'risk_level' column with default 'High'")

        # Normalize Control Id values for case-insensitive matching
        if "Control Id" in rcm_df.columns:
            rcm_df["Control Id"] = rcm_df["Control Id"].astype(str).str.strip()

        logger.info("Loading evidence from %s", evidence_folder)

        # ── Import new TOD engine ──
        engines_dir = os.path.join(os.path.dirname(__file__), "..", "..", "engines")
        engines_dir = os.path.abspath(engines_dir)
        if engines_dir not in sys.path:
            sys.path.insert(0, engines_dir)

        import TOD_Engine
        reload(TOD_Engine)

        # Pass pre-built extraction cache from Phase 2 (evidence validation)
        # so the engine reuses already-extracted content instead of calling
        # Document Intelligence again.
        pre_cache = getattr(state, 'evidence_extract_cache', None) or None
        if pre_cache:
            logger.info("Passing %d cached extractions to engine (no re-extraction)", len(pre_cache))
        # Pass RCM control IDs so the engine skips folders not in the RCM
        _rcm_cids = None
        if "Control Id" in rcm_df.columns:
            _rcm_cids = set(rcm_df["Control Id"].astype(str).str.strip().tolist())
        tod_bank = TOD_Engine.load_tod_evidence_folder(
            evidence_folder, pre_extract_cache=pre_cache,
            include_control_ids=_rcm_cids,
        )
        logger.info("Evidence loaded for %d controls", len(tod_bank))

        # Normalize evidence folder keys to match RCM Control IDs (case-insensitive)
        if "Control Id" in rcm_df.columns:
            rcm_ids = {cid.strip().upper(): cid.strip() for cid in rcm_df["Control Id"].astype(str)}
            normalized_bank = {}
            for folder_key, evidence in tod_bank.items():
                upper_key = folder_key.strip().upper()
                matched_id = rcm_ids.get(upper_key, folder_key.strip())
                normalized_bank[matched_id] = evidence
            tod_bank = normalized_bank
            logger.info("Normalized evidence keys to match RCM Control IDs")

        if not tod_bank:
            return ToolResult(
                success=False, data={},
                error=(
                    "No evidence files found in the folder. "
                    "Expected subfolders named by Control ID (e.g. C-P2P-001/)."
                ),
            )

        # Create tester — pass pre-normalised DataFrame to skip redundant I/O
        tester = TOD_Engine.RCMControlTester(
            normalized_rcm_df=rcm_df,
            openai_api_key=config.openai_api_key,
            openai_model=config.openai_model,
            azure_endpoint=config.azure_openai_endpoint,
            azure_api_key=config.openai_api_key,
            azure_deployment=config.openai_model,
            azure_api_version=config.azure_openai_api_version,
            original_rcm_df=getattr(state, 'original_rcm_df', None),
        )

        # Build pre-approved schemas from preview
        pre_schemas = {}
        for cid, schema_dict in state.pending_tod_schemas.items():
            pre_schemas[cid] = TOD_Engine.ControlSchema(
                control_id=cid,
                worksteps=schema_dict.get("worksteps", []),
                attributes=schema_dict.get("attributes", []),
                sample_columns=schema_dict.get("sample_columns", []),
            )
        logger.info("Using %d pre-approved schemas from attribute preview", len(pre_schemas))

        logger.info("Running Test of Design (new engine)...")
        total_controls = len(tod_bank)
        controls_done: List[Dict] = []

        def _progress_cb(event: str, current: int, total: int, detail: Optional[Dict]):
            """Bridge engine progress_callback to centralised update_tool_progress."""
            if event == "init":
                update_tool_progress(state, _TOOL_NAME, 0, total, f"Evaluating {total} controls...")
            elif event == "eval" and detail:
                controls_done.append(detail)
                cid = detail.get("control_id", "?")
                result = detail.get("result", "?")
                update_tool_progress(
                    state, _TOOL_NAME, current, total,
                    f"Evaluated {cid} — {result} ({current}/{total})",
                    controls_done=controls_done,
                )

        update_tool_progress(state, _TOOL_NAME, 0, total_controls, f"Evaluating {total_controls} controls...")
        results, schemas = tester.test_all_tod(
            tod_bank, max_workers=5, pre_schemas=pre_schemas,
            progress_callback=_progress_cb,
        )

        update_tool_progress(
            state, _TOOL_NAME, total_controls, total_controls,
            "TOD evaluation complete", controls_done=controls_done,
        )

        # Cache results and schemas in state
        state.tod_results = results
        state.tod_evidence_folder = evidence_folder
        state.tod_schemas = schemas  # Cache for TOE reuse

        # Clear pending TOD preview state after use
        state.pending_tod_schemas = None
        state.pending_tod_evidence_folder = None

        # Export workpaper
        tod_output = os.path.join(output_dir, "4_TOD_Results.xlsx")
        tester.export_tod_workpaper(results, tod_output, tod_bank=tod_bank)

        # Upload workpaper to Azure Blob Storage
        tod_blob_path = _upload_artifact(tod_output, state)
        logger.info("TOD workpaper uploaded to blob: %s", tod_blob_path)

        # Build RCM lookup for enriching results with process/control metadata
        rcm_lookup: Dict[str, Dict[str, Any]] = {}
        cid_col = "Control Id" if "Control Id" in rcm_df.columns else "Control ID"
        if cid_col in rcm_df.columns:
            for _, row in rcm_df.iterrows():
                cid = str(row.get(cid_col, "")).strip()
                if cid:
                    rcm_lookup[cid] = {
                        "process": str(row.get("Process", row.get("process", ""))),
                        "subprocess": str(row.get("Sub Process", row.get("Sub-Process", row.get("subprocess", "")))),
                        "control_description": str(row.get("Control Description", row.get("Control Activity", row.get("control_description", "")))),
                        "risk_level": str(row.get("risk_level", row.get("Risk Level", ""))),
                    }

        # Build results for display — single list with RCM metadata merged in
        # to avoid redundant control_summary that doubles the payload size
        result_list = []
        for r in results:
            rcm_meta = rcm_lookup.get(r.control_id, {})
            result_list.append({
                "control_id": r.control_id,
                "process": rcm_meta.get("process", ""),
                "risk_id": r.risk_id,
                "risk_level": rcm_meta.get("risk_level", ""),
                "result": r.result,
                "design_adequate": r.design_adequate,
                "confidence": r.confidence,
                "deficiency_type": r.deficiency_type,
                "gap_identified": str(r.gap_identified)[:150],
                "remarks": str(r.overall_remarks)[:150],
            })

        passed = sum(1 for r in results if r.result == "PASS")
        failed = sum(1 for r in results if r.result == "FAIL")
        no_evidence = sum(1 for r in results if r.result == "NO EVIDENCE")

        logger.info("TOD complete: %d PASS, %d FAIL, %d NO EVIDENCE / %d total",
                    passed, failed, no_evidence, len(results))
        logger.info("TOD schemas cached for %d controls (available for TOE reuse)", len(schemas))

        summary_parts = [f"TOD: {passed} PASS, {failed} FAIL"]
        if no_evidence:
            summary_parts.append(f"{no_evidence} No Evidence")
        summary_parts.append(f"out of {len(results)} controls (full RCM universe). {len(schemas)} schemas cached for TOE.")

        # Build control_summary for frontend (full metadata per control)
        control_summary = []
        for r in results:
            rcm_meta = rcm_lookup.get(r.control_id, {})
            control_summary.append({
                "control_id": r.control_id,
                "process": rcm_meta.get("process", ""),
                "subprocess": rcm_meta.get("subprocess", ""),
                "control_description": rcm_meta.get("control_description", ""),
                "control_type": getattr(r, "control_type", ""),
                "nature_of_control": getattr(r, "nature_of_control", ""),
                "control_frequency": getattr(r, "control_frequency", ""),
                "risk_id": r.risk_id,
                "risk_level": rcm_meta.get("risk_level", ""),
                "result": r.result,
                "design_adequate": r.design_adequate,
                "confidence": r.confidence,
                "deficiency_type": r.deficiency_type,
                "gap_identified": str(r.gap_identified)[:150],
                "remarks": str(r.overall_remarks)[:150],
            })

        data = {
            "controls_evaluated": len(results),
            "passed": passed,
            "failed": failed,
            "no_evidence": no_evidence,
            "controls_with_evidence": len(tod_bank),
            "total_rcm_controls": len(rcm_df),
            "results": result_list,
            "control_summary": control_summary,
            "schemas_cached": len(schemas),
            "output_excel": tod_blob_path or tod_output,
            "output_blob_path": tod_blob_path,
            "_note": "Present ALL control results at once. Do NOT stop mid-way or ask the user to continue.",
        }
        artifacts = [tod_blob_path or tod_output]

        return ToolResult(
            success=True,
            data=data,
            artifacts=artifacts,
            summary=", ".join(summary_parts),
        )
