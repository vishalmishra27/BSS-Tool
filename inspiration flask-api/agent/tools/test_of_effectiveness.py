"""Wrapper around TOE_Engine.py — Test of Operating Effectiveness (TOE).

Three-phase execution:
  Phase 1 (no evidence folder): Generate Required Documents list from approved
           attributes → return the list and ask user for evidence folder.
  Phase 2 (evidence folder provided): Validate evidence against Required Documents
           using embeddings; return match report if documents missing.
  Phase 3 (all matched or skip_validation=true): Run the actual TOE engine.

Supports Azure Blob Storage paths for evidence folders and uploads
generated workpapers back to blob.
"""

from __future__ import annotations

import logging
import os
import sys
from importlib import reload
from typing import Any, Dict, List, Optional

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..config import get_config
from ..core.progress import update_tool_progress, clear_tool_progress

logger = logging.getLogger("agent.tools.test_of_effectiveness")


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

_TOOL_NAME = "run_test_of_effectiveness"


class TestOfEffectivenessTool(Tool):
    @property
    def name(self) -> str:
        return "run_test_of_effectiveness"

    @property
    def description(self) -> str:
        return (
            "Run Test of Operating Effectiveness (TOE) — this is NOT Test of Design (TOD). "
            "TOE evaluates whether controls OPERATED EFFECTIVELY in practice using "
            "multiple samples per control. Use this ONLY when the user says 'TOE', "
            "'test of effectiveness', or 'effectiveness test'. Do NOT use this when "
            "the user asks for TOD/test of design — use run_test_of_design instead. "
            "When called WITHOUT an evidence folder, it auto-generates the Required "
            "Documents list and asks the user for the evidence folder path. "
            "When called WITH an evidence folder, it runs the full TOE test."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.ANALYSIS

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter("evidence_folder", "string",
                          "Absolute path to the TOE evidence folder. "
                          "If you do not have the path, call this tool WITHOUT "
                          "this parameter — it will generate the Required Documents "
                          "list first, then ask the user for the evidence folder.",
                          required=False),
            ToolParameter("skip_validation", "boolean",
                          "Set to true to skip evidence validation and proceed directly "
                          "to the TOE test. Only use when the user explicitly says to "
                          "skip or continue despite missing documents.",
                          required=False, default=False),
            ToolParameter("company_name", "string", "Company name for the workpaper",
                          required=False),
            ToolParameter("prepared_by", "string", "Preparer name", required=False),
            ToolParameter("reviewed_by", "string", "Reviewer name", required=False),
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
        if not evidence_folder and state.pending_toe_evidence_folder:
            evidence_folder = state.pending_toe_evidence_folder
            logger.info("Using cached evidence folder from Phase 2: %s", evidence_folder)
        output_dir = state.output_dir

        # ── Guard: require approved schemas ────────────────────────────
        if not state.pending_toe_schemas and not state.tod_schemas:
            return ToolResult(
                success=False,
                data={
                    "needs_preview": True,
                    "evidence_folder": evidence_folder or None,
                    "message": (
                        "TOE attributes have not been previewed yet and no TOD schemas "
                        "are cached. Run preview_toe_attributes first so the user can "
                        "review and approve the testing attributes before running TOE."
                    ),
                },
                error=(
                    "Attributes not previewed. Call preview_toe_attributes first "
                    "with the same evidence folder, wait for user approval, "
                    "then call run_test_of_effectiveness."
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

            doc_list_schemas = state.pending_toe_schemas or state.tod_schemas or {}
            # Filter to only TOD-passed controls (no need to generate doc list for failed ones)
            if getattr(state, 'tod_results', None) is not None:
                tod_pass_ids = {
                    r.control_id for r in state.tod_results
                    if getattr(r, 'result', None) == "PASS"
                }
                doc_list_schemas = {cid: s for cid, s in doc_list_schemas.items() if cid in tod_pass_ids}
                logger.info("Filtered document list schemas to %d TOD-passed controls", len(doc_list_schemas))
            doc_list_result = None
            try:
                from .document_list import generate_document_list_excel
                n_schemas = len(doc_list_schemas)
                logger.info(
                    "Phase 1: Generating Required Documents list for %d controls...",
                    n_schemas,
                )
                update_tool_progress(
                    state, _TOOL_NAME, 0, n_schemas,
                    f"Phase 1/3 — Generating Required Documents list for {n_schemas} controls…",
                )
                doc_list_result = generate_document_list_excel(
                    schemas=doc_list_schemas,
                    rcm_df=state.rcm_df,
                    output_dir=output_dir,
                    phase="TOE",
                    state=state,
                )
                update_tool_progress(
                    state, _TOOL_NAME, n_schemas, n_schemas,
                    "Phase 1/3 — Required Documents list generated",
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

            # Build the evidence-folder ask message
            tod_folder = state.tod_evidence_folder
            if tod_folder:
                ask_msg = (
                    "Required Documents list has been generated. "
                    "Now ASK the user for the TOE evidence folder path. "
                    f"This MUST be a SEPARATE folder from the TOD evidence at: {tod_folder}. "
                    "Do NOT reuse the TOD evidence folder. Do NOT guess — WAIT for the user."
                )
            else:
                ask_msg = (
                    "Required Documents list has been generated. "
                    "Now ASK the user for the TOE evidence folder path. "
                    "The folder should contain subfolders named by Control ID "
                    "with sample evidence files inside. "
                    "Do NOT guess or invent a path — WAIT for the user."
                )

            return ToolResult(
                success=True,
                data={
                    "phase": "document_list",
                    "document_list": doc_list_result,
                    "awaiting_evidence_folder": True,
                    "message": ask_msg,
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
                                  "TOE evidence folder. The folder should contain subfolders "
                                  "named by Control ID with sample evidence files inside."
                              ))

        # ── Phase 2: Evidence validation ──────────────────────────────────
        # If the user provided a DIFFERENT evidence folder than what was validated,
        # reset the validation flag so Phase 2 re-runs on the new folder.
        if (state.evidence_validated
                and state.pending_toe_evidence_folder
                and evidence_folder != state.pending_toe_evidence_folder):
            logger.info(
                "Evidence folder changed (%s → %s) — resetting validation",
                state.pending_toe_evidence_folder, evidence_folder,
            )
            state.evidence_validated = False

        if not skip_validation and not state.evidence_validated and state.document_list_rows:
            try:
                from .evidence_validator import validate_evidence_against_documents
                logger.info("Phase 2: Validating evidence against Required Documents list...")

                doc_schemas = state.pending_toe_schemas or state.tod_schemas or {}
                # Filter to only TOD-passed controls — skip extraction for failed/no-evidence controls
                if getattr(state, 'tod_results', None) is not None:
                    tod_pass_ids = {
                        r.control_id for r in state.tod_results
                        if getattr(r, 'result', None) == "PASS"
                    }
                    doc_schemas = {cid: s for cid, s in doc_schemas.items() if cid in tod_pass_ids}
                    logger.info("Filtered evidence validation to %d TOD-passed controls", len(doc_schemas))
                control_ids = list(doc_schemas.keys())
                n_validate = len(control_ids)
                update_tool_progress(
                    state, _TOOL_NAME, 0, n_validate,
                    f"Phase 2/3 — Validating evidence for {n_validate} controls…",
                )
                validation = validate_evidence_against_documents(
                    evidence_folder=evidence_folder,
                    document_list_rows=state.document_list_rows,
                    control_ids=control_ids,
                    api_key=config.openai_api_key,
                    endpoint=config.azure_openai_endpoint,
                    api_version=config.azure_openai_api_version,
                    embedding_model=config.azure_openai_embedding_deployment,
                    per_sample=True,  # TOE: validate each sample independently
                    llm_model=config.openai_model,
                )

                total_missing = validation.get("total_missing", 0)
                total_required = validation.get("total_required", 0)
                total_matched = validation.get("total_matched", 0)

                update_tool_progress(
                    state, _TOOL_NAME, n_validate, n_validate,
                    f"Phase 2/3 — Validation complete: {total_matched}/{total_required} matched",
                )

                logger.info(
                    "Evidence validation: %d/%d matched, %d missing",
                    total_matched, total_required, total_missing,
                )

                state.pending_toe_evidence_folder = evidence_folder

                # Cache extraction results so Phase 3 skips re-extraction
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
                        phase="TOE",
                        state=state,
                    )
                except Exception as _vex:
                    logger.warning("Validation Excel export failed (non-fatal): %s", _vex)

                if total_missing > 0:
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
                            "and call run_test_of_effectiveness again with the new path.\n"
                            "2. Skip validation and continue with the TOE test by calling "
                            "run_test_of_effectiveness with skip_validation=true."
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
                    state.evidence_validated = True
                    logger.info("All required documents matched — proceeding to TOE test")

            except Exception as exc:
                logger.warning("Evidence validation failed (non-fatal, proceeding): %s", exc)
                state.evidence_validated = True
        else:
            if skip_validation:
                logger.info("Evidence validation skipped by user request")
            state.evidence_validated = True

        # ── Phase 3: Run the actual TOE test ──────────────────────────────

        rcm_df = state.rcm_df.copy()

        # Ensure required columns
        if "risk_level" not in rcm_df.columns:
            rcm_df["risk_level"] = "High"
            logger.info("Auto-added 'risk_level' column with default 'High'")
        if "count_of_samples" not in rcm_df.columns:
            rcm_df["count_of_samples"] = ""

        # Normalize Control Id values for case-insensitive matching with evidence folders
        if "Control Id" in rcm_df.columns:
            rcm_df["Control Id"] = rcm_df["Control Id"].astype(str).str.strip()

        logger.info("Loading TOE evidence from %s", evidence_folder)
        update_tool_progress(
            state, _TOOL_NAME, 0, 1,
            "Phase 3/3 — Loading and extracting evidence files…",
        )

        # ── Import TOE engine ──
        engines_dir = os.path.join(os.path.dirname(__file__), "..", "..", "engines")
        engines_dir = os.path.abspath(engines_dir)
        if engines_dir not in sys.path:
            sys.path.insert(0, engines_dir)

        import TOE_Engine
        reload(TOE_Engine)

        include_control_ids = None
        if getattr(state, 'tod_results', None) is not None:
            include_control_ids = {
                r.control_id for r in state.tod_results
                if getattr(r, 'result', None) == "PASS"
            }
            logger.info(
                "Applying TOE evidence parsing filter from TOD PASS controls: %d control(s)",
                len(include_control_ids)
            )

        # Pass pre-built extraction cache from Phase 2 (evidence validation)
        pre_cache = getattr(state, 'evidence_extract_cache', None) or None
        if pre_cache:
            logger.info("Passing %d cached extractions to engine (no re-extraction)", len(pre_cache))
        toe_bank = TOE_Engine.load_toe_evidence_folder(
            evidence_folder,
            include_control_ids=include_control_ids,
            pre_extract_cache=pre_cache,
        )
        logger.info("TOE evidence loaded for %d controls", len(toe_bank))
        update_tool_progress(
            state, _TOOL_NAME, 1, 1,
            f"Phase 3/3 — Evidence loaded for {len(toe_bank)} controls",
        )

        # Normalize evidence folder keys to match RCM Control IDs (case-insensitive)
        if "Control Id" in rcm_df.columns:
            rcm_ids = {cid.strip().upper(): cid.strip() for cid in rcm_df["Control Id"].astype(str)}
            normalized_bank = {}
            for folder_key, evidence in toe_bank.items():
                upper_key = folder_key.strip().upper()
                matched_id = rcm_ids.get(upper_key, folder_key.strip())
                normalized_bank[matched_id] = evidence
            toe_bank = normalized_bank
            logger.info("Normalized TOE evidence keys to match RCM Control IDs")

        if not toe_bank:
            return ToolResult(
                success=False, data={},
                error="No evidence files found. Expected subfolders per Control ID with sample .txt files.",
            )

        company_name = args.get("company_name", "")
        prepared_by = args.get("prepared_by", "")
        reviewed_by = args.get("reviewed_by", "")

        # Create tester — pass pre-normalised DataFrame to skip redundant I/O
        tester = TOE_Engine.RCMControlTester(
            normalized_rcm_df=rcm_df,
            openai_api_key=config.openai_api_key,
            openai_model=config.openai_model,
            azure_endpoint=config.azure_openai_endpoint,
            azure_api_key=config.openai_api_key,
            azure_deployment=config.openai_model,
            azure_api_version=config.azure_openai_api_version,
            original_rcm_df=getattr(state, 'original_rcm_df', None),
        )

        # Build pre-approved schemas
        pre_schemas = None
        schema_source = None

        if state.pending_toe_schemas:
            import TOE_Engine as _toe
            pre_schemas = {}
            for cid, schema_dict in state.pending_toe_schemas.items():
                pre_schemas[cid] = _toe.ControlSchema(
                    control_id=cid,
                    worksteps=schema_dict.get("worksteps", []),
                    attributes=schema_dict.get("attributes", []),
                    sample_columns=schema_dict.get("sample_columns", []),
                )
            schema_source = "attribute preview"
            logger.info("Using %d pre-approved schemas from attribute preview", len(pre_schemas))

        elif state.tod_schemas:
            pre_schemas = state.tod_schemas
            schema_source = "TOD cache (already approved)"
            logger.info("Using %d cached schemas from TOD run (skipping schema generation and approval)", len(pre_schemas))

        else:
            return ToolResult(
                success=False,
                data={"needs_preview": True, "evidence_folder": evidence_folder},
                error="Attributes not previewed. Call preview_toe_attributes first.",
            )

        logger.info("Running Test of Operating Effectiveness...")
        total_controls = len(toe_bank)
        controls_done: List[Dict] = []

        _last_controls_completed = [0]  # mutable counter for closure

        def _progress_cb(event: str, current: int, total: int, detail: Optional[Dict]):
            """Bridge engine progress_callback to centralised update_tool_progress."""
            if event == "init":
                ts = (detail or {}).get("total_samples", total)
                update_tool_progress(
                    state, _TOOL_NAME, 0, total,
                    f"Phase 3/3 — Evaluating {total} controls ({ts} samples)…",
                )
            elif event == "schema":
                # Schema generation progress (Phase 1 of engine)
                cid = (detail or {}).get("control_id", "?")
                update_tool_progress(
                    state, _TOOL_NAME, 0, total,
                    f"Generating schemas… {current}/{total} ({cid})",
                )
            elif event == "schema_done":
                update_tool_progress(
                    state, _TOOL_NAME, 0, total,
                    f"Schemas ready — evaluating {total} controls…",
                )
            elif event == "eval" and detail:
                cid = detail.get("control_id", "?")
                result = detail.get("result", "?")
                sd = detail.get("samples_done", current)
                st = detail.get("samples_total", total)
                # Only add to controls_done when a NEW control finishes
                # (current = controls fully completed; only append on increment)
                if current > _last_controls_completed[0]:
                    _last_controls_completed[0] = current
                    controls_done.append({"control_id": cid, "result": result})
                update_tool_progress(
                    state, _TOOL_NAME, current, total,
                    f"Phase 3/3 — {cid}: {result} ({sd}/{st} samples, {current}/{total} controls)",
                    controls_done=controls_done,
                )

        update_tool_progress(state, _TOOL_NAME, 0, total_controls, f"Phase 3/3 — Evaluating {total_controls} controls…")

        if getattr(state, 'tod_results', None) is None:
            logger.warning(
                "TOE is running without tod_results gating data. "
                "All controls with TOE evidence may be evaluated (direct TOE mode)."
            )
        results = tester.test_all_toe(
            toe_bank, max_workers=5, pre_schemas=pre_schemas,
            progress_callback=_progress_cb,
            tod_results=getattr(state, 'tod_results', None),
        )

        update_tool_progress(
            state, _TOOL_NAME, total_controls, total_controls,
            "Phase 3/3 — TOE evaluation complete", controls_done=controls_done,
        )

        state.toe_results = results

        # Clear cached schemas after use
        state.pending_toe_schemas = None
        state.pending_toe_evidence_folder = None

        # Export workpaper
        update_tool_progress(
            state, _TOOL_NAME, total_controls, total_controls,
            "Exporting TOE workpaper…", controls_done=controls_done,
        )
        toe_output = os.path.join(output_dir, "5_TOE_Workpaper.xlsx")
        tester.export_toe_workpaper(
            results, toe_output, toe_bank=toe_bank,
            company_name=company_name,
            prepared_by=prepared_by,
            reviewed_by=reviewed_by,
        )

        # Upload workpaper to Azure Blob Storage
        toe_blob_path = _upload_artifact(toe_output, state)
        logger.info("TOE workpaper uploaded to blob: %s", toe_blob_path)

        # Generate report
        toe_report = tester.generate_toe_report(results)

        # Build results for display
        summary_list = []
        detail_list = []
        for r in results:
            summary_list.append({
                "control_id": r.control_id,
                "total_samples": r.total_samples,
                "passed_samples": r.passed_samples,
                "failed_samples": r.failed_samples,
                "deviation_rate": f"{r.deviation_rate:.1%}",
                "operating_effectiveness": r.operating_effectiveness,
                "deficiency_type": r.deficiency_type,
            })
            samples = []
            for sr in r.sample_results:
                samples.append({
                    "sample_id": sr.sample_id,
                    "result": sr.result,
                    "deviation_details": str(getattr(sr, "deviation_details", ""))[:100],
                    "remarks": str(sr.remarks)[:100],
                })
            detail_list.append({
                "control_id": r.control_id,
                "effectiveness": r.operating_effectiveness,
                "samples": samples,
            })

        effective = sum(1 for r in results if r.operating_effectiveness == "Effective")
        exceptions = sum(1 for r in results if r.operating_effectiveness == "Effective with Exceptions")
        not_effective = sum(1 for r in results if r.operating_effectiveness == "Not Effective")
        tod_failed = sum(1 for r in results if r.operating_effectiveness == "TOD Failed")
        no_evidence = sum(1 for r in results if r.operating_effectiveness == "No Evidence")

        logger.info("TOE complete: %d Effective, %d Exceptions, %d Not Effective, %d TOD Failed, %d No Evidence",
                     effective, exceptions, not_effective, tod_failed, no_evidence)

        summary_parts = [
            f"TOE: {effective} Effective, {exceptions} Exceptions, "
            f"{not_effective} Not Effective"
        ]
        if tod_failed:
            summary_parts.append(f"{tod_failed} TOD Failed (skipped)")
        if no_evidence:
            summary_parts.append(f"{no_evidence} No Evidence")
        summary_parts.append(f"out of {len(results)} controls (full RCM universe)")

        data = {
            "controls_evaluated": len(results),
            "effective": effective,
            "effective_with_exceptions": exceptions,
            "not_effective": not_effective,
            "tod_failed": tod_failed,
            "no_evidence": no_evidence,
            "total_rcm_controls": len(rcm_df),
            "summary": summary_list,
            "details": detail_list,
            "toe_report": toe_report,
            "schema_source": schema_source or "generated fresh",
            "output_excel": toe_blob_path or toe_output,
            "output_blob_path": toe_blob_path,
        }
        artifacts = [p for p in [toe_blob_path or toe_output] if p]

        return ToolResult(
            success=True,
            data=data,
            artifacts=artifacts,
            summary=", ".join(summary_parts),
        )
