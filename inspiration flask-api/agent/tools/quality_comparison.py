"""Wrapper around QualityComparison engine.

Runs the full autonomous control testing pipeline (TOD -> Sampling -> TOE),
then compares our results against a human auditor's workpapers to
identify gaps in the human's work.

Expected project folder structure:
  project_folder/
  ├── RCM/              <- contains the RCM Excel file
  ├── evidence/
  │   ├── TOD/          <- subfolders per Control ID with TOD evidence
  │   └── TOE/          <- subfolders per Control ID with TOE evidence
  └── OutputWork/
      ├── TOD/          <- contains the human auditor's TOD workpaper Excel
      └── TOE/          <- contains the human auditor's TOE workpaper Excel
"""

from __future__ import annotations

import glob
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..config import get_config

logger = logging.getLogger("agent.tools.quality_comparison")


def _find_excel(folder: str) -> Optional[str]:
    """Find the first Excel file (.xlsx / .xls) in a folder."""
    for ext in ("*.xlsx", "*.xls"):
        matches = sorted(glob.glob(os.path.join(folder, ext)))
        # Skip temp files (Excel lock files start with ~$)
        matches = [m for m in matches if not os.path.basename(m).startswith("~$")]
        if matches:
            return matches[0]
    return None


class QualityComparisonTool(Tool):
    """Run the full pipeline and compare results against human workpapers."""

    @property
    def name(self) -> str:
        return "run_quality_comparison"

    @property
    def description(self) -> str:
        return (
            "Run a Quality Comparison: autonomously executes the full control testing pipeline "
            "(TOD with auto-approved schemas -> Sampling -> TOE), then compares our tool's "
            "results against a human auditor's pre-existing workpapers. Produces a comparison "
            "report identifying gaps in the human's work at three levels: per-control verdict, "
            "per-attribute, and per-sample. Requires a single project folder containing "
            "subfolders: RCM/ (with the RCM Excel), evidence/TOD/ and evidence/TOE/ "
            "(with Control ID subfolders), OutputWork/TOD/ (human's TOD workpaper Excel), "
            "and OutputWork/TOE/ (human's TOE workpaper Excel)."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.ANALYSIS

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                "project_folder", "string",
                "Absolute path to the project folder. Must contain subfolders: "
                "RCM/ (with the RCM Excel file), evidence/TOD/ (Control ID subfolders "
                "with TOD evidence), evidence/TOE/ (Control ID subfolders with TOE "
                "evidence), OutputWork/TOD/ (human TOD workpaper Excel), and "
                "OutputWork/TOE/ (human TOE workpaper Excel).",
                required=True,
            ),
            ToolParameter(
                "company_name", "string",
                "Company name for the workpaper header.",
                required=False,
            ),
            ToolParameter(
                "prepared_by", "string",
                "Name of the preparer for the workpaper sign-off.",
                required=False,
            ),
            ToolParameter(
                "reviewed_by", "string",
                "Name of the reviewer for the workpaper sign-off.",
                required=False,
            ),
            ToolParameter(
                "sampling_methodology", "string",
                (
                    "Sampling methodology to use. 'kpmg' for the standard KPMG "
                    "sampling table, or 'custom' to use a custom sampling table. "
                    "If 'custom', also provide custom_sampling_file_path. "
                    "ASK the user before calling this tool — do not assume KPMG."
                ),
                required=False,
                enum=["kpmg", "custom"],
            ),
            ToolParameter(
                "custom_sampling_file_path", "string",
                (
                    "Path to a custom sampling table Excel/CSV file. "
                    "Required if sampling_methodology is 'custom'. "
                    "Must have columns: Frequency, Risk of Failure (Lower), "
                    "Risk of Failure (Higher)."
                ),
                required=False,
            ),
            ToolParameter(
                "risk_methodology", "string",
                (
                    "Risk level inference methodology. 'weighted' for the default "
                    "non-linear scoring (Low=1, Medium=3, High=6; Score=PxI), "
                    "or 'custom' if the user wants to provide custom weights/bands. "
                    "ASK the user before calling this tool — do not assume weighted."
                ),
                required=False,
                enum=["weighted", "custom"],
            ),
            ToolParameter(
                "custom_risk_weights", "string",
                (
                    "Custom score weights for risk level computation. "
                    "Format: 'Low=2, Medium=5, High=10'. "
                    "Only used if risk_methodology is 'custom'."
                ),
                required=False,
            ),
            ToolParameter(
                "custom_risk_bands", "string",
                (
                    "Custom band thresholds for risk level computation. "
                    "Format: '1-6=Low, 7-20=Medium, 21-40=High'. "
                    "Scores above the last upper bound map to Critical. "
                    "Only used if risk_methodology is 'custom'."
                ),
                required=False,
            ),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        # No preconditions — this tool loads its own RCM
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        config = get_config()
        start_time = time.time()

        project_folder = (args.get("project_folder") or "").strip().strip("'\"")
        company_name = args.get("company_name", "")
        prepared_by = args.get("prepared_by", "")
        reviewed_by = args.get("reviewed_by", "")
        sampling_methodology = (args.get("sampling_methodology") or "").strip().lower()
        custom_sampling_file = (args.get("custom_sampling_file_path") or "").strip()
        risk_methodology = (args.get("risk_methodology") or "").strip().lower()
        custom_risk_weights = (args.get("custom_risk_weights") or "").strip()
        custom_risk_bands_raw = (args.get("custom_risk_bands") or "").strip()

        # ── Ask methodology choices upfront ──────────────────────────
        if not sampling_methodology or not risk_methodology:
            missing = []
            if not sampling_methodology:
                missing.append(
                    "Sampling methodology: Should I use the **KPMG standard** "
                    "sampling table, or does the user have a **custom** sampling "
                    "table to upload?"
                )
            if not risk_methodology:
                missing.append(
                    "Risk level methodology: Should I use the **default weighted "
                    "scoring** (Low=1, Medium=3, High=6; Score=P×I), or does the "
                    "user want to provide **custom** weights and/or band thresholds?"
                )
            return ToolResult(
                success=False,
                data={
                    "awaiting_input": True,
                    "questions": missing,
                    "message": (
                        "Before running the Quality Comparison, ASK the user the "
                        "following questions and then re-call this tool with the answers:\n\n"
                        + "\n".join(f"{i+1}. {q}" for i, q in enumerate(missing))
                    ),
                },
                error=(
                    "Missing methodology choices. Ask the user: "
                    + " AND ".join(
                        ["sampling_methodology (kpmg or custom)"] * (not sampling_methodology and 1 or 0)
                        + ["risk_methodology (weighted or custom)"] * (not risk_methodology and 1 or 0)
                    )
                ),
            )

        # Validate custom sampling file if custom methodology chosen
        if sampling_methodology == "custom" and not custom_sampling_file:
            return ToolResult(
                success=False,
                data={"awaiting_input": True},
                error=(
                    "sampling_methodology is 'custom' but no custom_sampling_file_path "
                    "provided. Ask the user to upload their custom sampling table "
                    "(Excel/CSV with columns: Frequency, Risk of Failure Lower, "
                    "Risk of Failure Higher)."
                ),
            )

        # Parse custom risk weights/bands if provided
        custom_score_map = None
        custom_bands = None
        if risk_methodology == "custom":
            if custom_risk_weights:
                import re
                pairs = re.findall(r'(low|medium|high)\s*[=:]\s*(\d+)',
                                   custom_risk_weights, re.IGNORECASE)
                if len(pairs) >= 3:
                    custom_score_map = {k.lower(): int(v) for k, v in pairs}
                else:
                    return ToolResult(
                        success=False, data={},
                        error=(
                            f"Could not parse weights from: '{custom_risk_weights}'. "
                            "Expected format: 'Low=2, Medium=5, High=10'."
                        ),
                    )
            if custom_risk_bands_raw:
                import re
                matches = re.findall(
                    r'(\d+)\s*[-–]\s*(\d+)\s*[=:]\s*(Low|Medium|High)',
                    custom_risk_bands_raw, re.IGNORECASE,
                )
                if len(matches) >= 2:
                    custom_bands = [
                        (int(upper), label.capitalize())
                        for _, upper, label in sorted(matches, key=lambda m: int(m[1]))
                    ]
                else:
                    return ToolResult(
                        success=False, data={},
                        error=(
                            f"Could not parse bands from: '{custom_risk_bands_raw}'. "
                            "Expected format: '1-6=Low, 7-20=Medium, 21-40=High'."
                        ),
                    )

        # Resolve blob paths to local cache
        from server.blob_store import get_blob_store, BlobStore
        if BlobStore.is_blob_path(project_folder):
            store = get_blob_store()
            if not store.available:
                return ToolResult(
                    success=False, data={},
                    error="Azure Blob Storage is not available.",
                )
            local = store.ensure_local_directory(project_folder)
            if not local:
                return ToolResult(
                    success=False, data={},
                    error=f"Failed to download blob project folder: {project_folder}",
                )
            logger.info("Downloaded blob project folder %s → %s", project_folder, local)
            project_folder = local

        # ── Validate project folder ───────────────────────────────────
        if not project_folder:
            return ToolResult(
                success=False,
                data={"awaiting_input": True},
                error="Project folder not provided. Ask the user for the path to the project folder.",
            )
        if not os.path.isdir(project_folder):
            return ToolResult(
                success=False, data={"attempted_path": project_folder},
                error=f"Project folder not found at: {project_folder}. Ask the user for the correct path.",
            )

        # ── Resolve subfolders ────────────────────────────────────────
        rcm_dir = os.path.join(project_folder, "RCM")
        tod_evidence_dir = os.path.join(project_folder, "evidence", "TOD")
        toe_evidence_dir = os.path.join(project_folder, "evidence", "TOE")
        output_tod_dir = os.path.join(project_folder, "OutputWork", "TOD")
        output_toe_dir = os.path.join(project_folder, "OutputWork", "TOE")

        # Validate all required subfolders exist
        missing = []
        for label, path in [
            ("RCM/", rcm_dir),
            ("evidence/TOD/", tod_evidence_dir),
            ("evidence/TOE/", toe_evidence_dir),
            ("OutputWork/TOD/", output_tod_dir),
            ("OutputWork/TOE/", output_toe_dir),
        ]:
            if not os.path.isdir(path):
                missing.append(label)

        if missing:
            return ToolResult(
                success=False,
                data={"project_folder": project_folder, "missing_subfolders": missing},
                error=(
                    f"Project folder is missing required subfolders: {', '.join(missing)}. "
                    f"Expected structure inside {project_folder}:\n"
                    f"  RCM/              (contains the RCM Excel file)\n"
                    f"  evidence/TOD/     (Control ID subfolders with TOD evidence)\n"
                    f"  evidence/TOE/     (Control ID subfolders with TOE evidence)\n"
                    f"  OutputWork/TOD/   (human auditor's TOD workpaper Excel)\n"
                    f"  OutputWork/TOE/   (human auditor's TOE workpaper Excel)\n"
                    f"Ask the user to verify the folder structure."
                ),
            )

        # ── Auto-discover Excel files ─────────────────────────────────
        rcm_path = _find_excel(rcm_dir)
        if not rcm_path:
            return ToolResult(
                success=False, data={"searched_folder": rcm_dir},
                error=f"No Excel file found in {rcm_dir}. The RCM/ subfolder must contain an .xlsx or .xls file.",
            )

        human_tod_workpaper_path = _find_excel(output_tod_dir)
        if not human_tod_workpaper_path:
            return ToolResult(
                success=False, data={"searched_folder": output_tod_dir},
                error=f"No Excel file found in {output_tod_dir}. The OutputWork/TOD/ subfolder must contain the human's TOD workpaper (.xlsx or .xls).",
            )

        human_toe_workpaper_path = _find_excel(output_toe_dir)
        if not human_toe_workpaper_path:
            return ToolResult(
                success=False, data={"searched_folder": output_toe_dir},
                error=f"No Excel file found in {output_toe_dir}. The OutputWork/TOE/ subfolder must contain the human's TOE workpaper (.xlsx or .xls).",
            )

        # Output goes into a quality_comparison/ subfolder in the project
        output_dir = os.path.join(project_folder, "quality_comparison")

        logger.info(
            "Project folder resolved: rcm=%s, tod_evidence=%s, toe_evidence=%s, "
            "human_tod=%s, human_toe=%s, output=%s",
            rcm_path, tod_evidence_dir, toe_evidence_dir,
            human_tod_workpaper_path, human_toe_workpaper_path, output_dir,
        )

        # ── Import engine ─────────────────────────────────────────────
        engines_dir = os.path.join(os.path.dirname(__file__), "..", "..", "engines")
        engines_dir = os.path.abspath(engines_dir)
        if engines_dir not in sys.path:
            sys.path.insert(0, engines_dir)

        from QualityComparison import QualityComparisonEngine

        # ── Run ───────────────────────────────────────────────────────
        logger.info("Starting Quality Comparison engine")
        # Use explicit methodology params; fall back to agent state config
        effective_score_map = custom_score_map or getattr(state, "custom_risk_score_map", None)
        effective_bands = custom_bands or getattr(state, "custom_risk_bands", None)

        engine = QualityComparisonEngine(
            rcm_path=rcm_path,
            tod_evidence_folder=tod_evidence_dir,
            toe_evidence_folder=toe_evidence_dir,
            human_tod_workpaper_path=human_tod_workpaper_path,
            human_toe_workpaper_path=human_toe_workpaper_path,
            output_dir=output_dir,
            openai_api_key=config.openai_api_key,
            openai_model=config.openai_model,
            azure_endpoint=config.azure_openai_endpoint,
            azure_api_key=config.openai_api_key,
            azure_deployment=config.openai_model,
            azure_api_version=config.azure_openai_api_version,
            company_name=company_name,
            prepared_by=prepared_by,
            reviewed_by=reviewed_by,
            risk_score_map=effective_score_map,
            risk_bands=effective_bands,
            sampling_methodology=sampling_methodology,
            custom_sampling_file_path=custom_sampling_file or None,
        )

        # Wire up progress tracking for frontend polling
        from ..core.progress import update_tool_progress
        def _on_progress(current: int, total: int, message: str):
            update_tool_progress(state, "run_quality_comparison", current, total, message)
        engine.progress_callback = _on_progress

        try:
            report = engine.run()
        except Exception as e:
            logger.exception("Quality Comparison failed")
            return ToolResult(
                success=False, data={},
                error=f"Quality Comparison failed: {e}",
                duration_seconds=time.time() - start_time,
            )

        # Cache in state
        state.comparison_results = report

        duration = time.time() - start_time

        # Upload artifacts to Azure Blob Storage
        # Use the same session key as the artifacts listing endpoint (chat_id),
        # which is the basename of output_dir set by rcm_loader.
        tod_blob_path = report.tod_output_path
        toe_blob_path = report.toe_output_path
        comparison_blob_path = report.comparison_output_path
        blob_artifacts = []
        try:
            from server.blob_store import get_blob_store
            store = get_blob_store()
            if store.available:
                session_key = os.path.basename(getattr(state, "output_dir", None) or "default")
                for local_path, attr_name in [
                    (report.tod_output_path, "tod"),
                    (report.toe_output_path, "toe"),
                    (report.comparison_output_path, "comparison"),
                ]:
                    if local_path and os.path.isfile(local_path):
                        filename = os.path.basename(local_path)
                        blob_path = f"artifacts/{session_key}/{filename}"
                        result = store.upload_file(local_path, blob_path)
                        if result:
                            logger.info("QC artifact uploaded: %s → %s", local_path, blob_path)
                            blob_artifacts.append(blob_path)
                            if attr_name == "tod":
                                tod_blob_path = blob_path
                            elif attr_name == "toe":
                                toe_blob_path = blob_path
                            elif attr_name == "comparison":
                                comparison_blob_path = blob_path
                        else:
                            logger.warning("QC artifact upload failed: %s", local_path)
            else:
                logger.warning("Blob Storage not available — QC artifacts saved locally only")
        except Exception as upload_exc:
            logger.warning("QC artifact upload failed (non-fatal): %s", upload_exc)

        # Build result summary — prefer blob paths so downloads work after restart.
        # Filter out None entries so frontend only gets valid downloadable paths.
        artifacts = blob_artifacts if blob_artifacts else [
            p for p in [
                report.tod_output_path,
                report.toe_output_path,
                report.comparison_output_path,
            ] if p
        ]

        # Count verdict matches by phase
        tod_comps = [c for c in report.control_comparisons if c.phase == "TOD"]
        toe_comps = [c for c in report.control_comparisons if c.phase == "TOE"]
        tod_match = sum(1 for c in tod_comps if not c.verdict_gap)
        toe_match = sum(1 for c in toe_comps if not c.verdict_gap)
        summary = (
            f"Quality Comparison: {report.total_controls} controls compared. "
            f"TOD verdict agreement: {tod_match}/{len(tod_comps)}, "
            f"TOE verdict agreement: {toe_match}/{len(toe_comps)}. "
            f"{report.controls_with_gaps} control(s) with differences."
        )

        # Top gaps for the LLM to present
        top_gaps = []
        for comp in sorted(
            report.control_comparisons,
            key=lambda c: ["NONE", "LOW", "MEDIUM", "HIGH", "CRITICAL"].index(c.gap_severity),
            reverse=True,
        )[:5]:
            if comp.gap_severity == "NONE":
                continue
            gap_info = {
                "control_id": comp.control_id,
                "phase": comp.phase,
                "severity": comp.gap_severity,
                "our_verdict": comp.our_effectiveness,
                "human_verdict": comp.human_effectiveness,
                "verdict_gap": comp.verdict_gap,
                "attributes_missed": comp.attributes_human_missed,
                "samples_missed": len(comp.samples_human_missed),
                "attribute_differences": len(comp.attribute_differences),
                "sample_differences": len(comp.sample_differences),
            }
            if comp.verdict_detail:
                gap_info["detail"] = comp.verdict_detail
            if comp.overall_narrative:
                gap_info["narrative"] = comp.overall_narrative
            top_gaps.append(gap_info)

        return ToolResult(
            success=True,
            data={
                "project_folder": project_folder,
                "total_controls": report.total_controls,
                "controls_with_gaps": report.controls_with_gaps,
                "severity_breakdown": report.severity_counts,
                "top_gaps": top_gaps,
                "tod_output": tod_blob_path or None,
                "toe_output": toe_blob_path or None,
                "comparison_report": comparison_blob_path or None,
            },
            artifacts=artifacts,
            summary=summary,
            duration_seconds=duration,
        )
