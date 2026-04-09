"""Preview TOE attributes for user approval before running the full test.

No evidence folder required at preview time — schemas are generated for all
controls in the RCM (or reused from TOD if already approved).  The evidence
folder is requested later by run_test_of_effectiveness.
"""

from __future__ import annotations

import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from importlib import reload
from typing import Any, Dict, List, Optional

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..config import get_config
from ..core.progress import update_tool_progress, clear_tool_progress

logger = logging.getLogger("agent.tools.preview_toe_attributes")

_TOOL_NAME = "preview_toe_attributes"


class PreviewToeAttributesTool(Tool):
    @property
    def name(self) -> str:
        return "preview_toe_attributes"

    @property
    def description(self) -> str:
        return (
            "Generate and preview TOE testing attributes for user approval BEFORE "
            "running the full Test of Effectiveness. Shows the LLM-generated attributes, "
            "worksteps, and sample columns per control in a reviewable format. "
            "The user can then edit and approve them via the UI before proceeding. "
            "No evidence folder is needed at this stage — evidence is requested later."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.ANALYSIS

    @property
    def parameters(self) -> List[ToolParameter]:
        return []

    def preconditions(self, state: AgentState) -> Optional[str]:
        if state.rcm_df is None:
            return "No RCM loaded. Use load_rcm first."
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        config = get_config()

        # ── Short-circuit: reuse TOD schemas if already approved ──────
        if state.tod_schemas:
            schemas_for_state = {}
            schema_list = []
            for cid, schema in state.tod_schemas.items():
                # Handle both ControlSchema objects and plain dicts
                if hasattr(schema, 'worksteps'):
                    ws = schema.worksteps
                    attrs = schema.attributes
                    cols = schema.sample_columns
                else:
                    ws = schema.get("worksteps", [])
                    attrs = schema.get("attributes", [])
                    cols = schema.get("sample_columns", [])
                schema_dict = {
                    "worksteps": ws,
                    "attributes": attrs,
                    "sample_columns": cols,
                }
                schemas_for_state[cid] = schema_dict
                schema_list.append({"control_id": cid, **schema_dict})

            state.pending_toe_schemas = schemas_for_state
            state.pending_toe_evidence_folder = None

            logger.info(
                "Skipping TOE preview — reusing %d already-approved schemas from TOD",
                len(schemas_for_state),
            )
            return ToolResult(
                success=True,
                data={
                    "controls_count": len(schema_list),
                    "total_attributes": sum(len(s.get("attributes", [])) for s in schema_list),
                    "schemas": schema_list,
                    "requires_approval": False,
                    "reused_from_tod": True,
                    "_agent_notes": [
                        "These schemas were already approved during TOD preview. "
                        "No second approval needed — proceed directly to run_test_of_effectiveness."
                    ],
                },
                summary=(
                    f"Reused {len(schema_list)} already-approved schemas from TOD. "
                    "No preview needed — proceed to TOE."
                ),
            )

        # ── Fresh schema generation (no prior TOD) ─────────────────────
        rcm_df = state.rcm_df.copy()

        # Ensure required columns
        if "risk_level" not in rcm_df.columns:
            rcm_df["risk_level"] = "High"
        if "count_of_samples" not in rcm_df.columns:
            rcm_df["count_of_samples"] = ""

        # Normalize Control Id values
        if "Control Id" not in rcm_df.columns:
            return ToolResult(
                success=False, data={},
                error="RCM does not have a 'Control Id' column.",
            )
        rcm_df["Control Id"] = rcm_df["Control Id"].astype(str).str.strip()

        control_ids = rcm_df["Control Id"].dropna().unique().tolist()
        control_ids = [cid for cid in control_ids if cid and cid.lower() != "nan"]

        if not control_ids:
            return ToolResult(
                success=False, data={},
                error="No Control IDs found in the RCM.",
            )

        # ── Import TOE engine (for RCMControlTester + schema generation) ──
        engines_dir = os.path.join(os.path.dirname(__file__), "..", "..", "engines")
        engines_dir = os.path.abspath(engines_dir)
        if engines_dir not in sys.path:
            sys.path.insert(0, engines_dir)

        import TOE_Engine
        reload(TOE_Engine)

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

        valid_ids = [cid for cid in control_ids if cid in tester.rcm_lookup]

        if not valid_ids:
            return ToolResult(
                success=False, data={},
                error="No valid controls found in RCM lookup.",
            )

        # Generate schemas in parallel
        n_controls = len(valid_ids)
        logger.info("Generating schemas for %d controls...", n_controls)
        update_tool_progress(
            state, _TOOL_NAME, 0, n_controls,
            f"Generating attributes for {n_controls} controls…",
        )
        schemas = {}
        max_workers = 5
        done_count = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_cid = {}
            for cid in sorted(valid_ids):
                rcm = tester.rcm_lookup[cid]
                future = executor.submit(tester.evaluator.generate_schema, rcm)
                future_to_cid[future] = cid

            for future in as_completed(future_to_cid):
                cid = future_to_cid[future]
                done_count += 1
                try:
                    schema = future.result()
                    schemas[cid] = schema
                    logger.info(
                        "Schema for %s: %d attributes, %d columns",
                        cid, len(schema.attributes), len(schema.sample_columns),
                    )
                except Exception as e:
                    logger.error("Schema generation failed for %s: %s", cid, e)
                    schemas[cid] = TOE_Engine.ControlSchema(
                        control_id=cid, worksteps=[], attributes=[], sample_columns=[],
                    )
                update_tool_progress(
                    state, _TOOL_NAME, done_count, n_controls,
                    f"Generated attributes for {cid} ({done_count}/{n_controls})",
                )

        # Build structured output
        schema_list = []
        schemas_for_state = {}

        for cid in sorted(valid_ids):
            schema = schemas[cid]
            rcm = tester.rcm_lookup[cid]

            schema_dict = {
                "control_id": cid,
                "control_description": rcm.control_description or "",
                "control_type": rcm.control_type or "",
                "nature_of_control": rcm.nature_of_control or "",
                "control_frequency": rcm.control_frequency or "",
                "process": rcm.process or "",
                "subprocess": rcm.subprocess or "",
                "sample_count": 1,
                "worksteps": schema.worksteps,
                "attributes": schema.attributes,
                "sample_columns": schema.sample_columns,
            }
            schema_list.append(schema_dict)

            schemas_for_state[cid] = {
                "worksteps": schema.worksteps,
                "attributes": schema.attributes,
                "sample_columns": schema.sample_columns,
            }

        # Cache in state
        state.pending_toe_schemas = schemas_for_state
        state.pending_toe_evidence_folder = None

        total_attrs = sum(len(s["attributes"]) for s in schema_list)

        # Export editable attributes Excel
        editable_excel_path = None
        if state.output_dir is None:
            import tempfile
            state.output_dir = tempfile.mkdtemp(prefix="sox_agent_")
            os.makedirs(state.output_dir, exist_ok=True)
        try:
            from .export_attributes_excel import export_attributes_excel
            editable_excel_path = export_attributes_excel(
                schemas=schemas_for_state,
                output_dir=state.output_dir,
                phase="TOE",
                state=state,
            )
            logger.info("Editable attributes Excel: %s", editable_excel_path)
        except Exception as exc:
            logger.warning("Failed to export editable attributes Excel (non-fatal): %s", exc)

        logger.info(
            "Preview complete: %d controls, %d total attributes",
            len(schema_list), total_attrs,
        )

        result_data = {
            "controls_count": len(schema_list),
            "total_attributes": total_attrs,
            "schemas": schema_list,
            "requires_approval": True,
        }
        if editable_excel_path:
            result_data["editable_attributes_excel"] = editable_excel_path

        return ToolResult(
            success=True,
            data=result_data,
            artifacts=[editable_excel_path] if editable_excel_path else [],
            summary=(
                f"Generated TOE attributes for {len(schema_list)} controls "
                f"({total_attrs} total attributes). Awaiting user approval."
            ),
        )
