"""Preview TOD attributes for user approval before running the full test.

No evidence folder required — schemas are generated for all controls in the
RCM.  The evidence folder is requested later (after attribute approval and
document list generation) by run_test_of_design.
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

logger = logging.getLogger("agent.tools.preview_tod_attributes")


class PreviewTodAttributesTool(Tool):
    @property
    def name(self) -> str:
        return "preview_tod_attributes"

    @property
    def description(self) -> str:
        return (
            "Generate and preview TOD testing attributes for user approval BEFORE "
            "running the full Test of Design. Shows the LLM-generated attributes, "
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
        if "Control Id" not in state.rcm_df.columns:
            return "RCM does not have a 'Control Id' column."
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        config = get_config()

        rcm_df = state.rcm_df.copy()

        # Ensure required columns
        if "risk_level" not in rcm_df.columns:
            rcm_df["risk_level"] = "High"

        # Normalize Control Id values
        rcm_df["Control Id"] = rcm_df["Control Id"].astype(str).str.strip()

        # Get unique control IDs from RCM
        control_ids = rcm_df["Control Id"].dropna().unique().tolist()
        control_ids = [cid for cid in control_ids if cid and cid.lower() != "nan"]

        if not control_ids:
            return ToolResult(
                success=False, data={},
                error="No Control IDs found in the RCM.",
            )

        # ── Import TOD engine (for RCMControlTester + schema generation only) ──
        engines_dir = os.path.join(os.path.dirname(__file__), "..", "..", "engines")
        engines_dir = os.path.abspath(engines_dir)
        if engines_dir not in sys.path:
            sys.path.insert(0, engines_dir)

        import TOD_Engine
        reload(TOD_Engine)

        # Create tester (needed for rcm_lookup) — pass pre-normalised DataFrame
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

        # Filter to controls that exist in the RCM lookup
        valid_ids = [cid for cid in control_ids if cid in tester.rcm_lookup]

        if not valid_ids:
            return ToolResult(
                success=False, data={},
                error="No valid controls found in RCM lookup.",
            )

        # Generate schemas in parallel for all controls
        logger.info("Generating schemas for %d controls...", len(valid_ids))
        schemas = {}
        max_workers = 5

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_cid = {}
            for cid in sorted(valid_ids):
                rcm = tester.rcm_lookup[cid]
                future = executor.submit(tester.evaluator.generate_schema, rcm)
                future_to_cid[future] = cid

            for future in as_completed(future_to_cid):
                cid = future_to_cid[future]
                try:
                    schema = future.result()
                    schemas[cid] = schema
                    logger.info(
                        "Schema for %s: %d attributes, %d columns",
                        cid, len(schema.attributes), len(schema.sample_columns),
                    )
                except Exception as e:
                    logger.error("Schema generation failed for %s: %s", cid, e)
                    schemas[cid] = TOD_Engine.ControlSchema(
                        control_id=cid, worksteps=[], attributes=[], sample_columns=[],
                    )

        # Build structured output for the frontend
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
                "sample_count": 1,  # TOD always uses 1 sample
                "worksteps": schema.worksteps,
                "attributes": schema.attributes,
                "sample_columns": schema.sample_columns,
            }
            schema_list.append(schema_dict)

            # Store for state caching (keyed by control_id)
            schemas_for_state[cid] = {
                "worksteps": schema.worksteps,
                "attributes": schema.attributes,
                "sample_columns": schema.sample_columns,
            }

        # Cache in state for later use by run_test_of_design
        state.pending_tod_schemas = schemas_for_state
        # No evidence folder at this stage — will be asked after approval
        state.pending_tod_evidence_folder = None

        total_attrs = sum(len(s["attributes"]) for s in schema_list)

        # Export editable attributes Excel so user can download, edit, re-upload
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
                phase="TOD",
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
                f"Generated TOD attributes for {len(schema_list)} controls "
                f"({total_attrs} total attributes). Awaiting user approval."
            ),
        )
