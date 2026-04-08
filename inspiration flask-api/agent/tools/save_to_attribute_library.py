"""Save approved control attributes to the global training library."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from ..tools.base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult

logger = logging.getLogger("agent.tools.save_to_attribute_library")


class SaveToAttributeLibraryTool(Tool):

    @property
    def name(self) -> str:
        return "save_to_attribute_library"

    @property
    def description(self) -> str:
        return (
            "Save approved control attributes to the global training library. "
            "These become reference examples for future schema generation, "
            "improving attribute quality over time. Only saves user-approved "
            "attributes. Ask the user for confirmation before calling this."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.UTILITY

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                name="control_ids",
                type="string",
                description=(
                    "Comma-separated control IDs to save (e.g. 'CA1,CA2,CA3'), "
                    "or 'all' to save all controls with approved schemas."
                ),
                required=True,
            ),
            ToolParameter(
                name="source",
                type="string",
                description="Which schemas to save: 'tod' or 'toe'. Defaults to 'tod'.",
                required=False,
                enum=["tod", "toe"],
            ),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        try:
            from engines.config import ENABLE_ATTRIBUTE_LIBRARY
            if not ENABLE_ATTRIBUTE_LIBRARY:
                return (
                    "Attribute library is disabled. "
                    "Set ENABLE_ATTRIBUTE_LIBRARY = True in engines/config.py."
                )
        except ImportError:
            return "Cannot import engines.config."

        has_schemas = (
            bool(getattr(state, "tod_schemas", None))
            or bool(getattr(state, "pending_tod_schemas", None))
            or bool(getattr(state, "toe_schemas", None))
            or bool(getattr(state, "pending_toe_schemas", None))
        )
        if not has_schemas:
            return "No approved schemas available. Run TOD or TOE first."
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        t0 = time.time()

        from engines.AttributeLibrary import get_library
        lib = get_library()

        if not lib.enabled:
            return ToolResult(
                success=False, data={},
                error="Attribute library is disabled (ENABLE_ATTRIBUTE_LIBRARY=False).",
            )

        source = (args.get("source") or "tod").strip().lower()
        control_ids_raw = args.get("control_ids", "all").strip()

        # Resolve schemas
        if source == "toe":
            schemas = (
                getattr(state, "toe_schemas", None)
                or getattr(state, "pending_toe_schemas", None)
            )
        else:
            schemas = (
                getattr(state, "tod_schemas", None)
                or getattr(state, "pending_tod_schemas", None)
            )

        if not schemas:
            return ToolResult(
                success=False, data={},
                error=f"No {source.upper()} schemas found in session state.",
            )

        # Target IDs
        if control_ids_raw.lower() == "all":
            target_ids = list(schemas.keys())
        else:
            target_ids = [c.strip() for c in control_ids_raw.split(",") if c.strip()]

        rcm_df = state.rcm_df
        saved: List[Dict] = []
        skipped: List[Dict] = []

        for cid in target_ids:
            schema_data = schemas.get(cid)
            if not schema_data:
                skipped.append({"control_id": cid, "reason": "No schema found"})
                continue

            # Extract attributes — handle ControlSchema objects and plain dicts
            if hasattr(schema_data, "attributes"):
                attributes = schema_data.attributes or []
                worksteps = schema_data.worksteps or []
            elif isinstance(schema_data, dict):
                attributes = schema_data.get("attributes", [])
                worksteps = schema_data.get("worksteps", [])
            else:
                skipped.append({"control_id": cid, "reason": "Unknown schema format"})
                continue

            if not attributes:
                skipped.append({"control_id": cid, "reason": "No attributes"})
                continue

            # Look up control metadata from RCM
            control_desc, control_type, nature, process, subprocess = (
                _lookup_rcm(rcm_df, cid)
            )

            if not control_desc:
                skipped.append({"control_id": cid, "reason": "No description in RCM"})
                continue

            doc_id = lib.store(
                control_description=control_desc,
                attributes=attributes,
                worksteps=worksteps,
                control_id=cid,
                control_type=control_type,
                nature=nature,
                process=process,
                subprocess=subprocess,
            )

            if doc_id:
                saved.append({
                    "control_id": cid,
                    "attributes_count": len(attributes),
                    "doc_id": doc_id,
                })
            else:
                skipped.append({"control_id": cid, "reason": "Cosmos storage failed"})

        duration = time.time() - t0

        return ToolResult(
            success=len(saved) > 0,
            data={
                "saved": saved,
                "saved_count": len(saved),
                "skipped": skipped,
                "skipped_count": len(skipped),
            },
            duration_seconds=duration,
            summary=(
                f"Saved {len(saved)} control(s) to training library"
                f"{f', skipped {len(skipped)}' if skipped else ''}."
            ),
        )


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _lookup_rcm(rcm_df, cid: str):
    """Extract control metadata from the RCM DataFrame."""
    control_desc = control_type = nature = process = subprocess = ""

    if rcm_df is None or rcm_df.empty:
        return control_desc, control_type, nature, process, subprocess

    # Find ID column
    id_col = None
    for c in ("Control Id", "Control ID", "control_id"):
        if c in rcm_df.columns:
            id_col = c
            break
    if not id_col:
        return control_desc, control_type, nature, process, subprocess

    row = rcm_df[rcm_df[id_col].astype(str).str.strip() == str(cid).strip()]
    if row.empty:
        return control_desc, control_type, nature, process, subprocess

    r = row.iloc[0]

    def _get(cols):
        for c in cols:
            if c in r.index:
                v = str(r[c]).strip()
                if v and v.lower() != "nan":
                    return v
        return ""

    control_desc = _get(["Control Description", "control_description"])
    control_type = _get(["Control Type", "control_type"])
    nature = _get(["Nature Of Control", "nature_of_control", "Nature"])
    process = _get(["Process", "process"])
    subprocess = _get(["Sub Process", "subprocess", "Sub-Process"])

    return control_desc, control_type, nature, process, subprocess
