"""Tools for modifying, adding, and removing preview attributes via agent chat.

These operate on state.pending_tod_schemas or state.pending_toe_schemas
(whichever is active) BEFORE the user approves and runs TOD/TOE.
Changes made here persist through the TOD → tod_schemas → TOE chain.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult

logger = logging.getLogger("agent.tools.modify_attributes")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_pending_schemas(state: AgentState) -> tuple[Optional[Dict], str]:
    """Return (pending_schemas_dict, phase_label) for whichever preview is active."""
    if state.pending_tod_schemas:
        return state.pending_tod_schemas, "TOD"
    if state.pending_toe_schemas:
        return state.pending_toe_schemas, "TOE"
    return None, ""


def _resequence_ids(attributes: List[Dict]) -> List[Dict]:
    """Re-number attribute IDs as '1', '2', '3', ... after any mutation."""
    for idx, attr in enumerate(attributes, 1):
        attr["id"] = str(idx)
    return attributes


def _format_attrs_for_display(control_id: str, attributes: List[Dict]) -> List[Dict]:
    """Return a clean list for the ToolResult data."""
    return [
        {"no": attr["id"], "name": attr.get("name", ""), "description": attr.get("description", "")}
        for attr in attributes
    ]


def _sync_preview_cache(state: AgentState, phase: str) -> None:
    """Rebuild the cached preview_*_attributes entry in tool_results_cache
    so that page-refresh restore picks up in-session attribute edits."""
    import json

    if phase == "TOD" and state.pending_tod_schemas:
        schemas = state.pending_tod_schemas
        cache_key = "preview_tod_attributes"
    elif phase == "TOE" and state.pending_toe_schemas:
        schemas = state.pending_toe_schemas
        cache_key = "preview_toe_attributes"
    else:
        return

    # Rebuild the schema list in the same format preview_tod/toe_attributes returns
    schema_list = []
    for cid, schema in schemas.items():
        attrs = schema.get("attributes", [])
        schema_list.append({
            "control_id": cid,
            "worksteps": schema.get("worksteps", []),
            "attributes": [
                {"no": a.get("id", str(i + 1)), "name": a.get("name", ""), "description": a.get("description", "")}
                for i, a in enumerate(attrs)
            ],
            "sample_columns": schema.get("sample_columns", []),
        })

    updated_preview = {"requires_approval": True, "schemas": schema_list}
    state.tool_results_cache[cache_key] = json.dumps(updated_preview)
    logger.info("Synced %s preview cache after attribute edit (%d schemas)", cache_key, len(schema_list))


# ---------------------------------------------------------------------------
# Modify Attribute
# ---------------------------------------------------------------------------

class ModifyAttributeTool(Tool):
    @property
    def name(self) -> str:
        return "modify_attribute"

    @property
    def description(self) -> str:
        return (
            "Modify an existing testing attribute's name or description for a control "
            "during the TOD/TOE preview stage. Provide the control ID and attribute "
            "number (e.g. 1, 2, 3) along with the updated name and/or description."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.UTILITY

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter("control_id", "string", "The Control ID (e.g. C-P2P-001)", required=True),
            ToolParameter("attribute_no", "integer", "The attribute serial number to modify (e.g. 1, 2, 3)", required=True),
            ToolParameter("name", "string", "New attribute name (short, 3-6 words). Leave empty to keep current.", required=False),
            ToolParameter("description", "string", "New attribute description. Leave empty to keep current.", required=False),
        ]

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        schemas, phase = _get_pending_schemas(state)
        if schemas is None:
            return ToolResult(
                success=False, data={},
                error="No pending attribute preview found. Run preview_tod_attributes or preview_toe_attributes first.",
            )

        control_id = (args.get("control_id") or "").strip()
        if control_id not in schemas:
            available = ", ".join(sorted(schemas.keys()))
            return ToolResult(
                success=False, data={"available_controls": available},
                error=f"Control ID '{control_id}' not found in pending {phase} schemas. Available: {available}",
            )

        attr_no = args.get("attribute_no")
        try:
            attr_no = int(attr_no)
        except (TypeError, ValueError):
            return ToolResult(success=False, data={}, error="attribute_no must be an integer (e.g. 1, 2, 3).")

        attributes = schemas[control_id].get("attributes", [])
        if attr_no < 1 or attr_no > len(attributes):
            return ToolResult(
                success=False, data={},
                error=f"Attribute #{attr_no} does not exist for {control_id}. Valid range: 1-{len(attributes)}.",
            )

        attr = attributes[attr_no - 1]
        new_name = (args.get("name") or "").strip()
        new_desc = (args.get("description") or "").strip()

        if not new_name and not new_desc:
            return ToolResult(success=False, data={}, error="Provide at least one of 'name' or 'description' to update.")

        changes = []
        if new_name:
            old_name = attr.get("name", "")
            attr["name"] = new_name
            changes.append(f"name: '{old_name}' → '{new_name}'")
        if new_desc:
            old_desc = attr.get("description", "")
            attr["description"] = new_desc
            changes.append(f"description updated")

        logger.info("Modified attribute #%d for %s (%s): %s", attr_no, control_id, phase, ", ".join(changes))
        _sync_preview_cache(state, phase)

        return ToolResult(
            success=True,
            data={
                "control_id": control_id,
                "phase": phase,
                "modified_attribute": attr_no,
                "changes": changes,
                "attributes": _format_attrs_for_display(control_id, attributes),
            },
            summary=f"Modified attribute #{attr_no} for {control_id}: {', '.join(changes)}.",
        )


# ---------------------------------------------------------------------------
# Add Attribute
# ---------------------------------------------------------------------------

class AddAttributeTool(Tool):
    @property
    def name(self) -> str:
        return "add_attribute"

    @property
    def description(self) -> str:
        return (
            "Add a new testing attribute to a control during the TOD/TOE preview stage. "
            "Provide the control ID, attribute name, description, and optionally the "
            "position (serial number) where it should be inserted. If no position is "
            "given, the attribute is appended at the end."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.UTILITY

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter("control_id", "string", "The Control ID (e.g. C-P2P-001)", required=True),
            ToolParameter("name", "string", "Attribute name (short, 3-6 words)", required=True),
            ToolParameter("description", "string", "What evidence to verify for this attribute", required=True),
            ToolParameter("position", "integer",
                          "Insert at this serial number position (e.g. 2 inserts as #2, shifting others down). "
                          "If omitted, appends at the end.", required=False),
        ]

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        schemas, phase = _get_pending_schemas(state)
        if schemas is None:
            return ToolResult(
                success=False, data={},
                error="No pending attribute preview found. Run preview_tod_attributes or preview_toe_attributes first.",
            )

        control_id = (args.get("control_id") or "").strip()
        if control_id not in schemas:
            available = ", ".join(sorted(schemas.keys()))
            return ToolResult(
                success=False, data={"available_controls": available},
                error=f"Control ID '{control_id}' not found in pending {phase} schemas. Available: {available}",
            )

        attr_name = (args.get("name") or "").strip()
        attr_desc = (args.get("description") or "").strip()
        if not attr_name or not attr_desc:
            return ToolResult(success=False, data={}, error="Both 'name' and 'description' are required.")

        attributes = schemas[control_id].get("attributes", [])
        new_attr = {"id": "", "name": attr_name, "description": attr_desc}

        position = args.get("position")
        if position is not None:
            try:
                position = int(position)
            except (TypeError, ValueError):
                return ToolResult(success=False, data={}, error="position must be an integer.")
            if position < 1:
                position = 1
            if position > len(attributes) + 1:
                position = len(attributes) + 1
            attributes.insert(position - 1, new_attr)
            insert_label = f"at position #{position}"
        else:
            attributes.append(new_attr)
            insert_label = f"at position #{len(attributes)}"

        # Re-sequence all IDs
        _resequence_ids(attributes)
        schemas[control_id]["attributes"] = attributes

        logger.info("Added attribute '%s' to %s %s (%s)", attr_name, control_id, insert_label, phase)
        _sync_preview_cache(state, phase)

        return ToolResult(
            success=True,
            data={
                "control_id": control_id,
                "phase": phase,
                "added": {"name": attr_name, "position": insert_label},
                "total_attributes": len(attributes),
                "attributes": _format_attrs_for_display(control_id, attributes),
            },
            summary=f"Added attribute '{attr_name}' to {control_id} {insert_label}. Total: {len(attributes)} attributes.",
        )


# ---------------------------------------------------------------------------
# Remove Attribute
# ---------------------------------------------------------------------------

class RemoveAttributeTool(Tool):
    @property
    def name(self) -> str:
        return "remove_attribute"

    @property
    def description(self) -> str:
        return (
            "Remove a testing attribute from a control during the TOD/TOE preview stage. "
            "Provide the control ID and the attribute serial number to remove."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.UTILITY

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter("control_id", "string", "The Control ID (e.g. C-P2P-001)", required=True),
            ToolParameter("attribute_no", "integer", "The attribute serial number to remove (e.g. 1, 2, 3)", required=True),
        ]

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        schemas, phase = _get_pending_schemas(state)
        if schemas is None:
            return ToolResult(
                success=False, data={},
                error="No pending attribute preview found. Run preview_tod_attributes or preview_toe_attributes first.",
            )

        control_id = (args.get("control_id") or "").strip()
        if control_id not in schemas:
            available = ", ".join(sorted(schemas.keys()))
            return ToolResult(
                success=False, data={"available_controls": available},
                error=f"Control ID '{control_id}' not found in pending {phase} schemas. Available: {available}",
            )

        attr_no = args.get("attribute_no")
        try:
            attr_no = int(attr_no)
        except (TypeError, ValueError):
            return ToolResult(success=False, data={}, error="attribute_no must be an integer (e.g. 1, 2, 3).")

        attributes = schemas[control_id].get("attributes", [])
        if attr_no < 1 or attr_no > len(attributes):
            return ToolResult(
                success=False, data={},
                error=f"Attribute #{attr_no} does not exist for {control_id}. Valid range: 1-{len(attributes)}.",
            )

        removed = attributes.pop(attr_no - 1)
        _resequence_ids(attributes)
        schemas[control_id]["attributes"] = attributes

        logger.info(
            "Removed attribute #%d ('%s') from %s (%s). Remaining: %d",
            attr_no, removed.get("name", ""), control_id, phase, len(attributes),
        )
        _sync_preview_cache(state, phase)

        return ToolResult(
            success=True,
            data={
                "control_id": control_id,
                "phase": phase,
                "removed": {"no": attr_no, "name": removed.get("name", "")},
                "total_attributes": len(attributes),
                "attributes": _format_attrs_for_display(control_id, attributes),
            },
            summary=f"Removed attribute #{attr_no} ('{removed.get('name', '')}') from {control_id}. Remaining: {len(attributes)}.",
        )
