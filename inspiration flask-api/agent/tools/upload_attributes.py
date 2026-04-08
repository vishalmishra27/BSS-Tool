"""Upload a modified attributes Excel to replace pending TOD/TOE schemas.

When preview_tod_attributes or preview_toe_attributes runs, it also exports
an editable Excel (one row per attribute). The user can download it, edit
attribute names/descriptions, add rows, or delete rows, then upload it back.
This tool reads that Excel and replaces the pending schemas accordingly.

The Excel format (exported by preview):
  Column A: Control ID
  Column B: Attribute #  (1, 2, 3, …)
  Column C: Attribute Name
  Column D: Attribute Description

After uploading, the schemas are updated and the user is shown the new
attributes for re-approval.
"""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult

logger = logging.getLogger("agent.tools.upload_attributes")


def _resolve_file_path(path: str) -> Optional[str]:
    """If path is a blob path, download to local cache. Otherwise return as-is."""
    try:
        from server.blob_store import get_blob_store, BlobStore
        if BlobStore.is_blob_path(path):
            store = get_blob_store()
            if not store.available:
                return None
            local = store.ensure_local_file(path)
            if local:
                logger.info("Downloaded blob file %s → %s", path, local)
                return local
            return None
    except Exception:
        pass
    return path


def _format_attrs_for_display(control_id: str, attributes: List[Dict]) -> List[Dict]:
    return [
        {"no": str(i + 1), "name": a.get("name", ""), "description": a.get("description", "")}
        for i, a in enumerate(attributes)
    ]


class UploadModifiedAttributesTool(Tool):
    @property
    def name(self) -> str:
        return "upload_modified_attributes"

    @property
    def description(self) -> str:
        return (
            "Upload a modified attributes Excel file to replace the current pending "
            "TOD/TOE attributes. The user can download the attributes Excel exported "
            "during preview, edit it (change names, descriptions, add/remove rows), "
            "then upload it back. The tool reads the Excel, updates all pending schemas, "
            "and re-displays the attributes for approval. "
            "The Excel must have columns: Control ID, Attribute #, Attribute Name, "
            "Attribute Description."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.UTILITY

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                "file_path", "string",
                "Path to the uploaded modified attributes Excel file.",
                required=True,
            ),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if not state.pending_tod_schemas and not state.pending_toe_schemas:
            return (
                "No pending attribute preview found. Run preview_tod_attributes or "
                "preview_toe_attributes first before uploading modified attributes."
            )
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        file_path = (args.get("file_path") or "").strip().strip("'\"")
        if not file_path:
            return ToolResult(
                success=False, data={},
                error="No file path provided. Ask the user for the path to their modified attributes Excel.",
            )

        # Resolve blob paths
        resolved = _resolve_file_path(file_path)
        if resolved is None:
            return ToolResult(
                success=False, data={},
                error=f"Could not access file: {file_path}",
            )
        if not os.path.exists(resolved):
            return ToolResult(
                success=False, data={"attempted_path": resolved},
                error=f"File not found at: {resolved}. Please provide the correct path.",
            )

        # Determine which phase is active
        if state.pending_tod_schemas:
            schemas = state.pending_tod_schemas
            phase = "TOD"
        else:
            schemas = state.pending_toe_schemas
            phase = "TOE"

        # Read the Excel
        try:
            df = pd.read_excel(resolved, engine="openpyxl")
        except Exception as exc:
            return ToolResult(
                success=False, data={},
                error=f"Failed to read Excel file: {exc}",
            )

        # Normalize column names (flexible matching)
        col_map = {}
        for col in df.columns:
            lower = str(col).strip().lower()
            if lower in ("control id", "control_id", "controlid"):
                col_map[col] = "control_id"
            elif lower in ("attribute #", "attribute no", "attribute_no", "no", "#", "number"):
                col_map[col] = "attr_no"
            elif lower in ("attribute name", "attribute_name", "name"):
                col_map[col] = "attr_name"
            elif lower in ("attribute description", "attribute_description", "description"):
                col_map[col] = "attr_desc"

        df = df.rename(columns=col_map)

        # Validate required columns
        required = {"control_id", "attr_name", "attr_desc"}
        missing = required - set(df.columns)
        if missing:
            return ToolResult(
                success=False, data={
                    "found_columns": list(df.columns),
                    "expected": ["Control ID", "Attribute Name", "Attribute Description"],
                },
                error=(
                    f"Missing columns in uploaded Excel: {', '.join(missing)}. "
                    "Expected columns: Control ID, Attribute #, Attribute Name, Attribute Description."
                ),
            )

        # Drop rows with empty control_id or attribute name
        df["control_id"] = df["control_id"].astype(str).str.strip()
        df["attr_name"] = df["attr_name"].astype(str).str.strip()
        df["attr_desc"] = df["attr_desc"].astype(str).str.strip()
        df = df[df["control_id"].notna() & (df["control_id"] != "") & (df["control_id"].str.lower() != "nan")]
        df = df[df["attr_name"].notna() & (df["attr_name"] != "") & (df["attr_name"].str.lower() != "nan")]

        if df.empty:
            return ToolResult(
                success=False, data={},
                error="The uploaded Excel contains no valid attribute rows after filtering.",
            )

        # Group attributes by control ID
        new_attrs_by_control: Dict[str, List[Dict]] = defaultdict(list)
        for _, row in df.iterrows():
            cid = row["control_id"]
            new_attrs_by_control[cid].append({
                "id": str(len(new_attrs_by_control[cid]) + 1),
                "name": row["attr_name"],
                "description": row["attr_desc"],
            })

        # Update pending schemas — only update attributes, keep worksteps and sample_columns
        controls_updated = 0
        controls_added = 0
        controls_removed = []
        original_control_ids = set(schemas.keys())
        uploaded_control_ids = set(new_attrs_by_control.keys())

        # Update existing controls and add new ones
        for cid, attrs in new_attrs_by_control.items():
            if cid in schemas:
                schemas[cid]["attributes"] = attrs
                controls_updated += 1
            else:
                # New control in upload that wasn't in preview — add with empty worksteps/columns
                schemas[cid] = {
                    "worksteps": [],
                    "attributes": attrs,
                    "sample_columns": [],
                }
                controls_added += 1

        # Controls in original schemas but NOT in the upload — remove them
        for cid in original_control_ids - uploaded_control_ids:
            del schemas[cid]
            controls_removed.append(cid)

        # Re-number all attributes
        for cid in schemas:
            for i, attr in enumerate(schemas[cid].get("attributes", []), 1):
                attr["id"] = str(i)

        # Sync the preview cache for page-refresh
        from .modify_attributes import _sync_preview_cache
        _sync_preview_cache(state, phase)

        # Build display output
        schema_list = []
        for cid in sorted(schemas.keys()):
            attrs = schemas[cid].get("attributes", [])
            schema_list.append({
                "control_id": cid,
                "attributes": _format_attrs_for_display(cid, attrs),
                "attribute_count": len(attrs),
            })

        total_attrs = sum(len(schemas[cid].get("attributes", [])) for cid in schemas)

        summary_parts = [f"Updated {phase} attributes from uploaded Excel"]
        if controls_updated:
            summary_parts.append(f"{controls_updated} controls updated")
        if controls_added:
            summary_parts.append(f"{controls_added} controls added")
        if controls_removed:
            summary_parts.append(f"{len(controls_removed)} controls removed ({', '.join(controls_removed)})")
        summary_parts.append(f"{total_attrs} total attributes across {len(schemas)} controls")

        logger.info(
            "Attributes uploaded from Excel for %s: %d controls, %d total attributes "
            "(updated=%d, added=%d, removed=%d)",
            phase, len(schemas), total_attrs,
            controls_updated, controls_added, len(controls_removed),
        )

        return ToolResult(
            success=True,
            data={
                "phase": phase,
                "controls_count": len(schemas),
                "total_attributes": total_attrs,
                "controls_updated": controls_updated,
                "controls_added": controls_added,
                "controls_removed": controls_removed,
                "schemas": schema_list,
                "requires_approval": True,
            },
            summary=". ".join(summary_parts) + ".",
        )
