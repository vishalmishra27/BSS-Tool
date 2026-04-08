"""Tools for reading files and listing directories.

Files are stored exclusively in Azure Blob Storage.
Blob paths are detected automatically and downloaded to a local temp
cache before processing (engines require real file paths).
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult

logger = logging.getLogger("agent.tools.filesystem")


def _get_blob_store():
    """Lazy import to avoid circular imports at module level."""
    from server.blob_store import get_blob_store
    return get_blob_store()


def _is_blob_path(path: str) -> bool:
    """Check if a path is a blob storage path."""
    from server.blob_store import BlobStore
    return BlobStore.is_blob_path(path)


# ═══════════════════════════════════════════════════════════════════════════
# list_directory
# ═══════════════════════════════════════════════════════════════════════════

class ListDirectoryTool(Tool):
    @property
    def name(self) -> str:
        return "list_directory"

    @property
    def description(self) -> str:
        return (
            "List files and folders in a directory, optionally filtered by extension. "
            "Use recursive=true to walk subdirectories. "
            "Supports both local paths and Azure Blob Storage paths. "
            "Use this to find RCM files, evidence folders, or policy/SOP PDFs."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.FILESYSTEM

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter("path", "string", "Directory path to list (local or blob path)"),
            ToolParameter("extension", "string", "File extension filter, e.g. '.xlsx'", required=False),
            ToolParameter("recursive", "boolean", "Walk subdirectories (default false)", required=False),
        ]

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        path = args["path"].strip().strip("'\"")
        ext = args.get("extension", "")
        recursive = args.get("recursive", False)

        # Check if this is a blob path
        if _is_blob_path(path):
            return self._list_blob_directory(path, ext, recursive)

        # Local filesystem listing
        if not os.path.exists(path):
            return ToolResult(success=False, data={}, error=f"Directory not found: {path}")
        if not os.path.isdir(path):
            return ToolResult(success=False, data={}, error=f"Not a directory: {path}")

        items: List[Dict[str, Any]] = []

        if recursive:
            for root, dirs, files in os.walk(path):
                rel_root = os.path.relpath(root, path)
                if rel_root == ".":
                    rel_root = ""
                for fname in sorted(files):
                    if ext and not fname.lower().endswith(ext.lower()):
                        continue
                    full = os.path.join(root, fname)
                    rel_path = os.path.join(rel_root, fname) if rel_root else fname
                    items.append({
                        "name": rel_path, "type": "file",
                        "size_kb": round(os.path.getsize(full) / 1024, 1),
                    })
                    if len(items) >= 500:
                        break
                if len(items) >= 500:
                    break
        else:
            for entry in sorted(os.scandir(path), key=lambda e: e.name):
                if entry.is_file():
                    if ext and not entry.name.lower().endswith(ext.lower()):
                        continue
                    items.append({
                        "name": entry.name, "type": "file",
                        "size_kb": round(entry.stat().st_size / 1024, 1),
                    })
                elif entry.is_dir():
                    items.append({"name": entry.name + "/", "type": "directory"})

        return ToolResult(
            success=True,
            data={"directory": path, "items": items, "count": len(items),
                  "recursive": recursive},
            summary=f"{len(items)} items in {path}",
        )

    def _list_blob_directory(self, path: str, ext: str, recursive: bool) -> ToolResult:
        """List files in Azure Blob Storage under a prefix."""
        store = _get_blob_store()
        if not store.available:
            return ToolResult(
                success=False, data={},
                error="Azure Blob Storage is not available.",
            )

        clean = path.removeprefix("blob://")
        # Ensure prefix ends with /
        if not clean.endswith("/"):
            clean += "/"

        items: List[Dict[str, Any]] = []

        if recursive:
            # Flat listing of all blobs under prefix
            blobs = store.list_blobs(clean)
            for blob in blobs:
                name = blob["name"][len(clean):]
                if not name:
                    continue
                if ext and not name.lower().endswith(ext.lower()):
                    continue
                items.append({
                    "name": name,
                    "type": "file",
                    "size_kb": round(blob["size"] / 1024, 1) if blob["size"] else 0,
                })
                if len(items) >= 500:
                    break
        else:
            # Hierarchical listing (one level)
            blobs = store.list_blobs(clean, delimiter="/")
            for blob in blobs:
                if blob["is_prefix"]:
                    # Virtual directory
                    dir_name = blob["name"][len(clean):].rstrip("/")
                    items.append({"name": dir_name + "/", "type": "directory"})
                else:
                    name = blob["name"][len(clean):]
                    if not name:
                        continue
                    if ext and not name.lower().endswith(ext.lower()):
                        continue
                    items.append({
                        "name": name,
                        "type": "file",
                        "size_kb": round(blob["size"] / 1024, 1) if blob["size"] else 0,
                    })

        return ToolResult(
            success=True,
            data={"directory": path, "items": items, "count": len(items),
                  "recursive": recursive, "storage": "blob"},
            summary=f"{len(items)} items in {path} (blob storage)",
        )


# ═══════════════════════════════════════════════════════════════════════════
# read_file
# ═══════════════════════════════════════════════════════════════════════════

class ReadFileTool(Tool):
    @property
    def name(self) -> str:
        return "read_file"

    @property
    def description(self) -> str:
        return (
            "Read the contents of a file. Supports text files (.txt, .csv, .json, .py, .md, .log), "
            "Excel (.xlsx returns first sheet as table), and PDF (.pdf extracts text). "
            "Returns the first 200 lines by default. "
            "Supports both local paths and Azure Blob Storage paths. "
            "NOTE: Do NOT use this for RCM files — use load_rcm instead, which performs "
            "smart header detection, column mapping, and marker normalisation."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.FILESYSTEM

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter("file_path", "string", "Absolute path or blob path to the file"),
            ToolParameter("max_lines", "integer", "Max lines to return (default 200, max 500)", required=False),
            ToolParameter("encoding", "string", "Text encoding (default utf-8)", required=False),
        ]

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        file_path = args["file_path"].strip().strip("'\"")
        max_lines = min(args.get("max_lines", 200), 500)
        encoding = args.get("encoding", "utf-8")

        # If blob path, download to local cache first
        if _is_blob_path(file_path):
            store = _get_blob_store()
            if not store.available:
                return ToolResult(
                    success=False, data={},
                    error="Azure Blob Storage is not available.",
                )
            local_path = store.ensure_local(file_path)
            if not local_path:
                return ToolResult(
                    success=False, data={},
                    error=f"Failed to download blob: {file_path}",
                )
            file_path = local_path

        if not os.path.exists(file_path):
            return ToolResult(success=False, data={}, error=f"File not found: {file_path}")
        if os.path.isdir(file_path):
            return ToolResult(success=False, data={}, error=f"Path is a directory: {file_path}")

        size = os.path.getsize(file_path)
        if size > 50 * 1024 * 1024:
            return ToolResult(success=False, data={}, error=f"File too large: {size / 1e6:.1f} MB")

        ext = os.path.splitext(file_path)[1].lower()

        try:
            if ext in (".xlsx", ".xls", ".xlsm"):
                try:
                    import sys as _sys
                    from pathlib import Path as _Path
                    _di_dir = str(_Path(__file__).resolve().parent.parent.parent)
                    if _di_dir not in _sys.path:
                        _sys.path.insert(0, _di_dir)
                    from Document_Intelligence import parse_document
                    result = parse_document(file_path)
                    # Build preview from parsed tables
                    content_parts = []
                    sheet_names = result.metadata.sheet_names or []
                    total_rows = 0
                    columns = []
                    for tbl in result.tables:
                        sheet_label = f"[Sheet: {tbl.sheet_name}]\n" if tbl.sheet_name else ""
                        header_line = " | ".join(tbl.headers)
                        row_lines = [" | ".join(r) for r in tbl.rows[:max_lines]]
                        content_parts.append(f"{sheet_label}{header_line}\n" + "\n".join(row_lines))
                        total_rows += len(tbl.rows)
                        if not columns:
                            columns = tbl.headers
                    content = "\n\n".join(content_parts) if content_parts else result.full_text[:5000]
                    return ToolResult(success=True, data={
                        "file_path": file_path, "file_type": "excel",
                        "sheet_names": sheet_names, "total_rows": total_rows,
                        "columns": columns, "content": content,
                        "truncated": total_rows > max_lines,
                        "parse_status": result.parse_status.value,
                    })
                except Exception:
                    # Fallback to direct pandas
                    df = pd.read_excel(file_path, sheet_name=0, dtype=str)
                    content = df.head(max_lines).to_string(index=False)
                    return ToolResult(success=True, data={
                        "file_path": file_path, "file_type": "excel",
                        "total_rows": len(df), "columns": list(df.columns),
                        "content": content, "truncated": len(df) > max_lines,
                    })

            if ext == ".pdf":
                try:
                    import sys as _sys
                    from pathlib import Path as _Path
                    _di_dir = str(_Path(__file__).resolve().parent.parent.parent)
                    if _di_dir not in _sys.path:
                        _sys.path.insert(0, _di_dir)
                    from Document_Intelligence import parse_document
                    result = parse_document(file_path)
                    content = result.full_text or ""
                    lines = content.split("\n")
                    page_count = result.metadata.page_count or 1
                    return ToolResult(success=True, data={
                        "file_path": file_path, "file_type": "pdf",
                        "total_pages": page_count,
                        "content": "\n".join(lines[:max_lines]),
                        "truncated": len(lines) > max_lines,
                    })
                except Exception as e:
                    return ToolResult(success=False, data={},
                                      error=f"PDF reading failed: {e}")

            # Default: read as text
            with open(file_path, "r", encoding=encoding, errors="replace") as f:
                all_lines = f.readlines()
            content = "".join(all_lines[:max_lines])
            if len(content) > 20000:
                content = content[:20000] + "\n... (truncated at 20000 chars)"
            return ToolResult(success=True, data={
                "file_path": file_path, "file_type": ext or "text",
                "total_lines": len(all_lines), "lines_shown": min(max_lines, len(all_lines)),
                "content": content, "truncated": len(all_lines) > max_lines,
            })

        except Exception as exc:
            return ToolResult(success=False, data={}, error=f"Failed to read: {exc}")
