"""Tools for loading and inspecting the RCM DataFrame.

Files are stored exclusively in Azure Blob Storage.
Blob-stored files are downloaded to a local cache before processing.
Generated artifacts (normalised RCM) are uploaded back to Blob Storage.
"""

from __future__ import annotations

import os
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..utils import (
    RCM_REQUIRED_COLUMNS, normalize_rcm_columns, sanitize_for_json,
)

logger = logging.getLogger("agent.tools.rcm_loader")


def _get_blob_store():
    """Lazy import to avoid circular imports at module level."""
    from server.blob_store import get_blob_store
    return get_blob_store()


def _is_blob_path(path: str) -> bool:
    from server.blob_store import BlobStore
    return BlobStore.is_blob_path(path)


def _upload_artifact(local_path: str, state: Optional["AgentState"] = None) -> str:
    """Upload a generated artifact to blob storage. Returns blob path, or local path as fallback."""
    try:
        store = _get_blob_store()
        if not store.available:
            logger.warning("Blob Storage not available — artifact stays local: %s", local_path)
            return local_path
        filename = os.path.basename(local_path)
        # Use the output_dir basename as session key for organization
        session_key = "default"
        if state and state.output_dir:
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

def _read_excel_with_header_detection(file_path: str) -> tuple[pd.DataFrame, int, int]:
    """Read Excel with smart header detection and marker normalization.

    Delegates to the shared rcm_reader module which handles logos, metadata
    rows, merged cells, and Unicode marker characters (●, ✓, etc.).
    """
    from engines.rcm_reader import smart_read_excel
    return smart_read_excel(file_path, return_details=True)


# ═══════════════════════════════════════════════════════════════════════════
# load_rcm
# ═══════════════════════════════════════════════════════════════════════════

class LoadRCMTool(Tool):
    @property
    def name(self) -> str:
        return "load_rcm"

    @property
    def description(self) -> str:
        return (
            "Load an RCM Excel or CSV file into the working state. "
            "Returns row count, columns, and a preview of the first 5 rows."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.DATA

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter("file_path", "string", "Absolute path to the RCM file (.xlsx or .csv)"),
        ]

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        file_path = args["file_path"].strip().strip("'\"")

        # If blob path, download to local cache first
        if _is_blob_path(file_path):
            store = _get_blob_store()
            if not store.available:
                return ToolResult(success=False, data={}, error="Blob Storage not available")
            local_path = store.ensure_local(file_path)
            if not local_path:
                return ToolResult(success=False, data={}, error=f"Failed to download blob: {file_path}")
            logger.info("Downloaded blob %s to %s", file_path, local_path)
            file_path = local_path

        if not os.path.exists(file_path):
            return ToolResult(success=False, data={}, error=f"File not found: {file_path}")

        logger.info("Loading RCM from %s", file_path)
        # Full pipeline: header detect + markers + column normalisation (exact + LLM)
        from engines.rcm_reader import smart_read_file
        original_df = None

        # Detect multi-sheet Excel workbooks (e.g. scoping engine output with one sheet per process)
        multi_sheet = False
        sheet_names: List[str] = []
        if not file_path.endswith(".csv"):
            try:
                xl = pd.ExcelFile(file_path)
                sheet_names = list(dict.fromkeys(xl.sheet_names))  # dedupe preserving order
                if len(sheet_names) > 1:
                    multi_sheet = True
                    logger.info("Multi-sheet workbook detected: %d sheets %s", len(sheet_names), sheet_names)
            except Exception:
                pass

        try:
            if multi_sheet:
                # Read and concatenate all sheets for multi-sheet RCM workbooks
                original_frames = []
                normalised_frames = []
                detected_header_row = 1
                detected_header_score = 0
                col_map = {}
                for sheet in sheet_names:
                    try:
                        orig_sheet = smart_read_file(file_path, sheet_name=sheet, normalize_columns=False)
                        norm_sheet, info = smart_read_file(file_path, sheet_name=sheet, normalize_columns=True)
                        original_frames.append(orig_sheet)
                        normalised_frames.append(norm_sheet)
                        # Use info from the first successfully parsed sheet
                        if not col_map:
                            detected_header_row = info.get("header_row", 1)
                            detected_header_score = info.get("score", 0)
                            col_map = info.get("column_map", {})
                    except Exception as sheet_err:
                        logger.warning("Failed to read sheet '%s': %s", sheet, sheet_err)
                if not normalised_frames:
                    raise ValueError("No sheets could be read from multi-sheet workbook")
                original_df = pd.concat(original_frames, ignore_index=True)
                df = pd.concat(normalised_frames, ignore_index=True)
                # Drop duplicate rows from sheets that are copies of each other
                # (e.g. 'Sheet1' and 'Sheet1 (2)' containing identical data)
                # Use Control Id as the dedup key since full-row comparison may fail
                # when normalization produces slight differences between sheets.
                pre_dedup = len(df)
                if "Control Id" in df.columns:
                    df = df.drop_duplicates(subset=["Control Id"]).reset_index(drop=True)
                    # Keep original_df in sync — same Control Id dedup
                    _orig_cid_col = next(
                        (c for c in original_df.columns if c.strip().lower().replace(" ", "_") in
                         ("control_id", "controlid", "control_no", "control_no.")),
                        None,
                    )
                    if _orig_cid_col:
                        original_df = original_df.drop_duplicates(subset=[_orig_cid_col]).reset_index(drop=True)
                    else:
                        original_df = original_df.drop_duplicates().reset_index(drop=True)
                else:
                    original_df = original_df.drop_duplicates().reset_index(drop=True)
                    df = df.drop_duplicates().reset_index(drop=True)
                if len(df) < pre_dedup:
                    logger.info("Multi-sheet dedup: %d → %d rows (dropped %d duplicates)",
                                pre_dedup, len(df), pre_dedup - len(df))
                logger.info(
                    "Multi-sheet concat: %d sheets → %d total rows, mapped=%d missing=%s",
                    len(normalised_frames), len(df),
                    len(col_map), col_map.get("missing", []),
                )
            else:
                # 1. Read original (all columns, original names) — kept for output summary
                original_df = smart_read_file(file_path, normalize_columns=False)
                # 2. Read normalised (mapped column names) — used by engines
                df, read_info = smart_read_file(file_path, normalize_columns=True)
                detected_header_row = read_info.get("header_row", 1)
                detected_header_score = read_info.get("score", 0)
                col_map = read_info.get("column_map", {})
            missing_cols = [] if multi_sheet else read_info.get("missing", [])
            logger.info(
                "Smart reader: header=%s score=%s mapped=%d missing=%s",
                detected_header_row, detected_header_score,
                len(col_map), missing_cols,
            )
            if col_map:
                logger.info("Column mappings: %s", col_map)
        except Exception as e:
            logger.warning("Smart reader failed (%s), falling back", e)
            detected_header_row = 1
            detected_header_score = 0
            if file_path.endswith(".csv"):
                df = pd.read_csv(file_path, dtype=str)
                if original_df is None:
                    original_df = df.copy()
            else:
                df, detected_header_row, detected_header_score = _read_excel_with_header_detection(file_path)
                if original_df is None:
                    original_df = df.copy()
            df = normalize_rcm_columns(df)
        df = df.fillna("")

        # Deduplicate column names (some Excel files have duplicates from merged cells)
        if df.columns.duplicated().any():
            dupes = df.columns[df.columns.duplicated(keep=False)].tolist()
            logger.warning("Duplicate column names found (likely from merged cells): %s", dupes)
            seen: Dict[str, int] = {}
            new_cols = []
            for col in df.columns:
                if col in seen:
                    seen[col] += 1
                    new_cols.append(f"{col}_{seen[col]}")
                else:
                    seen[col] = 0
                    new_cols.append(col)
            df.columns = new_cols

        # Trim to engine-only columns for downstream pipeline
        from engines.rcm_reader import REQUIRED_COLUMNS, OPTIONAL_COLUMNS
        engine_cols = list(REQUIRED_COLUMNS.values()) + list(OPTIONAL_COLUMNS.values())
        keep_cols = [c for c in engine_cols if c in df.columns]
        df = df[keep_cols]

        state.rcm_df = df
        state.original_rcm_df = original_df
        logger.info("Final df columns (%d): %s", len(keep_cols), keep_cols)

        if state.output_dir is None:
            rcm_dir = os.path.dirname(os.path.abspath(file_path))
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            state.output_dir = os.path.join(rcm_dir, f"sox_agent_{ts}")
            os.makedirs(state.output_dir, exist_ok=True)

        present = [c for c in RCM_REQUIRED_COLUMNS if c in df.columns]
        missing = [c for c in RCM_REQUIRED_COLUMNS if c not in df.columns]
        preview_cols = [c for c in ["Process", "SubProcess", "Control Id", "Risk Id",
                                     "Risk Title"] if c in df.columns]
        preview = sanitize_for_json(df.head(5)[preview_cols].to_dict(orient="records"))

        # Derive summary stats for the frontend visualization
        processes: List[str] = []
        if "Process" in df.columns:
            processes = sorted(
                v for v in df["Process"].dropna().astype(str).unique().tolist()
                if v.strip() and v.strip() != ""
            )
        # Fall back to SubProcess if Process column is empty or missing
        if not processes and "SubProcess" in df.columns:
            processes = sorted(
                v for v in df["SubProcess"].dropna().astype(str).unique().tolist()
                if v.strip() and v.strip() != ""
            )

        # Use unique Control Ids for all counts (rows may duplicate controls across risks)
        control_ids: List[str] = []
        unique_controls = df
        if "Control Id" in df.columns:
            control_ids = sorted(
                v for v in df["Control Id"].dropna().astype(str).unique().tolist()
                if v.strip() and v.strip() != ""
            )
            unique_controls = df.drop_duplicates(subset=["Control Id"])

        high_risk_count = 0
        if "risk_level" in unique_controls.columns:
            risk_levels_lower = unique_controls["risk_level"].str.strip().str.lower()
            high_risk_count = int(
                (risk_levels_lower == "high").sum()
                + (risk_levels_lower == "critical").sum()
            )

        automated_count = 0
        if "Control Type" in unique_controls.columns:
            automated_count = int(
                (unique_controls["Control Type"].str.strip().str.lower() == "automated").sum()
            )

        # Build full RCM row records for frontend dashboard (capped at 500 rows)
        _DASHBOARD_COLS = [
            "Process", "SubProcess", "Risk Id", "Risk Title", "Risk Description",
            "risk_level", "Control Id", "Control Description", "Control Objective",
            "Nature of Control", "Control Type", "Control Frequency", "Control Owner",
            "Application/System",
        ]
        dashboard_cols = [c for c in _DASHBOARD_COLS if c in df.columns]
        rcm_rows = sanitize_for_json(
            df[dashboard_cols].head(500).to_dict(orient="records")
        ) if dashboard_cols else []

        # Auto-export normalised RCM for inspection (df is already trimmed)
        normalised_path = os.path.join(state.output_dir, "normalised_rcm.xlsx")
        normalised_blob_path = None
        try:
            df.to_excel(normalised_path, index=False, engine="openpyxl")
            logger.info("Normalised RCM saved to %s (%d cols)", normalised_path, len(df.columns))
            normalised_blob_path = _upload_artifact(normalised_path, state)
            state.artifacts.append(normalised_blob_path or normalised_path)
        except Exception:
            normalised_path = os.path.join(state.output_dir, "normalised_rcm.csv")
            try:
                df.to_csv(normalised_path, index=False)
                logger.info("Normalised RCM saved as CSV to %s (%d cols)", normalised_path, len(df.columns))
                normalised_blob_path = _upload_artifact(normalised_path, state)
                state.artifacts.append(normalised_blob_path or normalised_path)
            except Exception as e2:
                logger.warning("Failed to save normalised RCM: %s", e2)
                normalised_path = None

        return ToolResult(
            success=True,
            data={
                "rows": len(df),
                "total_controls": len(control_ids),
                "columns": list(df.columns),
                "required_columns_present": present,
                "missing_columns": missing,
                "preview": preview,
                "output_directory": state.output_dir,
                "detected_header_row": detected_header_row,
                "detected_header_score": detected_header_score,
                "processes": processes,
                "control_ids": control_ids,
                "high_risk_count": high_risk_count,
                "automated_count": automated_count,
                "normalised_rcm_path": normalised_path,
                "normalised_rcm_blob_path": normalised_blob_path,
                "rcm_rows": rcm_rows,
            },
            summary=f"Loaded RCM: {len(df)} rows, {len(df.columns)} cols",
        )


# ═══════════════════════════════════════════════════════════════════════════
# inspect_dataframe
# ═══════════════════════════════════════════════════════════════════════════

class InspectDataframeTool(Tool):
    @property
    def name(self) -> str:
        return "inspect_dataframe"

    @property
    def description(self) -> str:
        return (
            "Quick inspection of the current RCM DataFrame. "
            "Modes: 'info' (shape, dtypes, nulls), 'head'/'tail' (first/last N rows), "
            "'describe' (statistics), 'columns' (list all columns), "
            "'value_counts' (unique values for a column), 'sample' (random N rows), "
            "'query' (filter with pandas query syntax), "
            "'full' (return all rows, capped at 500 for context safety)."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.DATA

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                "mode", "string", "Inspection mode",
                enum=["info", "head", "tail", "describe", "columns",
                      "value_counts", "sample", "query", "full"],
            ),
            ToolParameter("column", "string", "Column name (for value_counts)", required=False),
            ToolParameter("n", "integer", "Number of rows (default 10)", required=False),
            ToolParameter("query_expr", "string", "Pandas query expression", required=False),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if state.rcm_df is None:
            return "No RCM loaded. Use load_rcm first."
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        df = state.rcm_df
        mode = args.get("mode", "info")
        n = args.get("n", 10)
        column = args.get("column", "")
        query_expr = args.get("query_expr", "")

        if mode == "info":
            dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()}
            nulls = {col: int(v) for col, v in df.isnull().sum().items() if v > 0}
            return ToolResult(success=True, data={
                "mode": "info",
                "shape": {"rows": df.shape[0], "columns": df.shape[1]},
                "dtypes": dtypes, "null_counts": nulls,
                "memory_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
            })

        if mode in ("head", "tail"):
            subset = df.head(n) if mode == "head" else df.tail(n)
            return ToolResult(success=True, data={
                "mode": mode, "n": n, "total_rows": len(df),
                "columns": list(df.columns),
                "data": sanitize_for_json(subset.to_dict(orient="records")),
            })

        if mode == "describe":
            return ToolResult(success=True, data={
                "mode": "describe",
                "statistics": sanitize_for_json(df.describe(include="all").to_dict()),
            })

        if mode == "columns":
            col_info = [
                {"name": col, "dtype": str(df[col].dtype),
                 "non_null": int(df[col].notna().sum()),
                 "unique": int(df[col].nunique())}
                for col in df.columns
            ]
            return ToolResult(success=True, data={
                "mode": "columns", "total_columns": len(df.columns), "columns": col_info,
            })

        if mode == "value_counts":
            if not column:
                return ToolResult(success=False, data={}, error="column is required for value_counts")
            if column not in df.columns:
                return ToolResult(success=False, data={},
                                  error=f"Column '{column}' not found. Available: {list(df.columns)}")
            vc = df[column].value_counts()
            return ToolResult(success=True, data={
                "mode": "value_counts", "column": column,
                "total_rows": len(df), "unique_values": int(vc.shape[0]),
                "counts": sanitize_for_json(vc.head(50).to_dict()),
                "null_count": int(df[column].isnull().sum()),
            })

        if mode == "sample":
            sample_n = min(n, len(df))
            return ToolResult(success=True, data={
                "mode": "sample", "n": sample_n, "total_rows": len(df),
                "data": sanitize_for_json(df.sample(sample_n).to_dict(orient="records")),
            })

        if mode == "query":
            if not query_expr:
                return ToolResult(success=False, data={}, error="query_expr is required for query mode")
            try:
                filtered = df.query(query_expr)
            except Exception as exc:
                return ToolResult(success=False, data={},
                                  error=f"Query failed: {exc}. Columns: {list(df.columns)}")
            total = len(filtered)
            cap = min(200, total)
            return ToolResult(success=True, data={
                "mode": "query", "query": query_expr,
                "matching_rows": total, "total_rows": len(df),
                "showing": cap,
                "data": sanitize_for_json(filtered.head(cap).to_dict(orient="records")),
            })

        if mode == "full":
            cap = min(len(df), 500)
            return ToolResult(success=True, data={
                "mode": "full", "total_rows": len(df), "showing": cap,
                "columns": list(df.columns),
                "data": sanitize_for_json(df.head(cap).to_dict(orient="records")),
                "truncated": len(df) > cap,
            })

        return ToolResult(success=False, data={}, error=f"Unknown mode: {mode}")
