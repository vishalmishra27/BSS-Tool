"""Tools for modifying the RCM: add/rename/update/delete columns, bulk ops, merge suggestions, remove duplicates."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..utils import parse_indices, sanitize_for_json

logger = logging.getLogger("agent.tools.rcm_mutator")


def _coerce_value(val: Any, dtype) -> Any:
    """Auto-convert a string value to match the target column's dtype.

    The LLM always sends values as strings (tool parameter type is "string"),
    but the DataFrame column may be int64, float64, etc.  This prevents
    pandas from raising or silently converting types incorrectly.
    """
    if val is None or val == "":
        return val
    dtype_str = str(dtype)
    try:
        if "int" in dtype_str:
            return int(float(str(val)))
        if "float" in dtype_str:
            return float(str(val))
        if "bool" in dtype_str:
            return str(val).strip().lower() in ("true", "1", "yes")
    except (ValueError, TypeError):
        pass  # Fall back to original string value
    return val


def _build_diff(action: str, df_before_rows: int, df_before_cols: int,
                df: "pd.DataFrame", **extra) -> Dict[str, Any]:
    """Build a standard diff summary dict for mutation actions."""
    return {
        "action": action,
        "rows_before": df_before_rows,
        "rows_after": len(df),
        "columns_before": df_before_cols,
        "columns_after": len(df.columns),
        "columns_list": list(df.columns),
        **extra,
    }


# ═══════════════════════════════════════════════════════════════════════════
# modify_rcm
# ═══════════════════════════════════════════════════════════════════════════

class ModifyRCMTool(Tool):
    @property
    def name(self) -> str:
        return "modify_rcm"

    @property
    def description(self) -> str:
        return (
            "Modify the current RCM DataFrame. Actions: "
            "add_column, rename_column, update_values (case-insensitive conditional), "
            "delete_column, delete_rows (by condition or query), "
            "bulk_update (multiple columns at once), sort, filter_view (read-only filtered view)."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.DATA

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter("action", "string", "Type of modification",
                          enum=["add_column", "rename_column", "update_values",
                                "delete_column", "delete_rows", "bulk_update",
                                "sort", "filter_view"]),
            ToolParameter("column_name", "string", "Target column name", required=False),
            ToolParameter("new_name", "string", "New name (rename_column only)", required=False),
            ToolParameter("value", "string", "Value for add/update", required=False),
            ToolParameter("condition_column", "string", "Filter column for conditional update/delete", required=False),
            ToolParameter("condition_value", "string", "Filter value for conditional update/delete", required=False),
            ToolParameter("updates", "object", "Dict of {column: value} for bulk_update", required=False),
            ToolParameter("ascending", "boolean", "Sort order (default True)", required=False),
            ToolParameter("query_expr", "string", "Pandas query expression for delete_rows/filter_view", required=False),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if state.rcm_df is None:
            return "No RCM loaded."
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        df = state.rcm_df
        action = args["action"]
        col = args.get("column_name", "").strip() if args.get("column_name") else ""
        rows_before = len(df)
        cols_before = len(df.columns)

        # ── add_column ────────────────────────────────────────────────
        if action == "add_column":
            val = args.get("value", "")
            if not col:
                return ToolResult(success=False, data={}, error="column_name is required for add_column")
            if col in df.columns:
                return ToolResult(success=False, data={},
                                  error=f"Column '{col}' already exists. Use update_values.")
            df = df.copy()
            df[col] = val
            state.rcm_df = df
            return ToolResult(success=True,
                data=_build_diff("add_column", rows_before, cols_before, df,
                                 column=col, default_value=val),
                summary=f"Added column '{col}'")

        # ── rename_column ─────────────────────────────────────────────
        if action == "rename_column":
            new_name = args.get("new_name", "").strip()
            if not new_name:
                return ToolResult(success=False, data={}, error="new_name is required")
            if not col:
                return ToolResult(success=False, data={}, error="column_name is required")
            if col not in df.columns:
                return ToolResult(success=False, data={},
                                  error=f"Column '{col}' not found. Available: {list(df.columns)}")
            state.rcm_df = df.rename(columns={col: new_name})
            return ToolResult(success=True,
                data=_build_diff("rename_column", rows_before, cols_before, state.rcm_df,
                                 old_name=col, new_name=new_name),
                summary=f"Renamed '{col}' -> '{new_name}'")

        # ── update_values (case-insensitive conditional) ──────────────
        if action == "update_values":
            val = args.get("value", "")
            if not col:
                return ToolResult(success=False, data={}, error="column_name is required")
            if col not in df.columns:
                return ToolResult(success=False, data={},
                                  error=f"Column '{col}' not found. Available: {list(df.columns)}")
            df = df.copy()
            # Auto-convert value to match target column dtype
            val = _coerce_value(val, df[col].dtype)
            cond_col = args.get("condition_column")
            cond_val = args.get("condition_value")
            if cond_col and cond_val is not None:
                if cond_col not in df.columns:
                    return ToolResult(success=False, data={},
                                      error=f"Condition column '{cond_col}' not found.")
                # Case-insensitive, whitespace-trimmed matching
                mask = df[cond_col].astype(str).str.strip().str.lower() == str(cond_val).strip().lower()
                df.loc[mask, col] = val
                updated = int(mask.sum())
            else:
                df[col] = val
                updated = len(df)
            state.rcm_df = df
            return ToolResult(success=True,
                data=_build_diff("update_values", rows_before, cols_before, df,
                                 column=col, value=str(val), rows_updated=updated),
                summary=f"Updated {updated} rows in '{col}'")

        # ── delete_column ─────────────────────────────────────────────
        if action == "delete_column":
            if not col:
                return ToolResult(success=False, data={}, error="column_name is required")
            if col not in df.columns:
                return ToolResult(success=False, data={},
                                  error=f"Column '{col}' not found. Available: {list(df.columns)}")
            df = df.drop(columns=[col])
            state.rcm_df = df
            return ToolResult(success=True,
                data=_build_diff("delete_column", rows_before, cols_before, df,
                                 deleted_column=col),
                summary=f"Deleted column '{col}'")

        # ── delete_rows ───────────────────────────────────────────────
        if action == "delete_rows":
            query_expr = args.get("query_expr", "")
            cond_col = args.get("condition_column")
            cond_val = args.get("condition_value")

            df = df.copy()
            if query_expr:
                try:
                    to_delete = df.query(query_expr)
                except Exception as exc:
                    return ToolResult(success=False, data={},
                                      error=f"Query failed: {exc}. Columns: {list(df.columns)}")
                df = df.drop(index=to_delete.index).reset_index(drop=True)
            elif cond_col and cond_val is not None:
                if cond_col not in df.columns:
                    return ToolResult(success=False, data={},
                                      error=f"Condition column '{cond_col}' not found.")
                mask = df[cond_col].astype(str).str.strip().str.lower() == str(cond_val).strip().lower()
                df = df[~mask].reset_index(drop=True)
            else:
                return ToolResult(success=False, data={},
                                  error="delete_rows requires either query_expr or condition_column+condition_value")

            deleted = rows_before - len(df)
            state.rcm_df = df
            return ToolResult(success=True,
                data=_build_diff("delete_rows", rows_before, cols_before, df,
                                 rows_deleted=deleted),
                summary=f"Deleted {deleted} rows -> {len(df)} remaining")

        # ── bulk_update ───────────────────────────────────────────────
        if action == "bulk_update":
            updates = args.get("updates")
            if not updates or not isinstance(updates, dict):
                return ToolResult(success=False, data={},
                                  error="bulk_update requires 'updates' dict: {column: value}")
            df = df.copy()
            cond_col = args.get("condition_column")
            cond_val = args.get("condition_value")

            # Validate all columns exist
            missing = [c for c in updates if c not in df.columns]
            if missing:
                return ToolResult(success=False, data={},
                                  error=f"Columns not found: {missing}. Available: {list(df.columns)}")

            if cond_col and cond_val is not None:
                if cond_col not in df.columns:
                    return ToolResult(success=False, data={},
                                      error=f"Condition column '{cond_col}' not found.")
                mask = df[cond_col].astype(str).str.strip().str.lower() == str(cond_val).strip().lower()
                for c, v in updates.items():
                    df.loc[mask, c] = _coerce_value(v, df[c].dtype)
                updated = int(mask.sum())
            else:
                for c, v in updates.items():
                    df[c] = _coerce_value(v, df[c].dtype)
                updated = len(df)

            state.rcm_df = df
            return ToolResult(success=True,
                data=_build_diff("bulk_update", rows_before, cols_before, df,
                                 columns_updated=list(updates.keys()), rows_updated=updated),
                summary=f"Bulk-updated {len(updates)} columns across {updated} rows")

        # ── sort ──────────────────────────────────────────────────────
        if action == "sort":
            if not col:
                return ToolResult(success=False, data={}, error="column_name is required for sort")
            if col not in df.columns:
                return ToolResult(success=False, data={},
                                  error=f"Column '{col}' not found. Available: {list(df.columns)}")
            ascending = args.get("ascending", True)
            if ascending is None:
                ascending = True
            state.rcm_df = df.sort_values(by=col, ascending=ascending).reset_index(drop=True)
            return ToolResult(success=True,
                data=_build_diff("sort", rows_before, cols_before, state.rcm_df,
                                 sort_column=col, ascending=ascending),
                summary=f"Sorted by '{col}' {'asc' if ascending else 'desc'}")

        # ── filter_view (read-only, does NOT modify the DataFrame) ────
        if action == "filter_view":
            query_expr = args.get("query_expr", "")
            if not query_expr:
                return ToolResult(success=False, data={},
                                  error="filter_view requires query_expr")
            try:
                filtered = df.query(query_expr)
            except Exception as exc:
                return ToolResult(success=False, data={},
                                  error=f"Query failed: {exc}. Columns: {list(df.columns)}")
            cap = min(len(filtered), 200)
            return ToolResult(success=True, data={
                "action": "filter_view", "query": query_expr,
                "matching_rows": len(filtered), "total_rows": len(df),
                "showing": cap,
                "data": sanitize_for_json(filtered.head(cap).to_dict(orient="records")),
            }, summary=f"Filter matched {len(filtered)} rows (showing {cap})")

        return ToolResult(success=False, data={}, error=f"Unknown action: {action}")


# ═══════════════════════════════════════════════════════════════════════════
# merge_suggestions
# ═══════════════════════════════════════════════════════════════════════════

class MergeSuggestionsTool(Tool):
    @property
    def name(self) -> str:
        return "merge_suggestions"

    @property
    def description(self) -> str:
        return (
            "Merge user-selected AI suggestions into the RCM. "
            "Call after run_ai_suggestions and after the user has chosen which to keep."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.DATA

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter("indices", "string",
                          "Which suggestions to keep: 'all', 'none', '1,3,5', '1-5,8,12'"),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if state.rcm_df is None:
            return "No RCM loaded."
        if state.suggestions_cache is None:
            return "No suggestions cached. Run run_ai_suggestions first."
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        suggestions = state.suggestions_cache
        keep = parse_indices(args["indices"], len(suggestions))

        if not keep:
            return ToolResult(success=True, data={
                "kept": 0, "original_rows": len(state.rcm_df),
                "new_total": len(state.rcm_df),
            }, summary="No suggestions kept")

        rcm_df = state.rcm_df.copy()
        rcm_columns = list(rcm_df.columns)

        new_rows = []
        for idx in keep:
            s = suggestions[idx - 1]
            row = {col: s.get(col, "") for col in rcm_columns}
            row["Row_Source"] = "AI_Suggestion"
            row["AI_Suggestion_ID"] = s.get("AI_Suggestion_ID", "")
            row["AI_Priority"] = s.get("AI_Priority", "")
            row["AI_Category"] = s.get("AI_Category", "")
            row["AI_Reason"] = s.get("AI_Reason", "")
            new_rows.append(row)

        if "Row_Source" not in rcm_df.columns:
            for col in ("Row_Source", "AI_Suggestion_ID", "AI_Priority", "AI_Category", "AI_Reason"):
                rcm_df[col] = "" if col != "Row_Source" else "Original"

        updated = pd.concat([rcm_df, pd.DataFrame(new_rows)], ignore_index=True)
        state.rcm_df = updated
        original = len(rcm_df)

        return ToolResult(success=True, data={
            "kept": len(keep), "original_rows": original, "new_total": len(updated),
        }, summary=f"Merged {len(keep)} suggestions → {len(updated)} rows")


# ═══════════════════════════════════════════════════════════════════════════
# remove_duplicates
# ═══════════════════════════════════════════════════════════════════════════

class RemoveDuplicatesTool(Tool):
    @property
    def name(self) -> str:
        return "remove_duplicates"

    @property
    def description(self) -> str:
        return (
            "Remove duplicate rows from the RCM based on user decisions. "
            "Call after run_deduplication and user review."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.DATA

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter("removals", "array",
                          "List of removal decisions: [{pair: 1, remove: 'a'|'b'|'both'}]",
                          items={
                              "type": "object",
                              "properties": {
                                  "pair": {"type": "integer", "description": "Pair number (1-based)"},
                                  "remove": {"type": "string", "enum": ["a", "b", "both"]},
                              },
                              "required": ["pair", "remove"],
                          }),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if state.rcm_df is None:
            return "No RCM loaded."
        if state.dedup_cache is None:
            return "No dedup results. Run run_deduplication first."
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        removals = args.get("removals", [])
        if not removals:
            return ToolResult(success=True, data={
                "removed_count": 0, "new_total": len(state.rcm_df),
            }, summary="No removals specified")

        rcm_df = state.rcm_df
        all_pairs = state.dedup_cache["pairs"]
        rows_to_drop: set[int] = set()

        has_process_col = "Process" in rcm_df.columns

        for removal in removals:
            pair_num = removal.get("pair", 0) - 1
            remove_which = removal.get("remove", "").lower()
            if pair_num < 0 or pair_num >= len(all_pairs):
                continue
            pair = all_pairs[pair_num]

            # Prefer pre-resolved global indices (stored in the cache at dedup
            # time, when the RCM was in the exact state the engine saw).
            # Fall back to local-index resolution only for old caches that
            # pre-date this fix, or when the Process column was missing.
            if remove_which in ("a", "both"):
                if "global_row_a" in pair:
                    rows_to_drop.add(pair["global_row_a"])
                elif has_process_col:
                    process_name = pair.get("_process", "")
                    local_a = pair.get("row_a")
                    group_indices = rcm_df[rcm_df["Process"] == process_name].index.tolist()
                    if isinstance(local_a, int) and local_a < len(group_indices):
                        rows_to_drop.add(group_indices[local_a])

            if remove_which in ("b", "both"):
                if "global_row_b" in pair:
                    rows_to_drop.add(pair["global_row_b"])
                elif has_process_col:
                    process_name = pair.get("_process", "")
                    local_b = pair.get("row_b")
                    group_indices = rcm_df[rcm_df["Process"] == process_name].index.tolist()
                    if isinstance(local_b, int) and local_b < len(group_indices):
                        rows_to_drop.add(group_indices[local_b])

        before = len(rcm_df)
        if rows_to_drop:
            rcm_df = rcm_df.drop(index=list(rows_to_drop)).reset_index(drop=True)
            state.rcm_df = rcm_df
        removed = before - len(rcm_df)

        return ToolResult(success=True, data={
            "removed_count": removed, "original_rows": before, "new_total": len(rcm_df),
        }, summary=f"Removed {removed} rows → {len(rcm_df)} remaining")
