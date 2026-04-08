"""Ad-hoc Python code execution tool for RCM data analysis.

NOTE: Code runs in the agent's own process with full access to the filesystem
and all installed packages via the ``os``, ``json``, and ``re`` namespace
variables.  There is no process isolation or resource limit — this tool is
intended for trusted, ad-hoc data analysis on the loaded RCM only.
"""

from __future__ import annotations

import io
import sys
import math
import logging
import traceback as tb_module
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..utils import sanitize_for_json

logger = logging.getLogger("agent.tools.python_exec")


class ExecutePythonTool(Tool):
    @property
    def name(self) -> str:
        return "execute_python"

    @property
    def description(self) -> str:
        return (
            "Execute a Python code snippet for ad-hoc RCM data analysis. "
            "Pre-loaded variables: df (current RCM DataFrame), pd, np, os, json, math, "
            "datetime, re, openpyxl. "
            "Assign to 'result' to return structured data. "
            "If you reassign 'df', the RCM will be updated."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.UTILITY

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter("code", "string", "Python code to execute"),
        ]

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        import os as _os
        import json as _json
        import re as _re
        from datetime import datetime as _dt

        code = args.get("code", "")
        if not code.strip():
            return ToolResult(success=False, data={}, error="No code provided.")

        state.python_exec_count += 1
        logger.info("Executing Python snippet #%d", state.python_exec_count)

        df = state.rcm_df
        original_df_id = id(df) if df is not None else None

        # Import openpyxl lazily for the namespace
        try:
            import openpyxl as _openpyxl
        except ImportError:
            _openpyxl = None

        namespace: Dict[str, Any] = {
            "df": df,
            "pd": pd,
            "np": np,
            "os": _os,
            "json": _json,
            "math": math,
            "datetime": _dt,
            "re": _re,
            "openpyxl": _openpyxl,
            "agent_state": state,
            "result": None,
        }

        old_stdout = sys.stdout
        sys.stdout = captured = io.StringIO()

        try:
            exec(code, namespace)  # noqa: S102
        except Exception as exc:
            sys.stdout = old_stdout
            tb_lines = tb_module.format_exc().strip().split("\n")
            logger.warning("Python exec error: %s", exc)
            return ToolResult(
                success=False,
                data={"error": str(exc), "traceback": "\n".join(tb_lines[-5:])},
                error=str(exc),
            )
        finally:
            sys.stdout = old_stdout

        stdout_text = captured.getvalue()

        # If df was reassigned, update state
        new_df = namespace.get("df")
        if new_df is not None and id(new_df) != original_df_id:
            state.rcm_df = new_df
            logger.info("DataFrame updated via exec: %d rows x %d cols",
                        new_df.shape[0], new_df.shape[1])

        result_val = namespace.get("result")

        # Stringify result for JSON
        if result_val is not None:
            try:
                if isinstance(result_val, pd.DataFrame):
                    result_val = sanitize_for_json(
                        result_val.head(200).to_dict(orient="records"))
                elif isinstance(result_val, pd.Series):
                    result_val = sanitize_for_json(result_val.head(200).to_dict())
                else:
                    result_val = sanitize_for_json(result_val)
            except Exception:
                result_val = str(result_val)

        # Truncate to avoid context bloat
        if isinstance(stdout_text, str) and len(stdout_text) > 8000:
            stdout_text = stdout_text[:8000] + "\n... (truncated)"
        result_str = str(result_val) if result_val is not None else None
        if result_str and len(result_str) > 8000:
            result_val = result_str[:8000] + "... (truncated)"

        current_df = state.rcm_df
        df_shape = ([current_df.shape[0], current_df.shape[1]]
                    if current_df is not None else None)

        return ToolResult(
            success=True,
            data={
                "result": result_val,
                "stdout": stdout_text if stdout_text else None,
                "df_shape": df_shape,
            },
            summary=f"Executed snippet #{state.python_exec_count}",
        )
