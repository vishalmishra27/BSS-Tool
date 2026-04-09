"""
SOX Audit Agent — Agentic AI Pipeline
=======================================
An LLM-driven agent that guides users through a 6-step SOX audit workflow.
The agent decides what to do next, asks for inputs conversationally, runs
the right tool at the right time, shows results, and handles human review.

Usage:
    cd flask-api
    python sox_agent.py

Author: Rishi
Date: February 2026
"""

import os
import sys
import io
import json
import time
import math
import traceback as tb_module
from datetime import datetime
from importlib import reload

import pandas as pd
import numpy as np
from openai import AzureOpenAI

# Ensure flask-api directory is on path for engine imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

# --- Agent LLM (Azure OpenAI) ---
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "https://entgptaiuat.openai.azure.com")
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY", "808cf0ccab8445b39c6d8767a7e2c433")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2023-07-01-preview")
AZURE_OPENAI_DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o-mini")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", AZURE_OPENAI_API_KEY)
AGENT_MODEL = os.getenv("OPENAI_MODEL", AZURE_OPENAI_DEPLOYMENT_NAME)
MAX_ROUNDS = 25  # Max LLM ↔ tool round-trips per user message

# Agent LLM client (Azure OpenAI for the conversation loop)
agent_client = AzureOpenAI(
    api_key=OPENAI_API_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=AZURE_OPENAI_API_VERSION,
)


RCM_REQUIRED_COLUMNS = [
    "Process", "SubProcess", "Control Objective", "Risk Id", "Risk Title",
    "Risk Description", "Control Id", "Control Description", "Control Owner",
    "Control Rating", "Nature of Control", "Control Type", "Control Frequency",
    "Application/System", "risk_level",
]

# Map common column name variations to the standard names we use
COLUMN_NORMALIZE_MAP = {
    "process": "Process",
    "sub process": "SubProcess",
    "subprocess": "SubProcess",
    "sub_process": "SubProcess",
    "control objective": "Control Objective",
    "control_objective": "Control Objective",
    "risk id": "Risk Id",
    "risk_id": "Risk Id",
    "riskid": "Risk Id",
    "risk title": "Risk Title",
    "risk_title": "Risk Title",
    "risk description": "Risk Description",
    "risk_description": "Risk Description",
    "control id": "Control Id",
    "control_id": "Control Id",
    "controlid": "Control Id",
    "control description": "Control Description",
    "control_description": "Control Description",
    "control owner": "Control Owner",
    "control_owner": "Control Owner",
    "control rating": "Control Rating",
    "control_rating": "Control Rating",
    "nature of control": "Nature of Control",
    "nature_of_control": "Nature of Control",
    "control type": "Control Type",
    "control_type": "Control Type",
    "control frequency": "Control Frequency",
    "control_frequency": "Control Frequency",
    "application/system": "Application/System",
    "application_system": "Application/System",
    "application / system": "Application/System",
    "risk level": "risk_level",
    "risk_level": "risk_level",
    "risklevel": "risk_level",
    "count_of_samples": "count_of_samples",
    "count of samples": "count_of_samples",
}


def normalize_rcm_columns(df):
    """Normalize RCM DataFrame column names to standard format."""
    new_cols = []
    for col in df.columns:
        stripped = str(col).strip()
        lookup = stripped.lower()
        if lookup in COLUMN_NORMALIZE_MAP:
            new_cols.append(COLUMN_NORMALIZE_MAP[lookup])
        else:
            new_cols.append(stripped)
    df.columns = new_cols
    return df

SUPPORTED_INDUSTRIES = [
    "Manufacturing", "Banking & Financial Services", "Healthcare",
    "Insurance", "Retail & Consumer", "Technology", "Telecommunications",
    "Energy & Utilities", "Pharmaceuticals", "Automotive",
    "Real Estate", "Mining & Metals", "Media & Entertainment",
    "Transportation & Logistics", "Government & Public Sector",
    "Education", "Hospitality & Tourism", "Agriculture",
    "Construction & Engineering", "Aerospace & Defense",
]

# ═══════════════════════════════════════════════════════════════════════════════
# SYSTEM PROMPT
# ═══════════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """You are a SOX (Sarbanes-Oxley) audit assistant that guides users through a complete 6-step audit pipeline interactively.

WORKFLOW — follow this exact order:

STEP 0 - LOAD RCM:
  Ask for the RCM Excel file path → call load_rcm → show the preview to the user.
  If unsure about the path, use list_directory to help them find it.

STEP 1 - AI SUGGESTIONS (Gap Analysis):
  Ask which industry this audit is for (show the numbered list).
  Call run_ai_suggestions with the industry.
  Display ALL suggestions as a numbered list with: #, Suggestion ID, Priority, Category, Risk Title, Control Description, Reason.
  Ask user which to keep (e.g. "1,3,5-8", "all", "none").
  Call merge_suggestions with their selection.
  Call save_excel with step_name="1_ai_suggestions".

STEP 2 - CONTROL DESIGN ASSESSMENT:
  Ask for policy PDF file paths (one or more).
  Ask for SOP PDF file paths (one or more).
  If user gives a folder path, use list_directory to find PDFs in it.
  Call run_control_assessment with the paths.
  Display results: Control ID, Policy Documented, Match %, Gaps.
  Ask if user wants to note any overrides.
  Call save_excel with step_name="2_control_assessment".

STEP 3 - OVERLAP ANALYSIS (Deduplication):
  Call run_deduplication (no extra inputs needed, uses current RCM).
  Display each duplicate pair with: pair #, Row A risk, Row B risk, Confidence, Reasoning, Recommendation.
  For each pair, ask: remove A, B, both, or neither?
  Collect all removal decisions, then call remove_duplicates with the full list.
  Call save_excel with step_name="3_overlap_analysis".

STEP 4 - TEST OF DESIGN (TOD):
  Ask for the evidence folder path.
  Call run_test_of_design.
  Display ALL results: Control ID, TOD Result (PASS/FAIL), Design Adequate, Confidence, Deficiency Type, Gap, Remarks.
  Show the summary report.
  Ask if user wants to override any results (human evaluation).
  Call save_excel with step_name="4_test_of_design".

STEP 5 - TEST OF EFFECTIVENESS (TOE):
  Ask for the TOE evidence/samples folder path.
  Ask for company name, prepared by, reviewed by (for the workpaper).
  Call run_test_of_effectiveness.
  Display per-control summary: Control ID, Samples Passed/Total, Deviation Rate, Effectiveness, Deficiency.
  Then show per-sample detail for each control.
  Ask if user wants to override any sample results (human evaluation).
  Call save_excel with step_name="5_test_of_effectiveness".

After all steps, summarize: total rows in final RCM, all output files created, key findings across all steps.

RULES:
- ALWAYS display results BEFORE asking for user decisions.
- ALWAYS call save_excel after any RCM modification (merge, removal, or column changes).
- Format results as numbered tables so users can reference items by number.
- After each step, ask "Ready to proceed to Step N?" before continuing.
- If a tool fails, explain the error clearly and ask if user wants to retry or skip.
- Be concise but thorough. Auditors value precision.
- When showing suggestions or results, show ALL items, not just a sample.
- The supported industries are: """ + ", ".join(SUPPORTED_INDUSTRIES) + """

RCM MODIFICATION:
- Use modify_rcm to add columns, rename columns, or update values in the current RCM.
- Example: If user says "add risk_level column with High" → call modify_rcm(action="add_column", column_name="risk_level", value="High").
- Example: If user says "change all risk_level to Medium" → call modify_rcm(action="update_values", column_name="risk_level", value="Medium").
- Example: If user says "rename SubProcess to Sub Process" → call modify_rcm(action="rename_column", column_name="SubProcess", new_name="Sub Process").
- After any modify_rcm call, call save_excel to checkpoint the change.
- Do NOT use merge_suggestions for adding columns — use modify_rcm instead.

GENERAL CAPABILITIES:
- Use execute_python for data analysis, statistics, transformations, or any operation on the RCM.
  Example: "What's the risk level distribution?" → execute_python with value_counts code.
  Example: "Create a summary pivot table" → execute_python with pd.pivot_table.
  The code has access to `df` (current RCM DataFrame), `pd`, `np`, `os`, `json`, `agent_state`.
  Set `result = ...` to return a value to the conversation.
  If you reassign `df = ...` the agent state will be updated automatically.
- Use inspect_dataframe for quick checks: shape, columns, head/tail, value_counts, query/filter.
  Prefer this over execute_python for simple lookups.
- Use read_file to read any file the user references (text, Excel, PDF, CSV, JSON).
- Use web_search to look up SOX regulations, PCAOB standards, COSO/COBIT frameworks,
  or answer general audit/compliance questions. Always use this rather than guessing about
  regulatory requirements.

IMPORTANT: You must call tools to perform actions. Do NOT make up results or file paths."""


# ═══════════════════════════════════════════════════════════════════════════════
# TOOL DEFINITIONS (OpenAI function-calling format)
# ═══════════════════════════════════════════════════════════════════════════════

TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "load_rcm",
            "description": "Load an RCM Excel file into the working state. Returns row count, columns, and a preview of the first 5 rows.",
            "parameters": {
                "type": "object",
                "properties": {
                    "file_path": {"type": "string", "description": "Absolute path to the RCM Excel file (.xlsx or .csv)"},
                },
                "required": ["file_path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_ai_suggestions",
            "description": "Run AI gap analysis on the current RCM. Returns executive summary, gap analysis, and a numbered list of suggestions with priority, category, and reason.",
            "parameters": {
                "type": "object",
                "properties": {
                    "industry": {
                        "type": "string",
                        "description": "Industry sector for context-aware suggestions",
                    },
                },
                "required": ["industry"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "merge_suggestions",
            "description": "Merge user-selected AI suggestions into the RCM. Call this after run_ai_suggestions and after the user has chosen which suggestions to keep.",
            "parameters": {
                "type": "object",
                "properties": {
                    "indices": {
                        "type": "string",
                        "description": "Which suggestions to keep. Examples: 'all', 'none', '1,3,5', '1-5,8,12'",
                    },
                },
                "required": ["indices"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_control_assessment",
            "description": "Run Control Design Assessment (OnGround Check) comparing the RCM against policy and SOP documents.",
            "parameters": {
                "type": "object",
                "properties": {
                    "policy_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of absolute paths to policy PDF files",
                    },
                    "sop_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of absolute paths to SOP PDF files",
                    },
                },
                "required": ["policy_paths", "sop_paths"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_deduplication",
            "description": "Run semantic deduplication on the current RCM to find overlapping or duplicate risks/controls. Returns numbered duplicate pairs with confidence and recommendations.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "remove_duplicates",
            "description": "Remove duplicate rows from the RCM based on user decisions. Call after run_deduplication and user review.",
            "parameters": {
                "type": "object",
                "properties": {
                    "removals": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "pair": {"type": "integer", "description": "Pair number (1-based)"},
                                "remove": {"type": "string", "enum": ["a", "b", "both"], "description": "Which row to remove"},
                            },
                            "required": ["pair", "remove"],
                        },
                        "description": "List of removal decisions for each duplicate pair",
                    },
                },
                "required": ["removals"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_test_of_design",
            "description": "Run Test of Design (TOD) to evaluate if controls are adequately designed. Requires an evidence folder with one subfolder per Control ID containing evidence files.",
            "parameters": {
                "type": "object",
                "properties": {
                    "evidence_folder": {"type": "string", "description": "Path to the evidence folder"},
                },
                "required": ["evidence_folder"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_test_of_effectiveness",
            "description": "Run Test of Operating Effectiveness (TOE) to evaluate if controls operated effectively across multiple transaction samples.",
            "parameters": {
                "type": "object",
                "properties": {
                    "evidence_folder": {"type": "string", "description": "Path to the TOE evidence folder (one subfolder per control, multiple sample files each)"},
                    "company_name": {"type": "string", "description": "Company name for the workpaper header"},
                    "prepared_by": {"type": "string", "description": "Name of the preparer"},
                    "reviewed_by": {"type": "string", "description": "Name of the reviewer"},
                },
                "required": ["evidence_folder"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "save_excel",
            "description": "Save the current RCM state to an Excel checkpoint file. Call this after any modification to the RCM.",
            "parameters": {
                "type": "object",
                "properties": {
                    "step_name": {"type": "string", "description": "Name for the checkpoint file, e.g. '1_ai_suggestions'"},
                },
                "required": ["step_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "modify_rcm",
            "description": "Modify the current RCM DataFrame: add a column, rename a column, or update values in a column. Use this when the user asks to add/change columns or fill values.",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["add_column", "rename_column", "update_values"],
                        "description": "Type of modification",
                    },
                    "column_name": {
                        "type": "string",
                        "description": "Target column name (for add_column/update_values) or current name (for rename_column)",
                    },
                    "new_name": {
                        "type": "string",
                        "description": "New column name (only for rename_column action)",
                    },
                    "value": {
                        "type": "string",
                        "description": "Default value for all rows (for add_column) or new value (for update_values)",
                    },
                    "condition_column": {
                        "type": "string",
                        "description": "Optional: only update rows where this column matches condition_value",
                    },
                    "condition_value": {
                        "type": "string",
                        "description": "Optional: the value to match in condition_column",
                    },
                },
                "required": ["action", "column_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_directory",
            "description": "List files and folders in a directory, optionally filtered by extension. Use this to help users find RCM files, evidence folders, or policy/SOP PDFs.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Directory path to list"},
                    "extension": {"type": "string", "description": "Optional file extension filter, e.g. '.xlsx', '.pdf'"},
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "execute_python",
            "description": "Execute Python code with access to the current RCM DataFrame (as `df`), pandas (`pd`), numpy (`np`), os, json, and the full agent_state dict. Use this for data analysis, calculations, transformations, or any operation not covered by other tools. The last expression's value is returned, or use `result = ...` to set the return value explicitly.",
            "parameters": {
                "type": "object",
                "properties": {
                    "code": {
                        "type": "string",
                        "description": "Python code to execute. Use `df` for the current RCM DataFrame. Set `result = ...` to return a value.",
                    },
                },
                "required": ["code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "Read the contents of a file. Supports text files (.txt, .csv, .json, .py, .md, .log, etc.), Excel files (.xlsx returns first sheet as table), and PDF files (.pdf extracts text). Returns the first 200 lines by default.",
            "parameters": {
                "type": "object",
                "properties": {
                    "file_path": {"type": "string", "description": "Absolute path to the file"},
                    "max_lines": {"type": "integer", "description": "Max lines to return (default 200, max 500)"},
                    "encoding": {"type": "string", "description": "Text encoding (default utf-8)"},
                },
                "required": ["file_path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "inspect_dataframe",
            "description": "Quick inspection of the current RCM DataFrame. Modes: 'info' (shape, dtypes, nulls), 'head'/'tail' (first/last N rows), 'describe' (statistics), 'columns' (list all columns), 'value_counts' (unique values for a column), 'sample' (random N rows), 'query' (filter with pandas query syntax).",
            "parameters": {
                "type": "object",
                "properties": {
                    "mode": {
                        "type": "string",
                        "enum": ["info", "head", "tail", "describe", "columns", "value_counts", "sample", "query"],
                        "description": "Inspection mode",
                    },
                    "column": {"type": "string", "description": "Column name (for value_counts mode)"},
                    "n": {"type": "integer", "description": "Number of rows (for head/tail/sample, default 10)"},
                    "query_expr": {"type": "string", "description": "Pandas query expression (for query mode), e.g. \"risk_level == 'High'\""},
                },
                "required": ["mode"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "Search the web for SOX regulations, PCAOB standards, audit frameworks (COSO, COBIT), industry best practices, or any general audit/compliance question. Returns summarized search results.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query, e.g. 'PCAOB AS 2201 test of controls requirements'",
                    },
                    "num_results": {"type": "integer", "description": "Number of results to return (default 5, max 10)"},
                },
                "required": ["query"],
            },
        },
    },
]


# ═══════════════════════════════════════════════════════════════════════════════
# DISPLAY HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def display_table(rows, columns, max_col_width=50):
    """Format a list of dicts as an ASCII table and return as string."""
    if not rows:
        return "(no data)"

    # Calculate column widths
    widths = {}
    for col in columns:
        header_w = len(str(col))
        data_w = max((len(str(row.get(col, ""))) for row in rows), default=0)
        widths[col] = min(max(header_w, data_w), max_col_width)

    # Build format string
    def fmt_cell(val, width):
        s = str(val) if val is not None else ""
        if len(s) > width:
            s = s[: width - 3] + "..."
        return s.ljust(width)

    lines = []
    # Header
    header = " | ".join(fmt_cell(col, widths[col]) for col in columns)
    lines.append(header)
    # Divider
    divider = "-+-".join("-" * widths[col] for col in columns)
    lines.append(divider)
    # Rows
    for row in rows:
        line = " | ".join(fmt_cell(row.get(col, ""), widths[col]) for col in columns)
        lines.append(line)

    return "\n".join(lines)


def sanitize_for_json(obj):
    """Replace NaN/Infinity with None for valid JSON serialization."""
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(item) for item in obj]
    elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    elif isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating,)):
        return None if (np.isnan(obj) or np.isinf(obj)) else float(obj)
    elif isinstance(obj, (np.bool_,)):
        return bool(obj)
    elif isinstance(obj, (np.ndarray,)):
        return sanitize_for_json(obj.tolist())
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    return obj


def parse_indices(indices_str, max_val):
    """Parse '1,3,5-8,12' or 'all' or 'none' into a list of 1-based ints."""
    s = indices_str.strip().lower()
    if s == "all":
        return list(range(1, max_val + 1))
    if s in ("none", "0", ""):
        return []

    result = set()
    for part in s.replace(" ", "").split(","):
        if not part:
            continue
        if "-" in part:
            lo, hi = part.split("-", 1)
            for i in range(int(lo), int(hi) + 1):
                if 1 <= i <= max_val:
                    result.add(i)
        else:
            i = int(part)
            if 1 <= i <= max_val:
                result.add(i)
    return sorted(result)


# ═══════════════════════════════════════════════════════════════════════════════
# TOOL EXECUTORS
# ═══════════════════════════════════════════════════════════════════════════════

# Shared agent state — mutable dict passed to all executors
agent_state = {
    "rcm_df": None,
    "output_dir": None,
    "suggestions_cache": None,
    "dedup_cache": None,
    "tod_results": None,
    "toe_results": None,
    "python_exec_count": 0,
}


def execute_tool(tool_name, args):
    """Route a tool call to the correct executor."""
    executors = {
        "load_rcm": exec_load_rcm,
        "run_ai_suggestions": exec_ai_suggestions,
        "merge_suggestions": exec_merge_suggestions,
        "run_control_assessment": exec_control_assessment,
        "run_deduplication": exec_deduplication,
        "remove_duplicates": exec_remove_duplicates,
        "run_test_of_design": exec_test_of_design,
        "run_test_of_effectiveness": exec_test_of_effectiveness,
        "modify_rcm": exec_modify_rcm,
        "save_excel": exec_save_excel,
        "list_directory": exec_list_directory,
        "execute_python": exec_execute_python,
        "read_file": exec_read_file,
        "inspect_dataframe": exec_inspect_dataframe,
        "web_search": exec_web_search,
    }
    executor = executors.get(tool_name)
    if not executor:
        return {"error": f"Unknown tool: {tool_name}"}
    try:
        return executor(args)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": f"{tool_name} failed: {str(e)}"}


# ── load_rcm ──────────────────────────────────────────────────────────────────

def exec_load_rcm(args):
    file_path = args["file_path"].strip().strip("'\"")
    if not os.path.exists(file_path):
        return {"error": f"File not found: {file_path}"}

    print(f"  [load_rcm] Loading {file_path}...")

    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path)
    else:
        df = pd.read_excel(file_path)

    # Normalize column names to standard format
    df = normalize_rcm_columns(df)

    # Auto-add risk_level with default "High" if missing
    if "risk_level" not in df.columns:
        df["risk_level"] = "High"
        print(f"  [load_rcm] Auto-added 'risk_level' column with default 'High'")

    agent_state["rcm_df"] = df

    # Create output directory (reuse existing if set)
    if agent_state["output_dir"] is None:
        rcm_dir = os.path.dirname(os.path.abspath(file_path))
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = os.path.join(rcm_dir, f"sox_agent_{ts}")
        os.makedirs(output_dir, exist_ok=True)
        agent_state["output_dir"] = output_dir
    else:
        output_dir = agent_state["output_dir"]

    # Check required columns
    present = [c for c in RCM_REQUIRED_COLUMNS if c in df.columns]
    missing = [c for c in RCM_REQUIRED_COLUMNS if c not in df.columns]

    # Preview
    preview_cols = [c for c in ["Process", "SubProcess", "Control Id", "Risk Id",
                                "Risk Title", "risk_level"] if c in df.columns]
    preview = sanitize_for_json(df.head(5)[preview_cols].to_dict(orient="records"))

    print(f"  [load_rcm] Loaded {len(df)} rows, {len(df.columns)} columns")
    print(f"  [load_rcm] Output dir: {output_dir}")

    return {
        "success": True,
        "rows": len(df),
        "columns": list(df.columns),
        "required_columns_present": present,
        "missing_columns": missing,
        "preview": preview,
        "output_directory": output_dir,
    }


# ── run_ai_suggestions ────────────────────────────────────────────────────────

def exec_ai_suggestions(args):
    if agent_state["rcm_df"] is None:
        return {"error": "No RCM loaded. Call load_rcm first."}

    industry = args["industry"]
    rcm_df = agent_state["rcm_df"]
    output_dir = agent_state["output_dir"]

    # Save current RCM to temp file
    temp_rcm = os.path.join(output_dir, "_temp_rcm_for_aisuggest.xlsx")
    rcm_df.to_excel(temp_rcm, index=False, engine="openpyxl")

    print(f"  [ai_suggestions] Running gap analysis for {industry}...")
    print(f"  [ai_suggestions] RCM: {len(rcm_df)} rows")

    # Import and configure engine
    import AiSuggest
    reload(AiSuggest)
    AiSuggest.OPENAI_API_KEY = OPENAI_API_KEY
    AiSuggest.OPENAI_MODEL = AGENT_MODEL
    AiSuggest.RCM_FILE_PATH = temp_rcm
    AiSuggest.INDUSTRY = industry
    AiSuggest.OUTPUT_EXCEL = os.path.join(output_dir, "1_AiSuggest_Combined.xlsx")
    AiSuggest.OUTPUT_JSON = os.path.join(output_dir, "1_AiSuggest_Response.json")
    AiSuggest.OUTPUT_TEXT = os.path.join(output_dir, "1_AiSuggest_Report.txt")
    AiSuggest.TEST_CONNECTION_FIRST = False

    # Run engine
    AiSuggest.main()

    # Read output JSON
    json_path = AiSuggest.OUTPUT_JSON
    if not os.path.exists(json_path):
        return {"error": "AI suggestions engine did not produce output. Check the logs above for errors."}

    with open(json_path, "r") as f:
        results = json.load(f)
    results = sanitize_for_json(results)

    suggestions = results.get("suggestions", [])
    agent_state["suggestions_cache"] = suggestions

    # Build numbered list for the agent to display
    numbered = []
    for i, s in enumerate(suggestions, 1):
        numbered.append({
            "#": i,
            "AI_Suggestion_ID": s.get("AI_Suggestion_ID", f"RCMAI-{i:03d}"),
            "AI_Priority": s.get("AI_Priority", ""),
            "AI_Category": s.get("AI_Category", ""),
            "Risk Title": s.get("Risk Title", ""),
            "Control Description": str(s.get("Control Description", ""))[:100],
            "AI_Reason": str(s.get("AI_Reason", ""))[:100],
        })

    print(f"  [ai_suggestions] Generated {len(suggestions)} suggestions")

    return {
        "success": True,
        "suggestion_count": len(suggestions),
        "executive_summary": results.get("executive_summary", ""),
        "gap_analysis": results.get("gap_analysis", ""),
        "suggestions": numbered,
        "output_excel": AiSuggest.OUTPUT_EXCEL,
    }


# ── merge_suggestions ─────────────────────────────────────────────────────────

def exec_merge_suggestions(args):
    if agent_state["rcm_df"] is None:
        return {"error": "No RCM loaded."}
    if agent_state["suggestions_cache"] is None:
        return {"error": "No suggestions available. Run run_ai_suggestions first."}

    suggestions = agent_state["suggestions_cache"]
    indices_str = args["indices"]
    keep_indices = parse_indices(indices_str, len(suggestions))

    if not keep_indices:
        return {"kept": 0, "original_rows": len(agent_state["rcm_df"]),
                "new_total": len(agent_state["rcm_df"]), "message": "No suggestions kept. RCM unchanged."}

    rcm_df = agent_state["rcm_df"].copy()
    rcm_columns = list(rcm_df.columns)

    # Build new rows from selected suggestions
    new_rows = []
    for idx in keep_indices:
        s = suggestions[idx - 1]
        row = {}
        for col in rcm_columns:
            row[col] = s.get(col, "")
        row["Row_Source"] = "AI_Suggestion"
        row["AI_Suggestion_ID"] = s.get("AI_Suggestion_ID", "")
        row["AI_Priority"] = s.get("AI_Priority", "")
        row["AI_Category"] = s.get("AI_Category", "")
        row["AI_Reason"] = s.get("AI_Reason", "")
        new_rows.append(row)

    # Mark original rows
    if "Row_Source" not in rcm_df.columns:
        rcm_df["Row_Source"] = "Original"
        rcm_df["AI_Suggestion_ID"] = ""
        rcm_df["AI_Priority"] = ""
        rcm_df["AI_Category"] = ""
        rcm_df["AI_Reason"] = ""

    new_df = pd.DataFrame(new_rows)
    updated = pd.concat([rcm_df, new_df], ignore_index=True)
    agent_state["rcm_df"] = updated

    original = len(rcm_df)
    print(f"  [merge_suggestions] Merged {len(keep_indices)} suggestions: {original} -> {len(updated)} rows")

    return {
        "success": True,
        "kept": len(keep_indices),
        "original_rows": original,
        "new_total": len(updated),
    }


# ── run_control_assessment ────────────────────────────────────────────────────

def exec_control_assessment(args):
    if agent_state["rcm_df"] is None:
        return {"error": "No RCM loaded."}

    policy_paths = args.get("policy_paths", [])
    sop_paths = args.get("sop_paths", [])
    output_dir = agent_state["output_dir"]

    # Validate paths
    valid_policies = [p for p in policy_paths if os.path.exists(p)]
    valid_sops = [p for p in sop_paths if os.path.exists(p)]
    if not valid_policies and not valid_sops:
        return {"error": "No valid policy or SOP files found at the given paths."}

    # Save RCM to temp
    temp_rcm = os.path.join(output_dir, "_temp_rcm_for_ca.xlsx")
    agent_state["rcm_df"].to_excel(temp_rcm, index=False, engine="openpyxl")

    out_excel = os.path.join(output_dir, "2_ControlAssessment.xlsx")
    out_json = os.path.join(output_dir, "2_ControlAssessment.json")

    print(f"  [control_assessment] {len(valid_policies)} policies, {len(valid_sops)} SOPs")
    print(f"  [control_assessment] Running OnGround Check...")

    # Import and configure
    import ControlAssesment
    reload(ControlAssesment)
    ControlAssesment.Config.OPENAI_API_KEY = OPENAI_API_KEY
    ControlAssesment.Config.OPENAI_MODEL = AGENT_MODEL
    ControlAssesment.Config.AZURE_OPENAI_ENDPOINT = AZURE_OPENAI_ENDPOINT
    ControlAssesment.Config.AZURE_OPENAI_API_VERSION = AZURE_OPENAI_API_VERSION

    checker = ControlAssesment.OnGroundCheck(
        rcm_path=temp_rcm,
        policy_paths=valid_policies if valid_policies else None,
        sop_paths=valid_sops if valid_sops else None,
        out_excel=out_excel,
        out_json=out_json,
    )
    checker.run()

    # Read results
    if not os.path.exists(out_json):
        return {"success": True, "message": "Assessment complete but no JSON output. Check Excel.",
                "output_excel": out_excel}

    with open(out_json, "r") as f:
        ca_results = json.load(f)
    ca_results = sanitize_for_json(ca_results)

    controls = ca_results.get("controls", [])
    summary = []
    for c in controls:
        summary.append({
            "control_id": c.get("control_id", ""),
            "policy_documented": c.get("policy_check", {}).get("documented", "N/A"),
            "match_pct": f"{c.get('match_pct', 0):.0f}%",
            "gaps": c.get("gaps", []),
        })

    print(f"  [control_assessment] Assessed {len(controls)} controls")

    return {
        "success": True,
        "controls_assessed": len(controls),
        "results": summary,
        "output_excel": out_excel,
        "policies_used": len(valid_policies),
        "sops_used": len(valid_sops),
    }


# ── run_deduplication ─────────────────────────────────────────────────────────

def exec_deduplication(args):
    if agent_state["rcm_df"] is None:
        return {"error": "No RCM loaded."}

    output_dir = agent_state["output_dir"]
    rcm_df = agent_state["rcm_df"]

    # Save RCM to temp
    temp_rcm = os.path.join(output_dir, "_temp_rcm_for_dedup.xlsx")
    rcm_df.to_excel(temp_rcm, index=False, engine="openpyxl")

    print(f"  [deduplication] Analyzing {len(rcm_df)} rows for duplicates...")

    # Import and configure
    import DeDupli
    reload(DeDupli)
    DeDupli.OPENAI_API_KEY = OPENAI_API_KEY
    DeDupli.OPENAI_MODEL = AGENT_MODEL
    DeDupli.RCM_INPUT = temp_rcm
    DeDupli.INPUT_IS_FOLDER = False
    DeDupli.OUTPUT_FOLDER = output_dir
    DeDupli.OUTPUT_EXCEL_NAME = "3_Dedup_Pairs"
    DeDupli.OUTPUT_JSON_NAME = "3_Dedup_Results"

    DeDupli.main()

    # Read results
    json_path = os.path.join(output_dir, "3_Dedup_Results.json")
    if not os.path.exists(json_path):
        return {"success": True, "pair_count": 0, "message": "No duplicates found. RCM is clean."}

    with open(json_path, "r") as f:
        dedup_results = json.load(f)
    dedup_results = sanitize_for_json(dedup_results)

    # Collect all pairs with process context
    all_pairs = []
    results_by_process = dedup_results.get("results_by_process", {})
    for process_name, pdata in results_by_process.items():
        for pair in pdata.get("pairs", []):
            pair["_process"] = process_name
            all_pairs.append(pair)

    agent_state["dedup_cache"] = {
        "pairs": all_pairs,
        "results_by_process": results_by_process,
    }

    # Build numbered pair list
    numbered = []
    for i, p in enumerate(all_pairs, 1):
        numbered.append({
            "#": i,
            "process": p.get("_process", ""),
            "row_a": p.get("row_a", "?"),
            "row_a_risk": str(p.get("row_a_risk", ""))[:60],
            "row_b": p.get("row_b", "?"),
            "row_b_risk": str(p.get("row_b_risk", ""))[:60],
            "confidence": p.get("confidence", ""),
            "reasoning": str(p.get("reasoning", ""))[:100],
            "recommendation": str(p.get("recommendation", ""))[:80],
        })

    print(f"  [deduplication] Found {len(all_pairs)} duplicate pairs")

    return {
        "success": True,
        "pair_count": len(all_pairs),
        "pairs": numbered,
        "summary": dedup_results.get("summary", {}),
        "output_excel": os.path.join(output_dir, "3_Dedup_Pairs.xlsx"),
    }


# ── remove_duplicates ─────────────────────────────────────────────────────────

def exec_remove_duplicates(args):
    if agent_state["rcm_df"] is None:
        return {"error": "No RCM loaded."}
    if agent_state["dedup_cache"] is None:
        return {"error": "No deduplication results. Run run_deduplication first."}

    removals = args.get("removals", [])
    if not removals:
        return {"removed_count": 0, "new_total": len(agent_state["rcm_df"]),
                "message": "No removals specified. RCM unchanged."}

    rcm_df = agent_state["rcm_df"]
    all_pairs = agent_state["dedup_cache"]["pairs"]
    results_by_process = agent_state["dedup_cache"]["results_by_process"]

    # Build index mapping: for each process group, map local row index to global DF index
    rows_to_drop = set()

    for removal in removals:
        pair_num = removal.get("pair", 0) - 1  # 0-based
        remove_which = removal.get("remove", "").lower()

        if pair_num < 0 or pair_num >= len(all_pairs):
            continue

        pair = all_pairs[pair_num]
        process_name = pair.get("_process", "")
        local_a = pair.get("row_a")
        local_b = pair.get("row_b")

        # Get global indices for this process group
        process_mask = rcm_df["Process"] == process_name
        global_indices = rcm_df[process_mask].index.tolist()

        if remove_which in ("a", "both"):
            if isinstance(local_a, int) and local_a < len(global_indices):
                rows_to_drop.add(global_indices[local_a])
        if remove_which in ("b", "both"):
            if isinstance(local_b, int) and local_b < len(global_indices):
                rows_to_drop.add(global_indices[local_b])

    before = len(rcm_df)
    if rows_to_drop:
        rcm_df = rcm_df.drop(index=list(rows_to_drop)).reset_index(drop=True)
        agent_state["rcm_df"] = rcm_df

    removed = before - len(rcm_df)
    print(f"  [remove_duplicates] Removed {removed} rows: {before} -> {len(rcm_df)}")

    return {
        "success": True,
        "removed_count": removed,
        "original_rows": before,
        "new_total": len(rcm_df),
    }


# ── run_test_of_design ────────────────────────────────────────────────────────

def exec_test_of_design(args):
    if agent_state["rcm_df"] is None:
        return {"error": "No RCM loaded."}

    evidence_folder = args["evidence_folder"].strip().strip("'\"")
    if not os.path.exists(evidence_folder):
        return {"error": f"Evidence folder not found: {evidence_folder}"}

    output_dir = agent_state["output_dir"]
    rcm_df = agent_state["rcm_df"].copy()

    # Auto-add missing required columns with defaults
    if "risk_level" not in rcm_df.columns:
        rcm_df["risk_level"] = "High"
        agent_state["rcm_df"] = rcm_df
        print(f"  [tod] Auto-added 'risk_level' column with default 'High'")

    # Save RCM to temp
    temp_rcm = os.path.join(output_dir, "_temp_rcm_for_tod.xlsx")
    rcm_df.to_excel(temp_rcm, index=False, engine="openpyxl")

    print(f"  [tod] Loading evidence from {evidence_folder}...")

    import rcm_tester
    reload(rcm_tester)

    sample_bank = rcm_tester.load_evidence_folder(evidence_folder)
    print(f"  [tod] Evidence loaded for {len(sample_bank)} controls")

    if not sample_bank:
        return {"error": "No evidence files found in the folder. Expected subfolders named by Control ID (e.g. C-P2P-001/)."}

    # Create tester and run
    tester = rcm_tester.RCMControlTester(
        rcm_path=temp_rcm,
        openai_api_key=OPENAI_API_KEY,
        openai_model=AGENT_MODEL,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        azure_api_key=OPENAI_API_KEY,
        azure_deployment=AGENT_MODEL,
        azure_api_version=AZURE_OPENAI_API_VERSION,
    )

    print(f"  [tod] Running Test of Design... (this may take several minutes)")
    results = tester.test_all(sample_bank, max_workers=5)
    agent_state["tod_results"] = results

    # Export
    tod_output = os.path.join(output_dir, "4_TOD_Results.xlsx")
    tester.export_results(results, tod_output, sample_bank=sample_bank)

    # Generate summary report
    summary_report = tester.generate_summary_report(results)

    # Build results for agent
    result_list = []
    for r in results:
        result_list.append({
            "control_id": r.control_id,
            "risk_id": r.risk_id,
            "result": r.result,
            "design_adequate": r.design_adequate,
            "confidence": r.confidence,
            "deficiency_type": r.deficiency_type,
            "gap_identified": str(r.gap_identified)[:150],
            "remarks": str(r.remarks)[:150],
        })

    passed = sum(1 for r in results if r.result == "PASS")
    failed = sum(1 for r in results if r.result == "FAIL")

    print(f"  [tod] Complete: {passed} PASS, {failed} FAIL out of {len(results)}")

    return {
        "success": True,
        "controls_evaluated": len(results),
        "passed": passed,
        "failed": failed,
        "controls_with_evidence": len(sample_bank),
        "results": result_list,
        "summary_report": summary_report,
        "output_excel": tod_output,
    }


# ── run_test_of_effectiveness ─────────────────────────────────────────────────

def exec_test_of_effectiveness(args):
    if agent_state["rcm_df"] is None:
        return {"error": "No RCM loaded."}

    evidence_folder = args["evidence_folder"].strip().strip("'\"")
    if not os.path.exists(evidence_folder):
        return {"error": f"Evidence folder not found: {evidence_folder}"}

    output_dir = agent_state["output_dir"]
    rcm_df = agent_state["rcm_df"].copy()

    # Ensure required columns exist with defaults
    if "risk_level" not in rcm_df.columns:
        rcm_df["risk_level"] = "High"
        print(f"  [toe] Auto-added 'risk_level' column with default 'High'")
    if "count_of_samples" not in rcm_df.columns:
        rcm_df["count_of_samples"] = ""
    agent_state["rcm_df"] = rcm_df

    # Save RCM to temp
    temp_rcm = os.path.join(output_dir, "_temp_rcm_for_toe.xlsx")
    rcm_df.to_excel(temp_rcm, index=False, engine="openpyxl")

    print(f"  [toe] Loading evidence from {evidence_folder}...")

    import TOE_Engine
    reload(TOE_Engine)

    toe_bank = TOE_Engine.load_toe_evidence_folder(evidence_folder)
    print(f"  [toe] Evidence loaded for {len(toe_bank)} controls")

    if not toe_bank:
        return {"error": "No evidence files found. Expected subfolders per Control ID with sample .txt files."}

    company_name = args.get("company_name", "")
    prepared_by = args.get("prepared_by", "")
    reviewed_by = args.get("reviewed_by", "")

    # Create tester and run
    tester = TOE_Engine.RCMControlTester(
        rcm_path=temp_rcm,
        openai_api_key=OPENAI_API_KEY,
        openai_model=AGENT_MODEL,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        azure_api_key=OPENAI_API_KEY,
        azure_deployment=AGENT_MODEL,
        azure_api_version=AZURE_OPENAI_API_VERSION,
    )

    print(f"  [toe] Running Test of Operating Effectiveness... (this may take several minutes)")
    results = tester.test_all_toe(toe_bank, max_workers=5)
    agent_state["toe_results"] = results

    # Export workpaper
    toe_output = os.path.join(output_dir, "5_TOE_Workpaper.xlsx")
    tester.export_toe_workpaper(
        results, toe_output, toe_bank=toe_bank,
        company_name=company_name,
        prepared_by=prepared_by,
        reviewed_by=reviewed_by,
    )

    # Generate report
    toe_report = tester.generate_toe_report(results)

    # Build results for agent
    summary_list = []
    detail_list = []
    for r in results:
        summary_list.append({
            "control_id": r.control_id,
            "total_samples": r.total_samples,
            "passed_samples": r.passed_samples,
            "failed_samples": r.failed_samples,
            "deviation_rate": f"{r.deviation_rate:.1%}",
            "operating_effectiveness": r.operating_effectiveness,
            "deficiency_type": r.deficiency_type,
        })
        samples = []
        for sr in r.sample_results:
            samples.append({
                "sample_id": sr.sample_id,
                "result": sr.result,
                "deviation_details": str(getattr(sr, "deviation_details", ""))[:100],
                "remarks": str(sr.remarks)[:100],
            })
        detail_list.append({
            "control_id": r.control_id,
            "effectiveness": r.operating_effectiveness,
            "samples": samples,
        })

    effective = sum(1 for r in results if r.operating_effectiveness == "Effective")
    exceptions = sum(1 for r in results if r.operating_effectiveness == "Effective with Exceptions")
    not_effective = sum(1 for r in results if r.operating_effectiveness == "Not Effective")

    print(f"  [toe] Complete: {effective} Effective, {exceptions} Exceptions, {not_effective} Not Effective")

    return {
        "success": True,
        "controls_evaluated": len(results),
        "effective": effective,
        "effective_with_exceptions": exceptions,
        "not_effective": not_effective,
        "summary": summary_list,
        "details": detail_list,
        "toe_report": toe_report,
        "output_excel": toe_output,
    }


# ── modify_rcm ────────────────────────────────────────────────────────────────

def exec_modify_rcm(args):
    if agent_state["rcm_df"] is None:
        return {"error": "No RCM loaded. Call load_rcm first."}

    rcm_df = agent_state["rcm_df"]
    action = args.get("action")
    column_name = args.get("column_name", "").strip()

    if action == "add_column":
        value = args.get("value", "")
        if column_name in rcm_df.columns:
            return {"error": f"Column '{column_name}' already exists. Use update_values to change its values."}
        rcm_df = rcm_df.copy()
        rcm_df[column_name] = value
        agent_state["rcm_df"] = rcm_df
        print(f"  [modify_rcm] Added column '{column_name}' with default value '{value}' ({len(rcm_df)} rows)")
        return {
            "success": True,
            "action": "add_column",
            "column": column_name,
            "default_value": value,
            "total_rows": len(rcm_df),
            "total_columns": len(rcm_df.columns),
        }

    elif action == "rename_column":
        new_name = args.get("new_name", "").strip()
        if not new_name:
            return {"error": "new_name is required for rename_column action."}
        if column_name not in rcm_df.columns:
            return {"error": f"Column '{column_name}' not found. Available: {list(rcm_df.columns)}"}
        rcm_df = rcm_df.rename(columns={column_name: new_name})
        agent_state["rcm_df"] = rcm_df
        print(f"  [modify_rcm] Renamed '{column_name}' → '{new_name}'")
        return {
            "success": True,
            "action": "rename_column",
            "old_name": column_name,
            "new_name": new_name,
        }

    elif action == "update_values":
        value = args.get("value", "")
        if column_name not in rcm_df.columns:
            return {"error": f"Column '{column_name}' not found. Available: {list(rcm_df.columns)}"}

        rcm_df = rcm_df.copy()
        cond_col = args.get("condition_column")
        cond_val = args.get("condition_value")

        if cond_col and cond_val is not None:
            if cond_col not in rcm_df.columns:
                return {"error": f"Condition column '{cond_col}' not found."}
            mask = rcm_df[cond_col].astype(str) == str(cond_val)
            rcm_df.loc[mask, column_name] = value
            updated = int(mask.sum())
            print(f"  [modify_rcm] Updated {updated} rows where {cond_col}='{cond_val}' → {column_name}='{value}'")
        else:
            rcm_df[column_name] = value
            updated = len(rcm_df)
            print(f"  [modify_rcm] Set all {updated} rows: {column_name}='{value}'")

        agent_state["rcm_df"] = rcm_df
        return {
            "success": True,
            "action": "update_values",
            "column": column_name,
            "value": value,
            "rows_updated": updated,
        }

    else:
        return {"error": f"Unknown action: {action}. Use add_column, rename_column, or update_values."}


# ── save_excel ────────────────────────────────────────────────────────────────

def exec_save_excel(args):
    if agent_state["rcm_df"] is None:
        return {"error": "No RCM loaded."}

    step_name = args["step_name"]
    output_dir = agent_state["output_dir"]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{step_name}_{ts}.xlsx"
    path = os.path.join(output_dir, filename)

    agent_state["rcm_df"].to_excel(path, index=False, engine="openpyxl")
    rows = len(agent_state["rcm_df"])
    cols = len(agent_state["rcm_df"].columns)

    print(f"  [save_excel] Saved: {path} ({rows} rows x {cols} cols)")

    return {
        "success": True,
        "path": path,
        "rows": rows,
        "columns": cols,
    }


# ── list_directory ────────────────────────────────────────────────────────────

def exec_list_directory(args):
    path = args["path"].strip().strip("'\"")
    ext = args.get("extension", "")

    if not os.path.exists(path):
        return {"error": f"Directory not found: {path}"}
    if not os.path.isdir(path):
        return {"error": f"Not a directory: {path}"}

    items = []
    for entry in sorted(os.scandir(path), key=lambda e: e.name):
        if entry.is_file():
            if ext and not entry.name.lower().endswith(ext.lower()):
                continue
            items.append({
                "name": entry.name,
                "type": "file",
                "size_kb": round(entry.stat().st_size / 1024, 1),
            })
        elif entry.is_dir():
            items.append({"name": entry.name + "/", "type": "directory"})

    return {"directory": path, "items": items, "count": len(items)}


# ── execute_python ─────────────────────────────────────────────────────────────

def exec_execute_python(args):
    code = args.get("code", "")
    if not code.strip():
        return {"error": "No code provided."}

    agent_state["python_exec_count"] = agent_state.get("python_exec_count", 0) + 1
    print(f"  [execute_python] Running code snippet #{agent_state['python_exec_count']}...")

    # Build sandbox namespace
    df = agent_state.get("rcm_df")
    original_df_id = id(df) if df is not None else None

    namespace = {
        "df": df,
        "pd": pd,
        "np": np,
        "os": os,
        "json": json,
        "math": math,
        "datetime": datetime,
        "agent_state": agent_state,
        "result": None,
    }

    # Capture stdout
    old_stdout = sys.stdout
    sys.stdout = captured = io.StringIO()

    try:
        exec(code, namespace)
    except Exception as e:
        sys.stdout = old_stdout
        tb_lines = tb_module.format_exc().strip().split("\n")
        print(f"  [execute_python] Error: {e}")
        return {
            "error": str(e),
            "traceback": "\n".join(tb_lines[-5:]),
        }
    finally:
        sys.stdout = old_stdout

    stdout_text = captured.getvalue()

    # If df was reassigned in the code, update agent_state
    new_df = namespace.get("df")
    if new_df is not None and id(new_df) != original_df_id:
        agent_state["rcm_df"] = new_df
        print(f"  [execute_python] DataFrame updated: {new_df.shape[0]} rows x {new_df.shape[1]} cols")

    result_val = namespace.get("result")

    # Stringify result for JSON
    if result_val is not None:
        try:
            if isinstance(result_val, pd.DataFrame):
                result_val = sanitize_for_json(result_val.head(50).to_dict(orient="records"))
            elif isinstance(result_val, pd.Series):
                result_val = sanitize_for_json(result_val.head(50).to_dict())
            else:
                result_val = sanitize_for_json(result_val)
        except Exception:
            result_val = str(result_val)

    # Truncate to avoid bloating LLM context
    if isinstance(stdout_text, str) and len(stdout_text) > 4000:
        stdout_text = stdout_text[:4000] + "\n... (truncated)"
    result_str = str(result_val) if result_val is not None else None
    if result_str and len(result_str) > 4000:
        result_str = result_str[:4000] + "... (truncated)"
        result_val = result_str

    current_df = agent_state.get("rcm_df")
    df_shape = [current_df.shape[0], current_df.shape[1]] if current_df is not None else None

    print(f"  [execute_python] Done. stdout={len(stdout_text)} chars, result={'set' if result_val is not None else 'None'}")

    return {
        "success": True,
        "result": result_val,
        "stdout": stdout_text if stdout_text else None,
        "df_shape": df_shape,
    }


# ── read_file ──────────────────────────────────────────────────────────────────

def exec_read_file(args):
    file_path = args["file_path"].strip().strip("'\"")
    max_lines = min(args.get("max_lines", 200), 500)
    encoding = args.get("encoding", "utf-8")

    if not os.path.exists(file_path):
        return {"error": f"File not found: {file_path}"}
    if os.path.isdir(file_path):
        return {"error": f"Path is a directory, not a file: {file_path}. Use list_directory instead."}

    file_size = os.path.getsize(file_path)
    if file_size > 50 * 1024 * 1024:
        return {"error": f"File too large ({file_size / 1024 / 1024:.1f} MB). Max 50 MB."}

    ext = os.path.splitext(file_path)[1].lower()
    print(f"  [read_file] Reading {file_path} ({file_size / 1024:.1f} KB, type={ext})")

    TEXT_EXTENSIONS = {".txt", ".csv", ".json", ".py", ".md", ".log", ".xml",
                       ".yaml", ".yml", ".ini", ".cfg", ".html", ".htm", ".js",
                       ".ts", ".sql", ".sh", ".bat", ".r", ".sas", ".toml"}

    try:
        if ext in (".xlsx", ".xls"):
            xls = pd.ExcelFile(file_path)
            sheet_names = xls.sheet_names
            df = pd.read_excel(file_path, sheet_name=0)
            total_rows = len(df)
            content = df.head(max_lines).to_string(index=False)
            return {
                "success": True,
                "file_path": file_path,
                "file_type": "excel",
                "sheet_names": sheet_names,
                "total_rows": total_rows,
                "lines_shown": min(max_lines, total_rows),
                "columns": list(df.columns),
                "content": content,
                "truncated": total_rows > max_lines,
            }

        elif ext == ".pdf":
            try:
                import PyPDF2
                with open(file_path, "rb") as f:
                    reader = PyPDF2.PdfReader(f)
                    pages = []
                    for page in reader.pages[:20]:
                        pages.append(page.extract_text() or "")
                    content = "\n\n--- Page Break ---\n\n".join(pages)
                    lines = content.split("\n")
                    total_lines = len(lines)
                    if total_lines > max_lines:
                        content = "\n".join(lines[:max_lines])
                    return {
                        "success": True,
                        "file_path": file_path,
                        "file_type": "pdf",
                        "total_pages": len(reader.pages),
                        "pages_read": min(20, len(reader.pages)),
                        "total_lines": total_lines,
                        "content": content,
                        "truncated": total_lines > max_lines,
                    }
            except ImportError:
                return {"error": "PDF reading requires PyPDF2. Install with: pip install PyPDF2"}

        else:
            # Read as text
            with open(file_path, "r", encoding=encoding, errors="replace") as f:
                all_lines = f.readlines()
            total_lines = len(all_lines)
            shown_lines = all_lines[:max_lines]
            content = "".join(shown_lines)
            if len(content) > 20000:
                content = content[:20000] + "\n... (content truncated at 20000 chars)"
            return {
                "success": True,
                "file_path": file_path,
                "file_type": ext if ext else "text",
                "total_lines": total_lines,
                "lines_shown": len(shown_lines),
                "content": content,
                "truncated": total_lines > max_lines,
            }

    except Exception as e:
        return {"error": f"Failed to read file: {str(e)}"}


# ── inspect_dataframe ──────────────────────────────────────────────────────────

def exec_inspect_dataframe(args):
    if agent_state["rcm_df"] is None:
        return {"error": "No RCM loaded. Call load_rcm first."}

    df = agent_state["rcm_df"]
    mode = args.get("mode", "info")
    n = args.get("n", 10)
    column = args.get("column", "")
    query_expr = args.get("query_expr", "")

    print(f"  [inspect_dataframe] mode={mode}, n={n}")

    if mode == "info":
        dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()}
        nulls = {col: int(v) for col, v in df.isnull().sum().items() if v > 0}
        return {
            "success": True,
            "mode": "info",
            "shape": {"rows": df.shape[0], "columns": df.shape[1]},
            "dtypes": dtypes,
            "null_counts": nulls,
            "memory_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
        }

    elif mode in ("head", "tail"):
        subset = df.head(n) if mode == "head" else df.tail(n)
        records = sanitize_for_json(subset.to_dict(orient="records"))
        return {
            "success": True,
            "mode": mode,
            "n": n,
            "total_rows": len(df),
            "columns": list(df.columns),
            "data": records,
        }

    elif mode == "describe":
        desc = df.describe(include="all")
        return {
            "success": True,
            "mode": "describe",
            "statistics": sanitize_for_json(desc.to_dict()),
        }

    elif mode == "columns":
        col_info = [{"name": col, "dtype": str(df[col].dtype), "non_null": int(df[col].notna().sum()),
                      "unique": int(df[col].nunique())} for col in df.columns]
        return {
            "success": True,
            "mode": "columns",
            "total_columns": len(df.columns),
            "columns": col_info,
        }

    elif mode == "value_counts":
        if not column:
            return {"error": "column parameter is required for value_counts mode."}
        if column not in df.columns:
            return {"error": f"Column '{column}' not found. Available: {list(df.columns)}"}
        vc = df[column].value_counts()
        return {
            "success": True,
            "mode": "value_counts",
            "column": column,
            "total_rows": len(df),
            "unique_values": int(vc.shape[0]),
            "counts": sanitize_for_json(vc.head(20).to_dict()),
            "null_count": int(df[column].isnull().sum()),
        }

    elif mode == "sample":
        sample_n = min(n, len(df))
        subset = df.sample(sample_n)
        records = sanitize_for_json(subset.to_dict(orient="records"))
        return {
            "success": True,
            "mode": "sample",
            "n": sample_n,
            "total_rows": len(df),
            "data": records,
        }

    elif mode == "query":
        if not query_expr:
            return {"error": "query_expr parameter is required for query mode."}
        try:
            filtered = df.query(query_expr)
        except Exception as e:
            return {"error": f"Query failed: {str(e)}. Available columns: {list(df.columns)}"}
        total_matching = len(filtered)
        records = sanitize_for_json(filtered.head(50).to_dict(orient="records"))
        return {
            "success": True,
            "mode": "query",
            "query": query_expr,
            "matching_rows": total_matching,
            "total_rows": len(df),
            "showing": min(50, total_matching),
            "data": records,
        }

    else:
        return {"error": f"Unknown mode: {mode}. Use: info, head, tail, describe, columns, value_counts, sample, query."}


# ── web_search ─────────────────────────────────────────────────────────────────

def exec_web_search(args):
    import requests as req

    query = args.get("query", "").strip()
    num_results = min(args.get("num_results", 5), 10)

    if not query:
        return {"error": "No query provided."}

    print(f"  [web_search] Searching: {query}")

    results = []
    ddg_summary = ""

    # Try DuckDuckGo Instant Answer API (free, no key)
    try:
        ddg_url = "https://api.duckduckgo.com/"
        resp = req.get(ddg_url, params={"q": query, "format": "json", "no_html": "1", "skip_disambig": "1"}, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("AbstractText"):
                ddg_summary = data["AbstractText"]
                results.append({
                    "title": data.get("AbstractSource", "DuckDuckGo"),
                    "snippet": data["AbstractText"],
                    "url": data.get("AbstractURL", ""),
                })
            for topic in data.get("RelatedTopics", [])[:num_results]:
                if isinstance(topic, dict) and topic.get("Text"):
                    results.append({
                        "title": topic.get("Text", "")[:80],
                        "snippet": topic.get("Text", ""),
                        "url": topic.get("FirstURL", ""),
                    })
    except Exception as e:
        print(f"  [web_search] DuckDuckGo API error: {e}")

    # Supplement with LLM knowledge if results are thin
    llm_answer = ""
    if len(results) < 2:
        try:
            llm_resp = agent_client.chat.completions.create(
                model=AGENT_MODEL,
                messages=[
                    {"role": "system", "content": "You are a SOX compliance and audit expert. Provide factual, concise answers about SOX regulations, PCAOB standards, COSO framework, COBIT, audit procedures, and internal controls. Cite specific standard numbers where possible. If unsure, say so."},
                    {"role": "user", "content": query},
                ],
                temperature=0.2,
                max_tokens=1000,
            )
            llm_answer = llm_resp.choices[0].message.content or ""
        except Exception as e:
            print(f"  [web_search] LLM knowledge call error: {e}")

    print(f"  [web_search] Found {len(results)} web results, LLM answer={'yes' if llm_answer else 'no'}")

    return {
        "success": True,
        "query": query,
        "web_results": results[:num_results],
        "llm_knowledge": llm_answer if llm_answer else None,
        "source_note": "Web results from DuckDuckGo. LLM knowledge from GPT model training data — verify critical regulatory details against official sources.",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SOX AGENT — LLM CONVERSATION LOOP
# ═══════════════════════════════════════════════════════════════════════════════

class SOXAgent:
    """LLM-driven agent that orchestrates the SOX audit pipeline."""

    def __init__(self):
        self.conversation = [{"role": "system", "content": SYSTEM_PROMPT}]

    def chat(self, user_message):
        """Process a user message through the agent loop."""
        self.conversation.append({"role": "user", "content": user_message})

        for _ in range(1, MAX_ROUNDS + 1):
            try:
                response = agent_client.chat.completions.create(
                    model=AGENT_MODEL,
                    messages=self.conversation,
                    tools=TOOL_DEFINITIONS,
                    tool_choice="auto",
                    temperature=0.3,
                    max_tokens=4096,
                )
            except Exception as e:
                print(f"\n  [ERROR] LLM API call failed: {e}")
                self.conversation.append({
                    "role": "assistant",
                    "content": f"I encountered an error calling the AI service: {e}",
                })
                return

            choice = response.choices[0]
            msg = choice.message

            # Display assistant text
            if msg.content:
                print(f"\nAgent: {msg.content}")

            # Build conversation entry
            entry = {"role": "assistant", "content": msg.content}
            if msg.tool_calls:
                entry["tool_calls"] = [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        },
                    }
                    for tc in msg.tool_calls
                ]
            self.conversation.append(entry)

            # No tool calls = agent is waiting for user input
            if not msg.tool_calls:
                return

            # Execute each tool call
            for tc in msg.tool_calls:
                name = tc.function.name
                try:
                    args = json.loads(tc.function.arguments or "{}")
                except json.JSONDecodeError:
                    args = {}

                print(f"\n  >> Tool: {name}({json.dumps(args, indent=2)[:200]})")
                t_start = time.time()
                result = execute_tool(name, args)
                duration = time.time() - t_start
                result_str = json.dumps(sanitize_for_json(result), default=str)
                print(f"  << {name} completed in {duration:.1f}s")

                self.conversation.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result_str,
                })

            # If finish_reason is stop, no more processing needed
            if choice.finish_reason == "stop":
                return


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 70)
    print("  SOX AUDIT AGENT")
    print("  AI-powered interactive audit pipeline")
    print("=" * 70)
    print()
    print("  Type your request to begin (e.g. 'Start the SOX audit')")
    print("  Commands:  /new = new session  |  /quit = exit")
    print()

    agent = SOXAgent()

    while True:
        try:
            user_input = input("\nYou: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not user_input:
            continue
        if user_input.lower() in ("/quit", "/exit", "quit", "exit"):
            print("Goodbye!")
            break
        if user_input.lower() in ("/new", "/reset"):
            agent = SOXAgent()
            agent_state["rcm_df"] = None
            agent_state["output_dir"] = None
            agent_state["suggestions_cache"] = None
            agent_state["dedup_cache"] = None
            agent_state["tod_results"] = None
            agent_state["toe_results"] = None
            agent_state["python_exec_count"] = 0
            print("--- New session started ---")
            continue

        agent.chat(user_input)


if __name__ == "__main__":
    main()
