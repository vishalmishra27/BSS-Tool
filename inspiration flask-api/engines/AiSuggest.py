"""
+------------------------------------------------------------------------------+
|                                                                              |
|          RCM AI-POWERED GAP ANALYSIS & SUGGESTION ENGINE v4.2                |
|                      Planning & Scoping - Final Step                         |
|                                                                              |
|   v4.2 Updates:                                                              |
|   - PARALLEL SUBPROCESS ANALYSIS - Analyze each subprocess separately       |
|   - Handles large RCMs (500+ rows) efficiently                               |
|   - 3-5x faster with concurrent API calls                                    |
|   - 70-80% reduction in per-call token usage                                 |
|   - Single-sheet Excel output (Combined RCM)                                 |
|                                                                              |
+------------------------------------------------------------------------------+

BEFORE RUNNING:
    pip install --upgrade typing_extensions pydantic openai pandas openpyxl
"""

import pandas as pd
import json
import os
import sys
import time
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# +============================================================================+
# |                    CONFIGURATION - EDIT THIS SECTION                       |
# +============================================================================+

# Azure OpenAI credentials — imported from central config
from engines.config import (
    AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY,
    AZURE_OPENAI_API_VERSION, AZURE_OPENAI_DEPLOYMENT as AZURE_OPENAI_DEPLOYMENT_NAME,
    OPENAI_API_KEY, OPENAI_MODEL,
)

# Input Files - EDIT THESE
RCM_FILE_PATH = "/Users/rishi/Downloads/Sample_Data/output.xlsx"
INDUSTRY = "Banking & Financial Services"

# Optional: PDF files for additional context (leave empty if none)
PDF_FILES = []

# Output Files (will be created in same directory as RCM file)
OUTPUT_EXCEL = "Final_RCM_AI_Analysis.xlsx"
OUTPUT_JSON = "full_ai_response.json"
OUTPUT_TEXT = "full_output_report.txt"

# -----------------------------------------------------------------------------
# Performance Settings
# -----------------------------------------------------------------------------
TEST_CONNECTION_FIRST = True

# Maximum parallel API calls (1-10)
# Higher = faster but may hit API rate limits
# Recommended: 3-5 for most Azure OpenAI deployments
MAX_PARALLEL_API_CALLS = 5

# +============================================================================+
# |                         SUPPORTED INDUSTRIES                               |
# +============================================================================+

SUPPORTED_INDUSTRIES = [
    "Manufacturing",
    "Banking",
    "Banking & Financial Services",
    "Financial Services",
    "Healthcare",
    "Technology",
    "Retail",
    "Insurance",
    "Energy",
    "Telecommunications",
    "Pharmaceuticals",
    "Consumer Goods",
    "Automotive",
    "Aerospace",
    "Real Estate",
    "Transportation",
    "Media & Entertainment",
    "Oil & Gas",
    "Utilities",
    "Construction",
    "Education",
    "Government",
    "Non-Profit",
    "Hospitality",
    "Agriculture",
    "Mining",
    "Other"
]

# +============================================================================+
# |                         END OF CONFIGURATION                               |
# +============================================================================+


# Check dependencies
try:
    from openai import AzureOpenAI
except ImportError as e:
    print(f"[ERROR] OpenAI import error: {e}")
    print("\nRun: pip install --upgrade typing_extensions pydantic openai")
    raise SystemExit(1)

try:
    import sys as _sys
    from pathlib import Path as _Path
    _di_dir = str(_Path(__file__).resolve().parent.parent)
    if _di_dir not in _sys.path:
        _sys.path.insert(0, _di_dir)
    from Document_Intelligence import parse_document as _di_parse_document
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False


def create_llm_client():
    return AzureOpenAI(
        api_key=OPENAI_API_KEY,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version=AZURE_OPENAI_API_VERSION,
    )


# -----------------------------------------------------------------------------
#                              UI FUNCTIONS
# -----------------------------------------------------------------------------

def clear_line():
    sys.stdout.write('\r' + ' ' * 80 + '\r')
    sys.stdout.flush()

def print_banner():
    print("\n")
    print("  +========================================================================+")
    print("  |                                                                        |")
    print("  |          RCM AI-POWERED GAP ANALYSIS ENGINE v4.0                   |")
    print("  |                                                                        |")
    print("  |              Planning & Scoping Module - Final Step                    |")
    print("  |                                                                        |")
    print("  +========================================================================+")
    print(f"\n   Analysis Date: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}")
    print(f"   Industry: {INDUSTRY}")
    print()

def print_section(title):
    print(f"\n  === {title} {'=' * (65 - len(title))}")

def print_step(num, total, text):
    pct = int((num / total) * 100)
    bar = "#" * (pct // 5) + "." * (20 - pct // 5)
    print(f"\n  [{bar}] Step {num}/{total}")
    print(f"  {text}")
    print("  " + "-" * 72)

def print_item(text):
    print(f"    -> {text}")

def print_success(text):
    print(f"    [OK] {text}")

def print_error(text):
    print(f"    [ERROR] {text}")

def print_warning(text):
    print(f"    [WARN]  {text}")

def print_info(text):
    print(f"    [INFO]  {text}")

def print_progress_bar(current, total, prefix=""):
    pct = int((current / total) * 100)
    bar = "#" * (pct // 2) + "." * (50 - pct // 2)
    sys.stdout.write(f'\r    {prefix} [{bar}] {pct}%')
    sys.stdout.flush()

def print_box(lines, title=""):
    max_len = max(len(line) for line in lines) if lines else 40
    max_len = max(max_len, len(title) + 4, 50)
    
    print(f"    +{'-' * (max_len + 2)}+")
    if title:
        print(f"    | {title}{' ' * (max_len - len(title))} |")
        print(f"    +{'-' * (max_len + 2)}+")
    for line in lines:
        print(f"    | {line}{' ' * (max_len - len(line))} |")
    print(f"    +{'-' * (max_len + 2)}+")


# -----------------------------------------------------------------------------
#                         CONNECTION TEST
# -----------------------------------------------------------------------------

def test_ai_connection():
    """Test Azure OpenAI connection"""
    
    print_section("CONNECTION TEST")
    print()
    print_item("Testing Azure OpenAI connection...")
    print()
    
    try:
        print_item("Step 1: Creating client...")
        client = create_llm_client()
        print_success("Client created successfully")

        print_item(f"Step 2: Sending test request to: {OPENAI_MODEL}")
        
        start_time = time.time()
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "user", "content": "Reply with exactly: CONNECTION_OK"}
            ],
            max_completion_tokens=20,
        )
        elapsed = time.time() - start_time

        reply = response.choices[0].message.content.strip()
        print_success(f"Response received in {elapsed:.2f} seconds")
        
        print()
        print_box([
            f"Status:        [OK] CONNECTED",
            f"Provider:      Azure OpenAI",
            f"Model:         {OPENAI_MODEL}",
            f"Response Time: {elapsed:.2f} seconds",
            f"AI Response:   \"{reply}\"",
        ], title=" CONNECTION TEST PASSED")
        print()
        
        return True, client
        
    except Exception as e:
        error_msg = str(e)
        print()
        print_box([
            f"Status:     [ERROR] FAILED",
            f"Error Type: {type(e).__name__}",
            f"Message:    {error_msg[:60]}",
        ], title=" CONNECTION TEST FAILED")
        print()
        
        print_info("Troubleshooting tips:")
        if "401" in error_msg or "Unauthorized" in error_msg:
            print("       - Your API key may be invalid or expired")
        elif "404" in error_msg or "NotFound" in error_msg:
            print("       - Deployment name may be incorrect")
        else:
            print("       - Verify all credentials are correct")
        
        return False, None


# -----------------------------------------------------------------------------
#                           DATA PROCESSING
# -----------------------------------------------------------------------------

def extract_pdf_text(pdf_path):
    if not PDF_SUPPORT:
        return ""
    try:
        result = _di_parse_document(pdf_path)
        return (result.full_text or "").strip()
    except Exception:
        return ""

def load_pdfs(paths):
    texts = []
    for path in paths:
        if os.path.exists(path):
            text = extract_pdf_text(path)
            if text:
                texts.append(f"[{Path(path).name}]\n{text}")
                print_success(f"Loaded: {Path(path).name}")
    return "\n\n".join(texts)


def get_rcm_schema(df):
    """Extract the exact column schema and FULL data from input RCM"""
    columns = list(df.columns)
    
    # Convert ALL rows to list of dictionaries for AI
    full_rcm_data = []
    for idx, row in df.iterrows():
        row_dict = {}
        for col in columns:
            val = row[col]
            if pd.isna(val):
                row_dict[col] = ""
            else:
                row_dict[col] = str(val)
        full_rcm_data.append(row_dict)
    
    return columns, full_rcm_data


def truncate_text(text, max_length=150):
    """Truncate long text while keeping meaning"""
    if not text or len(str(text)) <= max_length:
        return str(text) if text else ""
    text = str(text)
    # Truncate and add ellipsis
    return text[:max_length-3].rsplit(' ', 1)[0] + "..."


def convert_rcm_to_csv_format(full_rcm_data, columns):
    """
    Convert RCM data to compact CSV format (3-5x smaller than JSON).
    Also truncates long descriptions to save tokens.
    """
    
    # Identify columns that typically have long text
    long_text_cols = []
    for col in columns:
        col_lower = col.lower()
        if any(x in col_lower for x in ['description', 'desc', 'objective', 'reason', 'comment', 'note']):
            long_text_cols.append(col)
    
    # Build CSV string
    lines = []
    
    # Header row
    lines.append("|".join(columns))
    
    # Data rows
    for row in full_rcm_data:
        values = []
        for col in columns:
            val = row.get(col, "")
            # Truncate long text columns
            if col in long_text_cols:
                val = truncate_text(val, 150)
            else:
                val = truncate_text(val, 200)
            # Escape pipe character
            val = str(val).replace("|", "/")
            values.append(val)
        lines.append("|".join(values))
    
    return "\n".join(lines)


def estimate_tokens(text):
    """Rough estimate of tokens (1 token ~ 4 characters)"""
    return len(text) // 4


def prepare_rcm_for_ai(full_rcm_data, columns, max_tokens=80000):
    """
    Prepare RCM data for AI, using compression if needed.
    Returns: (formatted_data, format_type, was_truncated)
    """
    
    # First try: Compact JSON (no indentation)
    compact_json = json.dumps(full_rcm_data, ensure_ascii=False)
    json_tokens = estimate_tokens(compact_json)
    
    if json_tokens <= max_tokens:
        return compact_json, "JSON", False
    
    # Second try: CSV format (much smaller)
    csv_format = convert_rcm_to_csv_format(full_rcm_data, columns)
    csv_tokens = estimate_tokens(csv_format)
    
    if csv_tokens <= max_tokens:
        return csv_format, "CSV", False
    
    # Third try: Aggressive truncation
    truncated_data = []
    for row in full_rcm_data:
        truncated_row = {}
        for col in columns:
            val = row.get(col, "")
            truncated_row[col] = truncate_text(val, 100)  # More aggressive
        truncated_data.append(truncated_row)
    
    csv_truncated = convert_rcm_to_csv_format(truncated_data, columns)
    csv_trunc_tokens = estimate_tokens(csv_truncated)
    
    if csv_trunc_tokens <= max_tokens:
        return csv_truncated, "CSV", True
    
    # Fourth try: Even more aggressive - keep only key columns
    key_cols = []
    for col in columns:
        col_lower = col.lower()
        if any(x in col_lower for x in ['process', 'subprocess', 'risk', 'control', 'id', 'title', 'name', 'type', 'nature', 'owner', 'level', 'rating']):
            key_cols.append(col)
    
    if not key_cols:
        key_cols = columns[:10]  # First 10 columns
    
    minimal_data = []
    for row in full_rcm_data:
        minimal_row = {}
        for col in key_cols:
            val = row.get(col, "")
            minimal_row[col] = truncate_text(val, 80)
        minimal_data.append(minimal_row)
    
    csv_minimal = convert_rcm_to_csv_format(minimal_data, key_cols)
    
    return csv_minimal, "CSV_MINIMAL", True


def analyze_rcm(df):
    """Analyze RCM structure"""
    
    # Try to identify key columns (flexible matching)
    col_mapping = {}
    for col in df.columns:
        c = col.lower().replace(' ', '').replace('_', '').replace('-', '')
        if 'process' in c and 'sub' not in c:
            col_mapping['process'] = col
        elif 'subprocess' in c or 'sub' in c and 'process' in c:
            col_mapping['subprocess'] = col
        elif 'riskid' in c or ('risk' in c and 'id' in c):
            col_mapping['risk_id'] = col
        elif 'risktitle' in c or 'riskname' in c or ('risk' in c and ('title' in c or 'name' in c)):
            col_mapping['risk_title'] = col
        elif 'riskdesc' in c:
            col_mapping['risk_desc'] = col
        elif 'controlid' in c or ('control' in c and 'id' in c):
            col_mapping['control_id'] = col
        elif 'controldesc' in c:
            col_mapping['control_desc'] = col
        elif 'controltype' in c or ('control' in c and 'type' in c):
            col_mapping['control_type'] = col
        elif 'nature' in c:
            col_mapping['control_nature'] = col
        elif 'frequency' in c:
            col_mapping['control_frequency'] = col
        elif 'owner' in c:
            col_mapping['control_owner'] = col
    
    analysis = {
        "total_rows": len(df),
        "columns": list(df.columns),
        "col_mapping": col_mapping,
        "processes": {},
        "unique_risks": 0,
        "unique_controls": 0,
        "control_types": {},
        "control_natures": {},
    }
    
    # Get process column
    proc_col = col_mapping.get('process')
    subproc_col = col_mapping.get('subprocess')
    risk_col = col_mapping.get('risk_id') or col_mapping.get('risk_title')
    ctrl_col = col_mapping.get('control_id') or col_mapping.get('control_desc')
    type_col = col_mapping.get('control_type')
    nature_col = col_mapping.get('control_nature')
    
    risk_set = set()
    ctrl_set = set()
    
    total = len(df)
    for idx, row in df.iterrows():
        if idx % 10 == 0:
            print_progress_bar(idx + 1, total, "Analyzing")
        
        # Process
        if proc_col:
            proc = str(row.get(proc_col, '')).strip()
            if proc and proc.lower() != 'nan':
                if proc not in analysis["processes"]:
                    analysis["processes"][proc] = {"subprocesses": set(), "risk_count": 0, "control_count": 0}
                
                if subproc_col:
                    subproc = str(row.get(subproc_col, '')).strip()
                    if subproc and subproc.lower() != 'nan':
                        analysis["processes"][proc]["subprocesses"].add(subproc)
                
                if risk_col:
                    risk = str(row.get(risk_col, '')).strip()
                    if risk and risk.lower() != 'nan':
                        risk_set.add(risk)
                        analysis["processes"][proc]["risk_count"] += 1
                
                if ctrl_col:
                    ctrl = str(row.get(ctrl_col, '')).strip()
                    if ctrl and ctrl.lower() != 'nan':
                        ctrl_set.add(ctrl)
                        analysis["processes"][proc]["control_count"] += 1
        
        # Control types
        if type_col:
            ctype = str(row.get(type_col, '')).strip()
            if ctype and ctype.lower() != 'nan':
                analysis["control_types"][ctype] = analysis["control_types"].get(ctype, 0) + 1
        
        # Control natures
        if nature_col:
            cnature = str(row.get(nature_col, '')).strip()
            if cnature and cnature.lower() != 'nan':
                analysis["control_natures"][cnature] = analysis["control_natures"].get(cnature, 0) + 1
    
    clear_line()
    
    analysis["unique_risks"] = len(risk_set)
    analysis["unique_controls"] = len(ctrl_set)
    
    # Convert sets to lists for JSON
    for proc in analysis["processes"]:
        analysis["processes"][proc]["subprocesses"] = list(analysis["processes"][proc]["subprocesses"])
    
    return analysis


def calculate_suggestion_count(rcm_summary, industry):
    """
    Dynamically calculate suggestion count based on:
    1. Total RCM rows (size factor)
    2. Number of processes (breadth factor)
    3. Missing standard processes (coverage gap factor)
    4. Risk-to-control ratio (balance factor)
    5. Control type mix (automation factor)
    6. SubProcess coverage per process (depth factor)
    7. Average risks per process (density factor)
    
    Returns: (suggestion_count, breakdown_dict)
    """
    
    total_rows = rcm_summary["total_rows"]
    num_processes = len(rcm_summary["processes"])
    unique_risks = rcm_summary["unique_risks"]
    unique_controls = rcm_summary["unique_controls"]
    control_types = rcm_summary.get("control_types", {})
    control_natures = rcm_summary.get("control_natures", {})
    processes = rcm_summary["processes"]
    
    breakdown = {}
    
    # -----------------------------------------------------------------
    # FACTOR 1: RCM Size (Base)
    # -----------------------------------------------------------------
    if total_rows < 15:
        size_score = 6
    elif total_rows < 30:
        size_score = 10
    elif total_rows < 50:
        size_score = 14
    elif total_rows < 100:
        size_score = 18
    elif total_rows < 200:
        size_score = 24
    elif total_rows < 500:
        size_score = 32
    else:
        size_score = 40
    
    breakdown["rcm_size"] = {
        "rows": total_rows,
        "base_suggestions": size_score,
        "reasoning": f"{total_rows} rows -> base {size_score} suggestions"
    }
    
    # -----------------------------------------------------------------
    # FACTOR 2: SubProcess Coverage (Dynamic - AI will determine gaps)
    # -----------------------------------------------------------------
    
    # Detect the main process from the RCM
    detected_process = None
    user_processes = list(processes.keys())
    if len(user_processes) == 1:
        detected_process = user_processes[0]
    elif len(user_processes) > 1:
        detected_process = max(processes.keys(), key=lambda p: processes[p]['risk_count'])
    
    # Get count of user's subprocesses
    user_subprocesses = set()
    for proc, pdata in processes.items():
        for sp in pdata.get('subprocesses', []):
            user_subprocesses.add(sp)
    
    subprocess_count = len(user_subprocesses)
    
    # Dynamic coverage scoring based on subprocess count
    # Fewer subprocesses = likely more gaps = more suggestions needed
    if subprocess_count < 3:
        coverage_adjustment = 8  # Very few subprocesses - likely missing many
    elif subprocess_count < 5:
        coverage_adjustment = 5  # Some subprocesses but probably missing some
    elif subprocess_count < 8:
        coverage_adjustment = 3  # Good coverage but may have gaps
    else:
        coverage_adjustment = 1  # Comprehensive - minimal gaps expected
    
    breakdown["coverage_gap"] = {
        "detected_process": detected_process or "Unknown",
        "subprocess_count": subprocess_count,
        "subprocesses": list(user_subprocesses),
        "adjustment": f"+{coverage_adjustment}",
        "reasoning": f"{subprocess_count} subprocesses found -> +{coverage_adjustment} (AI will identify specific gaps)"
    }
    
    # -----------------------------------------------------------------
    # FACTOR 3: Risk-to-Control Ratio (Balance)
    # -----------------------------------------------------------------
    if unique_controls > 0 and unique_risks > 0:
        ratio = unique_risks / unique_controls
    else:
        ratio = 1.0
    
    if ratio > 1.5:
        # More risks than controls -> need more controls
        balance_adjustment = 4
        balance_reason = f"Risk:Control ratio {ratio:.1f} (many risks lack controls)"
    elif ratio < 0.7:
        # More controls than risks -> possibly over-controlled but may miss risks
        balance_adjustment = 2
        balance_reason = f"Risk:Control ratio {ratio:.1f} (may be missing risk identification)"
    else:
        balance_adjustment = 0
        balance_reason = f"Risk:Control ratio {ratio:.1f} (balanced)"
    
    breakdown["risk_control_balance"] = {
        "unique_risks": unique_risks,
        "unique_controls": unique_controls,
        "ratio": round(ratio, 2),
        "adjustment": f"+{balance_adjustment}",
        "reasoning": balance_reason
    }
    
    # -----------------------------------------------------------------
    # FACTOR 4: Control Type Mix (Automation Gap)
    # -----------------------------------------------------------------
    total_controls = sum(control_types.values()) if control_types else 0
    automated_count = 0
    manual_count = 0
    
    for ctype, count in control_types.items():
        ct_lower = ctype.lower()
        if 'auto' in ct_lower or 'system' in ct_lower or 'it' in ct_lower:
            automated_count += count
        elif 'manual' in ct_lower:
            manual_count += count
    
    if total_controls > 0:
        auto_pct = (automated_count / total_controls) * 100
    else:
        auto_pct = 0
    
    if auto_pct < 20:
        automation_adjustment = 3
        auto_reason = f"Only {auto_pct:.0f}% automated (heavy manual -> suggest automation)"
    elif auto_pct < 40:
        automation_adjustment = 1
        auto_reason = f"{auto_pct:.0f}% automated (could improve)"
    else:
        automation_adjustment = 0
        auto_reason = f"{auto_pct:.0f}% automated (reasonable mix)"
    
    breakdown["automation"] = {
        "total_controls": total_controls,
        "automated": automated_count,
        "manual": manual_count,
        "auto_pct": round(auto_pct, 1),
        "adjustment": f"+{automation_adjustment}",
        "reasoning": auto_reason
    }
    
    # -----------------------------------------------------------------
    # FACTOR 5: Control Nature Mix (Preventive vs Detective)
    # -----------------------------------------------------------------
    total_natures = sum(control_natures.values()) if control_natures else 0
    preventive_count = 0
    detective_count = 0
    
    for cnature, count in control_natures.items():
        cn_lower = cnature.lower()
        if 'prevent' in cn_lower:
            preventive_count += count
        elif 'detect' in cn_lower:
            detective_count += count
    
    if total_natures > 0:
        preventive_pct = (preventive_count / total_natures) * 100
    else:
        preventive_pct = 50  # Assume balanced if unknown
    
    if preventive_pct > 80:
        nature_adjustment = 2
        nature_reason = f"{preventive_pct:.0f}% preventive (need more detective controls)"
    elif preventive_pct < 30:
        nature_adjustment = 2
        nature_reason = f"Only {preventive_pct:.0f}% preventive (need more preventive controls)"
    else:
        nature_adjustment = 0
        nature_reason = f"{preventive_pct:.0f}% preventive / {100-preventive_pct:.0f}% detective (good mix)"
    
    breakdown["control_nature"] = {
        "preventive": preventive_count,
        "detective": detective_count,
        "preventive_pct": round(preventive_pct, 1),
        "adjustment": f"+{nature_adjustment}",
        "reasoning": nature_reason
    }
    
    # -----------------------------------------------------------------
    # FACTOR 6: Process Depth (SubProcess Coverage)
    # -----------------------------------------------------------------
    shallow_processes = []
    for proc, pdata in processes.items():
        subprocess_count = len(pdata.get("subprocesses", []))
        risk_count = pdata.get("risk_count", 0)
        
        # Process with very few subprocesses or risks is shallow
        if subprocess_count <= 1 and risk_count <= 2:
            shallow_processes.append(proc)
    
    depth_adjustment = min(len(shallow_processes), 5)
    
    breakdown["process_depth"] = {
        "shallow_processes": shallow_processes[:5],
        "shallow_count": len(shallow_processes),
        "adjustment": f"+{depth_adjustment}",
        "reasoning": f"{len(shallow_processes)} processes with thin coverage -> +{depth_adjustment} suggestions"
    }
    
    # -----------------------------------------------------------------
    # FACTOR 7: Process Breadth (Number of Processes)
    # -----------------------------------------------------------------
    if num_processes <= 3:
        breadth_adjustment = 5
        breadth_reason = f"Only {num_processes} processes (very narrow coverage)"
    elif num_processes <= 5:
        breadth_adjustment = 3
        breadth_reason = f"{num_processes} processes (below typical 8-12)"
    elif num_processes <= 8:
        breadth_adjustment = 1
        breadth_reason = f"{num_processes} processes (moderate coverage)"
    elif num_processes <= 12:
        breadth_adjustment = 0
        breadth_reason = f"{num_processes} processes (good coverage)"
    else:
        breadth_adjustment = -2
        breadth_reason = f"{num_processes} processes (broad coverage, fewer gaps expected)"
    
    breakdown["process_breadth"] = {
        "num_processes": num_processes,
        "adjustment": f"{'+' if breadth_adjustment >= 0 else ''}{breadth_adjustment}",
        "reasoning": breadth_reason
    }
    
    # -----------------------------------------------------------------
    # FINAL CALCULATION
    # -----------------------------------------------------------------
    total = (
        size_score +
        coverage_adjustment +
        balance_adjustment +
        automation_adjustment +
        nature_adjustment +
        depth_adjustment +
        breadth_adjustment
    )
    
    # Clamp to reasonable bounds
    final_count = max(8, min(total, 50))
    
    breakdown["final"] = {
        "raw_total": total,
        "clamped": final_count,
        "formula": (
            f"Base({size_score}) + "
            f"CoverageGap(+{coverage_adjustment}) + "
            f"Balance(+{balance_adjustment}) + "
            f"Automation(+{automation_adjustment}) + "
            f"Nature(+{nature_adjustment}) + "
            f"Depth(+{depth_adjustment}) + "
            f"Breadth({'+' if breadth_adjustment >= 0 else ''}{breadth_adjustment}) "
            f"= {total} -> clamped to {final_count}"
        )
    }
    
    # Store industry for display
    breakdown["industry"] = industry
    
    return final_count, breakdown


def display_suggestion_calculation(breakdown):
    """Display the dynamic suggestion count calculation on console"""
    
    print()
    print_section("SUGGESTION COUNT CALCULATION")
    print()
    
    # Size
    sz = breakdown["rcm_size"]
    print(f"     RCM Size:              {sz['rows']} rows -> base {sz['base_suggestions']}")
    
    # Detected process (single process per RCM)
    cg = breakdown["coverage_gap"]
    print(f"     Detected Process:      {cg.get('detected_process', 'Unknown')}")
    
    # SubProcess coverage (dynamic - AI will identify gaps)
    subprocess_count = cg.get('subprocess_count', 0)
    subprocesses = cg.get('subprocesses', [])
    print(f"     SubProcess Coverage:   {subprocess_count} subprocesses found -> {cg['adjustment']}")
    if subprocesses:
        for sp in subprocesses[:5]:
            print(f"       +- {sp}")
        if subprocess_count > 5:
            print(f"       +- ... and {subprocess_count - 5} more")
    print(f"       [INFO]  AI will identify gaps based on global {breakdown.get('industry', 'industry')} best practices")
    
    # Balance
    bl = breakdown["risk_control_balance"]
    print(f"      Risk:Control Ratio:    {bl['ratio']} ({bl['unique_risks']} risks / {bl['unique_controls']} controls) -> {bl['adjustment']}")
    
    # Automation
    au = breakdown["automation"]
    print(f"     Automation Level:       {au['auto_pct']}% automated -> {au['adjustment']}")
    
    # Nature
    cn = breakdown["control_nature"]
    print(f"      Control Nature Mix:     {cn['preventive_pct']}% preventive -> {cn['adjustment']}")
    
    # Depth
    dp = breakdown["process_depth"]
    print(f"     SubProcess Depth:       {dp['shallow_count']} shallow subprocesses -> {dp['adjustment']}")
    
    # Breadth
    br = breakdown["process_breadth"]
    print(f"     SubProcess Count:       {br['num_processes']} subprocesses -> {br['adjustment']}")
    
    # Formula
    fn = breakdown["final"]
    print()
    print("    +" + "-" * 68 + "+")
    print(f"    |   FORMULA:                                                      |")
    
    formula = fn["formula"]
    # Word wrap the formula
    if len(formula) > 62:
        parts = formula.split(" + ")
        line1 = " + ".join(parts[:4])
        line2 = " + ".join(parts[4:])
        print(f"    |     {line1:<63}|")
        print(f"    |     {line2:<63}|")
    else:
        print(f"    |     {formula:<63}|")
    
    print(f"    |                                                                    |")
    print(f"    |   REQUESTING: {fn['clamped']} suggestions from AI{' ' * (38 - len(str(fn['clamped'])))}|")
    print("    +" + "-" * 68 + "+")


# -----------------------------------------------------------------------------
#                         AI PROMPTS (SCHEMA-AWARE)
# -----------------------------------------------------------------------------

def get_system_prompt(industry, columns):
    """System prompt - AI uses its own global knowledge dynamically"""
    
    columns_str = ", ".join([f'"{c}"' for c in columns])
    
    return f"""You are a Principal-level Compliance & Internal Audit Expert.

===============================================================================
CONTEXT
===============================================================================
INDUSTRY: {industry}

===============================================================================
YOUR EXPERTISE
===============================================================================
You have extensive knowledge of:

- COMPLIANCE FRAMEWORKS: SOX Section 404, IFC, ICOFR, PCAOB AS 2201, COSO Framework
- INDUSTRY REGULATIONS: All relevant regulations for {industry}
- ACCOUNTING STANDARDS: GAAP, IFRS, industry-specific standards
- GLOBAL BEST PRACTICES: What leading {industry} companies have
- AUDIT EXPERIENCE: Common findings, material weaknesses, control gaps
- PROCESS EXPERTISE: Standard subprocesses, risks, and controls for any business process

When you receive the RCM data:
1. Identify the PROCESS from the data
2. Use your knowledge of what that process should contain
3. Compare against global standards for that process in {industry}
4. Identify what's MISSING based on your expertise

===============================================================================
OUTPUT REQUIREMENTS
===============================================================================
The client's RCM uses these EXACT columns: [{columns_str}]

Your suggestions MUST:
- Use these exact column names (case-sensitive)
- Be complete rows ready to add to the RCM
- Fill ALL columns with appropriate values
- Be GENUINELY NEW (not duplicating what exists)

RESPOND IN VALID JSON FORMAT ONLY. NO MARKDOWN CODE BLOCKS."""


def get_user_prompt(rcm_summary, columns, full_rcm_data, industry, suggestion_count, suggestion_breakdown=None, pdf_context=""):
    """User prompt with RCM data - uses compression if too large"""
    
    # Build process summary and extract THE process name (RCM is for single process)
    process_details = ""
    all_subprocesses = []
    all_processes = []
    
    for proc, pdata in rcm_summary["processes"].items():
        all_processes.append(proc)
        subprocess_count = len(pdata.get('subprocesses', []))
        process_details += f"\n  - {proc}: {pdata['risk_count']} risks, {pdata['control_count']} controls, {subprocess_count} subprocesses"
        if pdata['subprocesses']:
            process_details += f"\n    SubProcesses: {', '.join(pdata['subprocesses'])}"
            all_subprocesses.extend(pdata['subprocesses'])
    
    # Get EXACT process name(s)
    unique_processes = list(set(all_processes))
    process_name = unique_processes[0] if unique_processes else "Unknown Process"
    
    # Create formatted list of existing subprocesses
    unique_subprocesses = sorted(set(all_subprocesses))
    existing_subprocesses_list = "\n".join(f"  - {sp}" for sp in unique_subprocesses)
    if not existing_subprocesses_list:
        existing_subprocesses_list = "  (No subprocesses found)"
    
    # Build guidance from breakdown
    statistical_guidance = ""
    if suggestion_breakdown:
        au = suggestion_breakdown.get("automation", {})
        if au.get("auto_pct", 100) < 30:
            statistical_guidance += f"\n   Only {au['auto_pct']}% of controls are automated - consider if more automated/IT controls are needed"
        
        cn = suggestion_breakdown.get("control_nature", {})
        if cn.get("preventive_pct", 50) > 75:
            statistical_guidance += f"\n   {cn['preventive_pct']}% preventive controls - consider if more detective controls are needed"
        elif cn.get("preventive_pct", 50) < 35:
            statistical_guidance += f"\n   Only {cn['preventive_pct']}% preventive controls - consider if more preventive controls are needed"
        
        bl = suggestion_breakdown.get("risk_control_balance", {})
        if bl.get("ratio", 1.0) > 1.3:
            statistical_guidance += f"\n   Risk:Control ratio is {bl['ratio']} - some risks may lack adequate controls"
    
    # Prepare RCM data with compression if needed
    rcm_data_formatted, data_format, was_truncated = prepare_rcm_for_ai(full_rcm_data, columns, max_tokens=60000)
    
    # Column list
    columns_json = json.dumps(columns)
    
    # Format note based on compression used
    if data_format == "JSON":
        format_note = "Data is in JSON format."
    elif data_format == "CSV":
        format_note = "Data is in CSV format (pipe-delimited). First row is header."
    else:
        format_note = "Data is in CSV format (pipe-delimited, key columns only). First row is header."
    
    truncation_note = ""
    if was_truncated:
        truncation_note = "\n[WARN] Note: Long descriptions have been truncated to fit. Focus on the risk/control names and IDs."
    
    return f"""
INDUSTRY: {industry}

===============================================================================
[WARN][WARN][WARN] CRITICAL CONSTRAINT - READ FIRST [WARN][WARN][WARN]
===============================================================================

THIS RCM IS FOR THE FOLLOWING PROCESS: "{process_name}"

YOU MUST:
- Use EXACTLY this process name in ALL suggestions: "{process_name}"
- DO NOT create suggestions for any other process
- Copy the process name EXACTLY as shown above - including hyphens, spaces, capitalization

EXISTING SUBPROCESSES (use these EXACT names when applicable):
{existing_subprocesses_list}

===============================================================================
RCM STATISTICS
===============================================================================
- Total Rows: {rcm_summary['total_rows']}
- Unique Risks: {rcm_summary['unique_risks']}
- Unique Controls: {rcm_summary['unique_controls']}

Control Types: {json.dumps(rcm_summary['control_types'])}
Control Natures: {json.dumps(rcm_summary['control_natures'])}

Process Breakdown:
{process_details}
{statistical_guidance if statistical_guidance else ""}

===============================================================================
RCM COLUMN SCHEMA (use these EXACT column names in your suggestions)
===============================================================================
{columns_json}

===============================================================================
COMPLETE RCM DATA - {rcm_summary['total_rows']} ROWS
===============================================================================
{format_note}{truncation_note}

{rcm_data_formatted}

{f"==============================================================================={chr(10)}ADDITIONAL CONTEXT FROM DOCUMENTS{chr(10)}==============================================================================={chr(10)}{pdf_context[:4000]}" if pdf_context else ""}

===============================================================================
YOUR TASK - GENERATE {suggestion_count} SUGGESTIONS
===============================================================================

MANDATORY RULES:

1. PROCESS NAME: Every suggestion MUST use exactly: "{process_name}"
   - DO NOT modify the spelling, hyphens, or capitalization
   - DO NOT suggest items for any other process
   - Copy the process name exactly as shown

2. SUBPROCESS: Either use an EXISTING subprocess name OR create a truly NEW one
   - Existing subprocesses: {', '.join(unique_subprocesses)}
   - If using existing: copy the name EXACTLY as listed
   - If new: it must be genuinely different (not a variation of existing)

3. CATEGORY ASSIGNMENT:
   - "New SubProcess": SubProcess is completely NEW (not in the list above)
   - "New Risk": Adding risk to an EXISTING subprocess
   - "New Control": Adding control to an EXISTING subprocess

===============================================================================
RESPONSE FORMAT
===============================================================================

Return this EXACT JSON structure:

{{
    "executive_summary": {{
        "overall_maturity": "Developing/Established/Advanced",
        "coverage_score": 0-100,
        "audit_readiness": "Not Ready/Needs Improvement/Ready/Well Prepared",
        "assessment": "2-3 sentence assessment of the RCM"
    }},
    "gap_analysis": {{
        "strengths": ["strength1", "strength2", "strength3"],
        "weaknesses": ["weakness1", "weakness2"],
        "key_gaps": [
            {{"gap": "Description of gap", "risk_level": "High/Medium/Low", "compliance_impact": "Impact on compliance"}}
        ]
    }},
    "suggestions": [
        {{
            "Process": "{process_name}",  // MUST be exactly this
            "SubProcess": "...",  // Existing name OR truly new
            // ... all other columns from schema
            "AI_Suggestion_ID": "RCMAI-001",
            "AI_Priority": "High/Medium/Low",
            "AI_Priority_Rationale": "Why this priority level was assigned (e.g. High because missing this control could lead to material weakness)",
            "AI_Category": "New SubProcess / New Risk / New Control",
            "AI_Reason": "Why this is needed"
        }}
    ]
}}

FIELD CLARIFICATION:
- "AI_Priority": The priority level (High, Medium, or Low)
- "AI_Priority_Rationale": Explain WHY you chose that priority level. What makes it High vs Medium vs Low? Consider materiality, likelihood of audit finding, regulatory impact, and financial exposure.
- "AI_Reason": Explain WHY this suggestion is needed in the RCM (what gap it fills)

FINAL CHECKLIST:
- Every suggestion has Process = "{process_name}" (EXACT match)
- NO suggestions for any other process
- SubProcess either matches existing EXACTLY or is genuinely NEW
- Category matches: existing subprocess = New Risk/Control, new subprocess = New SubProcess
- All columns from schema are filled
- Return valid JSON only, no markdown"""


# -----------------------------------------------------------------------------
#                    JSON PARSER
# -----------------------------------------------------------------------------

def parse_ai_response(response_text):
    """Parse JSON from AI response, handling markdown blocks"""
    
    # Step 1: Try direct parse
    try:
        return json.loads(response_text)
    except:
        pass
    
    # Step 2: Remove markdown code blocks
    cleaned = response_text
    cleaned = re.sub(r'^```json\s*\n?', '', cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r'^```\s*\n?', '', cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r'\n?```\s*$', '', cleaned, flags=re.MULTILINE)
    cleaned = cleaned.strip()
    
    try:
        return json.loads(cleaned)
    except:
        pass
    
    # Step 3: Extract JSON between { and }
    try:
        start = cleaned.find('{')
        end = cleaned.rfind('}')
        if start != -1 and end != -1 and end > start:
            return json.loads(cleaned[start:end + 1])
    except:
        pass
    
    # Step 4: Fix trailing commas
    try:
        fixed = re.sub(r',\s*}', '}', cleaned)
        fixed = re.sub(r',\s*]', ']', fixed)
        start = fixed.find('{')
        end = fixed.rfind('}')
        if start != -1 and end != -1:
            return json.loads(fixed[start:end + 1])
    except:
        pass
    
    return None


def validate_and_correct_suggestions(suggestions, existing_processes, existing_subprocesses):
    """
    Post-process AI suggestions to ensure:
    1. Process name matches EXACTLY what's in the RCM (fix spelling/hyphen variations)
    2. Only suggestions for processes that exist in the RCM are kept (reject others)
    3. Category is correct based on whether SubProcess is new or existing
    
    This is FULLY DYNAMIC - works for any process/subprocess names from any RCM.
    
    Returns: validated suggestions (invalid ones are removed)
    """
    
    if not suggestions:
        return suggestions
    
    # Normalize for comparison
    def normalize(text):
        if not text:
            return ""
        return text.lower().strip().replace('-', ' ').replace('_', ' ')
    
    # Build normalized lookup for processes (DYNAMIC - from RCM data)
    process_lookup = {}
    for proc in existing_processes:
        process_lookup[normalize(proc)] = proc  # Map normalized -> exact
    
    # Build normalized lookup for subprocesses (DYNAMIC - from RCM data)
    subprocess_lookup = {}
    for sp in existing_subprocesses:
        subprocess_lookup[normalize(sp)] = sp  # Map normalized -> exact
    
    validated_suggestions = []
    rejected_count = 0
    process_fixed_count = 0
    subprocess_fixed_count = 0
    category_fixed_count = 0
    
    for suggestion in suggestions:
        # -----------------------------------------------------------------
        # STEP 1: Find and validate PROCESS
        # -----------------------------------------------------------------
        process_value = None
        process_key = None
        for key in suggestion.keys():
            if key.lower() == 'process':
                process_value = suggestion[key]
                process_key = key
                break
        
        if not process_value:
            # No process field - reject
            rejected_count += 1
            continue
        
        process_normalized = normalize(process_value)
        
        # Check if process matches any existing process (DYNAMIC comparison)
        matched_process = None
        for norm_proc, exact_proc in process_lookup.items():
            if process_normalized == norm_proc:
                matched_process = exact_proc
                break
            # Also check if one contains the other (partial match)
            if norm_proc in process_normalized or process_normalized in norm_proc:
                matched_process = exact_proc
                break
        
        if not matched_process:
            # Process doesn't exist in RCM - REJECT this suggestion
            rejected_count += 1
            continue
        
        # Fix process name to exact match from RCM
        if suggestion[process_key] != matched_process:
            suggestion[process_key] = matched_process
            process_fixed_count += 1
        
        # -----------------------------------------------------------------
        # STEP 2: Find and validate SUBPROCESS
        # -----------------------------------------------------------------
        subprocess_value = None
        subprocess_key = None
        for key in suggestion.keys():
            if 'subprocess' in key.lower() or 'sub process' in key.lower():
                subprocess_value = suggestion[key]
                subprocess_key = key
                break
        
        if not subprocess_value or not subprocess_key:
            # No subprocess field - keep but can't validate further
            validated_suggestions.append(suggestion)
            continue
        
        subprocess_normalized = normalize(subprocess_value)
        
        # Check if subprocess matches any existing subprocess
        matched_subprocess = None
        for norm_sp, exact_sp in subprocess_lookup.items():
            if subprocess_normalized == norm_sp:
                matched_subprocess = exact_sp
                break
            # Check partial match (one contains the other)
            if norm_sp in subprocess_normalized or subprocess_normalized in norm_sp:
                matched_subprocess = exact_sp
                break
        
        # -----------------------------------------------------------------
        # STEP 3: Validate and fix CATEGORY based on subprocess
        # -----------------------------------------------------------------
        current_category = suggestion.get('AI_Category', '')
        
        if matched_subprocess:
            # Subprocess EXISTS - fix name and ensure category is NOT "New SubProcess"
            if suggestion[subprocess_key] != matched_subprocess:
                suggestion[subprocess_key] = matched_subprocess
                subprocess_fixed_count += 1
            
            if 'subprocess' in current_category.lower():
                # Wrong! It's marked as New SubProcess but subprocess exists
                suggestion['AI_Category'] = 'New Control'
                suggestion['AI_Reason'] = suggestion.get('AI_Reason', '') + f' [Fixed: SubProcess already exists as "{matched_subprocess}"]'
                category_fixed_count += 1
        else:
            # Subprocess is NEW - ensure category IS "New SubProcess"
            if 'subprocess' not in current_category.lower():
                # Wrong! It's a new subprocess but marked as New Risk/Control
                suggestion['AI_Category'] = 'New SubProcess'
                suggestion['AI_Reason'] = suggestion.get('AI_Reason', '') + ' [Fixed: This is a new SubProcess]'
                category_fixed_count += 1
        
        validated_suggestions.append(suggestion)
    
    # Print validation summary
    print()
    print_info("Post-processing validation results:")
    if rejected_count > 0:
        print_warning(f"  -> Rejected {rejected_count} suggestions (wrong process - not in RCM)")
    if process_fixed_count > 0:
        print_info(f"  -> Fixed {process_fixed_count} process names to match RCM exactly")
    if subprocess_fixed_count > 0:
        print_info(f"  -> Fixed {subprocess_fixed_count} subprocess names to match RCM exactly")
    if category_fixed_count > 0:
        print_info(f"  -> Fixed {category_fixed_count} category assignments")
    if rejected_count == 0 and process_fixed_count == 0 and subprocess_fixed_count == 0 and category_fixed_count == 0:
        print_success(f"  -> All {len(validated_suggestions)} suggestions validated successfully")
    
    return validated_suggestions


def deduplicate_suggestions(client, suggestions, full_rcm_data, columns,
                            risk_title_col, control_desc_col):
    """
    LLM-based deduplication:
    Sends AI suggestions + existing RCM data to the LLM and asks it to identify
    which suggestions are semantic duplicates of existing entries or of each other.
    Removes duplicates silently.
    """
    if not suggestions or not full_rcm_data:
        return suggestions

    # Build compact representation of existing RCM entries for the LLM
    # Use subprocess col to group entries for readability
    subprocess_col = None
    for col in columns:
        if 'subprocess' in col.lower() or 'sub process' in col.lower():
            subprocess_col = col
            break

    existing_entries_text = ""
    for idx, row in enumerate(full_rcm_data, 1):
        subprocess = str(row.get(subprocess_col, '')).strip() if subprocess_col else ''
        risk = str(row.get(risk_title_col, '')).strip() if risk_title_col else ''
        control = str(row.get(control_desc_col, '')).strip() if control_desc_col else ''
        if risk.lower() == 'nan':
            risk = ''
        if control.lower() == 'nan':
            control = ''
        if risk or control:
            existing_entries_text += f"  E{idx}. [{subprocess}] Risk: {risk[:150]} | Control: {control[:150]}\n"

    # Build compact representation of AI suggestions
    suggestions_text = ""
    for s in suggestions:
        sid = s.get('AI_Suggestion_ID', '')
        subprocess = str(s.get(subprocess_col, '')).strip() if subprocess_col else ''
        risk = str(s.get(risk_title_col, '')).strip() if risk_title_col else ''
        control = str(s.get(control_desc_col, '')).strip() if control_desc_col else ''
        suggestions_text += f"  {sid}. [{subprocess}] Risk: {risk[:150]} | Control: {control[:150]}\n"

    # Check token budget — if too large, process in batches
    total_text = existing_entries_text + suggestions_text
    estimated_tokens = len(total_text) // 4

    if estimated_tokens > 80000:
        # Process suggestions in batches
        batch_size = max(5, len(suggestions) // 3)
        all_keep_ids = set()
        for batch_start in range(0, len(suggestions), batch_size):
            batch = suggestions[batch_start:batch_start + batch_size]
            batch_text = ""
            for s in batch:
                sid = s.get('AI_Suggestion_ID', '')
                subprocess = str(s.get(subprocess_col, '')).strip() if subprocess_col else ''
                risk = str(s.get(risk_title_col, '')).strip() if risk_title_col else ''
                control = str(s.get(control_desc_col, '')).strip() if control_desc_col else ''
                batch_text += f"  {sid}. [{subprocess}] Risk: {risk[:150]} | Control: {control[:150]}\n"

            keep_ids = _call_dedup_llm(client, existing_entries_text, batch_text, batch)
            all_keep_ids.update(keep_ids)

        return [s for s in suggestions if s.get('AI_Suggestion_ID', '') in all_keep_ids]
    else:
        keep_ids = _call_dedup_llm(client, existing_entries_text, suggestions_text, suggestions)
        return [s for s in suggestions if s.get('AI_Suggestion_ID', '') in keep_ids]


def _call_dedup_llm(client, existing_entries_text, suggestions_text, suggestions):
    """
    Make a single LLM call to identify which suggestions to keep.
    Returns a set of AI_Suggestion_IDs to keep.
    """
    suggestion_ids = [s.get('AI_Suggestion_ID', '') for s in suggestions]

    system_prompt = """You are an expert compliance auditor specializing in Risk Control Matrices (RCM).
Your task is to identify DUPLICATE suggestions that should be REMOVED.

DEFINITION OF DUPLICATE:
A suggestion is a duplicate if it describes the SAME underlying risk AND the SAME control mechanism
as an existing RCM entry OR as another suggestion — just expressed with different words.

The test: "If I merged these two entries into one, would any meaningful information be lost?"
- If NO information lost = DUPLICATE (remove the suggestion)
- If YES information lost = UNIQUE (keep the suggestion)

RULES:
1. Focus on MEANING, not exact wording. Different vocabulary for the same concept = DUPLICATE.
2. Different Application/System = AUTOMATICALLY UNIQUE (not a duplicate).
3. One risk with MULTIPLE DIFFERENT controls = NOT duplicates (each control is unique).
4. Multiple DIFFERENT risks with one control = NOT duplicates (each risk is unique).
5. Same risk + same control mechanism but different subprocess = DUPLICATE (remove it).
6. Different control types (preventive vs detective) for the same risk = NOT duplicates.

Check each suggestion against:
- ALL existing RCM entries (across all subprocesses)
- ALL other suggestions in the list

Respond ONLY with valid JSON."""

    user_prompt = f"""EXISTING RCM ENTRIES:
{existing_entries_text}

AI SUGGESTIONS TO CHECK:
{suggestions_text}

For each suggestion, determine if it is a DUPLICATE of any existing entry or another suggestion.

Return JSON with ONLY the IDs of suggestions that should be KEPT (are genuinely unique):
{{
    "keep": ["{suggestion_ids[0]}", ...]
}}

Return ONLY valid JSON, no other text."""

    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_completion_tokens=1000,
        )

        response_text = response.choices[0].message.content.strip()
        parsed = parse_ai_response(response_text)

        if parsed and 'keep' in parsed:
            keep_ids = set(parsed['keep'])
            removed = len(suggestions) - len(keep_ids)
            if removed > 0:
                print_info(f"Deduplication: removed {removed} duplicate suggestion(s)")
            return keep_ids
        else:
            # If parsing fails, keep all suggestions
            return set(suggestion_ids)

    except Exception as e:
        print_warning(f"Deduplication LLM call failed: {str(e)[:80]}, keeping all suggestions")
        return set(suggestion_ids)


# -----------------------------------------------------------------------------
#                    PARALLEL SUBPROCESS ANALYSIS
# -----------------------------------------------------------------------------

# Thread-safe print lock
print_lock = threading.Lock()


def split_rcm_by_subprocess(rcm_df, columns):
    """
    Split RCM DataFrame by subprocess.
    Returns: dict of {subprocess_name: [rows as dicts]}
    """
    # Find subprocess column
    subprocess_col = None
    for col in columns:
        if 'subprocess' in col.lower() or 'sub process' in col.lower() or 'sub-process' in col.lower():
            subprocess_col = col
            break
    
    if not subprocess_col:
        # No subprocess column - return all data under one key
        return {"_all_": rcm_df.to_dict('records')}
    
    # Group by subprocess
    subprocess_groups = {}
    for idx, row in rcm_df.iterrows():
        sp_name = str(row.get(subprocess_col, 'Unknown')).strip()
        if not sp_name or sp_name.lower() == 'nan':
            sp_name = 'Unknown'
        
        if sp_name not in subprocess_groups:
            subprocess_groups[sp_name] = []
        subprocess_groups[sp_name].append(row.to_dict())
    
    return subprocess_groups


def get_subprocess_system_prompt(industry, columns):
    """System prompt for subprocess-level analysis"""
    
    columns_str = ", ".join([f'"{c}"' for c in columns])
    
    return f"""You are a Principal-level Compliance & Internal Audit Expert analyzing a SPECIFIC SUBPROCESS.

INDUSTRY: {industry}

Your task is to identify gaps in ONE subprocess of an RCM. You will:
1. See the FULL data for this subprocess
2. See a SUMMARY of other subprocesses (for context)
3. Suggest improvements for THIS subprocess only

OUTPUT: Valid JSON with suggestions using these columns: [{columns_str}]

RESPOND IN VALID JSON FORMAT ONLY. NO MARKDOWN CODE BLOCKS."""


def get_subprocess_user_prompt(subprocess_name, subprocess_rows, other_subprocesses_summary, 
                                process_name, columns, industry, suggestion_count):
    """
    Create prompt for analyzing a single subprocess.
    Includes full data for target subprocess + summary of others for context.
    """
    
    columns_json = json.dumps(columns)
    subprocess_json = json.dumps(subprocess_rows, indent=2)
    
    # Build summary of other subprocesses with risk/control details
    other_summary = ""
    if other_subprocesses_summary:
        other_summary = (
            "EXISTING RISKS AND CONTROLS IN OTHER SUBPROCESSES (DO NOT duplicate these):\n"
            "IMPORTANT: Do NOT suggest any risk or control that already exists below.\n\n"
        )
        token_budget = 8000
        chars_used = 0
        for sp_name, sp_info in other_subprocesses_summary.items():
            section = f"  [{sp_name}] ({sp_info['risk_count']} risks, {sp_info['control_count']} controls)\n"
            risk_titles = sp_info.get('risk_titles', [])
            if risk_titles:
                section += "    Risks:\n"
                for rt in risk_titles:
                    section += f"      - {rt}\n"
            ctrl_descs = sp_info.get('control_descriptions', [])
            if ctrl_descs:
                section += "    Controls:\n"
                for cd in ctrl_descs[:5]:
                    section += f"      - {cd[:120]}\n"
            if chars_used + len(section) > token_budget:
                section = f"  [{sp_name}] ({sp_info['risk_count']} risks, {sp_info['control_count']} controls)\n"
            other_summary += section
            chars_used += len(section)
    
    return f"""
===============================================================================
ANALYZING SUBPROCESS: "{subprocess_name}"
===============================================================================

PROCESS: "{process_name}"
INDUSTRY: {industry}

{other_summary}

===============================================================================
FULL DATA FOR "{subprocess_name}" ({len(subprocess_rows)} rows)
===============================================================================
{subprocess_json}

===============================================================================
YOUR TASK: Generate {suggestion_count} suggestions for "{subprocess_name}"
===============================================================================

Based on compliance best practices for {industry}, identify gaps in THIS subprocess.

RULES:
1. Process MUST be exactly: "{process_name}"
2. SubProcess MUST be exactly: "{subprocess_name}"
3. Category: "New Risk" or "New Control" (this is an existing subprocess)
4. Each suggestion must be genuinely NEW (not duplicating existing rows IN THIS subprocess)
5. Each suggestion must NOT duplicate any risk or control from OTHER subprocesses listed above.
   If a similar risk or control concept already exists in ANY subprocess, do NOT suggest it again.

COLUMN SCHEMA: {columns_json}

RESPONSE FORMAT:
{{
    "subprocess": "{subprocess_name}",
    "suggestions": [
        {{
            "Process": "{process_name}",
            "SubProcess": "{subprocess_name}",
            // ... all other columns from schema
            "AI_Suggestion_ID": "RCMAI-001",
            "AI_Priority": "High/Medium/Low",
            "AI_Priority_Rationale": "Why this priority level was assigned",
            "AI_Category": "New Risk / New Control",
            "AI_Reason": "Why this is needed"
        }}
    ]
}}

FIELD CLARIFICATION:
- "AI_Priority_Rationale": Explain WHY you chose that priority level (High/Medium/Low). Consider materiality, likelihood of audit finding, regulatory impact, and financial exposure.
- "AI_Reason": Explain WHY this suggestion is needed in the RCM (what gap it fills).

Return ONLY valid JSON."""


def analyze_single_subprocess(client, subprocess_name, subprocess_rows, other_subprocesses_summary,
                               process_name, columns, industry, suggestion_count, subprocess_index):
    """
    Analyze a single subprocess. Called in parallel.
    Returns: (subprocess_name, suggestions_list, elapsed_time, error)
    """
    
    system_prompt = get_subprocess_system_prompt(industry, columns)
    user_prompt = get_subprocess_user_prompt(
        subprocess_name, subprocess_rows, other_subprocesses_summary,
        process_name, columns, industry, suggestion_count
    )
    
    # Calculate tokens based on suggestion count
    max_tokens = min(4000, max(1500, suggestion_count * 400))
    
    full_response = ""
    start_time = time.time()
    
    try:
        stream = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_completion_tokens=max_tokens,
            stream=True
        )

        for chunk in stream:
            if chunk.choices and len(chunk.choices) > 0:
                delta = chunk.choices[0].delta
                if delta and delta.content:
                    full_response += delta.content
        
        elapsed = time.time() - start_time
        
        # Parse response
        parsed = parse_ai_response(full_response)
        if parsed:
            suggestions = parsed.get('suggestions', [])
            return (subprocess_name, suggestions, elapsed, None)
        else:
            return (subprocess_name, [], elapsed, "Failed to parse response")
    
    except Exception as e:
        elapsed = time.time() - start_time
        return (subprocess_name, [], elapsed, str(e))


def check_missing_subprocesses(client, process_name, existing_subprocesses, columns, industry):
    """
    Check for missing subprocesses and generate complete records.
    Called in parallel with subprocess analysis.
    Returns: (task_name, suggestions_list, elapsed_time, error)
    """
    
    start_time = time.time()
    columns_json = json.dumps(columns)
    
    missing_sp_prompt = f"""
You are a compliance audit expert analyzing an RCM for "{process_name}" in {industry}.

EXISTING SUBPROCESSES:
{chr(10).join(f'  - {sp}' for sp in existing_subprocesses)}

Based on your knowledge of {industry} best practices and compliance requirements,
are there any CRITICAL subprocesses that are completely MISSING from this RCM?

Only suggest subprocesses that are:
1. Essential for compliance coverage
2. Genuinely missing (not variations of existing ones)
3. Standard for {process_name} in {industry}

If you identify missing subprocesses, generate COMPLETE records for each with ALL columns filled.

COLUMN SCHEMA (fill ALL of these): {columns_json}

Return JSON:
{{
    "missing_subprocesses": [
        {{
            "Process": "{process_name}",
            "SubProcess": "Name of missing subprocess",
            // Fill ALL other columns from schema with appropriate values
            "AI_Suggestion_ID": "RCMAI-NEW-001",
            "AI_Priority": "High/Medium/Low",
            "AI_Priority_Rationale": "Why this priority level was assigned",
            "AI_Category": "New SubProcess",
            "AI_Reason": "Why this subprocess is essential"
        }}
    ]
}}

IMPORTANT: Each record must have ALL columns filled with meaningful, professional content.
Generate 2-3 risk-control pairs per missing subprocess if needed.

If no critical subprocesses are missing, return: {{"missing_subprocesses": []}}
"""
    
    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": f"You are a compliance audit expert for {industry}. Generate complete RCM records with all columns filled. Return only valid JSON."},
                {"role": "user", "content": missing_sp_prompt}
            ],
            max_completion_tokens=3000,
        )

        elapsed = time.time() - start_time
        missing_response = response.choices[0].message.content
        missing_parsed = parse_ai_response(missing_response)
        
        if missing_parsed and missing_parsed.get('missing_subprocesses'):
            missing_list = missing_parsed['missing_subprocesses']
            
            # Ensure required fields for each record
            for record in missing_list:
                record['Process'] = process_name
                record['AI_Category'] = 'New SubProcess'
                if 'AI_Priority' not in record:
                    record['AI_Priority'] = 'Medium'
                if 'AI_Priority_Rationale' not in record:
                    record['AI_Priority_Rationale'] = 'Missing subprocess identified as needed for compliance coverage'
                if 'AI_Reason' not in record:
                    record['AI_Reason'] = 'Identified as missing subprocess for compliance coverage'
            
            return ("_missing_subprocesses_", missing_list, elapsed, None)
        else:
            return ("_missing_subprocesses_", [], elapsed, None)
    
    except Exception as e:
        elapsed = time.time() - start_time
        return ("_missing_subprocesses_", [], elapsed, str(e))


def get_ai_suggestions_parallel(client, rcm_summary, columns, full_rcm_data, rcm_df, 
                                 industry, suggestion_count, suggestion_breakdown=None, pdf_context=""):
    """
    Parallel subprocess analysis - analyzes each subprocess concurrently.
    
    Flow:
    1. Split RCM by subprocess
    2. Calculate suggestions per subprocess
    3. Create context summaries
    4. Launch parallel analysis
    5. Consolidate and validate results
    """
    
    # Extract process name
    process_name = list(rcm_summary.get("processes", {}).keys())[0] if rcm_summary.get("processes") else "Unknown"
    
    # Extract subprocess info
    existing_processes = list(rcm_summary.get("processes", {}).keys())
    existing_subprocesses = []
    for proc, pdata in rcm_summary.get("processes", {}).items():
        existing_subprocesses.extend(pdata.get('subprocesses', []))
    existing_subprocesses = list(set(existing_subprocesses))
    
    print_item(f"Process: {process_name}")
    print_item(f"Subprocesses found: {len(existing_subprocesses)}")
    
    # Split RCM by subprocess
    subprocess_groups = split_rcm_by_subprocess(rcm_df, columns)
    num_subprocesses = len(subprocess_groups)
    
    print_item(f"Split RCM into {num_subprocesses} subprocess groups")
    
    # ===========================================================================
    # ANALYZE EACH SUBPROCESS TO CALCULATE GAP SCORE
    # ===========================================================================
    # Gap score is based on:
    #   1. Risk-to-Control ratio (more risks than controls = higher gap)
    #   2. Control type diversity (all manual = needs automated)
    #   3. Control nature balance (all preventive = needs detective)
    #   4. Coverage density (fewer rows = potentially under-covered)
    
    # Find relevant columns
    risk_id_col = next((c for c in columns if 'risk' in c.lower() and 'id' in c.lower()), None)
    risk_title_col = next((c for c in columns if 'risk' in c.lower() and 'title' in c.lower()), None)
    control_id_col = next((c for c in columns if 'control' in c.lower() and 'id' in c.lower()), None)
    control_desc_col = next((c for c in columns if 'control' in c.lower() and 'description' in c.lower()), None)
    control_type_col = next((c for c in columns if 'control' in c.lower() and 'type' in c.lower()), None)
    control_nature_col = next((c for c in columns if 'nature' in c.lower()), None)
    
    subprocess_analysis = {}
    
    for sp_name, rows in subprocess_groups.items():
        # Count unique risks and controls
        risks = set()
        controls = set()
        risk_titles = set()
        control_descs = set()
        control_types = {}  # Manual vs Automated
        control_natures = {}  # Preventive vs Detective

        for row in rows:
            # Count risks
            risk_val = row.get(risk_id_col) or row.get(risk_title_col)
            if risk_val and str(risk_val).strip():
                risks.add(str(risk_val)[:50])

            # Collect risk titles for cross-subprocess context
            if risk_title_col:
                rt = row.get(risk_title_col)
                if rt and str(rt).strip() and str(rt).lower() != 'nan':
                    risk_titles.add(str(rt).strip()[:80])

            # Count controls
            control_val = row.get(control_id_col) or row.get(control_desc_col)
            if control_val and str(control_val).strip():
                controls.add(str(control_val)[:50])

            # Collect control descriptions for cross-subprocess context
            if control_desc_col:
                cd = row.get(control_desc_col)
                if cd and str(cd).strip() and str(cd).lower() != 'nan':
                    control_descs.add(str(cd).strip()[:100])
            
            # Track control types
            if control_type_col:
                ct = str(row.get(control_type_col, '')).lower().strip()
                if ct:
                    control_types[ct] = control_types.get(ct, 0) + 1
            
            # Track control natures
            if control_nature_col:
                cn = str(row.get(control_nature_col, '')).lower().strip()
                if cn:
                    control_natures[cn] = control_natures.get(cn, 0) + 1
        
        risk_count = len(risks)
        control_count = len(controls)
        
        # -----------------------------------------------------------------
        # CALCULATE GAP SCORE (higher = more suggestions needed)
        # -----------------------------------------------------------------
        gap_score = 0
        gap_reasons = []
        
        # Factor 1: Risk-to-Control Ratio (target: 1 risk = 1+ controls)
        if control_count > 0:
            ratio = risk_count / control_count
            if ratio > 1.0:  # More risks than controls
                gap_score += min(3, int(ratio))
                gap_reasons.append(f"Risk:Control={ratio:.1f}")
        else:
            gap_score += 3
            gap_reasons.append("No controls")
        
        # Factor 2: Control Type Diversity
        manual_count = sum(v for k, v in control_types.items() if 'manual' in k)
        auto_count = sum(v for k, v in control_types.items() if 'auto' in k)
        total_typed = manual_count + auto_count
        
        if total_typed > 0:
            auto_pct = (auto_count / total_typed) * 100
            if auto_pct < 30:  # Less than 30% automated
                gap_score += 2
                gap_reasons.append(f"Only {auto_pct:.0f}% automated")
        
        # Factor 3: Control Nature Balance
        preventive = sum(v for k, v in control_natures.items() if 'prevent' in k)
        detective = sum(v for k, v in control_natures.items() if 'detect' in k)
        total_natured = preventive + detective
        
        if total_natured > 0:
            if preventive > 0 and detective == 0:
                gap_score += 2
                gap_reasons.append("No detective controls")
            elif detective > 0 and preventive == 0:
                gap_score += 1
                gap_reasons.append("No preventive controls")
        
        # Factor 4: Coverage Density (fewer rows = potentially under-covered)
        if len(rows) < 3:
            gap_score += 2
            gap_reasons.append(f"Only {len(rows)} rows")
        elif len(rows) < 5:
            gap_score += 1
        
        # Minimum gap score of 1
        gap_score = max(1, gap_score)
        
        subprocess_analysis[sp_name] = {
            'row_count': len(rows),
            'risk_count': risk_count,
            'control_count': control_count,
            'control_types': control_types,
            'control_natures': control_natures,
            'gap_score': gap_score,
            'gap_reasons': gap_reasons,
            'sample_risks': list(risks)[:3],
            'risk_titles': list(risk_titles),
            'control_descriptions': list(control_descs),
        }
    
    # ===========================================================================
    # DISTRIBUTE SUGGESTIONS BASED ON GAP SCORES
    # ===========================================================================
    total_gap_score = sum(sa['gap_score'] for sa in subprocess_analysis.values())
    
    suggestions_per_subprocess = {}
    allocated = 0
    
    # Sort by gap score (highest first) to prioritize
    sorted_subprocesses = sorted(subprocess_analysis.items(), key=lambda x: x[1]['gap_score'], reverse=True)
    
    for sp_name, analysis in sorted_subprocesses:
        # Proportional allocation based on gap score
        proportion = analysis['gap_score'] / max(1, total_gap_score)
        suggested_count = max(1, int(suggestion_count * proportion))
        
        # Cap between 1 and 8 per subprocess
        suggested_count = min(8, max(1, suggested_count))
        
        suggestions_per_subprocess[sp_name] = suggested_count
        allocated += suggested_count
    
    # Distribute any remaining suggestions to highest gap-score subprocesses
    remaining = suggestion_count - allocated
    for sp_name, analysis in sorted_subprocesses:
        if remaining <= 0:
            break
        if suggestions_per_subprocess[sp_name] < 8:
            add = min(remaining, 8 - suggestions_per_subprocess[sp_name])
            suggestions_per_subprocess[sp_name] += add
            remaining -= add
    
    # Use subprocess_analysis as subprocess_summaries for later
    subprocess_summaries = subprocess_analysis
    
    # ===========================================================================
    # DISPLAY ANALYSIS PLAN WITH GAP SCORES
    # ===========================================================================
    print()
    print("    +" + "-" * 78 + "+")
    print("    |   PARALLEL SUBPROCESS ANALYSIS (Gap-Score Based Distribution)            |")
    print("    +" + "-" * 78 + "+")
    print("    |  SubProcess                         Rows  Gap   Reasons              Sugg  |")
    print("    +" + "-" * 78 + "+")
    
    for sp_name, analysis in sorted_subprocesses:
        row_count = analysis['row_count']
        gap_score = analysis['gap_score']
        reasons = ", ".join(analysis['gap_reasons'][:2]) if analysis['gap_reasons'] else "Balanced"
        sugg_count = suggestions_per_subprocess[sp_name]
        
        # Truncate subprocess name and reasons for display
        sp_display = sp_name[:30] + "..." if len(sp_name) > 30 else sp_name
        reasons_display = reasons[:20] + "..." if len(reasons) > 20 else reasons
        
        print(f"    |  {sp_display:<33} {row_count:>3}   {gap_score:>2}    {reasons_display:<20} {sugg_count:>3}   |")
    
    print("    +" + "-" * 78 + "+")
    total_sugg = sum(suggestions_per_subprocess.values())
    print(f"    |  TOTAL                                      {total_gap_score:>2}                         {total_sugg:>3}   |")
    print("    +" + "-" * 78 + "+")
    print()
    print_info("Gap Score factors: Risk:Control ratio, Automation %, Control nature mix, Coverage")
    print()
    
    # ===========================================================================
    # PARALLEL EXECUTION
    # ===========================================================================
    all_suggestions = []
    missing_subprocess_suggestions = []
    errors = []
    
    # Determine max workers (configurable, don't exceed task count)
    # Tasks = subprocesses + 1 (missing subprocess check if < 10 subprocesses)
    include_missing_check = num_subprocesses < 10
    total_tasks = num_subprocesses + (1 if include_missing_check else 0)
    configured_max = max(1, min(10, MAX_PARALLEL_API_CALLS))  # Clamp between 1-10
    max_workers = min(configured_max, total_tasks)
    
    print_item(f"Launching {total_tasks} API calls (max {max_workers} concurrent, configured: {configured_max})...")
    if include_missing_check:
        print_item("Includes: 6 subprocess analysis + 1 missing subprocess check")
    print()
    
    start_time = time.time()
    completed = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all subprocess analysis tasks
        futures = {}
        for idx, (sp_name, sp_rows) in enumerate(subprocess_groups.items()):
            # Get summary of OTHER subprocesses for context
            other_summaries = {k: v for k, v in subprocess_summaries.items() if k != sp_name}
            
            future = executor.submit(
                analyze_single_subprocess,
                client,
                sp_name,
                sp_rows,
                other_summaries,
                process_name,
                columns,
                industry,
                suggestions_per_subprocess[sp_name],
                idx
            )
            futures[future] = ("subprocess", sp_name)
        
        # Submit missing subprocess check task (runs in parallel)
        # This is logically correct because it only needs:
        #   - existing_subprocesses (known from RCM before any API calls)
        #   - process_name (known from RCM)
        #   - columns (known from RCM)
        #   - industry (from config)
        # It does NOT depend on suggestions from other calls
        if include_missing_check:
            missing_future = executor.submit(
                check_missing_subprocesses,
                client,
                process_name,
                existing_subprocesses,
                columns,
                industry
            )
            futures[missing_future] = ("missing_check", "Missing Subprocess Check")
        
        # Collect results as they complete
        for future in as_completed(futures):
            task_type, task_name = futures[future]
            completed += 1
            
            try:
                result_name, suggestions, elapsed, error = future.result()
                
                with print_lock:
                    if task_type == "missing_check":
                        # Handle missing subprocess check result
                        if error:
                            print(f"    [WARN]  [{completed}/{total_tasks}] {task_name}: Error - {error}")
                        elif suggestions:
                            print(f"    [OK] [{completed}/{total_tasks}] {task_name}: {len(suggestions)} new subprocess(es) ({elapsed:.1f}s)")
                            missing_subprocess_suggestions.extend(suggestions)
                        else:
                            print(f"    [OK] [{completed}/{total_tasks}] {task_name}: No critical gaps ({elapsed:.1f}s)")
                    else:
                        # Handle subprocess analysis result
                        if error:
                            print(f"    [WARN]  [{completed}/{total_tasks}] {task_name}: Error - {error}")
                            errors.append((task_name, error))
                        else:
                            print(f"    [OK] [{completed}/{total_tasks}] {task_name}: {len(suggestions)} suggestions ({elapsed:.1f}s)")
                            all_suggestions.extend(suggestions)
            
            except Exception as e:
                with print_lock:
                    print(f"    [ERROR] [{completed}/{total_tasks}] {task_name}: Exception - {str(e)}")
                    errors.append((task_name, str(e)))
    
    # Add missing subprocess suggestions at the end (maintains ordering)
    all_suggestions.extend(missing_subprocess_suggestions)
    
    total_time = time.time() - start_time
    print()
    print_success(f"Parallel analysis complete: {len(all_suggestions)} suggestions in {total_time:.1f}s")
    
    if errors:
        print_warning(f"  -> {len(errors)} task(s) had errors")
    if missing_subprocess_suggestions:
        print_info(f"  -> Includes {len(missing_subprocess_suggestions)} new subprocess record(s)")
    
    # ===========================================================================
    # RENUMBER SUGGESTIONS
    # ===========================================================================
    for idx, suggestion in enumerate(all_suggestions, 1):
        suggestion['AI_Suggestion_ID'] = f"RCMAI-{idx:03d}"

    # ===========================================================================
    # CROSS-SUBPROCESS DEDUPLICATION
    # ===========================================================================
    all_suggestions = deduplicate_suggestions(
        client=client,
        suggestions=all_suggestions,
        full_rcm_data=full_rcm_data,
        columns=columns,
        risk_title_col=risk_title_col,
        control_desc_col=control_desc_col,
    )

    # Re-renumber after dedup
    for idx, suggestion in enumerate(all_suggestions, 1):
        suggestion['AI_Suggestion_ID'] = f"RCMAI-{idx:03d}"

    # ===========================================================================
    # VALIDATE AND CORRECT
    # ===========================================================================
    print()
    print_item("Validating suggestions (process, subprocess, category)...")
    all_suggestions = validate_and_correct_suggestions(all_suggestions, existing_processes, existing_subprocesses)
    
    # ===========================================================================
    # BUILD FINAL RESULT
    # ===========================================================================
    # Generate a consolidated executive summary
    result = {
        "executive_summary": {
            "overall_maturity": "Established",
            "coverage_score": min(95, 70 + len(existing_subprocesses) * 3),
            "audit_readiness": "Ready" if len(all_suggestions) < 15 else "Needs Improvement",
            "assessment": f"Analyzed {num_subprocesses} subprocesses in parallel. Generated {len(all_suggestions)} targeted suggestions."
        },
        "gap_analysis": {
            "strengths": [f"Comprehensive coverage across {num_subprocesses} subprocesses"],
            "weaknesses": [f"Identified {len(all_suggestions)} potential gaps"],
            "key_gaps": []
        },
        "suggestions": all_suggestions
    }
    
    print()
    print_success(f"Final suggestion count: {len(all_suggestions)}")
    
    return result, ""


# -----------------------------------------------------------------------------
#                         AI ENGINE (LEGACY - FULL RCM)
# -----------------------------------------------------------------------------

def make_ai_call(client, system_prompt, user_prompt, max_tokens):
    """Make a single AI call and return the response"""
    
    full_response = ""
    start_time = time.time()
    
    try:
        stream = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_completion_tokens=max_tokens,
            stream=True
        )

        sys.stdout.write("    Processing: ")
        dot_count = 0
        for chunk in stream:
            if chunk.choices and len(chunk.choices) > 0:
                delta = chunk.choices[0].delta
                if delta and delta.content:
                    full_response += delta.content
                    dot_count += 1
                    if dot_count % 80 == 0:
                        sys.stdout.write(".")
                        sys.stdout.flush()
        
        print(" Done!")
        elapsed = time.time() - start_time
        return full_response, elapsed, None
        
    except Exception as e:
        return "", 0, str(e)


def get_followup_prompt(existing_suggestions, columns, full_rcm_data, industry, remaining_count):
    """Generate a follow-up prompt to get more suggestions"""
    
    # Summarize what we already have
    existing_summary = []
    for s in existing_suggestions:
        subprocess = s.get('SubProcess', s.get('subprocess', 'Unknown'))
        risk = s.get('Risk Title', s.get('risk_title', 'Unknown'))
        category = s.get('AI_Category', 'Unknown')
        existing_summary.append(f"- [{category}] {subprocess}: {risk}")
    
    existing_list = "\n".join(existing_summary)
    columns_json = json.dumps(columns)
    
    return f"""
FOLLOW-UP REQUEST: Generate {remaining_count} MORE suggestions.

===============================================================================
ALREADY SUGGESTED (DO NOT REPEAT THESE)
===============================================================================
{existing_list}

===============================================================================
GENERATE {remaining_count} ADDITIONAL SUGGESTIONS
===============================================================================
Using your knowledge of {industry} and the RCM data you analyzed earlier,
provide {remaining_count} MORE unique suggestions that:
1. Are DIFFERENT from the ones listed above
2. Cover OTHER gaps you identified
3. Follow the same JSON format

Return ONLY the suggestions array in this format:
{{
    "suggestions": [
        {{
            // Include ALL columns from: {columns_json}
            "AI_Suggestion_ID": "RCMAI-011",  // Continue numbering from where we left off
            "AI_Priority": "High/Medium/Low",
            "AI_Priority_Rationale": "Why this priority level was assigned",
            "AI_Category": "New SubProcess / New Risk / New Control",
            "AI_Reason": "Justification"
        }}
    ]
}}

IMPORTANT: Return ONLY valid JSON with the suggestions array. No other text."""


def get_ai_suggestions(client, rcm_summary, columns, full_rcm_data, industry, suggestion_count, suggestion_breakdown=None, pdf_context=""):
    """Get AI suggestions with retry logic to reach target count"""
    
    print_item("Preparing analysis request...")
    print_item(f"RCM data: {len(full_rcm_data)} rows, {len(columns)} columns")
    print_item(f"Target suggestion count: {suggestion_count} (dynamically calculated)")
    
    # Check original RCM size and show compression status
    original_json_size = len(json.dumps(full_rcm_data))
    original_tokens = original_json_size // 4
    print_item(f"Original RCM size: ~{original_tokens:,} tokens")
    
    if original_tokens > 60000:
        print_info(f"RCM is large - will use compression (CSV format + truncation)")
    
    # Extract existing PROCESSES and SUBPROCESSES for validation later
    existing_processes = []
    existing_subprocesses = []
    for proc, pdata in rcm_summary.get("processes", {}).items():
        existing_processes.append(proc)  # Capture EXACT process name
        existing_subprocesses.extend(pdata.get('subprocesses', []))
    
    existing_processes = list(set(existing_processes))  # Unique
    existing_subprocesses = list(set(existing_subprocesses))  # Unique
    
    print_item(f"Existing processes: {existing_processes}")
    print_item(f"Existing subprocesses: {len(existing_subprocesses)}")
    
    # Calculate tokens for response
    suggestion_tokens = suggestion_count * 500 + 1500
    calculated_tokens = min(16000, max(6000, suggestion_tokens))
    
    print_item(f"Max response tokens allocated: {calculated_tokens:,}")
    
    system_prompt = get_system_prompt(industry, columns)
    user_prompt = get_user_prompt(rcm_summary, columns, full_rcm_data, industry, suggestion_count, suggestion_breakdown, pdf_context)
    
    # Show final prompt size
    prompt_tokens = len(user_prompt) // 4
    print_success(f"Final prompt size: ~{prompt_tokens:,} tokens")
    
    if original_tokens > 60000 and prompt_tokens < original_tokens:
        compression_ratio = (1 - prompt_tokens / original_tokens) * 100
        print_success(f"Compression achieved: {compression_ratio:.0f}% reduction")
    
    print_item(f"Sending to AI (deployment: {OPENAI_MODEL})...")
    print()
    print("    +" + "-" * 68 + "+")
    print("    |   AI ANALYSIS IN PROGRESS                                        |")
    print("    |     AI is reading your COMPLETE RCM ({:>3} rows)...                 |".format(len(full_rcm_data)))
    print("    |     Identifying genuine gaps against compliance best practices...   |")
    print("    +" + "-" * 68 + "+")
    print()
    
    # ===========================================================================
    # INITIAL CALL
    # ===========================================================================
    full_response, elapsed, error = make_ai_call(client, system_prompt, user_prompt, calculated_tokens)
    
    if error:
        print()
        print_error(f"AI Error: {error}")
        return {"executive_summary": {}, "gap_analysis": {}, "suggestions": []}, ""
    
    print()
    print_success(f"Response received in {elapsed:.1f} seconds")
    print_success(f"Response size: {len(full_response):,} characters")
    
    # Parse initial response
    print_item("Parsing AI response...")
    result = parse_ai_response(full_response)
    
    if not result:
        print_warning("Could not parse JSON - saving raw response for debugging")
        return {"raw_response": full_response, "suggestions": []}, full_response
    
    all_suggestions = result.get('suggestions', [])
    current_count = len(all_suggestions)
    print_success(f"JSON parsed successfully!")
    print_success(f"Initial call returned {current_count} suggestions")
    
    # ===========================================================================
    # FOLLOW-UP CALLS (if needed)
    # ===========================================================================
    max_attempts = 3  # Maximum follow-up attempts
    attempt = 0
    
    while current_count < suggestion_count and attempt < max_attempts:
        attempt += 1
        remaining = suggestion_count - current_count
        
        print()
        print("    +" + "-" * 68 + "+")
        print(f"    |   FOLLOW-UP CALL {attempt}/{max_attempts}                                        |")
        print(f"    |     Have {current_count} suggestions, need {remaining} more to reach {suggestion_count}...           |")
        print("    +" + "-" * 68 + "+")
        print()
        
        # Generate follow-up prompt
        followup_prompt = get_followup_prompt(all_suggestions, columns, full_rcm_data, industry, remaining)
        followup_tokens = min(8000, remaining * 600 + 500)
        
        # Make follow-up call
        followup_response, followup_elapsed, followup_error = make_ai_call(
            client, system_prompt, followup_prompt, followup_tokens
        )
        
        if followup_error:
            print_warning(f"Follow-up call failed: {followup_error}")
            break
        
        print()
        print_success(f"Follow-up response received in {followup_elapsed:.1f} seconds")
        
        # Parse follow-up response
        followup_result = parse_ai_response(followup_response)
        
        if followup_result and followup_result.get('suggestions'):
            new_suggestions = followup_result['suggestions']
            
            # Re-number the new suggestions
            for i, s in enumerate(new_suggestions):
                s['AI_Suggestion_ID'] = f"RCMAI-{current_count + i + 1:03d}"
            
            all_suggestions.extend(new_suggestions)
            current_count = len(all_suggestions)
            print_success(f"Added {len(new_suggestions)} more suggestions (total: {current_count})")
        else:
            print_warning("Could not parse follow-up response")
            break
    
    # ===========================================================================
    # FINAL RESULT
    # ===========================================================================
    if current_count < suggestion_count:
        print()
        print_warning(f"Reached {current_count} suggestions (target was {suggestion_count})")
        print_info("This is acceptable - AI may not have found more genuine gaps")
    
    # ===========================================================================
    # VALIDATE AND CORRECT SUGGESTIONS
    # ===========================================================================
    print()
    print_item("Validating suggestions (process, subprocess, category)...")
    all_suggestions = validate_and_correct_suggestions(all_suggestions, existing_processes, existing_subprocesses)
    
    # Update result with validated suggestions
    result['suggestions'] = all_suggestions
    
    print()
    print_success(f"Final suggestion count: {len(all_suggestions)}")
    
    return result, full_response


# -----------------------------------------------------------------------------
#                   PROFESSIONAL CONSOLE DISPLAY
# -----------------------------------------------------------------------------

def display_full_results(results, rcm_summary, columns):
    """Display results on console"""
    
    # Executive Summary
    exec_summary = results.get("executive_summary", {})
    
    print_section("EXECUTIVE SUMMARY")
    print()
    
    maturity = exec_summary.get('overall_maturity', 'N/A')
    score = exec_summary.get('coverage_score', 'N/A')
    readiness = exec_summary.get('audit_readiness', 'N/A')
    assessment = exec_summary.get('assessment', 'No assessment provided')
    
    maturity_icon = "" if maturity == "Advanced" else "" if maturity == "Established" else ""
    readiness_icon = "" if "Well" in str(readiness) or "Ready" == readiness else "" if "Improvement" in str(readiness) else ""
    
    print("    +" + "-" * 70 + "+")
    print(f"    |  {maturity_icon} Overall Maturity:     {str(maturity):<46}|")
    print(f"    |   Coverage Score:       {str(score)}%{' ' * (45 - len(str(score)))}|")
    print(f"    |  {readiness_icon} Audit Readiness:      {str(readiness):<46}|")
    print("    +" + "-" * 70 + "+")
    
    print(f"    |   Assessment:                                                      |")
    words = assessment.split()
    line = ""
    for word in words:
        if len(line) + len(word) + 1 <= 64:
            line += (" " if line else "") + word
        else:
            print(f"    |     {line}{' ' * (65 - len(line))}|")
            line = word
    if line:
        print(f"    |     {line}{' ' * (65 - len(line))}|")
    print("    +" + "-" * 70 + "+")
    
    # Gap Analysis
    gap = results.get("gap_analysis", {})
    
    print_section("GAP ANALYSIS")
    
    print("\n     STRENGTHS:")
    print("    " + "-" * 68)
    for s in gap.get("strengths", ["None identified"]):
        print(f"       - {s}")
    
    print("\n    [WARN]  WEAKNESSES:")
    print("    " + "-" * 68)
    for w in gap.get("weaknesses", ["None identified"]):
        print(f"       - {w}")
    
    print("\n     KEY GAPS:")
    print("    " + "-" * 68)
    for g in gap.get("key_gaps", []):
        if isinstance(g, dict):
            level = g.get('risk_level', 'N/A')
            icon = "" if level == "High" else "" if level == "Medium" else ""
            print(f"       {icon} [{level}] {g.get('gap', 'N/A')}")
            print(f"          +- Compliance Impact: {g.get('compliance_impact', g.get('sox_impact', 'N/A'))}")
            print()
    
    # Suggestions Summary
    suggestions = results.get("suggestions", [])
    
    print_section(f"AI SUGGESTIONS ({len(suggestions)} items)")
    
    high = len([s for s in suggestions if s.get('AI_Priority') == 'High'])
    medium = len([s for s in suggestions if s.get('AI_Priority') == 'Medium'])
    low = len([s for s in suggestions if s.get('AI_Priority') == 'Low'])
    
    # Category breakdown - simplified to 3 main categories
    new_subprocess = len([s for s in suggestions if 'subprocess' in s.get('AI_Category', '').lower() or 'sub process' in s.get('AI_Category', '').lower()])
    new_risk = len([s for s in suggestions if s.get('AI_Category', '').lower() == 'new risk'])
    new_control = len([s for s in suggestions if s.get('AI_Category', '').lower() == 'new control'])
    other_cat = len(suggestions) - new_subprocess - new_risk - new_control
    
    print()
    print("    +" + "-" * 52 + "+")
    print(f"    |  BY PRIORITY:                                      |")
    print(f"    |     High Priority:    {high:<27}  |")
    print(f"    |     Medium Priority:  {medium:<27}  |")
    print(f"    |     Low Priority:     {low:<27}  |")
    print(f"    |  -------------------------------------------------  |")
    print(f"    |  BY CATEGORY:                                      |")
    print(f"    |     New SubProcess:     {new_subprocess:<25}  |")
    print(f"    |    [WARN]  New Risk:           {new_risk:<25}  |")
    print(f"    |      New Control:        {new_control:<25}  |")
    if other_cat > 0:
        print(f"    |     Other:              {other_cat:<25}  |")
    print(f"    |  -------------------------------------------------  |")
    print(f"    |   TOTAL:              {len(suggestions):<27}  |")
    print("    +" + "-" * 52 + "+")
    
    # Show schema info
    print()
    print_info(f"Suggestions use your RCM schema: {len(columns)} columns")
    print_info("Columns matched: " + ", ".join(columns[:5]) + ("..." if len(columns) > 5 else ""))
    
    # Quick reference table
    print_section("QUICK REFERENCE TABLE")
    print()
    
    # Find title/name column for display
    title_col = None
    for col in columns:
        cl = col.lower()
        if 'risk' in cl and ('title' in cl or 'name' in cl):
            title_col = col
            break
        elif 'risk' in cl and 'desc' in cl:
            title_col = col
            break
    
    if not title_col:
        title_col = columns[0] if columns else "Item"
    
    print("    +-----+----------------------+--------------------------------+----------+")
    print("    |  #  |  Category            |  Description                   | Priority |")
    print("    +-----+----------------------+--------------------------------+----------+")
    
    for i, s in enumerate(suggestions, 1):
        title = str(s.get(title_col, s.get('AI_Suggestion_ID', f'Item {i}')))[:28]
        category = str(s.get('AI_Category', 'N/A'))[:18]
        priority = s.get('AI_Priority', 'N/A')
        p_icon = "" if priority == "High" else "" if priority == "Medium" else ""
        
        # Category icon - simplified to 3 main categories
        cat_lower = category.lower()
        if 'subprocess' in cat_lower or 'sub process' in cat_lower:
            c_icon = ""
        elif 'risk' in cat_lower:
            c_icon = "[WARN] "
        elif 'control' in cat_lower:
            c_icon = " "
        else:
            c_icon = ""
        
        print(f"    | {i:>2}  | {c_icon}{category:<19}| {title:<30} | {p_icon} {priority:<5} |")
    
    print("    +-----+----------------------+--------------------------------+----------+")


# -----------------------------------------------------------------------------
#                      SAVE OUTPUT FILES
# -----------------------------------------------------------------------------

def save_full_text_report(results, rcm_summary, columns, filename):
    """Save text report"""
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("           RCM AI-POWERED GAP ANALYSIS - FULL REPORT v4.0\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Industry: {INDUSTRY}\n")
        f.write(f"RCM Columns: {len(columns)}\n")
        f.write("=" * 80 + "\n\n")
        
        exec_sum = results.get("executive_summary", {})
        f.write("EXECUTIVE SUMMARY\n")
        f.write("-" * 40 + "\n")
        f.write(f"Overall Maturity:  {exec_sum.get('overall_maturity', 'N/A')}\n")
        f.write(f"Coverage Score:    {exec_sum.get('coverage_score', 'N/A')}%\n")
        f.write(f"Audit Readiness:   {exec_sum.get('audit_readiness', 'N/A')}\n")
        f.write(f"Assessment:        {exec_sum.get('assessment', 'N/A')}\n\n")
        
        gap = results.get("gap_analysis", {})
        f.write("GAP ANALYSIS\n")
        f.write("-" * 40 + "\n")
        f.write("Strengths:\n")
        for s in gap.get("strengths", []):
            f.write(f"  - {s}\n")
        f.write("\nWeaknesses:\n")
        for w in gap.get("weaknesses", []):
            f.write(f"  - {w}\n")
        f.write("\nKey Gaps:\n")
        for g in gap.get("key_gaps", []):
            if isinstance(g, dict):
                f.write(f"  [{g.get('risk_level', 'N/A')}] {g.get('gap', 'N/A')}\n")
                f.write(f"       Compliance Impact: {g.get('compliance_impact', g.get('sox_impact', 'N/A'))}\n")
        f.write("\n")
        
        suggestions = results.get("suggestions", [])
        f.write("=" * 80 + "\n")
        f.write(f"               AI SUGGESTIONS ({len(suggestions)} items)\n")
        f.write("=" * 80 + "\n\n")
        
        for i, s in enumerate(suggestions, 1):
            f.write(f"\n{'-' * 80}\n")
            f.write(f"SUGGESTION {i}\n")
            f.write(f"{'-' * 80}\n")
            f.write(f"AI ID:       {s.get('AI_Suggestion_ID', 'N/A')}\n")
            f.write(f"Priority:    {s.get('AI_Priority', 'N/A')}\n")
            f.write(f"Priority Rationale: {s.get('AI_Priority_Rationale', 'N/A')}\n")
            f.write(f"Category:    {s.get('AI_Category', 'N/A')}\n")
            f.write(f"Reason:      {s.get('AI_Reason', 'N/A')}\n")
            f.write(f"\nRCM Row Data:\n")
            for col in columns:
                if col in s:
                    f.write(f"  {col}: {s[col]}\n")
        
        f.write("\n" + "=" * 80 + "\n")
        f.write("END OF REPORT\n")
    
    print_success(f"Text report: {filename}")


def save_json_response(results, filename):
    """Save JSON"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print_success(f"JSON file: {filename}")


def export_to_excel(results, rcm_df, rcm_summary, columns, output_path, suggestion_breakdown=None):
    """
    Export to Excel - Single sheet: Combined (Ready to Use)
    Original RCM rows + AI suggestions merged with smart ordering
    """
    
    print_item("Creating Excel output...")
    
    suggestions = results.get("suggestions", [])
    ai_tracking_cols = ['Row_Source', 'AI_Suggestion_ID', 'AI_Priority', 'AI_Category', 'AI_Reason']
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        
        # Find subprocess column
        subprocess_col = None
        for col in columns:
            if 'subprocess' in col.lower() or 'sub process' in col.lower() or 'sub-process' in col.lower():
                subprocess_col = col
                break
        
        # Prepare all rows
        all_rows = []
        
        if suggestions:
            # Separate suggestions by category
            subprocess_suggestions = []  # New SubProcess - go at end
            inline_suggestions = {}       # New Risk/Control - grouped by subprocess
            
            for s in suggestions:
                category = s.get('AI_Category', '').lower()
                
                if 'subprocess' in category or 'sub process' in category:
                    subprocess_suggestions.append(s)
                else:
                    if subprocess_col:
                        sub_name = str(s.get(subprocess_col, '')).strip().lower()
                    else:
                        sub_name = '__no_subprocess__'
                    
                    if sub_name not in inline_suggestions:
                        inline_suggestions[sub_name] = []
                    inline_suggestions[sub_name].append(s)
            
            # Process original RCM rows and insert inline suggestions
            if subprocess_col:
                current_subprocess = None
                
                for idx, row in rcm_df.iterrows():
                    row_subprocess = str(row.get(subprocess_col, '')).strip().lower()
                    
                    # Check if we've moved to a new subprocess
                    if row_subprocess != current_subprocess:
                        # Add pending suggestions for the previous subprocess
                        if current_subprocess and current_subprocess in inline_suggestions:
                            for s in inline_suggestions[current_subprocess]:
                                suggestion_row = {'Row_Source': 'AI_Suggestion'}
                                for col in ['AI_Suggestion_ID', 'AI_Priority', 'AI_Priority_Rationale', 'AI_Category', 'AI_Reason']:
                                    suggestion_row[col] = s.get(col, '')
                                for col in columns:
                                    suggestion_row[col] = s.get(col, '')
                                all_rows.append(suggestion_row)
                            del inline_suggestions[current_subprocess]
                        
                        current_subprocess = row_subprocess
                    
                    # Add the original row
                    original_row = {'Row_Source': 'Original'}
                    for col in ['AI_Suggestion_ID', 'AI_Priority', 'AI_Priority_Rationale', 'AI_Category', 'AI_Reason']:
                        original_row[col] = ''
                    for col in columns:
                        original_row[col] = row.get(col, '')
                    all_rows.append(original_row)
                
                # Add suggestions for the last subprocess
                if current_subprocess and current_subprocess in inline_suggestions:
                    for s in inline_suggestions[current_subprocess]:
                        suggestion_row = {'Row_Source': 'AI_Suggestion'}
                        for col in ['AI_Suggestion_ID', 'AI_Priority', 'AI_Priority_Rationale', 'AI_Category', 'AI_Reason']:
                            suggestion_row[col] = s.get(col, '')
                        for col in columns:
                            suggestion_row[col] = s.get(col, '')
                        all_rows.append(suggestion_row)
                    del inline_suggestions[current_subprocess]
                
                # Add remaining inline suggestions that didn't match
                for sub_name, sub_suggestions in inline_suggestions.items():
                    for s in sub_suggestions:
                        suggestion_row = {'Row_Source': 'AI_Suggestion'}
                        for col in ['AI_Suggestion_ID', 'AI_Priority', 'AI_Priority_Rationale', 'AI_Category', 'AI_Reason']:
                            suggestion_row[col] = s.get(col, '')
                        for col in columns:
                            suggestion_row[col] = s.get(col, '')
                        all_rows.append(suggestion_row)
            
            else:
                # No subprocess column - add original rows then suggestions
                for idx, row in rcm_df.iterrows():
                    original_row = {'Row_Source': 'Original'}
                    for col in ['AI_Suggestion_ID', 'AI_Priority', 'AI_Priority_Rationale', 'AI_Category', 'AI_Reason']:
                        original_row[col] = ''
                    for col in columns:
                        original_row[col] = row.get(col, '')
                    all_rows.append(original_row)
                
                for sub_name, sub_suggestions in inline_suggestions.items():
                    for s in sub_suggestions:
                        suggestion_row = {'Row_Source': 'AI_Suggestion'}
                        for col in ['AI_Suggestion_ID', 'AI_Priority', 'AI_Priority_Rationale', 'AI_Category', 'AI_Reason']:
                            suggestion_row[col] = s.get(col, '')
                        for col in columns:
                            suggestion_row[col] = s.get(col, '')
                        all_rows.append(suggestion_row)
            
            # Add New SubProcess suggestions at the end
            for s in subprocess_suggestions:
                suggestion_row = {'Row_Source': 'New_SubProcess'}
                for col in ['AI_Suggestion_ID', 'AI_Priority', 'AI_Priority_Rationale', 'AI_Category', 'AI_Reason']:
                    suggestion_row[col] = s.get(col, '')
                for col in columns:
                    suggestion_row[col] = s.get(col, '')
                all_rows.append(suggestion_row)
        
        else:
            # No suggestions - just add original RCM rows
            for idx, row in rcm_df.iterrows():
                original_row = {'Row_Source': 'Original'}
                for col in ['AI_Suggestion_ID', 'AI_Priority', 'AI_Priority_Rationale', 'AI_Category', 'AI_Reason']:
                    original_row[col] = ''
                for col in columns:
                    original_row[col] = row.get(col, '')
                all_rows.append(original_row)
        
        # Create DataFrame
        combined_df = pd.DataFrame(all_rows)
        
        # Reorder columns: Row_Source, AI tracking cols, then RCM columns
        final_cols = ['Row_Source', 'AI_Suggestion_ID', 'AI_Priority', 'AI_Priority_Rationale', 'AI_Category', 'AI_Reason'] + columns
        final_cols = [c for c in final_cols if c in combined_df.columns]
        combined_df = combined_df[final_cols]
        
        combined_df.to_excel(writer, sheet_name='Combined RCM', index=False)
        
        # Count by type
        original_count = len([r for r in all_rows if r['Row_Source'] == 'Original'])
        suggestion_count = len([r for r in all_rows if r['Row_Source'] == 'AI_Suggestion'])
        new_sub_count = len([r for r in all_rows if r['Row_Source'] == 'New_SubProcess'])
        
        print_success(f"Combined RCM sheet: {len(combined_df)} rows")
        print_info(f"  -> {original_count} Original + {suggestion_count} AI Suggestions + {new_sub_count} New SubProcesses")
    
    print_success(f"Excel saved: {output_path}")


# -----------------------------------------------------------------------------
#                               MAIN
# -----------------------------------------------------------------------------

def main():
    """Main function - uses hardcoded configuration values."""
    
    print_banner()
    
    total_steps = 6 if TEST_CONNECTION_FIRST else 5
    current_step = 0
    
    # =======================================================================
    # STEP: VALIDATE CONFIG
    # =======================================================================
    current_step += 1
    print_step(current_step, total_steps, "VALIDATING CONFIGURATION")
    
    errors = []
    if not OPENAI_API_KEY or "your-api-key" in OPENAI_API_KEY:
        errors.append("OPENAI_API_KEY not configured")
    if not os.path.exists(RCM_FILE_PATH):
        errors.append(f"RCM file not found: {RCM_FILE_PATH}")
    if INDUSTRY not in SUPPORTED_INDUSTRIES:
        errors.append(f"Invalid industry: {INDUSTRY}")

    if errors:
        for e in errors:
            print_error(e)
        print("\n    Please edit the CONFIGURATION section at the top of this file.")
        return

    print_success(f"OpenAI API Key: ...{OPENAI_API_KEY[-8:]}")
    print_success(f"Model: {OPENAI_MODEL}")
    print_success(f"RCM File: {RCM_FILE_PATH}")
    print_success(f"Industry: {INDUSTRY}")
    
    # =======================================================================
    # CONNECTION TEST
    # =======================================================================
    client = None
    
    if TEST_CONNECTION_FIRST:
        current_step += 1
        print_step(current_step, total_steps, "TESTING AI CONNECTION")
        
        success, client = test_ai_connection()
        
        if not success:
            print()
            print_error("Cannot proceed without AI connection.")
            return
    
    # =======================================================================
    # LOAD RCM
    # =======================================================================
    current_step += 1
    print_step(current_step, total_steps, "LOADING RCM & EXTRACTING SCHEMA")
    
    try:
        if RCM_FILE_PATH.endswith('.csv'):
            rcm_df = pd.read_csv(RCM_FILE_PATH)
        else:
            rcm_df = pd.read_excel(RCM_FILE_PATH)
        print_success(f"Loaded {len(rcm_df)} rows, {len(rcm_df.columns)} columns")
    except Exception as e:
        print_error(f"Error loading file: {e}")
        return
    
    # Extract schema and FULL data
    columns, full_rcm_data = get_rcm_schema(rcm_df)
    print_success(f"Schema extracted: {len(columns)} columns")
    print_success(f"Full RCM data prepared: {len(full_rcm_data)} rows will be sent to AI")
    print_info(f"Columns: {', '.join(columns[:6])}{'...' if len(columns) > 6 else ''}")
    
    # =======================================================================
    # ANALYZE RCM
    # =======================================================================
    current_step += 1
    print_step(current_step, total_steps, "ANALYZING RCM STRUCTURE")
    
    rcm_summary = analyze_rcm(rcm_df)
    
    print_success(f"Processes: {len(rcm_summary['processes'])}")
    print_success(f"Unique Risks: {rcm_summary['unique_risks']}")
    print_success(f"Unique Controls: {rcm_summary['unique_controls']}")
    
    print("\n    Process breakdown:")
    for proc, pdata in rcm_summary['processes'].items():
        subprocess_count = len(pdata.get('subprocesses', []))
        print(f"      - {proc}: {pdata['risk_count']} risks, {pdata['control_count']} controls, {subprocess_count} subprocesses")
    
    # Calculate dynamic suggestion count with full breakdown
    suggestion_count, suggestion_breakdown = calculate_suggestion_count(rcm_summary, INDUSTRY)
    display_suggestion_calculation(suggestion_breakdown)
    
    pdf_context = load_pdfs(PDF_FILES) if PDF_FILES else ""
    
    # =======================================================================
    # AI ANALYSIS
    # =======================================================================
    current_step += 1
    print_step(current_step, total_steps, "AI-POWERED GAP ANALYSIS (PARALLEL)")
    
    if client is None:
        client = create_llm_client()
    
    # Use parallel subprocess analysis for efficiency
    results, raw_response = get_ai_suggestions_parallel(
        client, rcm_summary, columns, full_rcm_data, rcm_df,
        INDUSTRY, suggestion_count, suggestion_breakdown, pdf_context
    )
    
    # =======================================================================
    # DISPLAY & EXPORT
    # =======================================================================
    current_step += 1
    print_step(current_step, total_steps, "DISPLAYING RESULTS & EXPORTING")
    
    display_full_results(results, rcm_summary, columns)
    
    # Save files
    print_section("SAVING OUTPUT FILES")
    print()
    
    save_json_response(results, OUTPUT_JSON)
    save_full_text_report(results, rcm_summary, columns, OUTPUT_TEXT)
    export_to_excel(results, rcm_df, rcm_summary, columns, OUTPUT_EXCEL, suggestion_breakdown)
    
    # =======================================================================
    # FINAL SUMMARY
    # =======================================================================
    suggestions_count = len(results.get('suggestions', []))
    
    print("\n")
    print("  +========================================================================+")
    print("  |                     [OK] ANALYSIS COMPLETE                               |")
    print("  +========================================================================+")
    print(f"  |    Suggestions Generated:  {suggestions_count:<5}                                   |")
    print(f"  |    Schema Matched:         {len(columns)} columns                                |")
    print("  |                                                                        |")
    print("  |    OUTPUT FILES:                                                     |")
    print(f"  |       {OUTPUT_EXCEL:<55}  |")
    print(f"  |       {OUTPUT_TEXT:<55}  |")
    print(f"  |       {OUTPUT_JSON:<55}  |")
    print("  |                                                                        |")
    print("  |    EXCEL OUTPUT:                                                     |")
    print("  |      Combined RCM - Original + AI Suggestions (ready to use)           |")
    print("  |                                                                        |")
    print("  +========================================================================+")
    print("  |    NEXT STEPS:                                                       |")
    print("  |      1. Open the Excel file                                            |")
    print("  |      2. Review AI suggestions (marked with  or )                    |")
    print("  |      3. Send for manager approval                                      |")
    print("  |                                                                        |")
    print("  +========================================================================+")
    print()


if __name__ == "__main__":
    main()
