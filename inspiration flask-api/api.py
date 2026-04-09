"""
Flask API for RCM Analysis Tools
==================================

Four endpoints with JSON and Excel download support:
1. /api/ai-suggest
2. /api/control-assessment
3. /api/deduplication
4. /api/tod-test

Usage:
    pip install flask flask-cors
    python api.py

Author: Rishi
Date: February 2026
"""

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import os
import json
import traceback
from datetime import datetime
from pathlib import Path
import sys
import time
import math
import pandas as pd
import numpy as np

app = Flask(__name__)
CORS(app)


def sanitize_for_json(obj):
    """
    Recursively replace NaN, Infinity, -Infinity with None for valid JSON.

    Python's json.dumps() outputs NaN/Infinity by default (allow_nan=True),
    but these are NOT valid JSON and cause JavaScript's JSON.parse() to fail.
    Pandas DataFrames produce NaN for empty cells, so this is critical.
    """
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(item) for item in obj]
    elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    elif isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating,)):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)
    elif isinstance(obj, (np.bool_,)):
        return bool(obj)
    elif isinstance(obj, (np.ndarray,)):
        return sanitize_for_json(obj.tolist())
    return obj

# Azure OpenAI config — imported from central engines/config.py
from engines.config import (
    AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY,
    AZURE_OPENAI_API_VERSION, AZURE_OPENAI_DEPLOYMENT as AZURE_OPENAI_DEPLOYMENT_NAME,
    OPENAI_API_KEY, OPENAI_MODEL,
)
TOD_MAX_WORKERS = 5
TOE_MAX_WORKERS = 5

# Store latest results for each endpoint type
latest_results = {
    'ai-suggest': None,
    'control-assessment': None,
    'deduplication': None,
    'tod-test': None,
    'toe-test': None
}


# =============================================================================
# HELPER FUNCTION: Convert Excel to JSON
# =============================================================================

def excel_to_json(excel_path):
    """
    Convert an Excel file to JSON format.
    Returns a dictionary with all sheets and their data.
    """
    if not os.path.exists(excel_path):
        return {"error": "Excel file not found"}

    try:
        # Read all sheets from Excel
        excel_file = pd.ExcelFile(excel_path)
        result = {
            "file_path": excel_path,
            "sheets": {}
        }

        for sheet_name in excel_file.sheet_names:
            df = pd.read_excel(excel_path, sheet_name=sheet_name)

            # Convert DataFrame to list of dictionaries (records)
            # df.where(pd.notnull(df), None) does NOT reliably remove NaN
            # because pandas converts None back to NaN in float columns.
            # Instead, convert to dict first, then sanitize all NaN/Inf values.
            sheet_data = df.to_dict(orient='records')
            sheet_data = sanitize_for_json(sheet_data)

            result["sheets"][sheet_name] = {
                "columns": list(df.columns),
                "row_count": len(df),
                "data": sheet_data
            }

        return result
    except Exception as e:
        return {"error": f"Failed to convert Excel to JSON: {str(e)}"}


# =============================================================================
# ENDPOINT 1: AI SUGGEST
# =============================================================================

@app.route('/api/ai-suggest', methods=['POST', 'GET'])
def ai_suggest_endpoint():
    """AI-Powered Gap Analysis & Suggestions"""

    if request.method == 'GET':
        download = request.args.get('download', '0') == '1'

        result = latest_results.get('ai-suggest')
        if not result:
            return jsonify({"error": "No AI Suggest analysis has been run yet. Please POST first."}), 404

        if download:
            excel_path = result.get('excel_output')
            if not excel_path or not os.path.exists(excel_path):
                return jsonify({"error": "Excel file not found"}), 404
            return send_file(
                excel_path,
                as_attachment=True,
                download_name=os.path.basename(excel_path),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        else:
            return jsonify(result)

    # POST - Run analysis
    t_start = time.time()
    try:
        data = request.get_json() or {}

        print(f"\n{'='*70}")
        print(f"[FLASK] /api/ai-suggest  POST received at {datetime.now().isoformat()}")
        print(f"[FLASK] Step 1/6: Parsing request body...")
        print(f"[FLASK]   rcm_file_path = {data.get('rcm_file_path', '(not provided)')}")
        print(f"[FLASK]   industry      = {data.get('industry', '(not provided, default Manufacturing)')}")

        # Import the module
        print(f"[FLASK] Step 2/6: Importing AiSuggest module...")
        t_import = time.time()
        import AiSuggest
        from importlib import reload
        reload(AiSuggest)
        print(f"[FLASK]   Module imported in {time.time()-t_import:.2f}s")

        # Update configuration if provided
        if 'rcm_file_path' in data:
            AiSuggest.RCM_FILE_PATH = data['rcm_file_path']
        if 'industry' in data:
            AiSuggest.INDUSTRY = data['industry']

        # Validate input file exists
        print(f"[FLASK] Step 3/6: Validating input file...")
        rcm_path = AiSuggest.RCM_FILE_PATH
        if not os.path.exists(rcm_path):
            print(f"[FLASK]   ERROR: RCM file not found: {rcm_path}")
            return jsonify({"status": "error", "error": f"RCM file not found: {rcm_path}"}), 400

        # Read input to log column info
        try:
            input_df = pd.read_excel(rcm_path)
            print(f"[FLASK]   Input file: {rcm_path}")
            print(f"[FLASK]   Input rows: {len(input_df)}, columns: {list(input_df.columns)}")
        except Exception as read_err:
            print(f"[FLASK]   WARNING: Could not preview input: {read_err}")

        # Generate unique output files
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = Path(rcm_path).parent

        AiSuggest.OUTPUT_EXCEL = str(output_dir / f"AI_Analysis_{timestamp}.xlsx")
        AiSuggest.OUTPUT_JSON = str(output_dir / f"AI_Analysis_{timestamp}.json")
        AiSuggest.OUTPUT_TEXT = str(output_dir / f"AI_Analysis_{timestamp}.txt")

        print(f"[FLASK]   Output Excel: {AiSuggest.OUTPUT_EXCEL}")
        print(f"[FLASK]   Output JSON:  {AiSuggest.OUTPUT_JSON}")

        # Run the analysis
        print(f"[FLASK] Step 4/6: Running AiSuggest.main() — this calls Azure OpenAI...")
        t_analysis = time.time()
        AiSuggest.main()
        analysis_time = time.time() - t_analysis
        print(f"[FLASK]   AiSuggest.main() completed in {analysis_time:.2f}s")

        # Check output files exist
        print(f"[FLASK] Step 5/6: Checking output files...")
        excel_exists = os.path.exists(AiSuggest.OUTPUT_EXCEL)
        json_exists = os.path.exists(AiSuggest.OUTPUT_JSON)
        text_exists = os.path.exists(AiSuggest.OUTPUT_TEXT)
        print(f"[FLASK]   Excel output exists: {excel_exists}")
        print(f"[FLASK]   JSON output exists:  {json_exists}")
        print(f"[FLASK]   Text output exists:  {text_exists}")

        # Read JSON content inline so backend doesn't need filesystem access
        json_inline = None
        if json_exists:
            try:
                with open(AiSuggest.OUTPUT_JSON, 'r') as jf:
                    json_inline = json.load(jf)
                    json_inline = sanitize_for_json(json_inline)  # Remove NaN/Inf
                    sugg_count = len(json_inline.get('suggestions', []))
                    has_exec_summary = bool(json_inline.get('executive_summary'))
                    has_gap = bool(json_inline.get('gap_analysis'))
                    print(f"[FLASK]   JSON: {sugg_count} suggestions, exec_summary={has_exec_summary}, gap_analysis={has_gap}")
            except Exception as je:
                print(f"[FLASK]   WARNING: Could not read JSON output: {je}")

        # Convert Excel to JSON
        print(f"[FLASK] Step 6/6: Converting Excel to JSON for response...")
        t_convert = time.time()
        excel_data = excel_to_json(AiSuggest.OUTPUT_EXCEL)
        print(f"[FLASK]   Excel converted in {time.time()-t_convert:.2f}s")
        if 'sheets' in excel_data:
            for sheet_name, sheet_info in excel_data['sheets'].items():
                print(f"[FLASK]   Sheet '{sheet_name}': {sheet_info.get('row_count', 0)} rows, columns={sheet_info.get('columns', [])}")

        # Prepare response with full Excel data AND inline JSON
        result = {
            "status": "success",
            "timestamp": timestamp,
            "excel_output": AiSuggest.OUTPUT_EXCEL,
            "json_output": AiSuggest.OUTPUT_JSON,
            "json_data": json_inline,
            "text_output": AiSuggest.OUTPUT_TEXT,
            "input_file": AiSuggest.RCM_FILE_PATH,
            "industry": AiSuggest.INDUSTRY,
            "download_url": f"/api/ai-suggest?download=1",
            "excel_data": excel_data
        }

        # Store latest result
        latest_results['ai-suggest'] = result

        total_time = time.time() - t_start
        print(f"[FLASK] /api/ai-suggest RESPONSE SUMMARY:")
        print(f"[FLASK]   Response keys: [{', '.join(result.keys())}]")
        print(f"[FLASK]   .status    = '{result['status']}'")
        print(f"[FLASK]   .json_data = {'PRESENT (' + str(len(json_inline.get('suggestions', []))) + ' suggestions)' if json_inline else 'null'}")
        print(f"[FLASK]   .excel_data= {'PRESENT' if 'sheets' in (excel_data or {}) else 'null/error'}")
        print(f"[FLASK] /api/ai-suggest COMPLETED in {total_time:.2f}s")
        print(f"{'='*70}\n")

        return jsonify(sanitize_for_json(result))

    except Exception as e:
        total_time = time.time() - t_start
        print(f"[FLASK] /api/ai-suggest FAILED after {total_time:.2f}s")
        print(f"[FLASK] ERROR: {str(e)}")
        print(f"[FLASK] TRACEBACK:\n{traceback.format_exc()}")
        print(f"{'='*70}\n")
        return jsonify({
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# =============================================================================
# ENDPOINT 2: CONTROL ASSESSMENT
# =============================================================================

@app.route('/api/control-assessment', methods=['POST', 'GET'])
def control_assessment_endpoint():
    """OnGround Check - Control Assessment"""

    if request.method == 'GET':
        download = request.args.get('download', '0') == '1'

        result = latest_results.get('control-assessment')
        if not result:
            return jsonify({"error": "No Control Assessment has been run yet. Please POST first."}), 404

        if download:
            excel_path = result.get('excel_output')
            if not excel_path or not os.path.exists(excel_path):
                return jsonify({"error": "Excel file not found"}), 404
            return send_file(
                excel_path,
                as_attachment=True,
                download_name=os.path.basename(excel_path),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        else:
            return jsonify(result)

    # POST - Run assessment
    t_start = time.time()
    try:
        data = request.get_json() or {}

        print(f"\n{'='*70}")
        print(f"[FLASK] /api/control-assessment  POST received at {datetime.now().isoformat()}")
        print(f"[FLASK] Step 1/7: Parsing request body...")
        print(f"[FLASK]   rcm_file_path = {data.get('rcm_file_path', '(not provided)')}")
        print(f"[FLASK]   policy_paths  = {data.get('policy_paths', '(not provided)')}")
        print(f"[FLASK]   sop_paths     = {data.get('sop_paths', '(not provided)')}")

        # Import the module
        print(f"[FLASK] Step 2/7: Importing ControlAssesment module...")
        t_import = time.time()
        import ControlAssesment
        from importlib import reload
        reload(ControlAssesment)
        print(f"[FLASK]   Module imported in {time.time()-t_import:.2f}s")

        # Get configuration
        rcm_path = data.get('rcm_file_path', ControlAssesment.Config.RCM_EXCEL_PATH)
        policy_paths = data.get('policy_paths', ControlAssesment.Config.POLICY_PDF_PATHS)
        sop_paths = data.get('sop_paths', ControlAssesment.Config.SOP_PDF_PATHS)

        # Validate input files
        print(f"[FLASK] Step 3/7: Validating input files...")
        if not os.path.exists(rcm_path):
            print(f"[FLASK]   ERROR: RCM file not found: {rcm_path}")
            return jsonify({"status": "error", "error": f"RCM file not found: {rcm_path}"}), 400

        try:
            input_df = pd.read_excel(rcm_path)
            print(f"[FLASK]   RCM file: {rcm_path}")
            print(f"[FLASK]   RCM rows: {len(input_df)}, columns: {list(input_df.columns)}")
        except Exception as read_err:
            print(f"[FLASK]   WARNING: Could not preview RCM input: {read_err}")

        valid_policies = [p for p in (policy_paths or []) if os.path.exists(p)]
        valid_sops = [s for s in (sop_paths or []) if os.path.exists(s)]
        print(f"[FLASK]   Policy files: {len(valid_policies)} valid out of {len(policy_paths or [])}")
        for p in valid_policies:
            print(f"[FLASK]     - {p}")
        print(f"[FLASK]   SOP files: {len(valid_sops)} valid out of {len(sop_paths or [])}")
        for s in valid_sops:
            print(f"[FLASK]     - {s}")

        # Generate unique output files
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = Path(rcm_path).parent

        excel_output = str(output_dir / f"Control_Assessment_{timestamp}.xlsx")
        json_output = str(output_dir / f"Control_Assessment_{timestamp}.json")
        print(f"[FLASK] Step 4/7: Output paths configured")
        print(f"[FLASK]   Output Excel: {excel_output}")
        print(f"[FLASK]   Output JSON:  {json_output}")

        # Create and run checker
        print(f"[FLASK] Step 5/7: Creating OnGroundCheck instance...")
        checker = ControlAssesment.OnGroundCheck(
            rcm_path=rcm_path,
            policy_paths=policy_paths,
            sop_paths=sop_paths,
            out_excel=excel_output,
            out_json=json_output
        )

        print(f"[FLASK] Step 6/7: Running checker.run() — this calls Azure OpenAI for each control...")
        t_run = time.time()
        checker.run()
        run_time = time.time() - t_run
        print(f"[FLASK]   checker.run() completed in {run_time:.2f}s")

        # Check output files
        excel_exists = os.path.exists(excel_output)
        json_out_exists = os.path.exists(json_output)
        print(f"[FLASK]   Excel output exists: {excel_exists}")
        print(f"[FLASK]   JSON output exists:  {json_out_exists}")

        # Convert Excel to JSON
        print(f"[FLASK] Step 7/7: Converting Excel to JSON for response...")
        t_convert = time.time()
        excel_data = excel_to_json(excel_output)
        print(f"[FLASK]   Excel converted in {time.time()-t_convert:.2f}s")
        if 'sheets' in excel_data:
            for sheet_name, sheet_info in excel_data['sheets'].items():
                print(f"[FLASK]   Sheet '{sheet_name}': {sheet_info.get('row_count', 0)} rows, columns={sheet_info.get('columns', [])}")

        # Prepare response with full Excel data as JSON
        result = {
            "status": "success",
            "timestamp": timestamp,
            "excel_output": excel_output,
            "json_output": json_output,
            "input_file": rcm_path,
            "policy_files": policy_paths,
            "sop_files": sop_paths,
            "download_url": f"/api/control-assessment?download=1",
            "excel_data": excel_data
        }

        # Store latest result
        latest_results['control-assessment'] = result

        total_time = time.time() - t_start
        cda_rows = 0
        if 'sheets' in (excel_data or {}):
            for si in excel_data['sheets'].values():
                cda_rows += si.get('row_count', 0)
        print(f"[FLASK] /api/control-assessment RESPONSE SUMMARY:")
        print(f"[FLASK]   Response keys: [{', '.join(result.keys())}]")
        print(f"[FLASK]   .status    = '{result['status']}'")
        print(f"[FLASK]   .excel_data= {'PRESENT (' + str(cda_rows) + ' rows)' if 'sheets' in (excel_data or {}) else 'null/error'}")
        print(f"[FLASK] /api/control-assessment COMPLETED in {total_time:.2f}s")
        print(f"{'='*70}\n")

        return jsonify(sanitize_for_json(result))

    except Exception as e:
        total_time = time.time() - t_start
        print(f"[FLASK] /api/control-assessment FAILED after {total_time:.2f}s")
        print(f"[FLASK] ERROR: {str(e)}")
        print(f"[FLASK] TRACEBACK:\n{traceback.format_exc()}")
        print(f"{'='*70}\n")
        return jsonify({
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# =============================================================================
# ENDPOINT 3: DEDUPLICATION
# =============================================================================

@app.route('/api/deduplication', methods=['POST', 'GET'])
def deduplication_endpoint():
    """Semantic Deduplication Engine"""

    if request.method == 'GET':
        download = request.args.get('download', '0') == '1'

        result = latest_results.get('deduplication')
        if not result:
            return jsonify({"error": "No Deduplication analysis has been run yet. Please POST first."}), 404

        if download:
            excel_path = result.get('excel_output')
            if not excel_path or not os.path.exists(excel_path):
                return jsonify({"error": "Excel file not found"}), 404
            return send_file(
                excel_path,
                as_attachment=True,
                download_name=os.path.basename(excel_path),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        else:
            return jsonify(result)

    # POST - Run deduplication
    t_start = time.time()
    try:
        data = request.get_json() or {}

        print(f"\n{'='*70}")
        print(f"[FLASK] /api/deduplication  POST received at {datetime.now().isoformat()}")
        print(f"[FLASK] Step 1/6: Parsing request body...")
        print(f"[FLASK]   rcm_input      = {data.get('rcm_input', '(not provided)')}")
        print(f"[FLASK]   input_is_folder = {data.get('input_is_folder', '(not provided, default False)')}")

        # Import the module
        print(f"[FLASK] Step 2/6: Importing DeDupli module...")
        t_import = time.time()
        import DeDupli
        from importlib import reload
        reload(DeDupli)
        print(f"[FLASK]   Module imported in {time.time()-t_import:.2f}s")

        # Update configuration if provided
        if 'rcm_input' in data:
            DeDupli.RCM_INPUT = data['rcm_input']
        if 'input_is_folder' in data:
            DeDupli.INPUT_IS_FOLDER = data['input_is_folder']

        # Validate input
        print(f"[FLASK] Step 3/6: Validating input file...")
        if not os.path.exists(DeDupli.RCM_INPUT):
            print(f"[FLASK]   ERROR: Input file not found: {DeDupli.RCM_INPUT}")
            return jsonify({"status": "error", "error": f"Input file not found: {DeDupli.RCM_INPUT}"}), 400

        try:
            input_df = pd.read_excel(DeDupli.RCM_INPUT)
            print(f"[FLASK]   Input file: {DeDupli.RCM_INPUT}")
            print(f"[FLASK]   Input rows: {len(input_df)}, columns: {list(input_df.columns)}")
        except Exception as read_err:
            print(f"[FLASK]   WARNING: Could not preview input: {read_err}")

        # Generate unique output files
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if DeDupli.INPUT_IS_FOLDER:
            output_dir = Path(DeDupli.RCM_INPUT)
        else:
            output_dir = Path(DeDupli.RCM_INPUT).parent

        DeDupli.OUTPUT_FOLDER = str(output_dir)
        DeDupli.OUTPUT_EXCEL_NAME = f"Duplicates_{timestamp}"
        DeDupli.OUTPUT_JSON_NAME = f"Duplicates_{timestamp}"
        print(f"[FLASK] Step 4/6: Output configured")
        print(f"[FLASK]   Output folder: {output_dir}")
        print(f"[FLASK]   Output Excel:  Duplicates_{timestamp}.xlsx")
        print(f"[FLASK]   Output JSON:   Duplicates_{timestamp}.json")

        # Run deduplication
        print(f"[FLASK] Step 5/6: Running DeDupli.main() — this calls Azure OpenAI for pair comparison...")
        t_dedup = time.time()
        DeDupli.main()
        dedup_time = time.time() - t_dedup
        print(f"[FLASK]   DeDupli.main() completed in {dedup_time:.2f}s")

        # Check output files
        excel_path = str(output_dir / f"Duplicates_{timestamp}.xlsx")
        json_path = str(output_dir / f"Duplicates_{timestamp}.json")
        excel_exists = os.path.exists(excel_path)
        json_exists = os.path.exists(json_path)
        print(f"[FLASK]   Excel output exists: {excel_exists}")
        print(f"[FLASK]   JSON output exists:  {json_exists}")

        # Read JSON content inline so backend doesn't need filesystem access
        json_inline = None
        if json_exists:
            try:
                with open(json_path, 'r') as jf:
                    json_inline = json.load(jf)
                    json_inline = sanitize_for_json(json_inline)  # Remove NaN/Inf
                    summary = json_inline.get('summary', {})
                    processes = list(json_inline.get('results_by_process', {}).keys())
                    total_pairs = sum(
                        len(pdata.get('pairs', []))
                        for pdata in json_inline.get('results_by_process', {}).values()
                    )
                    print(f"[FLASK]   JSON summary: {summary}")
                    print(f"[FLASK]   Processes found: {processes}")
                    print(f"[FLASK]   Total duplicate pairs: {total_pairs}")
            except Exception as je:
                print(f"[FLASK]   WARNING: Could not read JSON output: {je}")

        # Convert Excel to JSON
        print(f"[FLASK] Step 6/6: Converting Excel to JSON for response...")
        t_convert = time.time()
        excel_data = excel_to_json(excel_path)
        print(f"[FLASK]   Excel converted in {time.time()-t_convert:.2f}s")
        if 'sheets' in excel_data:
            for sheet_name, sheet_info in excel_data['sheets'].items():
                print(f"[FLASK]   Sheet '{sheet_name}': {sheet_info.get('row_count', 0)} rows, columns={sheet_info.get('columns', [])}")

        # Prepare response with full Excel data AND inline JSON
        result = {
            "status": "success",
            "timestamp": timestamp,
            "excel_output": excel_path,
            "json_output": json_path,
            "json_data": json_inline,
            "input_file": DeDupli.RCM_INPUT,
            "download_url": f"/api/deduplication?download=1",
            "excel_data": excel_data
        }

        # Store latest result
        latest_results['deduplication'] = result

        total_time = time.time() - t_start
        total_pairs = 0
        if json_inline and 'results_by_process' in json_inline:
            total_pairs = sum(len(p.get('pairs', [])) for p in json_inline['results_by_process'].values())
        print(f"[FLASK] /api/deduplication RESPONSE SUMMARY:")
        print(f"[FLASK]   Response keys: [{', '.join(result.keys())}]")
        print(f"[FLASK]   .status    = '{result['status']}'")
        print(f"[FLASK]   .json_data = {'PRESENT (' + str(total_pairs) + ' pairs)' if json_inline else 'null'}")
        print(f"[FLASK]   .excel_data= {'PRESENT' if 'sheets' in (excel_data or {}) else 'null/error'}")
        print(f"[FLASK] /api/deduplication COMPLETED in {total_time:.2f}s")
        print(f"{'='*70}\n")

        return jsonify(sanitize_for_json(result))

    except Exception as e:
        total_time = time.time() - t_start
        print(f"[FLASK] /api/deduplication FAILED after {total_time:.2f}s")
        print(f"[FLASK] ERROR: {str(e)}")
        print(f"[FLASK] TRACEBACK:\n{traceback.format_exc()}")
        print(f"{'='*70}\n")
        return jsonify({
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# =============================================================================
# ENDPOINT 4: TOD TEST (RCM TESTER)
# =============================================================================

@app.route('/api/tod-test', methods=['POST', 'GET'])
def tod_test_endpoint():
    """Test of Design (TOD) runner using TOD_Engine."""

    if request.method == 'GET':
        download = request.args.get('download', '0') == '1'

        result = latest_results.get('tod-test')
        if not result:
            return jsonify({"error": "No TOD test has been run yet. Please POST first."}), 404

        if download:
            excel_path = result.get('excel_output')
            if not excel_path or not os.path.exists(excel_path):
                return jsonify({"error": "Excel file not found"}), 404
            return send_file(
                excel_path,
                as_attachment=True,
                download_name=os.path.basename(excel_path),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        else:
            return jsonify(result)

    # POST - Run TOD test
    t_start = time.time()
    try:
        data = request.get_json() or {}

        print(f"\n{'='*70}")
        print(f"[FLASK] /api/tod-test  POST received at {datetime.now().isoformat()}")
        print(f"[FLASK] Step 1/5: Parsing request body...")
        print(f"[FLASK]   rcm_path        = {data.get('rcm_path', '(not provided)')}")
        print(f"[FLASK]   evidence_folder = {data.get('evidence_folder', '(not provided)')}")
        print(f"[FLASK]   company_name    = {data.get('company_name', '(not provided)')}")
        print(f"[FLASK]   prepared_by     = {data.get('prepared_by', '(not provided)')}")
        print(f"[FLASK]   reviewed_by     = {data.get('reviewed_by', '(not provided)')}")

        # Required inputs
        rcm_path = data.get('rcm_path') or data.get('rcm_file_path')
        evidence_folder = data.get('evidence_folder')
        if not rcm_path:
            return jsonify({"status": "error", "error": "Missing required field: rcm_path"}), 400
        if not evidence_folder:
            return jsonify({"status": "error", "error": "Missing required field: evidence_folder"}), 400

        # Optional workpaper header fields
        company_name = data.get('company_name', '')
        prepared_by = data.get('prepared_by', '')
        reviewed_by = data.get('reviewed_by', '')

        if not OPENAI_API_KEY:
            return jsonify({
                "status": "error",
                "error": "Azure/OpenAI API key is not set. Configure AZURE_OPENAI_API_KEY or OPENAI_API_KEY before starting the API."
            }), 400

        # Validate input files
        print(f"[FLASK] Step 2/5: Validating input files...")
        if not os.path.exists(rcm_path):
            print(f"[FLASK]   ERROR: RCM file not found: {rcm_path}")
            return jsonify({"status": "error", "error": f"RCM file not found: {rcm_path}"}), 400
        if not os.path.exists(evidence_folder):
            print(f"[FLASK]   ERROR: Evidence folder not found: {evidence_folder}")
            return jsonify({"status": "error", "error": f"Evidence folder not found: {evidence_folder}"}), 400

        try:
            input_df = pd.read_excel(rcm_path)
            print(f"[FLASK]   RCM file: {rcm_path}")
            print(f"[FLASK]   RCM rows: {len(input_df)}, columns: {list(input_df.columns)}")
        except Exception as read_err:
            print(f"[FLASK]   WARNING: Could not preview RCM input: {read_err}")

        # Import the TOD_Engine module
        print(f"[FLASK] Step 3/5: Importing TOD_Engine module...")
        t_import = time.time()
        import TOD_Engine
        from importlib import reload
        reload(TOD_Engine)
        print(f"[FLASK]   Module imported in {time.time()-t_import:.2f}s")

        # Generate unique output paths
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = Path(rcm_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        output_xlsx = str(output_dir / f"TOD_Results_{timestamp}.xlsx")

        print(f"[FLASK] Step 4/5: Running TOD engine...")
        print(f"[FLASK]   Output Excel: {output_xlsx}")

        # Load evidence and run TOD
        tod_bank = TOD_Engine.load_tod_evidence_folder(evidence_folder)
        tester = TOD_Engine.RCMControlTester(
            rcm_path=rcm_path,
            openai_api_key=OPENAI_API_KEY,
            openai_model=OPENAI_MODEL,
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            azure_api_key=OPENAI_API_KEY,
            azure_deployment=OPENAI_MODEL,
            azure_api_version=AZURE_OPENAI_API_VERSION,
        )

        results, schemas = tester.test_all_tod(tod_bank, max_workers=TOD_MAX_WORKERS)
        tester.export_tod_workpaper(
            results, output_xlsx,
            tod_bank=tod_bank,
            company_name=company_name,
            prepared_by=prepared_by,
            reviewed_by=reviewed_by,
        )

        # Convert Excel to JSON for response
        print(f"[FLASK] Step 5/5: Converting Excel to JSON for response...")
        t_convert = time.time()
        excel_data = excel_to_json(output_xlsx)
        print(f"[FLASK]   Excel converted in {time.time()-t_convert:.2f}s")
        if 'sheets' in excel_data:
            for sheet_name, sheet_info in excel_data['sheets'].items():
                print(f"[FLASK]   Sheet '{sheet_name}': {sheet_info.get('row_count', 0)} rows")

        pass_count = sum(1 for r in results if r.result == "PASS")
        fail_count = sum(1 for r in results if r.result == "FAIL")

        result = {
            "status": "success",
            "timestamp": timestamp,
            "excel_output": output_xlsx,
            "input_file": rcm_path,
            "evidence_folder": evidence_folder,
            "max_workers": TOD_MAX_WORKERS,
            "download_url": "/api/tod-test?download=1",
            "summary": {
                "controls_evaluated": len(results),
                "pass": pass_count,
                "fail": fail_count,
                "schemas_generated": len(schemas),
            },
            "excel_data": excel_data
        }

        latest_results['tod-test'] = result
        # Cache raw TOD objects so the TOE endpoint can filter by TOD-passed controls
        latest_results['_tod_raw_results'] = results
        latest_results['_tod_raw_schemas'] = schemas

        total_time = time.time() - t_start
        print(f"[FLASK] /api/tod-test RESPONSE SUMMARY:")
        print(f"[FLASK]   Controls: {len(results)} | Pass: {pass_count} | Fail: {fail_count}")
        print(f"[FLASK]   Schemas: {len(schemas)}")
        print(f"[FLASK] /api/tod-test COMPLETED in {total_time:.2f}s")
        print(f"{'='*70}\n")

        return jsonify(sanitize_for_json(result))

    except Exception as e:
        total_time = time.time() - t_start
        print(f"[FLASK] /api/tod-test FAILED after {total_time:.2f}s")
        print(f"[FLASK] ERROR: {str(e)}")
        print(f"[FLASK] TRACEBACK:\n{traceback.format_exc()}")
        print(f"{'='*70}\n")
        return jsonify({
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# =============================================================================
# ENDPOINT 5: TOE TEST (TOE ENGINE)
# =============================================================================

@app.route('/api/toe-test', methods=['POST', 'GET'])
def toe_test_endpoint():
    """Test of Operating Effectiveness (TOE) runner for TOE_Engine.py"""

    if request.method == 'GET':
        download = request.args.get('download', '0') == '1'

        result = latest_results.get('toe-test')
        if not result:
            return jsonify({"error": "No TOE test has been run yet. Please POST first."}), 404

        if download:
            excel_path = result.get('excel_output')
            if not excel_path or not os.path.exists(excel_path):
                return jsonify({"error": "Excel file not found"}), 404
            return send_file(
                excel_path,
                as_attachment=True,
                download_name=os.path.basename(excel_path),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        else:
            return jsonify(result)

    # POST - Run TOE test
    t_start = time.time()
    try:
        data = request.get_json() or {}

        print(f"\n{'='*70}")
        print(f"[FLASK] /api/toe-test  POST received at {datetime.now().isoformat()}")
        print(f"[FLASK] Step 1/5: Parsing request body...")
        print(f"[FLASK]   rcm_path        = {data.get('rcm_path', '(not provided)')}")
        print(f"[FLASK]   evidence_folder = {data.get('evidence_folder', '(not provided)')}")
        print(f"[FLASK]   company_name    = {data.get('company_name', '(not provided)')}")
        print(f"[FLASK]   prepared_by     = {data.get('prepared_by', '(not provided)')}")
        print(f"[FLASK]   reviewed_by     = {data.get('reviewed_by', '(not provided)')}")

        # Required inputs
        rcm_path = data.get('rcm_path') or data.get('rcm_file_path')
        evidence_folder = data.get('evidence_folder')
        if not rcm_path:
            return jsonify({"status": "error", "error": "Missing required field: rcm_path"}), 400
        if not evidence_folder:
            return jsonify({"status": "error", "error": "Missing required field: evidence_folder"}), 400

        # Optional workpaper header fields
        company_name = data.get('company_name', '')
        prepared_by = data.get('prepared_by', '')
        reviewed_by = data.get('reviewed_by', '')

        if not OPENAI_API_KEY:
            return jsonify({
                "status": "error",
                "error": "Azure/OpenAI API key is not set. Configure AZURE_OPENAI_API_KEY or OPENAI_API_KEY before starting the API."
            }), 400

        # Validate input files
        print(f"[FLASK] Step 2/5: Validating input files...")
        if not os.path.exists(rcm_path):
            print(f"[FLASK]   ERROR: RCM file not found: {rcm_path}")
            return jsonify({"status": "error", "error": f"RCM file not found: {rcm_path}"}), 400
        if not os.path.exists(evidence_folder):
            print(f"[FLASK]   ERROR: Evidence folder not found: {evidence_folder}")
            return jsonify({"status": "error", "error": f"Evidence folder not found: {evidence_folder}"}), 400

        try:
            input_df = pd.read_excel(rcm_path)
            print(f"[FLASK]   RCM file: {rcm_path}")
            print(f"[FLASK]   RCM rows: {len(input_df)}, columns: {list(input_df.columns)}")
        except Exception as read_err:
            print(f"[FLASK]   WARNING: Could not preview RCM input: {read_err}")

        # Import the TOE_Engine module
        print(f"[FLASK] Step 3/5: Importing TOE_Engine module...")
        t_import = time.time()
        import TOE_Engine
        from importlib import reload
        reload(TOE_Engine)
        print(f"[FLASK]   Module imported in {time.time()-t_import:.2f}s")

        # Generate unique output paths
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = Path(rcm_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        output_xlsx = str(output_dir / f"TOE_Workpaper_{timestamp}.xlsx")

        print(f"[FLASK] Step 4/5: Running TOE engine...")
        print(f"[FLASK]   Output Excel: {output_xlsx}")

        # Load evidence and run TOE
        # Use cached TOD results to only extract evidence for TOD-passed controls
        tod_results = latest_results.get('_tod_raw_results')
        tod_schemas = latest_results.get('_tod_raw_schemas')

        include_control_ids = None
        if tod_results is not None:
            include_control_ids = {
                r.control_id for r in tod_results
                if getattr(r, "result", None) == "PASS"
            }
            print(f"[FLASK]   Filtering TOE evidence to {len(include_control_ids)} TOD-passed controls")
        else:
            print(f"[FLASK]   No cached TOD results found — extracting evidence for all controls")

        toe_bank = TOE_Engine.load_toe_evidence_folder(
            evidence_folder,
            include_control_ids=include_control_ids,
        )
        tester = TOE_Engine.RCMControlTester(
            rcm_path=rcm_path,
            openai_api_key=OPENAI_API_KEY,
            openai_model=OPENAI_MODEL,
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            azure_api_key=OPENAI_API_KEY,
            azure_deployment=OPENAI_MODEL,
            azure_api_version=AZURE_OPENAI_API_VERSION,
        )

        results = tester.test_all_toe(
            toe_bank, max_workers=TOE_MAX_WORKERS,
            pre_schemas=tod_schemas,
            tod_results=tod_results,
        )
        tester.export_toe_workpaper(
            results, output_xlsx,
            toe_bank=toe_bank,
            company_name=company_name,
            prepared_by=prepared_by,
            reviewed_by=reviewed_by,
        )

        # Convert Excel to JSON for response
        print(f"[FLASK] Step 5/5: Converting Excel to JSON for response...")
        t_convert = time.time()
        excel_data = excel_to_json(output_xlsx)
        print(f"[FLASK]   Excel converted in {time.time()-t_convert:.2f}s")
        if 'sheets' in excel_data:
            for sheet_name, sheet_info in excel_data['sheets'].items():
                print(f"[FLASK]   Sheet '{sheet_name}': {sheet_info.get('row_count', 0)} rows")

        # Build summary counts
        effective = sum(1 for r in results if r.operating_effectiveness == "Effective")
        exceptions = sum(1 for r in results if r.operating_effectiveness == "Effective with Exceptions")
        not_effective = sum(1 for r in results if r.operating_effectiveness == "Not Effective")
        total_samples = sum(r.total_samples for r in results)
        total_failed = sum(r.failed_samples for r in results)

        result = {
            "status": "success",
            "timestamp": timestamp,
            "excel_output": output_xlsx,
            "input_file": rcm_path,
            "evidence_folder": evidence_folder,
            "max_workers": TOE_MAX_WORKERS,
            "download_url": "/api/toe-test?download=1",
            "summary": {
                "controls_evaluated": len(results),
                "effective": effective,
                "effective_with_exceptions": exceptions,
                "not_effective": not_effective,
                "total_samples": total_samples,
                "total_failed_samples": total_failed,
            },
            "excel_data": excel_data
        }

        latest_results['toe-test'] = result

        total_time = time.time() - t_start
        print(f"[FLASK] /api/toe-test RESPONSE SUMMARY:")
        print(f"[FLASK]   Controls: {len(results)} | Samples: {total_samples} | Failed: {total_failed}")
        print(f"[FLASK]   Effective: {effective} | Exceptions: {exceptions} | Not Effective: {not_effective}")
        print(f"[FLASK] /api/toe-test COMPLETED in {total_time:.2f}s")
        print(f"{'='*70}\n")

        return jsonify(sanitize_for_json(result))

    except Exception as e:
        total_time = time.time() - t_start
        print(f"[FLASK] /api/toe-test FAILED after {total_time:.2f}s")
        print(f"[FLASK] ERROR: {str(e)}")
        print(f"[FLASK] TRACEBACK:\n{traceback.format_exc()}")
        print(f"{'='*70}\n")
        return jsonify({
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


# =============================================================================
# UTILITY ENDPOINTS
# =============================================================================

@app.route('/api/health', methods=['GET'])
def health():
    """Health check"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    })


@app.route('/', methods=['GET'])
def index():
    """API documentation"""
    return jsonify({
        "name": "RCM Analysis API",
        "version": "1.0.0",
        "endpoints": {
            "/": "API documentation (this page)",
            "/api/health": "Health check",
            "/api/ai-suggest": {
                "POST": "Run AI gap analysis (body: {rcm_file_path, industry})",
                "GET": "Get latest JSON results",
                "GET?download=1": "Download latest Excel file"
            },
            "/api/control-assessment": {
                "POST": "Run control assessment (body: {rcm_file_path, policy_paths, sop_paths})",
                "GET": "Get latest JSON results",
                "GET?download=1": "Download latest Excel file"
            },
            "/api/deduplication": {
                "POST": "Run deduplication (body: {rcm_input, input_is_folder})",
                "GET": "Get latest JSON results",
                "GET?download=1": "Download latest Excel file"
            },
            "/api/tod-test": {
                "POST": "Run TOD test (body: {rcm_path, evidence_folder})",
                "GET": "Get latest JSON results",
                "GET?download=1": "Download latest Excel file"
            },
            "/api/toe-test": {
                "POST": "Run TOE test (body: {rcm_path, evidence_folder, company_name, prepared_by, reviewed_by})",
                "GET": "Get latest JSON results",
                "GET?download=1": "Download latest Excel workpaper"
            }
        },
        "examples": {
            "curl_post": "curl -X POST http://localhost:5000/api/ai-suggest -H 'Content-Type: application/json' -d '{}'",
            "curl_get_json": "curl http://localhost:5000/api/ai-suggest",
            "curl_download": "curl -O -J http://localhost:5000/api/ai-suggest?download=1"
        }
    })


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    print("\n" + "="*80)
    print(" RCM ANALYSIS API SERVER")
    print("="*80)
    print("\n📡 Server starting at: http://localhost:5002")
    print("\n📚 Available Endpoints:")
    print("   - GET  http://localhost:5002/              (Documentation)")
    print("   - GET  http://localhost:5002/api/health    (Health Check)")
    print("\n🔬 Analysis Endpoints:")
    print("   - POST http://localhost:5002/api/ai-suggest")
    print("   - GET  http://localhost:5002/api/ai-suggest")
    print("   - GET  http://localhost:5002/api/ai-suggest?download=1")
    print()
    print("   - POST http://localhost:5002/api/control-assessment")
    print("   - GET  http://localhost:5002/api/control-assessment")
    print("   - GET  http://localhost:5002/api/control-assessment?download=1")
    print()
    print("   - POST http://localhost:5002/api/deduplication")
    print("   - GET  http://localhost:5002/api/deduplication")
    print("   - GET  http://localhost:5002/api/deduplication?download=1")
    print()
    print("   - POST http://localhost:5002/api/tod-test")
    print("   - GET  http://localhost:5002/api/tod-test")
    print("   - GET  http://localhost:5002/api/tod-test?download=1")
    print()
    print("   - POST http://localhost:5002/api/toe-test")
    print("   - GET  http://localhost:5002/api/toe-test")
    print("   - GET  http://localhost:5002/api/toe-test?download=1")
    print("\n" + "="*80 + "\n")

    app.run(
        host='0.0.0.0',
        port=5002,
        debug=True,
        threaded=True
    )
