"""Flask Blueprint for UAT Automation endpoints.

Mirrors every route in the original uat-automation-backend/app.py and adds
a /runs list endpoint for the React frontend.

Registered at: /api/uat/automation
"""
import os
import threading
import logging
from flask import Blueprint, request, jsonify, send_from_directory, abort
from werkzeug.utils import secure_filename

from uat_automation import db
from uat_automation.excel_parser import parse_excel, ExcelParseError
from uat_automation.playwright_runner import run_test_cases

logger = logging.getLogger(__name__)

uat_automation_bp = Blueprint("uat_automation", __name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UPLOAD_DIR    = os.path.join(BASE_DIR, "uat_uploads")
SCREENSHOT_DIR = os.path.join(BASE_DIR, "uat_screenshots")
ALLOWED_EXTENSIONS = {".xlsx", ".xls"}

os.makedirs(UPLOAD_DIR,     exist_ok=True)
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

# Initialise SQLite schema on import
db.init_db()


def _allowed_file(filename: str) -> bool:
    return os.path.splitext(filename)[1].lower() in ALLOWED_EXTENSIONS


# ── Health ─────────────────────────────────────────────────────────────────────

@uat_automation_bp.route("/health", methods=["GET"])
def health():
    """Simple liveness check — mirrors GET /health in the original backend."""
    return jsonify({"status": "ok"})


# ── Run list (extra endpoint for the React frontend) ──────────────────────────

@uat_automation_bp.route("/runs", methods=["GET"])
def list_runs():
    """Return all test runs newest-first, enriched with step/result counts."""
    try:
        return jsonify(db.get_all_test_runs())
    except Exception as e:
        logger.error(f"list_runs error: {e}")
        return jsonify({"error": str(e)}), 500


# ── Upload ────────────────────────────────────────────────────────────────────

@uat_automation_bp.route("/upload", methods=["POST"])
def upload():
    """Upload an Excel file, validate it, and create a pending test run.

    Mirrors POST /upload from the original backend.
    """
    if "file" not in request.files:
        return jsonify({"error": "No file part in request"}), 400

    file = request.files["file"]
    if not file or file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    if not _allowed_file(file.filename):
        return jsonify({"error": "Only .xlsx or .xls files are allowed"}), 400

    filename  = secure_filename(file.filename)
    save_path = os.path.join(UPLOAD_DIR, filename)
    file.save(save_path)

    try:
        steps = parse_excel(save_path)
    except ExcelParseError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Failed to parse Excel: {e}"}), 500

    test_run_id = db.create_test_run(filename)
    db.insert_test_steps(test_run_id, steps)

    return jsonify(
        {
            "test_run_id": test_run_id,
            "filename":    filename,
            "steps_count": len(steps),
            "status":      "pending",
        }
    )


# ── Trigger execution ─────────────────────────────────────────────────────────

@uat_automation_bp.route("/run/<int:test_run_id>", methods=["POST"])
def run(test_run_id):
    """Kick off Playwright execution in a background thread.

    Mirrors POST /run/<test_run_id> from the original backend.

    Query params:
      headless — "true" | "false"  (default "false"; headed mode avoids
                  BSS-portal detection that blocks headless browsers)
    """
    run_row = db.get_test_run(test_run_id)
    if not run_row:
        return jsonify({"error": "test_run_id not found"}), 404

    if run_row["status"] == "running":
        return jsonify({"error": "Test run is already in progress"}), 409

    # Match original default: headless=False (headed browser)
    headless_param = request.args.get("headless", "false").lower() == "true"

    # Clear any results from a previous execution of the same run so that
    # counts and the results table stay accurate on re-runs.
    db.delete_results_for_run(test_run_id)

    thread = threading.Thread(
        target=run_test_cases,
        args=(test_run_id,),
        kwargs={"headless": headless_param},
        daemon=True,
    )
    thread.start()

    return jsonify({"test_run_id": test_run_id, "status": "running"})


# ── Status (live polling) ─────────────────────────────────────────────────────

@uat_automation_bp.route("/status/<int:test_run_id>", methods=["GET"])
def status(test_run_id):
    """Return live progress counters for a test run.

    Mirrors GET /status/<test_run_id> from the original backend.
    """
    run_row = db.get_test_run(test_run_id)
    if not run_row:
        return jsonify({"error": "test_run_id not found"}), 404

    steps   = db.get_test_steps(test_run_id)
    results = db.get_test_results(test_run_id)

    return jsonify(
        {
            "test_run_id":     test_run_id,
            "status":          run_row["status"],
            "filename":        run_row["filename"],
            "created_at":      run_row["created_at"],
            "started_at":      run_row["started_at"],
            "finished_at":     run_row["finished_at"],
            "total_steps":     len(steps),
            "completed_steps": len(results),
            "passed":  sum(1 for r in results if r["status"] == "passed"),
            "failed":  sum(1 for r in results if r["status"] == "failed"),
        }
    )


# ── Full results ──────────────────────────────────────────────────────────────

@uat_automation_bp.route("/results/<int:test_run_id>", methods=["GET"])
def results(test_run_id):
    """Return full per-step results merged with step metadata.

    Steps not yet executed are shown with status = "pending".
    Mirrors GET /results/<test_run_id> from the original backend.
    """
    run_row = db.get_test_run(test_run_id)
    if not run_row:
        return jsonify({"error": "test_run_id not found"}), 404

    steps       = db.get_test_steps(test_run_id)
    result_rows = db.get_test_results(test_run_id)

    # Index results by (test_case_id, step_id) so we can merge with step metadata.
    # If the same step has multiple rows (shouldn't happen after the clear-on-rerun
    # fix, but belt-and-braces), the latest row wins because results are ordered
    # by id ASC and dict assignment overwrites.
    result_index = {}
    for r in result_rows:
        result_index[(r["test_case_id"], r["step_id"])] = r

    detailed = []
    for s in steps:
        r = result_index.get((s["test_case_id"], s["step_id"]))
        detailed.append(
            {
                "test_case_id":   s["test_case_id"],
                "step_id":        s["step_id"],
                "action":         s["action"],
                "selector":       s["selector"],
                "input_value":    s["input_value"],
                "expected_result":s["expected_result"],
                "status":         r["status"]          if r else "pending",
                "error_message":  r["error_message"]   if r else None,
                "screenshot_path":r["screenshot_path"] if r else None,
                "timestamp":      r["timestamp"]       if r else None,
            }
        )

    return jsonify(
        {
            "test_run_id": test_run_id,
            "status":      run_row["status"],
            "filename":    run_row["filename"],
            "results":     detailed,
        }
    )


# ── Screenshot serving ────────────────────────────────────────────────────────

@uat_automation_bp.route("/screenshot/<path:filename>", methods=["GET"])
def screenshot(filename):
    """Serve a screenshot PNG by filename.

    Mirrors GET /screenshot/<filename> from the original backend.
    Uses os.path.basename to prevent directory-traversal attacks.
    """
    safe_name = os.path.basename(filename)
    full_path  = os.path.join(SCREENSHOT_DIR, safe_name)
    if not os.path.isfile(full_path):
        abort(404)
    return send_from_directory(SCREENSHOT_DIR, safe_name)
