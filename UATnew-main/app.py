"""Flask entry point for the UAT Automation backend."""
import os
import threading
from flask import Flask, request, jsonify, send_from_directory, abort
from flask_cors import CORS
from werkzeug.utils import secure_filename

from models import db
from runner.excel_parser import parse_excel, ExcelParseError
from runner.playwright_runner import run_test_cases

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
SCREENSHOT_DIR = os.path.join(BASE_DIR, "screenshots")
ALLOWED_EXTENSIONS = {".xlsx", ".xls"}

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

app = Flask(__name__)
CORS(app)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MB

db.init_db()


def _allowed_file(filename: str) -> bool:
    return os.path.splitext(filename)[1].lower() in ALLOWED_EXTENSIONS


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return jsonify({"error": "No file part in request"}), 400

    file = request.files["file"]
    if not file or file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    if not _allowed_file(file.filename):
        return jsonify({"error": "Only .xlsx or .xls files are allowed"}), 400

    filename = secure_filename(file.filename)
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
            "filename": filename,
            "steps_count": len(steps),
            "status": "pending",
        }
    )


@app.route("/run/<int:test_run_id>", methods=["POST"])
def run(test_run_id):
    run_row = db.get_test_run(test_run_id)
    if not run_row:
        return jsonify({"error": "test_run_id not found"}), 404

    if run_row["status"] == "running":
        return jsonify({"error": "Test run is already in progress"}), 409

    headless_param = request.args.get("headless", "false").lower() == "true"

    thread = threading.Thread(
        target=run_test_cases,
        args=(test_run_id,),
        kwargs={"headless": headless_param},
        daemon=True,
    )
    thread.start()

    return jsonify({"test_run_id": test_run_id, "status": "running"})


@app.route("/status/<int:test_run_id>", methods=["GET"])
def status(test_run_id):
    run_row = db.get_test_run(test_run_id)
    if not run_row:
        return jsonify({"error": "test_run_id not found"}), 404

    steps = db.get_test_steps(test_run_id)
    results = db.get_test_results(test_run_id)

    return jsonify(
        {
            "test_run_id": test_run_id,
            "status": run_row["status"],
            "created_at": run_row["created_at"],
            "started_at": run_row["started_at"],
            "finished_at": run_row["finished_at"],
            "total_steps": len(steps),
            "completed_steps": len(results),
            "passed": sum(1 for r in results if r["status"] == "passed"),
            "failed": sum(1 for r in results if r["status"] == "failed"),
        }
    )


@app.route("/results/<int:test_run_id>", methods=["GET"])
def results(test_run_id):
    run_row = db.get_test_run(test_run_id)
    if not run_row:
        return jsonify({"error": "test_run_id not found"}), 404

    steps = db.get_test_steps(test_run_id)
    results_rows = db.get_test_results(test_run_id)

    # Index results by (test_case_id, step_id) so we can merge with step metadata
    result_index = {}
    for r in results_rows:
        result_index[(r["test_case_id"], r["step_id"])] = r

    detailed = []
    for s in steps:
        r = result_index.get((s["test_case_id"], s["step_id"]))
        detailed.append(
            {
                "test_case_id": s["test_case_id"],
                "step_id": s["step_id"],
                "action": s["action"],
                "selector": s["selector"],
                "input_value": s["input_value"],
                "expected_result": s["expected_result"],
                "status": r["status"] if r else "pending",
                "error_message": r["error_message"] if r else None,
                "screenshot_path": r["screenshot_path"] if r else None,
                "timestamp": r["timestamp"] if r else None,
            }
        )

    return jsonify(
        {
            "test_run_id": test_run_id,
            "status": run_row["status"],
            "filename": run_row["filename"],
            "results": detailed,
        }
    )


@app.route("/screenshot/<path:filename>", methods=["GET"])
def screenshot(filename):
    # Prevent directory traversal
    safe_name = os.path.basename(filename)
    full_path = os.path.join(SCREENSHOT_DIR, safe_name)
    if not os.path.isfile(full_path):
        abort(404)
    return send_from_directory(SCREENSHOT_DIR, safe_name)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
