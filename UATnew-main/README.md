# UAT Automation Backend

Flask backend for a UAT (User Acceptance Testing) automation tool. Users upload an Excel file containing test cases; the backend parses it, queues the steps, and executes them on a real browser via Playwright. Results — pass/fail, error messages, and post-step screenshots — are stored in SQLite and exposed through a REST API.

## Tech Stack
- **Python 3.10+** / **Flask** — REST API
- **Playwright** — browser automation (headed by default)
- **pandas** + **openpyxl** — Excel parsing
- **SQLite** — persistence (no external DB needed)
- **Flask-CORS** — frontend (React) can connect directly

## Project Structure
```
uat-automation-backend/
├── app.py                        # Flask entry point & REST routes
├── runner/
│   ├── playwright_runner.py      # Core Playwright execution logic
│   └── excel_parser.py           # Parse & validate Excel uploads
├── models/
│   └── db.py                     # SQLite setup and data access
├── uploads/                      # Uploaded Excel files (runtime)
├── screenshots/                  # Per-step screenshots (runtime)
├── sample/
│   ├── generate_sample.py        # Script to regenerate the sample Excel
│   └── sample_test_cases.xlsx    # Ready-to-use sample file
├── requirements.txt
└── README.md
```

## Installation

```bash
# 1. Create & activate a virtualenv (recommended)
python3 -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Install the Playwright browsers (one-time)
playwright install chromium
```

## Running the server

```bash
python app.py
```

The API will be available at `http://localhost:5000`.

## Excel Schema

Your uploaded `.xlsx` file must contain these columns (header row, case-insensitive):

| test_case_id | step_id | action | selector | input_value | expected_result |
|--------------|---------|--------|----------|-------------|-----------------|

### Supported actions

| Action | `selector` | `input_value` | `expected_result` | Notes |
|---|---|---|---|---|
| `navigate` | — | URL | — | Opens the URL; detects login redirects |
| `click` | required | — | — | Clicks the element |
| `type` | required | text to type | — | Fills an input |
| `assert_text` | required | (optional) | expected substring | Checks text contains value |
| `assert_visible` | required | — | — | Waits for element to be visible |
| `wait` | — | seconds (<100) or ms | — | Pauses execution |
| `select_dropdown` | required | option value | — | Selects a `<select>` option |
| `hover` | required | — | — | Hovers the element |

See [`sample/sample_test_cases.xlsx`](sample/sample_test_cases.xlsx) for a ready-to-use example. Regenerate it anytime via:

```bash
python sample/generate_sample.py
```

## API Endpoints

### `POST /upload`
Upload an Excel file. Returns a `test_run_id`.

```bash
curl -F "file=@sample/sample_test_cases.xlsx" http://localhost:5000/upload
```

**Response**
```json
{
  "test_run_id": 1,
  "filename": "sample_test_cases.xlsx",
  "steps_count": 7,
  "status": "pending"
}
```

### `POST /run/<test_run_id>`
Triggers Playwright execution in a background thread. By default the browser launches in **headed** mode (some BSS portals block headless). Pass `?headless=true` to override.

```bash
curl -X POST http://localhost:5000/run/1
```

### `GET /status/<test_run_id>`
Live status + progress counters.

```json
{
  "test_run_id": 1,
  "status": "running",
  "total_steps": 7,
  "completed_steps": 3,
  "passed": 2,
  "failed": 1
}
```

Possible `status` values: `pending`, `running`, `completed`, `failed`.

### `GET /results/<test_run_id>`
Full per-step results, merged with the original step metadata.

```json
{
  "test_run_id": 1,
  "status": "completed",
  "filename": "sample_test_cases.xlsx",
  "results": [
    {
      "test_case_id": "TC001",
      "step_id": "S1",
      "action": "navigate",
      "selector": null,
      "input_value": "https://example.com",
      "expected_result": "Page loads",
      "status": "passed",
      "error_message": null,
      "screenshot_path": "run1_TC001_S1_20260411120000123456.png",
      "timestamp": "2026-04-11T12:00:00.123456"
    }
  ]
}
```

### `GET /screenshot/<filename>`
Serves a PNG captured after a step. Use the `screenshot_path` value from `/results/...`.

```bash
curl -o step.png http://localhost:5000/screenshot/run1_TC001_S1_20260411120000123456.png
```

### `GET /health`
Simple health check.

## Implementation Notes

- **Non-aborting runs** — If a step fails, its failure is recorded and execution continues with the next step. One bad assertion does not kill the whole run.
- **Screenshot every step** — A screenshot is captured after *every* step (pass or fail) and its filename is stored in `test_results.screenshot_path`.
- **Headed by default** — `playwright_runner.run_test_cases` calls `chromium.launch(headless=False)`. Override per-run with `POST /run/<id>?headless=true`.
- **Login redirect detection** — If a `navigate` step ends up on a URL containing `login`/`signin`/`auth`/`sso` (and the target didn't), the step fails with a clear "Login redirect detected" message so you can refresh your session.
- **Default 10s timeout** — Every Playwright action uses a 10-second default timeout (navigate uses 30s). Timeouts are reported as `Timeout: ...` in `error_message`.
- **Dialog auto-dismiss** — Unexpected `alert`/`confirm`/`prompt` dialogs are auto-dismissed so they don't freeze the runner.
- **SQLite schema** — Three tables: `test_runs`, `test_steps`, `test_results`. The DB file (`uat.db`) is created on first run.

## End-to-end quick test

```bash
# 1. Start the server
python app.py

# 2. In another shell — upload the sample
curl -F "file=@sample/sample_test_cases.xlsx" http://localhost:5000/upload
# => {"test_run_id": 1, ...}

# 3. Trigger execution
curl -X POST http://localhost:5000/run/1

# 4. Poll status
curl http://localhost:5000/status/1

# 5. Get full results
curl http://localhost:5000/results/1
```

## CORS

`Flask-CORS` is enabled for all origins so a React frontend on `localhost:3000` (or anywhere else) can call these endpoints directly during development. Lock down origins in `app.py` before deploying.
