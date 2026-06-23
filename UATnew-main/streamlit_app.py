"""Simple Streamlit frontend for the UAT Automation backend.

Run:
    streamlit run streamlit_app.py

Backend must be running at BACKEND_URL (default: http://localhost:5000).
"""

import time
import requests
import streamlit as st

BACKEND_URL = "http://localhost:5000"

st.set_page_config(page_title="UAT Automation", layout="wide")
st.title("UAT Automation")

# --- Session state ---
if "test_run_id" not in st.session_state:
    st.session_state.test_run_id = None
if "running" not in st.session_state:
    st.session_state.running = False

# --- Sidebar: upload & controls ---
with st.sidebar:
    st.header("Upload & Run")
    uploaded = st.file_uploader("Test cases Excel", type=["xlsx", "xls"])
    headless = st.checkbox("Headless browser", value=True)

    if st.button("Upload", disabled=uploaded is None):
        files = {"file": (uploaded.name, uploaded.getvalue())}
        r = requests.post(f"{BACKEND_URL}/upload", files=files)
        if r.ok:
            data = r.json()
            st.session_state.test_run_id = data["test_run_id"]
            st.session_state.running = False
            st.success(f"Uploaded. test_run_id = {data['test_run_id']} ({data['steps_count']} steps)")
        else:
            st.error(r.json().get("error", "Upload failed"))

    if st.button("Run", disabled=st.session_state.test_run_id is None):
        rid = st.session_state.test_run_id
        r = requests.post(f"{BACKEND_URL}/run/{rid}?headless={'true' if headless else 'false'}")
        if r.ok:
            st.session_state.running = True
        else:
            st.error(r.json().get("error", "Run failed"))

    st.caption(f"Backend: {BACKEND_URL}")

# --- Main: live status & step-by-step results ---
rid = st.session_state.test_run_id

if rid is None:
    st.info("Upload an Excel file to begin.")
    st.stop()

status_placeholder = st.empty()
progress_placeholder = st.empty()
results_placeholder = st.empty()


def render(status_data, results_data):
    with status_placeholder.container():
        cols = st.columns(5)
        cols[0].metric("Status", status_data["status"])
        cols[1].metric("Total", status_data["total_steps"])
        cols[2].metric("Done", status_data["completed_steps"])
        cols[3].metric("Passed", status_data["passed"])
        cols[4].metric("Failed", status_data["failed"])

    total = status_data["total_steps"] or 1
    progress_placeholder.progress(min(status_data["completed_steps"] / total, 1.0))

    with results_placeholder.container():
        for step in results_data.get("results", []):
            icon = {"passed": "✅", "failed": "❌", "pending": "⏳"}.get(step["status"], "·")
            header = f"{icon} {step['test_case_id']} / {step['step_id']} — {step['action']}"
            with st.expander(header, expanded=(step["status"] == "failed")):
                left, right = st.columns([1, 2])
                with left:
                    st.write(f"**Selector:** {step.get('selector') or '—'}")
                    st.write(f"**Input:** {step.get('input_value') or '—'}")
                    st.write(f"**Expected:** {step.get('expected_result') or '—'}")
                    st.write(f"**Status:** {step['status']}")
                    if step.get("error_message"):
                        st.error(step["error_message"])
                    if step.get("timestamp"):
                        st.caption(step["timestamp"])
                with right:
                    shot = step.get("screenshot_path")
                    if shot:
                        st.image(
                            f"{BACKEND_URL}/screenshot/{shot}",
                            use_container_width=True,
                        )
                    else:
                        st.caption("No screenshot yet")


# Poll loop while running; otherwise render once
while True:
    try:
        status_data = requests.get(f"{BACKEND_URL}/status/{rid}").json()
        results_data = requests.get(f"{BACKEND_URL}/results/{rid}").json()
    except Exception as e:
        st.error(f"Backend unreachable: {e}")
        break

    render(status_data, results_data)

    if status_data["status"] in ("completed", "failed") or not st.session_state.running:
        break

    time.sleep(1)
