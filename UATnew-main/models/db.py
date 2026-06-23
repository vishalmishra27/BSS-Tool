"""SQLite database setup and helper functions for UAT Automation."""
import sqlite3
import os
from datetime import datetime
from contextlib import contextmanager

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "uat.db")


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    """Create tables if they don't exist."""
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS test_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS test_steps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                test_run_id INTEGER NOT NULL,
                test_case_id TEXT NOT NULL,
                step_id TEXT NOT NULL,
                action TEXT NOT NULL,
                selector TEXT,
                input_value TEXT,
                expected_result TEXT,
                step_order INTEGER NOT NULL,
                FOREIGN KEY (test_run_id) REFERENCES test_runs(id)
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS test_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                test_run_id INTEGER NOT NULL,
                test_case_id TEXT NOT NULL,
                step_id TEXT NOT NULL,
                status TEXT NOT NULL,
                error_message TEXT,
                screenshot_path TEXT,
                timestamp TEXT NOT NULL,
                FOREIGN KEY (test_run_id) REFERENCES test_runs(id)
            )
        """)


def create_test_run(filename: str) -> int:
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            "INSERT INTO test_runs (filename, status, created_at) VALUES (?, ?, ?)",
            (filename, "pending", datetime.utcnow().isoformat()),
        )
        return c.lastrowid


def insert_test_steps(test_run_id: int, steps: list):
    """steps is a list of dicts with keys matching the Excel schema."""
    with get_conn() as conn:
        c = conn.cursor()
        for idx, s in enumerate(steps):
            c.execute(
                """INSERT INTO test_steps
                   (test_run_id, test_case_id, step_id, action, selector,
                    input_value, expected_result, step_order)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    test_run_id,
                    str(s.get("test_case_id", "")),
                    str(s.get("step_id", "")),
                    str(s.get("action", "")),
                    s.get("selector"),
                    s.get("input_value"),
                    s.get("expected_result"),
                    idx,
                ),
            )


def get_test_run(test_run_id: int):
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM test_runs WHERE id = ?", (test_run_id,))
        row = c.fetchone()
        return dict(row) if row else None


def get_test_steps(test_run_id: int):
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT * FROM test_steps WHERE test_run_id = ? ORDER BY step_order ASC",
            (test_run_id,),
        )
        return [dict(r) for r in c.fetchall()]


def get_test_results(test_run_id: int):
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT * FROM test_results WHERE test_run_id = ? ORDER BY id ASC",
            (test_run_id,),
        )
        return [dict(r) for r in c.fetchall()]


def update_run_status(test_run_id: int, status: str):
    with get_conn() as conn:
        c = conn.cursor()
        now = datetime.utcnow().isoformat()
        if status == "running":
            c.execute(
                "UPDATE test_runs SET status = ?, started_at = ? WHERE id = ?",
                (status, now, test_run_id),
            )
        elif status in ("completed", "failed"):
            c.execute(
                "UPDATE test_runs SET status = ?, finished_at = ? WHERE id = ?",
                (status, now, test_run_id),
            )
        else:
            c.execute(
                "UPDATE test_runs SET status = ? WHERE id = ?",
                (status, test_run_id),
            )


def insert_result(
    test_run_id: int,
    test_case_id: str,
    step_id: str,
    status: str,
    error_message: str = None,
    screenshot_path: str = None,
):
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            """INSERT INTO test_results
               (test_run_id, test_case_id, step_id, status, error_message,
                screenshot_path, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                test_run_id,
                test_case_id,
                step_id,
                status,
                error_message,
                screenshot_path,
                datetime.utcnow().isoformat(),
            ),
        )
