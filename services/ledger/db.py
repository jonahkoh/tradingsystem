"""
Shared SQLite helpers — fills persistence.

Schema
  fills — one row per successful order fill, storing enough state to restore
  the ledger's in-memory position on restart without replaying every message.
"""

import os
import sqlite3
from contextlib import contextmanager

DB_PATH = os.getenv("DB_FILE", "/data/trades.db")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def _cursor():
    conn = _connect()
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    """Create the fills table if it doesn't already exist."""
    with _cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fills (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp    INTEGER NOT NULL,
                side         TEXT    NOT NULL,
                price        REAL    NOT NULL,
                qty          REAL    NOT NULL,
                position     REAL    NOT NULL,
                avg_entry    REAL    NOT NULL,
                realized_pnl REAL    NOT NULL
            )
        """)


def insert_fill(
    timestamp: int,
    side: str,
    price: float,
    qty: float,
    position: float,
    avg_entry: float,
    realized_pnl: float,
) -> None:
    """Persist one fill. Called by the ledger after every successful order."""
    with _cursor() as cur:
        cur.execute(
            """
            INSERT INTO fills (timestamp, side, price, qty, position, avg_entry, realized_pnl)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (timestamp, side, price, qty, position, avg_entry, realized_pnl),
        )


def load_last_state() -> dict | None:
    """
    Return the most recent fill row, or None if no fills exist.
    Used by the ledger on startup to restore position state after a restart.
    """
    with _cursor() as cur:
        cur.execute("SELECT * FROM fills ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
    return dict(row) if row else None


def get_trades(limit: int = 100) -> list[dict]:
    """Return the most recent fills, newest first."""
    with _cursor() as cur:
        cur.execute(
            "SELECT * FROM fills ORDER BY id DESC LIMIT ?", (limit,)
        )
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def get_pnl_summary() -> dict:
    """
    Return PnL stats derived from the fills table.
    position and avg_entry come from the last fill row (current live state).
    realized_pnl is the cumulative value stored on the last row.
    """
    with _cursor() as cur:
        cur.execute("SELECT COUNT(*) as count FROM fills")
        total = cur.fetchone()["count"]

        cur.execute("SELECT * FROM fills ORDER BY id DESC LIMIT 1")
        last = cur.fetchone()

    if last is None:
        return {
            "realized_pnl": 0.0,
            "position":     0.0,
            "avg_entry":    0.0,
            "total_fills":  0,
        }

    last = dict(last)
    return {
        "realized_pnl": round(last["realized_pnl"], 4),
        "position":     round(last["position"], 6),
        "avg_entry":    round(last["avg_entry"], 2),
        "total_fills":  total,
    }
