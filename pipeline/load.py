import logging
import sqlite3
from pathlib import Path
from typing import Iterable, Tuple

import pandas as pd

log = logging.getLogger(__name__)


CREATE_FACT = """
CREATE TABLE IF NOT EXISTS fact_events (
  event_id TEXT PRIMARY KEY,
  ts TEXT NOT NULL,
  user_id INTEGER,
  event TEXT NOT NULL,
  page TEXT,
  amount REAL,
  event_date TEXT,
  event_hour INTEGER,
  country TEXT,
  signup_source TEXT
);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_schema(conn: sqlite3.Connection, schema_path: Path) -> None:
    sql = schema_path.read_text(encoding="utf-8")
    conn.executescript(sql)
    conn.commit()


def upsert_dim_users(conn: sqlite3.Connection, users: pd.DataFrame) -> int:
    if users.empty:
        return 0
    rows = users[["user_id", "country", "signup_source"]].drop_duplicates().copy()
    rows["user_id"] = pd.to_numeric(rows["user_id"], errors="coerce").astype("Int64")

    payload = [
        (int(r.user_id), None if pd.isna(r.country) else str(r.country), None if pd.isna(r.signup_source) else str(r.signup_source))
        for r in rows.itertuples(index=False)
        if pd.notna(r.user_id)
    ]

    conn.executemany(
        """
        INSERT INTO dim_users(user_id, country, signup_source)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
          country=excluded.country,
          signup_source=excluded.signup_source;
        """,
        payload,
    )
    conn.commit()
    return len(payload)


def upsert_dim_event_types(conn: sqlite3.Connection, cleaned: pd.DataFrame) -> None:
    # Insert unique event strings
    if cleaned.empty:
        return
    unique_events = sorted(set(cleaned["event"].dropna().astype(str).tolist()))
    conn.executemany(
        "INSERT OR IGNORE INTO dim_event_types(event) VALUES (?);",
        [(e,) for e in unique_events],
    )
    conn.commit()


def upsert_dim_dates(conn: sqlite3.Connection, cleaned: pd.DataFrame) -> None:
    if cleaned.empty:
        return
    dates = sorted(set(cleaned["event_date"].dropna().astype(str).tolist()))
    payload = []
    for d in dates:
        # d is 'YYYY-MM-DD'
        try:
            y, m, day = d.split("-")
            payload.append((d, int(y), int(m), int(day)))
        except Exception:
            continue

    conn.executemany(
        """
        INSERT OR IGNORE INTO dim_dates(date_key, year, month, day)
        VALUES (?, ?, ?, ?);
        """,
        payload,
    )
    conn.commit()


def event_type_id_map(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute("SELECT event, event_type_id FROM dim_event_types;").fetchall()
    return {str(e): int(i) for (e, i) in rows}

def to_rows(df: pd.DataFrame) -> Iterable[Tuple]:
    # Ensure required columns exist
    cols = [
        "event_id",
        "ts",
        "user_id",
        "event",
        "page",
        "amount",
        "event_date",
        "event_hour",
        "country",
        "signup_source",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    # SQLite prefers Python primitives; convert pandas NA/NaN to None and stringify ts
    df = df.copy()
    df["ts"] = df["ts"].astype(str)
    frame = df[cols].astype(object).where(pd.notna(df[cols]), None)
    return (tuple(row) for row in frame.itertuples(index=False, name=None))


def upsert_fact_events(conn: sqlite3.Connection, cleaned: pd.DataFrame) -> int:
    if cleaned.empty:
        return 0

    # Ensure dim tables are ready for fact load
    upsert_dim_event_types(conn, cleaned)
    upsert_dim_dates(conn, cleaned)

    et_map = event_type_id_map(conn)

    rows = cleaned.copy()
    rows["event_type_id"] = rows["event"].astype(str).map(et_map).astype("Int64")

    payload = []
    for r in rows.itertuples(index=False):
        payload.append(
            (
                str(r.event_id),
                str(r.ts),
                None if pd.isna(r.user_id) else int(r.user_id),
                int(r.event_type_id),
                None if pd.isna(r.amount) else float(r.amount),
                str(r.event_date),
                None if pd.isna(r.event_hour) else int(r.event_hour),
            )
        )

    conn.executemany(
        """
        INSERT INTO fact_events(event_id, ts, user_id, event_type_id, amount, event_date, event_hour)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
          ts=excluded.ts,
          user_id=excluded.user_id,
          event_type_id=excluded.event_type_id,
          amount=excluded.amount,
          event_date=excluded.event_date,
          event_hour=excluded.event_hour;
        """,
        payload,
    )
    conn.commit()
    return len(payload)

