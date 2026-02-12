import logging
import sqlite3
from pathlib import Path
from typing import Iterable, Tuple

import pandas as pd

log = logging.getLogger(__name__)


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
    """
    Upserts dim_users. For real-world datasets, user_id is often TEXT (not int).
    We keep it as string and ensure country/signup_source exist.
    """
    if users.empty:
        return 0

    u = users.copy()

    # Ensure required columns exist
    if "user_id" not in u.columns:
        log.warning("dim_users: missing user_id column; skipping")
        return 0
    if "country" not in u.columns:
        u["country"] = "unknown"
    if "signup_source" not in u.columns:
        u["signup_source"] = "unknown"

    # Normalize types
    u["user_id"] = u["user_id"].astype(str)
    u["country"] = u["country"].fillna("unknown").astype(str)
    u["signup_source"] = u["signup_source"].fillna("unknown").astype(str)

    rows = u[["user_id", "country", "signup_source"]].drop_duplicates().copy()

    payload = [
        (str(r.user_id), str(r.country), str(r.signup_source))
        for r in rows.itertuples(index=False)
        if pd.notna(r.user_id) and str(r.user_id).strip() != ""
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

def upsert_dim_customers(conn: sqlite3.Connection, intl: pd.DataFrame) -> None:
    if intl.empty:
        return
    if "customer" not in intl.columns:
        return
    customers = sorted(set(intl["customer"].dropna().astype(str).tolist()))
    conn.executemany(
        "INSERT OR IGNORE INTO dim_customers(customer_name) VALUES (?);",
        [(c,) for c in customers],
    )
    conn.commit()

def upsert_dim_products(conn: sqlite3.Connection, intl: pd.DataFrame) -> None:
    if intl.empty:
        return
    if "sku" not in intl.columns:
        return
    skus = sorted(set(intl["sku"].dropna().astype(str).tolist()))
    conn.executemany(
        "INSERT OR IGNORE INTO dim_products(sku) VALUES (?);",
        [(s,) for s in skus],
    )
    conn.commit()

def customer_id_map(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute("SELECT customer_name, customer_id FROM dim_customers;").fetchall()
    return {str(name): int(cid) for (name, cid) in rows}

def product_id_map(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute("SELECT sku, product_id FROM dim_products;").fetchall()
    return {str(sku): int(pid) for (sku, pid) in rows}

def upsert_fact_international_sales(conn: sqlite3.Connection, intl: pd.DataFrame) -> int:
    """
    intl DataFrame expects columns:
      sale_id, ts, date_key, customer, sku, pcs, rate, gross_amt, currency, source_dataset
    """
    if intl.empty:
        return 0

    # Ensure required columns exist
    required = ["sale_id", "ts", "date_key", "customer", "sku", "gross_amt"]
    for c in required:
        if c not in intl.columns:
            raise ValueError(f"international sales missing required column: {c}")

    # Ensure dim_dates exists for date_key
    if "event_date" not in intl.columns:
        intl = intl.copy()
        intl["event_date"] = intl["date_key"]

    upsert_dim_dates(conn, intl.rename(columns={"event_date": "event_date"}))

    # Upsert customer/product dims
    upsert_dim_customers(conn, intl)
    upsert_dim_products(conn, intl)

    c_map = customer_id_map(conn)
    p_map = product_id_map(conn)

    rows = intl.copy()
    rows["customer_id"] = rows["customer"].astype(str).map(c_map).astype("Int64")
    rows["product_id"] = rows["sku"].astype(str).map(p_map).astype("Int64")

    # Convert types safely
    rows["pcs"] = pd.to_numeric(rows.get("pcs", pd.NA), errors="coerce").astype("Int64")
    rows["rate"] = pd.to_numeric(rows.get("rate", pd.NA), errors="coerce")
    rows["gross_amt"] = pd.to_numeric(rows["gross_amt"], errors="coerce")
    rows["ts"] = rows["ts"].astype(str)
    rows["date_key"] = rows["date_key"].astype(str)

    payload = []
    for r in rows.itertuples(index=False):
        if pd.isna(r.customer_id) or pd.isna(r.product_id) or pd.isna(r.gross_amt):
            continue
        payload.append(
            (
                str(r.sale_id),
                str(r.ts),
                str(r.date_key),
                int(r.customer_id),
                int(r.product_id),
                None if pd.isna(r.pcs) else int(r.pcs),
                None if pd.isna(r.rate) else float(r.rate),
                float(r.gross_amt),
                None if pd.isna(getattr(r, "currency", pd.NA)) else str(getattr(r, "currency")),
                None if pd.isna(getattr(r, "source_dataset", pd.NA)) else str(getattr(r, "source_dataset")),
            )
        )

    conn.executemany(
        """
        INSERT INTO fact_international_sales(
          sale_id, ts, date_key, customer_id, product_id, pcs, rate, gross_amt, currency, source_dataset
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(sale_id) DO UPDATE SET
          ts=excluded.ts,
          date_key=excluded.date_key,
          customer_id=excluded.customer_id,
          product_id=excluded.product_id,
          pcs=excluded.pcs,
          rate=excluded.rate,
          gross_amt=excluded.gross_amt,
          currency=excluded.currency,
          source_dataset=excluded.source_dataset;
        """,
        payload,
    )
    conn.commit()
    return len(payload)



def to_rows(df: pd.DataFrame) -> Iterable[Tuple]:
    """
    Helper retained (not used in upsert_fact_events below), but made safe.
    """
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
    frame = df.copy()
    for c in cols:
        if c not in frame.columns:
            frame[c] = pd.NA

    frame["ts"] = frame["ts"].astype(str)
    frame["user_id"] = frame["user_id"].astype(str)

    out = frame[cols].astype(object).where(pd.notna(frame[cols]), None)
    return (tuple(row) for row in out.itertuples(index=False, name=None))


def upsert_fact_events(conn: sqlite3.Connection, cleaned: pd.DataFrame) -> int:
    if cleaned.empty:
        return 0

    # Ensure dim tables are ready for fact load
    upsert_dim_event_types(conn, cleaned)
    upsert_dim_dates(conn, cleaned)

    et_map = event_type_id_map(conn)

    rows = cleaned.copy()
    rows["event_type_id"] = rows["event"].astype(str).map(et_map).astype("Int64")

    # Ensure user_id is string to match dim_users TEXT
    if "user_id" in rows.columns:
        rows["user_id"] = rows["user_id"].astype(str)

    payload = []
    for r in rows.itertuples(index=False):
        payload.append(
            (
                str(r.event_id),
                str(r.ts),
                None if pd.isna(r.user_id) or str(r.user_id).strip() == "" else str(r.user_id),
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


