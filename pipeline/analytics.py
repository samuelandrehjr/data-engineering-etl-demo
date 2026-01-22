from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd


@dataclass
class QueryResult:
    name: str
    df: pd.DataFrame
    csv_path: Path


def _read_sql(conn: sqlite3.Connection, sql: str) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn)


def query_dau(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Daily Active Users (DAU): distinct users per event_date.
    Note: user_id can be NULL; we exclude NULL from DAU by default.
    """
    sql = """
    SELECT
      event_date,
      COUNT(DISTINCT user_id) AS dau
    FROM fact_events
    WHERE user_id IS NOT NULL
    GROUP BY event_date
    ORDER BY event_date;
    """
    return _read_sql(conn, sql)


def query_revenue(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Revenue: sum(amount) per day for purchase events only.
    """
    sql = """
    SELECT
      f.event_date,
      ROUND(SUM(COALESCE(f.amount, 0)), 2) AS revenue
    FROM fact_events f
    JOIN dim_event_types e ON e.event_type_id = f.event_type_id
    WHERE e.event = 'purchase'
    GROUP BY f.event_date
    ORDER BY f.event_date;
    """
    return _read_sql(conn, sql)


def query_event_counts(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Event volume by type per day.
    Good for sanity-checking traffic mix.
    """
    sql = """
    SELECT
      f.event_date,
      e.event,
      COUNT(*) AS events
    FROM fact_events f
    JOIN dim_event_types e ON e.event_type_id = f.event_type_id
    GROUP BY f.event_date, e.event
    ORDER BY f.event_date, e.event;
    """   
    return _read_sql(conn, sql)


def query_funnel(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Simple funnel by day:
      - signup_users: distinct users who signed up
      - purchasers: distinct users who purchased
      - signup_to_purchase_rate: purchasers / signup_users (same-day)
    """
    sql = """
    WITH daily AS (
      SELECT
        f.event_date,
        COUNT(DISTINCT CASE WHEN e.event='signup' THEN f.user_id END) AS signup_users,
        COUNT(DISTINCT CASE WHEN e.event='purchase' THEN f.user_id END) AS purchasers
      FROM fact_events f
      JOIN dim_event_types e ON e.event_type_id = f.event_type_id
      WHERE f.user_id IS NOT NULL
      GROUP BY f.event_date
    )
    SELECT
      event_date,
      signup_users,
      purchasers,
      CASE
        WHEN signup_users = 0 THEN 0.0
        ELSE ROUND(1.0 * purchasers / signup_users, 4)
      END AS signup_to_purchase_rate
    FROM daily
    ORDER BY event_date;
    """    
    return _read_sql(conn, sql)


def export_query(df: pd.DataFrame, export_dir: Path, filename: str) -> Path:
    export_dir.mkdir(parents=True, exist_ok=True)
    out_path = export_dir / filename
    df.to_csv(out_path, index=False)
    return out_path


def run_all(db_path: Path, export_dir: Path) -> list[QueryResult]:
    with sqlite3.connect(db_path) as conn:
        results: list[QueryResult] = []

        dau = query_dau(conn)
        results.append(QueryResult("DAU", dau, export_query(dau, export_dir, "dau.csv")))

        revenue = query_revenue(conn)
        results.append(QueryResult("Revenue", revenue, export_query(revenue, export_dir, "revenue.csv")))

        event_counts = query_event_counts(conn)
        results.append(QueryResult("EventCounts", event_counts, export_query(event_counts, export_dir, "event_counts.csv")))

        funnel = query_funnel(conn)
        results.append(QueryResult("Funnel", funnel, export_query(funnel, export_dir, "funnel.csv")))

        return results
