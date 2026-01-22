import logging
import sqlite3
from pathlib import Path

import pandas as pd


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
    )


def main() -> int:
    _setup_logging()
    root = Path(__file__).resolve().parents[1]
    db_path = root / "data" / "output" / "warehouse.db"
    queries_path = root / "sql" / "analytics_queries.sql"
    export_dir = root / "data" / "output" / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(str(db_path)) as conn:
        sql = queries_path.read_text(encoding="utf-8")
        # Split on blank lines between queries
        chunks = [q.strip() for q in sql.split("\n\n") if q.strip() and not q.strip().startswith("--")]
        # A simpler approach: run named queries manually below

        dau = pd.read_sql_query(
            """
            SELECT event_date, COUNT(DISTINCT user_id) AS dau
            FROM fact_events
            WHERE event = 'page_view'
            GROUP BY event_date
            ORDER BY event_date;
            """,
            conn,
        )
        revenue = pd.read_sql_query(
            """
            SELECT event_date, ROUND(SUM(amount), 2) AS revenue
            FROM fact_events
            WHERE event = 'purchase'
            GROUP BY event_date
            ORDER BY event_date;
            """,
            conn,
        )

    print("\nDAU")
    print(dau.to_string(index=False))
    print("\nRevenue")
    print(revenue.to_string(index=False))

    dau.to_csv(export_dir / "dau.csv", index=False)
    revenue.to_csv(export_dir / "revenue.csv", index=False)
    logging.getLogger(__name__).info("exported: %s", export_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
