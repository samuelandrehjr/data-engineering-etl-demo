import logging
from pathlib import Path

import pandas as pd

from pipeline.ingest import read_events_jsonl, read_users_csv, write_bad_records
from pipeline.load import connect, upsert_fact_events
from pipeline.transform import transform


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
    )


def main() -> int:
    _setup_logging()
    root = Path(__file__).resolve().parents[1]
    raw_events = root / "data" / "raw" / "events.jsonl"
    raw_users = root / "data" / "raw" / "users.csv"
    out_dir = root / "data" / "output"
    bad_path = out_dir / "bad_records.jsonl"
    db_path = out_dir / "warehouse.db"
    export_dir = out_dir / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)

    ing = read_events_jsonl(raw_events)
    users = read_users_csv(raw_users)
    write_bad_records(bad_path, ing.bad_records)

    cleaned = transform(ing.events, users)

    with connect(db_path) as conn:
        upsert_fact_events(conn, cleaned)

        # Export a couple of handy views for quick inspection
        preview = pd.read_sql_query(
            "SELECT event_id, ts, user_id, event, amount, country FROM fact_events ORDER BY ts LIMIT 50;",
            conn,
        )
        preview.to_csv(export_dir / "fact_events_preview.csv", index=False)

    logging.getLogger(__name__).info("done: db=%s bad_records=%s", db_path, bad_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
