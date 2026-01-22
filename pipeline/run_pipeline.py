import logging
from pathlib import Path

import pandas as pd

from pipeline.ingest import read_events_jsonl, read_users_csv, write_bad_records
from pipeline.load import connect, init_schema, upsert_dim_users, upsert_fact_events
from pipeline.transform import transform

# Data Quality Report helpers
from pipeline.quality import QualityReport, now_utc_iso, write_quality_report


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
    out_dir.mkdir(parents=True, exist_ok=True)

    bad_path = out_dir / "bad_records.jsonl"
    quality_path = out_dir / "data_quality_report.json"
    db_path = out_dir / "warehouse.db"
    export_dir = out_dir / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)

    # Ingest
    ing = read_events_jsonl(raw_events)
    users = read_users_csv(raw_users)

    # Transform (returns clean df + transform-layer bad records)
    cleaned, bad_from_transform, metrics = transform(ing.events, users)
    
    # Combine ingest bad + transform bad and write once
    all_bad_records = list(ing.bad_records)
    all_bad_records.extend(bad_from_transform)
    write_bad_records(bad_path, all_bad_records)

    # Load
    with connect(db_path) as conn:
        schema_path = root / "sql" / "warehouse_star.sql"
        init_schema(conn, schema_path)
        upsert_dim_users(conn, cleaned)

        upsert_fact_events(conn, cleaned)

        # Export a handy preview for inspection
        preview = pd.read_sql_query(
            """
            SELECT
                f.event_id,
                f.ts,
                f.user_id,
                f.event_type_id,
                f.amount,
                f.event_date,
                f.event_hour,
                u.country,
                u.signup_source
            FROM fact_events f
            LEFT JOIN dim_users u
                ON f.user_id = u.user_id
            ORDER BY f.ts
            LIMIT 50;            
            """,
            conn,
        )
        preview.to_csv(export_dir / "fact_events_preview.csv", index=False)

    # Data quality report (written every run)
    raw_lines = int(metrics.get("raw_lines_in", len(ing.events) + len(ing.bad_records)))
    
    report = QualityReport(
        run_utc=now_utc_iso(),
        raw_lines=raw_lines,
        ingest_good=int(metrics.get("ingest_good", len(ing.events))),
        ingest_bad=int(metrics.get("ingest_bad", len(ing.bad_records))),
        transform_invalid_event_type=int(metrics.get("invalid_event_type", len(bad_from_transform))),
        loaded_rows=int(metrics.get("rows_out", len(cleaned))),
        dedup_removed=int(metrics.get("dedup_removed", 0)),
        null_user_id=int(metrics.get("null_user_id", 0)),
        
    )
    write_quality_report(quality_path, report)
  
    logging.getLogger(__name__).info("done: db=%s bad_records=%s", db_path, bad_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
