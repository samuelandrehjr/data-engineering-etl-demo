import logging
from typing import Dict, List, Tuple

import pandas as pd

log = logging.getLogger(__name__)

ALLOWED_EVENTS = {"pageview", "signup", "purchase"}


def transform(events: pd.DataFrame, users: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict], Dict]:
    """Clean, normalize, and enrich raw events.

    Rules:
      - De-dup by event_id (keep latest timestamp)
      - Cast amount
      - Enforce allowed event types (quarantine invalid)
      - Derive event_date, event_hour
      - Left join users dimension

    Returns:
      (clean_events_df, bad_records_list, metrics_dict)
    """
    if events.empty:
        return events, [], {"dedup_removed": 0, "null_user_id": 0, "invalid_event_type": 0, "rows_out": 0}

    bad_records: List[Dict] = []
    events = events.copy()

    # Ensure ts is datetime
    events["ts"] = pd.to_datetime(events["ts"], utc=True, errors="coerce")

    # Normalize event type (lowercase/trim + canonicalization)
    if "event" in events.columns:
        events["event"] = (
            events["event"]
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace("-", "_", regex=False)
            .str.replace(" ", "_", regex=False)
        )

        # Collapse known variants into canonical forms
        events["event"] = events["event"].replace({
            "page_view": "pageview",
            "page view": "pageview",
        })
    else:
        events["event"] = ""

    # Identify invalid event types
    invalid_mask = ~events["event"].isin(list(ALLOWED_EVENTS))
    invalid_count = int(invalid_mask.sum())

    if invalid_count > 0:
        invalid_rows = events.loc[invalid_mask, ["event_id", "ts", "user_id", "event"]].copy()
        for _, r in invalid_rows.iterrows():
            bad_records.append(
                {
                    "event_id": None if pd.isna(r.get("event_id")) else str(r.get("event_id")),
                    "ts": None if pd.isna(r.get("ts")) else str(r.get("ts")),
                    "user_id": None if pd.isna(r.get("user_id")) else str(r.get("user_id")),
                    "event": None if pd.isna(r.get("event")) else str(r.get("event")),
                    "_reason": "invalid_event_type",
                }
            )

    # Drop invalid events from clean pipeline flow
    events = events.loc[~invalid_mask].copy()

    # De-dup: keep latest row per event_id
    before = len(events)
    events = events.sort_values("ts").drop_duplicates("event_id", keep="last")
    dedup_removed = before - len(events)

    # Type normalization
    # IMPORTANT: Keep user_id as TEXT for real-world IDs (order IDs, UUIDs, etc.)
    if "user_id" in events.columns:
        events["user_id"] = (
            events["user_id"]
            .astype(str)
            .str.strip()
            .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "<NA>": pd.NA})
        )
    else:
        events["user_id"] = pd.NA

    if "amount" in events.columns:
        events["amount"] = pd.to_numeric(events["amount"], errors="coerce")
    else:
        events["amount"] = pd.NA

    # Derived columns
    events["event_date"] = events["ts"].dt.date.astype(str)
    events["event_hour"] = events["ts"].dt.hour.astype("Int64")

    # Make sure users user_id type matches for join
    users = users.copy()
    if "user_id" in users.columns:
        users["user_id"] = users["user_id"].astype(str).str.strip()

    out = events.merge(users, on="user_id", how="left")
    null_user_id = int(out["user_id"].isna().sum())

    log.info(
        "transform: rows=%d dedup_removed=%d null_user_id=%d invalid_event_type=%d",
        len(out),
        dedup_removed,
        null_user_id,
        invalid_count,
    )

    metrics = {
        "dedup_removed": int(dedup_removed),
        "null_user_id": int(null_user_id),
        "invalid_event_type": int(invalid_count),
        "rows_out": int(len(out)),
    }
    return out, bad_records, metrics
