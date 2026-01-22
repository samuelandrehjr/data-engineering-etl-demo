import logging

import pandas as pd

log = logging.getLogger(__name__)


def transform(events: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    """Clean, normalize, and enrich raw events.

    Rules:
      - De-dup by event_id (keep latest timestamp)
      - Cast user_id, amount
      - Derive event_date, event_hour
      - Left join users dimension
    """
    if events.empty:
        return events

    # Safety: ensure ts is datetime
    events = events.copy()
    events["ts"] = pd.to_datetime(events["ts"], utc=True, errors="coerce")

    # De-dup: keep latest row per event_id
    before = len(events)
    events = events.sort_values("ts").drop_duplicates("event_id", keep="last")
    dedup_removed = before - len(events)

    # Type normalization
    if "user_id" in events.columns:
        events["user_id"] = pd.to_numeric(events["user_id"], errors="coerce").astype("Int64")
    else:
        events["user_id"] = pd.Series([pd.NA] * len(events), dtype="Int64")

    if "amount" in events.columns:
        events["amount"] = pd.to_numeric(events["amount"], errors="coerce")
    else:
        events["amount"] = pd.NA

    # Derived columns
    events["event_date"] = events["ts"].dt.date.astype(str)
    events["event_hour"] = events["ts"].dt.hour.astype("Int64")

    out = events.merge(users, on="user_id", how="left")
    null_user_id = int(out["user_id"].isna().sum())

    log.info(
        "transform: rows=%d dedup_removed=%d null_user_id=%d",
        len(out),
        dedup_removed,
        null_user_id,
    )
    return out
