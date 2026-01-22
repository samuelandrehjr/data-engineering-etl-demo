import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

log = logging.getLogger(__name__)


REQUIRED_FIELDS = {"event_id", "ts", "event"}


@dataclass
class IngestResult:
    events: pd.DataFrame
    bad_records: List[Dict]


def _validate(obj: Dict) -> Tuple[bool, str]:
    missing = REQUIRED_FIELDS - set(obj.keys())
    if missing:
        return False, f"missing_fields={sorted(missing)}"
    return True, ""


def read_events_jsonl(path: str | Path) -> IngestResult:
    """Read JSONL events with basic validation + quarantine bad records.

    Returns:
      - events: DataFrame of good records
      - bad_records: list of dicts with a reason field
    """
    path = Path(path)
    good: List[Dict] = []
    bad: List[Dict] = []

    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                bad.append({"_line": i, "_reason": f"json_decode_error={e.msg}", "_raw": line})
                continue

            ok, reason = _validate(obj)
            if not ok:
                obj["_line"] = i
                obj["_reason"] = reason
                bad.append(obj)
                continue

            # Parse timestamp strictly; quarantine if invalid
            ts = pd.to_datetime(obj.get("ts"), errors="coerce", utc=True)
            if pd.isna(ts):
                obj["_line"] = i
                obj["_reason"] = "invalid_timestamp"
                bad.append(obj)
                continue
            obj["ts"] = ts
            good.append(obj)

    events_df = pd.DataFrame(good)
    log.info("ingest: read_lines=%d good=%d bad=%d", i, len(good), len(bad))
    return IngestResult(events=events_df, bad_records=bad)


def read_users_csv(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    users = pd.read_csv(path)
    # ensure integer user_id (nullable safe)
    users["user_id"] = pd.to_numeric(users["user_id"], errors="coerce").astype("Int64")
    return users


def write_bad_records(path: str | Path, bad_records: Iterable[Dict]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for obj in bad_records:
            f.write(json.dumps(obj, default=str) + "\n")
