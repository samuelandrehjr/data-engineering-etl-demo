import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class QualityReport:
    run_utc: str
    raw_lines: int
    ingest_good: int
    ingest_bad: int
    transform_invalid_event_type: int
    loaded_rows: int
    dedup_removed: int
    null_user_id: int

    @property
    def rejected_total(self) -> int:
        return int(self.ingest_bad + self.transform_invalid_event_type)

    @property
    def reject_rate(self) -> float:
        denom = self.raw_lines if self.raw_lines else 0
        return float(self.rejected_total / denom) if denom else 0.0


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_quality_report(path: Path, report: QualityReport) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: Dict[str, Any] = asdict(report)
    payload["rejected_total"] = report.rejected_total
    payload["reject_rate"] = report.reject_rate
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
