import pandas as pd

from pipeline.transform import transform


def test_dedup_keeps_latest():
    events = pd.DataFrame(
        [
            {"event_id": "e1", "ts": "2026-01-01T00:00:01Z", "user_id": "1", "event": "signup"},
            {"event_id": "e1", "ts": "2026-01-01T00:00:02Z", "user_id": "1", "event": "signup"},
        ]
    )
    events["ts"] = pd.to_datetime(events["ts"], utc=True)
    users = pd.DataFrame([{"user_id": 1, "country": "US", "signup_source": "organic"}])

    out, bad, metrics = transform(events, users)
    assert len(out) == 1
    assert bad == []
    assert metrics["dedup_removed"] == 1
    assert out.iloc[0]["ts"].isoformat().startswith("2026-01-01T00:00:02")


def test_amount_casting():
    events = pd.DataFrame(
        [
            {"event_id": "e2", "ts": "2026-01-01T00:00:01Z", "user_id": "1", "event": "purchase", "amount": "19.99"},
        ]
    )
    events["ts"] = pd.to_datetime(events["ts"], utc=True)
    users = pd.DataFrame([{"user_id": 1, "country": "US", "signup_source": "organic"}])

    out, bad, metrics = transform(events, users)
    assert float(out.iloc[0]["amount"]) == 19.99
    assert bad == []
    assert metrics["invalid_event_type"] == 0


def test_invalid_event_type_quarantined():
    events = pd.DataFrame(
        [
            {"event_id": "e1", "ts": "2026-01-01T00:00:01Z", "user_id": "1", "event": "logout"},
            {"event_id": "e2", "ts": "2026-01-01T00:00:02Z", "user_id": "1", "event": "signup"},
        ]
    )
    events["ts"] = pd.to_datetime(events["ts"], utc=True)
    users = pd.DataFrame([{"user_id": 1, "country": "US", "signup_source": "organic"}])

    out, bad, metrics = transform(events, users)

    # only the allowed event remains
    assert len(out) == 1
    assert out.iloc[0]["event"] == "signup"

    # invalid event is quarantined
    assert len(bad) == 1
    assert bad[0]["event_id"] == "e1"
    assert bad[0]["_reason"] == "invalid_event_type"

    # metrics reflect it
    assert metrics["invalid_event_type"] == 1

def test_invalid_event_type_goes_to_bad_records():
    events = pd.DataFrame(
        [
            {"event_id": "e_bad", "ts": "2026-01-01T00:00:01Z", "user_id": "1", "event": "logout"},
            {"event_id": "e_ok", "ts": "2026-01-01T00:00:02Z", "user_id": "1", "event": "signup"},
        ]
    )
    events["ts"] = pd.to_datetime(events["ts"], utc=True)
    users = pd.DataFrame([{"user_id": 1, "country": "US", "signup_source": "organic"}])

    out, bad, metrics = transform(events, users)

    assert len(out) == 1
    assert out.iloc[0]["event"] == "signup"
    assert len(bad) == 1
    assert bad[0]["_reason"] == "invalid_event_type"
    assert metrics["invalid_event_type"] == 1


def test_metrics_rows_out_matches_output_rows():
    events = pd.DataFrame(
        [
            {"event_id": "e1", "ts": "2026-01-01T00:00:01Z", "user_id": "1", "event": "signup"},
            {"event_id": "e2", "ts": "2026-01-01T00:00:02Z", "user_id": None, "event": "pageview"},
        ]
    )
    events["ts"] = pd.to_datetime(events["ts"], utc=True)
    users = pd.DataFrame([{"user_id": 1, "country": "US", "signup_source": "organic"}])

    out, bad, metrics = transform(events, users)

    assert metrics["rows_out"] == len(out)
    assert metrics["null_user_id"] == int(out["user_id"].isna().sum())

def test_event_normalization_page_view_variants():
    events = pd.DataFrame(
        [
            {"event_id": "e1", "ts": "2026-01-01T00:00:01Z", "user_id": "1", "event": "page_view"},
            {"event_id": "e2", "ts": "2026-01-01T00:00:02Z", "user_id": "1", "event": "Page View"},
            {"event_id": "e3", "ts": "2026-01-01T00:00:03Z", "user_id": "1", "event": "pageview"},
        ]
    )
    events["ts"] = pd.to_datetime(events["ts"], utc=True)
    users = pd.DataFrame([{"user_id": 1, "country": "US", "signup_source": "organic"}])

    out, bad, metrics = transform(events, users)

    assert metrics["invalid_event_type"] == 0
    assert bad == []
    assert set(out["event"].unique()) == {"pageview"}

