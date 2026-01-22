import json, random, pathlib, datetime

raw_path = pathlib.Path("data/raw/events.jsonl")
text = raw_path.read_text(encoding="utf-8").splitlines()

# Parse the existing sample events
base = []
for line in text:
    try:
        base.append(json.loads(line))
    except Exception:
        pass

if not base:
    raise RuntimeError("No valid JSON lines found in data/raw/events.jsonl")

allowed = ["pageview", "signup", "purchase"]
invalid = ["click", "logout", "refund_requested"]  # intentionally not allowed

out_lines = []

# Generate ~120 events by cloning and tweaking
for i in range(120):
    e = dict(random.choice(base))
    e["event_id"] = f"gen_{i:04d}"

    # Spread timestamps across a few days
    day = datetime.date(2026, 1, 5) + datetime.timedelta(days=random.randint(0, 2))
    hh = random.randint(0, 23)
    mm = random.randint(0, 59)
    ss = random.randint(0, 59)
    e["ts"] = f"{day.isoformat()}T{hh:02d}:{mm:02d}:{ss:02d}Z"

    # Inject invalid event types ~10% of the time
    e["event"] = random.choice(invalid) if random.random() < 0.10 else random.choice(allowed)

    # Sometimes introduce a null-ish user_id (keeps realism)
    if random.random() < 0.05:
        e["user_id"] = None

    out_lines.append(json.dumps(e))

# Append one known-bad timestamp row for your existing validation to catch
out_lines.append(json.dumps({
    "event_id": "bad_time_1",
    "ts": "BAD_TIME",
    "user_id": "17",
    "event": "signup"
}))

raw_path.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
print(f"Wrote {len(out_lines)} lines to {raw_path}")
