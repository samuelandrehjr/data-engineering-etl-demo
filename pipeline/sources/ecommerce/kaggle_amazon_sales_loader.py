import csv
import hashlib
import json
import os
import re
from datetime import datetime
from typing import Dict, Tuple

INCOMING_ZIP_CSV_DIR = r"data/raw/ecommerce/kaggle/_incoming"  # where archive.zip lives

# OUTPUTS
OUTPUT_EVENTS_JSONL = r"data/staging/ecommerce/canonical/events.jsonl"
OUTPUT_INTL_JSONL   = r"data/staging/ecommerce/canonical/international_sales.jsonl"
OUTPUT_REPORT_JSON  = r"data/output/loader_report.json"

# Files inside your extracted folder
TARGET_FILES = [
    "Amazon Sale Report.csv",
    "International sale Report.csv",
    # Intentionally skipped (not event-grain):
    # "Sale Report.csv",
    # "May-2022.csv",
]

def _norm(s: str) -> str:
    return (s or "").strip()

def _hash_id(*parts: str) -> str:
    h = hashlib.sha256()
    h.update(("|".join([_norm(p) for p in parts])).encode("utf-8"))
    return h.hexdigest()[:24]

_DATE_TOKEN = re.compile(r"^\d{2}-\d{2}-\d{2}$")  # e.g. 04-30-22, 06-05-21

def _try_parse(raw: str) -> str:
    raw = _norm(raw)
    if not raw:
        return ""

    fmts = (
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%d/%m/%Y %H:%M",
        "%m-%d-%y",  # important for your datasets
        "%d-%m-%y",
    )

    for fmt in fmts:
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.hour == 0 and dt.minute == 0 and dt.second == 0 and len(raw) <= 10:
                dt = dt.replace(hour=12, minute=0, second=0)
            return dt.isoformat()
        except ValueError:
            pass

    return ""

def _parse_ts(row: dict) -> str:
    """
    Returns ISO8601 string or "" if no timestamp can be found.
      1) try known timestamp keys
      2) if that fails, scan all row values for a date token like 'MM-DD-YY'
    """
    candidates = [
        "Date", "DATE", "Order Date", "OrderDate", "order_date", "date",
        "Order Date & Time", "Timestamp", "ts"
    ]

    for c in candidates:
        if c in row and _norm(row[c]):
            ts = _try_parse(row[c])
            if ts:
                return ts

    for val in row.values():
        s = _norm(val)
        if _DATE_TOKEN.match(s):
            ts = _try_parse(s)
            if ts:
                return ts

    return ""

def _pick(row: dict, keys: list[str]) -> str:
    for k in keys:
        if k in row and _norm(row[k]):
            return _norm(row[k])
    return ""

def _to_float(x: str) -> float:
    x = _norm(x).replace(",", "")
    if x == "":
        return 0.0
    for sym in ["$", "₹", "€", "£"]:
        x = x.replace(sym, "")
    try:
        return float(x)
    except ValueError:
        return 0.0

def _to_int(x: str) -> int:
    x = _norm(x).replace(",", "")
    if x == "":
        return 0
    try:
        return int(float(x))
    except ValueError:
        return 0

def _ensure_dirs() -> None:
    os.makedirs(os.path.dirname(OUTPUT_EVENTS_JSONL), exist_ok=True)
    os.makedirs(os.path.dirname(OUTPUT_INTL_JSONL), exist_ok=True)
    os.makedirs(os.path.dirname(OUTPUT_REPORT_JSON), exist_ok=True)

def load_amazon_events(csv_path: str, out_f) -> Dict:
    """
    Amazon Sale Report.csv -> canonical purchase events JSONL
    """
    stats = {
        "rows_total": 0,
        "written": 0,
        "skipped_no_ts": 0,
        "skipped_amount_outlier": 0,
    }

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stats["rows_total"] += 1

            ts = _parse_ts(row)
            if not ts:
                stats["skipped_no_ts"] += 1
                continue

            order_id = _pick(row, ["Order ID", "Order Id", "order_id", "OrderID", "ID"])
            user_raw = _pick(
                row,
                ["Customer Email", "Email", "Buyer Email", "Phone", "Customer", "Buyer", "Ship Name", "Name"],
            )
            user_id = user_raw if user_raw else (order_id if order_id else "unknown_user")

            product_raw = _pick(
                row,
                ["ASIN", "SKU", "SKU Code", "Product ID", "product_id", "Product", "Item", "Title", "Product Name", "Style"],
            )
            product_id = product_raw if product_raw else "unknown_product"

            qty = _to_int(_pick(row, ["Qty", "Quantity", "quantity", "Units"]))
            unit_price = _to_float(_pick(row, ["Unit Price", "Price", "Item Price", "unit_price"]))
            amount = _to_float(_pick(row, ["Amount", "Sales", "Total", "Order Total", "line_total"]))

            if amount == 0.0 and unit_price > 0.0 and qty > 0:
                amount = unit_price * qty

            # Optional: outlier guardrail (prevents another “billions/day” scenario)
            # Adjust threshold if you want; for consumer order lines this is safe.
            if amount > 250000:
                stats["skipped_amount_outlier"] += 1
                continue

            currency = _pick(row, ["Currency", "currency"]) or "USD"
            country = _pick(row, ["Ship Country", "ship-country", "Country", "country"]) or "unknown"

            event_id = _hash_id(os.path.basename(csv_path), order_id, product_id, str(amount), ts)

            event_record = {
                "event_id": event_id,
                "ts": ts,
                "user_id": user_id,
                "event": "purchase",
                "amount": amount,
                "currency": currency,
                "country": country,
                "order_id": order_id,
                "product_id": product_id,
                "source_dataset": os.path.basename(csv_path),
            }
            out_f.write(json.dumps(event_record, ensure_ascii=False) + "\n")
            stats["written"] += 1

    return stats

def load_international_sales(csv_path: str, out_f) -> Dict:
    """
    International sale Report.csv -> invoice/wholesale-like lines JSONL
    We ONLY accept rows where DATE is truly a date token.
    """
    stats = {
        "rows_total": 0,
        "written": 0,
        "skipped_no_ts": 0,
        "skipped_bad_date_value": 0,
        "skipped_amount_outlier": 0,
    }

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stats["rows_total"] += 1

            # Strict: DATE must be a real date token (your file has SKU/customer junk in DATE)
            raw_date = _pick(row, ["DATE", "Date", "date"])
            if raw_date and not _DATE_TOKEN.match(raw_date.strip()):
                stats["skipped_bad_date_value"] += 1
                continue

            ts = _parse_ts(row)
            if not ts:
                stats["skipped_no_ts"] += 1
                continue

            customer = _pick(row, ["CUSTOMER", "Customer", "customer"]) or "unknown_customer"
            sku = _pick(row, ["SKU", "Sku", "sku"]) or "unknown_sku"

            pcs = _to_int(_pick(row, ["PCS", "Qty", "Quantity", "quantity"]))
            rate = _to_float(_pick(row, ["RATE", "Rate", "rate"]))
            gross_amt = _to_float(_pick(row, ["GROSS AMT", "Gross Amt", "gross_amt", "Amount", "amount"]))

            # Outlier guardrail (wholesale can be larger, but billions means a broken parse)
            if gross_amt > 5_000_000:
                stats["skipped_amount_outlier"] += 1
                continue

            currency = _pick(row, ["Currency", "currency"]) or "USD"

            sale_id = _hash_id(os.path.basename(csv_path), customer, sku, str(gross_amt), ts)

            rec = {
                "sale_id": sale_id,
                "ts": ts,                     # ISO string
                "date_key": ts[:10],          # YYYY-MM-DD
                "customer": customer,
                "sku": sku,
                "pcs": pcs,
                "rate": rate,
                "gross_amt": gross_amt,
                "currency": currency,
                "source_dataset": os.path.basename(csv_path),
            }
            out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            stats["written"] += 1

    return stats

def main():
    _ensure_dirs()

    extracted_dir = os.path.join(INCOMING_ZIP_CSV_DIR, "extracted")
    if not os.path.isdir(extracted_dir):
        raise SystemExit(
            f"Expected extracted CSVs at: {extracted_dir}\n"
            "Next step: extract archive.zip into that folder."
        )

    report = {
        "run_local": datetime.now().isoformat(timespec="seconds"),
        "files": {},
        "outputs": {
            "events_jsonl": OUTPUT_EVENTS_JSONL,
            "international_sales_jsonl": OUTPUT_INTL_JSONL,
            "report_json": OUTPUT_REPORT_JSON,
        },
    }

    with open(OUTPUT_EVENTS_JSONL, "w", encoding="utf-8") as out_events, \
         open(OUTPUT_INTL_JSONL, "w", encoding="utf-8") as out_intl:

        for fname in TARGET_FILES:
            fpath = os.path.join(extracted_dir, fname)
            if not os.path.exists(fpath):
                print(f"[WARN] Missing expected file: {fname}")
                report["files"][fname] = {"missing": True}
                continue

            if fname == "Amazon Sale Report.csv":
                stats = load_amazon_events(fpath, out_events)
            elif fname == "International sale Report.csv":
                stats = load_international_sales(fpath, out_intl)
            else:
                stats = {"rows_total": 0, "written": 0, "skipped_no_ts": 0}

            report["files"][fname] = stats
            # Keep the console output you’re used to:
            skipped = (
                stats.get("skipped_no_ts", 0)
                + stats.get("skipped_bad_date_value", 0)
                + stats.get("skipped_amount_outlier", 0)
            )
            print(f"[INFO] {fname}: written={stats.get('written',0)} skipped={skipped}")

    with open(OUTPUT_REPORT_JSON, "w", encoding="utf-8") as rf:
        json.dump(report, rf, indent=2, ensure_ascii=False)

    print(f"[OK] Wrote canonical events JSONL -> {OUTPUT_EVENTS_JSONL}")
    print(f"[OK] Wrote international sales JSONL -> {OUTPUT_INTL_JSONL}")
    print(f"[OK] Wrote loader report -> {OUTPUT_REPORT_JSON}")

if __name__ == "__main__":
    main()
