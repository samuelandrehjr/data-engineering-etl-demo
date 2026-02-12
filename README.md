# Junior Data Engineer ETL Warehouse (Version 2 – Multi-Source + International Analytics)

This project is a **production-style, end-to-end ETL warehouse pipeline** demonstrating junior-to-intermediate data engineering fundamentals.

It began as a single-source e-commerce pipeline and was upgraded to support **multi-source ingestion, international sales modeling, and expanded analytics exports**.

---

## What This Project Demonstrates

### Core Data Engineering Skills

- Ingest raw **JSON Lines + CSV**
- Validate and quarantine bad records
- Transform + enrich data
  - Type casting
  - De-duplication
  - Derived columns
  - Date key generation
- Load into a **star-schema warehouse (SQLite)**
- Build indexed fact + dimension tables
- Generate analytics-ready exports
- Data quality reporting
- Modular pipeline design
- Unit testing
- Git-based version control

---

# Architecture Overview

## Version 1 (Original)

Single-source behavioral event pipeline:

Raw:
- `events.jsonl`
- `users.csv`

Warehouse:
- `fact_events`
- `dim_users`
- `dim_dates`
- `dim_event_types`

Analytics:
- Daily Active Users (DAU)
- Revenue
- Event counts
- Funnel conversion

---

## Version 2 (Upgrade)

Expanded to support:

### International Sales Dataset (Kaggle-style ingestion)

New fact table:

- `fact_international_sales`

New dimensions:

- `dim_customers`
- `dim_products`

New analytics export:

- **International Revenue by Day**

---

# Star Schema (Current Warehouse)

Fact Tables:
- `fact_events`
- `fact_international_sales`

Dimensions:
- `dim_users`
- `dim_event_types`
- `dim_dates`
- `dim_customers`
- `dim_products`

Indexes added on:
- Event date
- User ID
- Event type
- Foreign keys

---

# Analytics Outputs

Running:

```bash
python -m pipeline.run_analytics
