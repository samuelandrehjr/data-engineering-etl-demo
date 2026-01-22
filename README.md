
# Junior Data Engineer ETL Demo (Runnable)

This project is a **proof-of-work** ETL pipeline you can run locally to demonstrate junior data engineering fundamentals:

- Ingest raw **JSON Lines** + **CSV**
- Validate and quarantine bad records
- Transform + enrich data (de-dup, type casting, derived columns)
- Load into **SQLite** (warehouse stand-in)
- Run **SQL analytics queries**
- Unit tests + minimal CI

> Why SQLite? It keeps everything runnable on any laptop while still showing the same SQL patterns you’d use in Redshift/Snowflake.

## Quickstart

### 1) Create a virtual environment

**macOS / Linux**
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**Windows (PowerShell)**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**Windows (Git Bash)**
```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt
```

### 2) Run the pipeline
```bash
python -m pipeline.run_pipeline
```

Outputs:
- `data/output/warehouse.db`
- `data/output/bad_records.jsonl`
- `data/output/exports/*.csv`

### 3) Run example SQL queries
```bash
python -m pipeline.run_queries
```

### 4) Run tests
```bash
pytest -q
```

## Repo structure
```
jde-etl-demo/
  data/
    raw/
      events.jsonl
      users.csv
    output/
      warehouse.db
      exports/
  pipeline/
    ingest.py
    transform.py
    load.py
    run_pipeline.py
    run_queries.py
  sql/
    analytics_queries.sql
  tests/
    test_transform.py
  .github/workflows/
    ci.yml
```

## “Cloud mapping” (conceptual)

Local component → Typical AWS equivalent

- `data/raw/*` → **S3**
- `pipeline/*.py` → **Glue job** (Spark or Python shell) / **Lambda** for small tasks
- `data/output/warehouse.db` → **Redshift** / **Snowflake**
- `logs + counts` → **CloudWatch Logs + Metrics**
=======
# data-engineering-etl-demo
Runnable end-to-end ETL pipeline demonstrating data ingestion, validation, transformation, star-schema modeling, analytics queries, and testing.
>>>>>>> b46be3c7a46d28f561d3eeb46908cf01ecb6cd0e
