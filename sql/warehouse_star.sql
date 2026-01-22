-- Star-schema-ish warehouse for SQLite
-- Safe to run multiple times (DROP + CREATE)

DROP TABLE IF EXISTS fact_events;
DROP TABLE IF EXISTS dim_users;
DROP TABLE IF EXISTS dim_dates;
DROP TABLE IF EXISTS dim_event_types;

CREATE TABLE dim_users (
  user_id INTEGER PRIMARY KEY,
  country TEXT,
  signup_source TEXT
);

CREATE TABLE dim_event_types (
  event_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
  event TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_dates (
  date_key TEXT PRIMARY KEY,          -- 'YYYY-MM-DD'
  year INTEGER NOT NULL,
  month INTEGER NOT NULL,
  day INTEGER NOT NULL
);

CREATE TABLE fact_events (
  event_id TEXT PRIMARY KEY,
  ts TEXT NOT NULL,                   -- ISO timestamp
  user_id INTEGER,                    -- FK to dim_users
  event_type_id INTEGER NOT NULL,     -- FK to dim_event_types
  amount REAL,
  event_date TEXT NOT NULL,           -- FK to dim_dates.date_key
  event_hour INTEGER,

  FOREIGN KEY (user_id) REFERENCES dim_users(user_id),
  FOREIGN KEY (event_type_id) REFERENCES dim_event_types(event_type_id),
  FOREIGN KEY (event_date) REFERENCES dim_dates(date_key)
);

CREATE INDEX idx_fact_events_date ON fact_events(event_date);
CREATE INDEX idx_fact_events_event_type ON fact_events(event_type_id);
CREATE INDEX idx_fact_events_user ON fact_events(user_id);
