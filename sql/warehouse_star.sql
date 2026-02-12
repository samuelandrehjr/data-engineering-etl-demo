-- Star-schema-ish warehouse for SQLite
-- Safe to run multiple times (DROP + CREATE)

DROP TABLE IF EXISTS fact_events;
DROP TABLE IF EXISTS fact_international_sales;
DROP TABLE IF EXISTS dim_users;
DROP TABLE IF EXISTS dim_customers;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_dates;
DROP TABLE IF EXISTS dim_event_types;

CREATE TABLE dim_users (
  user_id TEXT PRIMARY KEY,
  country TEXT,
  signup_source TEXT
);

CREATE TABLE dim_customers (
  customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
  customer_name TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_products (
  product_id INTEGER PRIMARY KEY AUTOINCREMENT,
  sku TEXT NOT NULL UNIQUE
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
  user_id TEXT,                       -- FK to dim_users
  event_type_id INTEGER NOT NULL,     -- FK to dim_event_types
  amount REAL,
  event_date TEXT NOT NULL,           -- FK to dim_dates.date_key
  event_hour INTEGER,

  FOREIGN KEY (user_id) REFERENCES dim_users(user_id),
  FOREIGN KEY (event_type_id) REFERENCES dim_event_types(event_type_id),
  FOREIGN KEY (event_date) REFERENCES dim_dates(date_key)
);

CREATE TABLE fact_international_sales (
  sale_id TEXT PRIMARY KEY,
  ts TEXT NOT NULL,
  date_key TEXT NOT NULL,             -- FK to dim_dates
  customer_id INTEGER NOT NULL,        -- FK to dim_customers
  product_id INTEGER NOT NULL,         -- FK to dim_products
  pcs INTEGER,
  rate REAL,
  gross_amt REAL NOT NULL,
  currency TEXT,
  source_dataset TEXT,

  FOREIGN KEY (date_key) REFERENCES dim_dates(date_key),
  FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
  FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
);

CREATE INDEX idx_fact_events_date ON fact_events(event_date);
CREATE INDEX idx_fact_events_event_type ON fact_events(event_type_id);
CREATE INDEX idx_fact_events_user ON fact_events(user_id);

CREATE INDEX idx_intl_sales_date ON fact_international_sales(date_key);
CREATE INDEX idx_intl_sales_customer ON fact_international_sales(customer_id);
CREATE INDEX idx_intl_sales_product ON fact_international_sales(product_id);
