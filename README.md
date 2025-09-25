
# Procurement KPI Pipeline (SQL → CSV/Sheets → Dash)

What you get:
- Load procurement CSV → PostgreSQL
- Compute supplier/month KPIs (on-time, lead time, delivery completion, savings, defects, compliance)
- Export aggregates to CSV (connect to Google Sheets or Looker Studio)
- Alert rules to flag KPI breaches

## Quick Start
1) Create venv and install deps
```
python3 -m venv .venv && source .venv/bin/activate
pip install pandas sqlalchemy psycopg2-binary
```

2) (Optional) Load into Postgres
```
export DATABASE_URL='postgresql+psycopg2://user:pass@localhost:5432/procuredb'
python etl/load_csv_to_sql.py data/procurement_orders.csv
```

3) Compute KPIs (writes data/supplier_monthly_kpis.csv)
```
python etl/compute_kpis.py
```

4) Run alerts (writes data/alerts.csv)
```
python alerts/rules.py
```
