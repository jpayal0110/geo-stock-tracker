# Geo Stock Tracker
Last-Mile Route Intelligence + KPI Alerts

## Overview
In this project I've simulated an Amazon-style last-mile delivery analytics + Alert system.  
It ingests delivery orders, routes, GPS logs, and defect reports, then computes daily KPIs to evaluate delivery performance.  
It also generates a human-friendly alert log so managers can quickly see where things are going wrong.

The goal: spot patterns → fix root causes → escalate fast.
---

## Pipeline (Simple View)

## Pipeline (Simple View)

```mermaid
flowchart TD
    A[Delivery Data Files<br>orders.csv, routes.csv, gps.csv, defects.csv] --> B[Python Script<br>compute KPIs + check problems]
    B --> C[Daily Scoreboard<br>daily_kpis.csv]
    B --> D[Alerts Report<br>alerts_friendly.csv]
    C --> E[GitHub<br>(public links)]
    D --> E
    E --> F[Google Sheets<br>imports the files]
    F --> G[Charts & Dashboards<br>(see trends + alerts)]
```

---
## Data Sources
Synthetic datasets (CSV files in `/data`):

- `orders.csv` – each delivery order with promised vs actual delivery timestamps  
- `routes.csv` – planned route schedule, type, and SLA info  
- `gps_logs.csv` – per-route GPS trace (location, speed, idle time)  
- `defects.csv` – defect events (NDR, late, damaged, etc.)
---
## KPIs Tracked
- On-Time Delivery Rate (OTD) – % of deliveries before promised time  
- Defect Rate – % of orders with a defect (lost, damaged, late, NDR)  
- First-Attempt Success – % delivered on first try  
- Stops per Hour – delivered orders ÷ driver active hours  
- Route Duration vs SLA – actual vs target route duration  
- Distance Variance – actual vs planned distance  
- Idle Time Ratio – % of time idling while engine on  

---

## Alerts
Alerts are written to `data/alerts_friendly.csv` in a plain-English format:

| Date       | Station | Route         | KPI Alert                  | Details                                | Severity  |
|------------|---------|---------------|----------------------------|----------------------------------------|-----------|
| 2023-05-01 | BOS-3   | R-2023-05-01-A | On-Time Delivery below target | OTD was 85.7% vs target 95%            | WARN      |
| 2023-05-01 | BOS-3   | R-2023-05-01-A | Defect Rate too high       | Defect Rate was 6.7% vs target 2%       | CRITICAL  |

---

## Architecture
- Ingestion → CSVs  
- Transform → Python (Pandas)  
- Storage → Outputs saved back to `/data`  
- Serving → Google Sheets (`IMPORTDATA`) or Looker Studio for dashboards  

---

## How to Run
1. Clone the repo:
   ```bash
   git clone https://github.com/jpayal0110/geo-stock-tracker.git
   cd geo-stock-tracker
