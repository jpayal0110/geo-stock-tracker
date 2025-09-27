# etl/compute_lastmile_kpis.py
import pandas as pd
import numpy as np
from pathlib import Path
from math import radians, sin, cos, atan2, sqrt

# -------- Paths --------
ROOT = Path(__file__).resolve().parents[1]
ORD = ROOT / "data" / "orders.csv"
ROU = ROOT / "data" / "routes.csv"
GPS = ROOT / "data" / "gps_logs.csv"
DEF = ROOT / "data" / "defects.csv"

OUT_KPI = ROOT / "data" / "daily_kpis.csv"
OUT_ALERTS_FRIENDLY = ROOT / "data" / "alerts_friendly.csv"

# -------- Business configs (tweak as needed) --------
TARGET_HOURS = {           # route-type SLA (hours)
    "Same-Day": 6.5,
    "Prime": 8.0,
    "Standard": 9.0,
}
THRESHOLDS = {             # alert rules
    "otd_min": 0.95,       # < 95% on-time → WARN
    "defect_max": 0.02,    # > 2% defects → CRITICAL
    "first_attempt_min": 0.95,  # < 95% first-attempt → WARN
    "idle_ratio_max": 0.25,     # > 25% idle → INFO
    "dist_var_max_km": 10.0,    # |distance variance| > 10km → INFO
}

# -------- Helpers --------
def hav_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = map(radians, [lat1, lat2])
    dphi = radians(lat2 - lat1)
    dlmb = radians(lon2 - lon1)
    a = sin(dphi/2)**2 + cos(phi1)*cos(phi2)*sin(dlmb/2)**2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))

def compute_route_actuals(gps: pd.DataFrame) -> pd.DataFrame:
    """
    From GPS logs, compute per-route:
      - actual_start / actual_end
      - active_seconds (sum of engine_on minutes * 60)
      - idle_seconds (sum of idle_flag minutes * 60)
      - actual_km (Haversine path length)
    Assumes each log ~= 1 minute. If yours is per second, change *60 → *1.
    """
    g = gps.sort_values(["route_id", "ts"]).copy()
    agg = g.groupby("route_id").agg(
        actual_start=("ts", "min"),
        actual_end=("ts", "max"),
        idle_seconds=("idle_flag", lambda x: int(x.sum()) * 60),
        active_seconds=("engine_on", lambda x: int(x.sum()) * 60),
    )
    # distances
    dists = []
    for rid, chunk in g.groupby("route_id"):
        chunk = chunk.dropna(subset=["lat", "lon"])
        km = 0.0
        prev = None
        for _, r in chunk.iterrows():
            p = (r["lat"], r["lon"])
            if prev is not None:
                km += hav_km(prev[0], prev[1], p[0], p[1])
            prev = p
        dists.append((rid, km))
    dist_df = pd.DataFrame(dists, columns=["route_id", "actual_km"])
    return agg.reset_index().merge(dist_df, on="route_id", how="left")

def main():
    # ---- Load data ----
    if not ORD.exists() or not ROU.exists() or not GPS.exists():
        raise FileNotFoundError("Missing one or more input CSVs in data/: orders.csv, routes.csv, gps_logs.csv")

    orders = pd.read_csv(ORD, parse_dates=["promised_at", "delivered_at"])
    routes = pd.read_csv(ROU, parse_dates=["route_date", "planned_start", "planned_end"])
    gps = pd.read_csv(GPS, parse_dates=["ts"])
    defects = pd.read_csv(DEF) if DEF.exists() else pd.DataFrame(columns=["order_id", "defect_type", "created_at", "resolved_at"])

    # ---- Actuals from GPS ----
    actuals = compute_route_actuals(gps)

    # ---- Order-level flags ----
    orders["delivered_flag"] = orders["delivered_at"].notna().astype(int)
    orders["on_time_flag"] = ((orders["delivered_at"] <= orders["promised_at"]) & orders["delivered_at"].notna()).astype(int)
    orders["first_attempt_flag"] = orders["first_attempt"].fillna(0).astype(int)
    orders["defect_flag"] = orders["order_id"].isin(defects["order_id"].unique()).astype(int)

    # ---- Join → per-route data ----
    merged = (
        orders.merge(routes, on=["route_id", "station_id", "region", "carrier_id"], how="left")
              .merge(actuals, on="route_id", how="left")
    )

    # Derived times/distances
    merged["active_hours"] = merged["active_seconds"].fillna(0) / 3600.0
    merged["idle_ratio"] = merged["idle_seconds"] / merged["active_seconds"].replace(0, np.nan)
    merged["target_hours"] = merged["route_type"].map(TARGET_HOURS).fillna(8.0)
    merged["actual_hours"] = (merged["actual_end"] - merged["actual_start"]).dt.total_seconds() / 3600.0
    merged["duration_vs_sla"] = merged["actual_hours"] - merged["target_hours"]
    merged["dist_var_km"] = merged["actual_km"] - merged["planned_km"]

    # ---- Aggregate to route-day level ----
    key = ["route_date", "station_id", "region", "carrier_id", "route_id", "route_type"]
    grp = merged.groupby(key, dropna=False)

    kpis = grp.apply(lambda g: pd.Series({
        "orders": g.shape[0],
        "delivered": int(g["delivered_flag"].sum()),
        "otd": g["on_time_flag"].sum() / max(1, g["delivered_flag"].sum()),
        "defect_rate": g["defect_flag"].sum() / max(1, g.shape[0]),
        "first_attempt": g["first_attempt_flag"].sum() / max(1, g["delivered_flag"].sum()),
        "stops_per_hour": g["delivered_flag"].sum() / max(0.1, g["active_hours"].max()),  # rough proxy
        "duration_vs_sla_hr": g["duration_vs_sla"].mean(skipna=True),
        "dist_variance_km": g["dist_var_km"].mean(skipna=True),
        "idle_ratio": g["idle_ratio"].mean(skipna=True),
    })).reset_index()

    # Pretty columns for Sheets
    kpis["otd_pct"] = (kpis["otd"] * 100).round(1)
    kpis["defect_rate_pct"] = (kpis["defect_rate"] * 100).round(2)
    kpis["first_attempt_pct"] = (kpis["first_attempt"] * 100).round(1)
    kpis["stops_per_hour"] = kpis["stops_per_hour"].round(2)
    kpis["duration_vs_sla_hr"] = kpis["duration_vs_sla_hr"].round(2)
    kpis["dist_variance_km"] = kpis["dist_variance_km"].round(2)
    kpis["idle_ratio_pct"] = (kpis["idle_ratio"] * 100).round(1)

    # Select/export
    out_cols = [
        "route_date", "station_id", "region", "carrier_id", "route_id", "route_type",
        "orders", "delivered", "otd_pct", "defect_rate_pct", "first_attempt_pct",
        "stops_per_hour", "duration_vs_sla_hr", "dist_variance_km", "idle_ratio_pct"
    ]
    kpis[out_cols].to_csv(OUT_KPI, index=False)

    # ---- Friendly alerts only ----
    friendly_rows = []
    for _, r in kpis.iterrows():
        # OTD
        if pd.notna(r["otd_pct"]) and r["otd_pct"] < THRESHOLDS["otd_min"] * 100:
            friendly_rows.append({
                "Date": r["route_date"],
                "Station": r["station_id"],
                "Route": r["route_id"],
                "KPI Alert": "On-Time Delivery below target",
                "Details": f"OTD was {r['otd_pct']:.1f}% vs target {THRESHOLDS['otd_min']*100:.1f}%",
                "Severity": "WARN",
            })
        # Defect rate
        if pd.notna(r["defect_rate_pct"]) and r["defect_rate_pct"] > THRESHOLDS["defect_max"] * 100:
            friendly_rows.append({
                "Date": r["route_date"],
                "Station": r["station_id"],
                "Route": r["route_id"],
                "KPI Alert": "Defect Rate too high",
                "Details": f"Defect Rate was {r['defect_rate_pct']:.1f}% vs target {THRESHOLDS['defect_max']*100:.1f}%",
                "Severity": "CRITICAL",
            })
        # First attempt
        if pd.notna(r["first_attempt_pct"]) and r["first_attempt_pct"] < THRESHOLDS["first_attempt_min"] * 100:
            friendly_rows.append({
                "Date": r["route_date"],
                "Station": r["station_id"],
                "Route": r["route_id"],
                "KPI Alert": "First-Attempt Delivery below target",
                "Details": f"First attempt success was {r['first_attempt_pct']:.1f}% vs target {THRESHOLDS['first_attempt_min']*100:.1f}%",
                "Severity": "WARN",
            })
        # Idle ratio
        if pd.notna(r["idle_ratio_pct"]) and r["idle_ratio_pct"] > THRESHOLDS["idle_ratio_max"] * 100:
            friendly_rows.append({
                "Date": r["route_date"],
                "Station": r["station_id"],
                "Route": r["route_id"],
                "KPI Alert": "Idle Time High",
                "Details": f"Idle time ratio was {r['idle_ratio_pct']:.1f}% vs target {THRESHOLDS['idle_ratio_max']*100:.1f}%",
                "Severity": "INFO",
            })
        # Distance variance
        if pd.notna(r["dist_variance_km"]) and abs(r["dist_variance_km"]) > THRESHOLDS["dist_var_max_km"]:
            sign = "over" if r["dist_variance_km"] > 0 else "under"
            friendly_rows.append({
                "Date": r["route_date"],
                "Station": r["station_id"],
                "Route": r["route_id"],
                "KPI Alert": f"Distance {sign} plan",
                "Details": f"Actual distance was {abs(r['dist_variance_km']):.1f} km {sign} plan (threshold {THRESHOLDS['dist_var_max_km']:.1f} km)",
                "Severity": "INFO",
            })

    pd.DataFrame(friendly_rows, columns=["Date","Station","Route","KPI Alert","Details","Severity"]).to_csv(
        OUT_ALERTS_FRIENDLY, index=False
    )

    print(f"Wrote {OUT_KPI}")
    print(f"Wrote {OUT_ALERTS_FRIENDLY}")

if __name__ == "__main__":
    main()
