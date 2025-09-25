
# alerts/rules.py
import os, pandas as pd

THRESHOLDS = {
    'on_time_rate_min': 0.95,
    'defect_rate_max': 0.02,
    'compliance_rate_min': 0.95,
    'avg_savings_rate_min': 0.03
}

def evaluate(kpis_csv='data/supplier_monthly_kpis_pretty.csv', out_csv='data/alerts.csv'):
    if not os.path.exists(kpis_csv):
        raise FileNotFoundError(f"{kpis_csv} not found. Compute KPIs first.")
    df = pd.read_csv(kpis_csv, parse_dates=['month'])
    rows = []
    for _, r in df.iterrows():
        def add(kpi, val, thr, sev):
            rows.append([r['supplier'], r['month'], kpi, val, thr, sev])
        if pd.notna(r.get('on_time_rate')) and r['on_time_rate'] < THRESHOLDS['on_time_rate_min']:
            add('on_time_rate', r['on_time_rate'], THRESHOLDS['on_time_rate_min'], 'WARN')
        if pd.notna(r.get('defect_rate')) and r['defect_rate'] > THRESHOLDS['defect_rate_max']:
            add('defect_rate', r['defect_rate'], THRESHOLDS['defect_rate_max'], 'CRITICAL')
        if pd.notna(r.get('compliance_rate')) and r['compliance_rate'] < THRESHOLDS['compliance_rate_min']:
            add('compliance_rate', r['compliance_rate'], THRESHOLDS['compliance_rate_min'], 'WARN')
        if pd.notna(r.get('avg_savings_rate')) and r['avg_savings_rate'] < THRESHOLDS['avg_savings_rate_min']:
            add('avg_savings_rate', r['avg_savings_rate'], THRESHOLDS['avg_savings_rate_min'], 'INFO')
    out = pd.DataFrame(rows, columns=['supplier','month','kpi','value','threshold','severity'])
    out.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv}")

if __name__ == '__main__':
    evaluate()
