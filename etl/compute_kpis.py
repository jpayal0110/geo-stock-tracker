# etl/compute_kpis.py
import os
import pandas as pd

# SLA defaults (days) per category — tweak as needed
SLA_BY_CATEGORY = {
    'Raw Materials': 14,
    'Office Supplies': 7,
    'MRO': 10,
    'Packaging': 9,
    'IT Equipment': 12
}

def prepare(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        'PO_ID': 'po_id',
        'Supplier': 'supplier',
        'Order_Date': 'order_date',
        'Delivery_Date': 'delivery_date',
        'Item_Category': 'item_category',
        'Order_Status': 'order_status',
        'Quantity': 'quantity',
        'Unit_Price': 'unit_price',
        'Negotiated_Price': 'negotiated_price',
        'Defective_Units': 'defective_units',
        'Compliance': 'compliance'
    }).copy()

    # types
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    df['delivery_date'] = pd.to_datetime(df['delivery_date'], errors='coerce')
    df['defective_units'] = pd.to_numeric(df['defective_units'], errors='coerce')
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['negotiated_price'] = pd.to_numeric(df['negotiated_price'], errors='coerce')

    # row metrics
    df['lead_time_days'] = (df['delivery_date'] - df['order_date']).dt.days
    df['sla_days'] = df['item_category'].map(SLA_BY_CATEGORY).fillna(10)
    df['on_time_flag'] = (df['lead_time_days'] <= df['sla_days']).astype('float')
    df.loc[df['delivery_date'].isna(), 'on_time_flag'] = None

    df['savings_rate'] = (df['unit_price'] - df['negotiated_price']) / df['unit_price']
    df['defect_rate_row'] = df['defective_units'] / df['quantity']
    return df

def kpis_by_supplier_month(df: pd.DataFrame) -> pd.DataFrame:
    df['month'] = df['order_date'].dt.to_period('M').dt.to_timestamp()
    grp = df.groupby(['supplier', 'month'], dropna=False)

    out = grp.apply(lambda g: pd.Series({
        'orders': g.shape[0],
        'on_time_rate': g['on_time_flag'].mean(skipna=True),
        'avg_lead_time_days': g['lead_time_days'].mean(skipna=True),
        'delivery_completion_rate': g['order_status'].eq('Delivered').mean() if g.shape[0] else None,
        'avg_savings_rate': g['savings_rate'].mean(skipna=True),
        'defect_rate': (g['defective_units'].sum(skipna=True) / g['quantity'].sum(skipna=True)
                        if g['quantity'].sum(skipna=True) else None),
        'compliance_rate': g['compliance'].eq('Yes').mean()
    })).reset_index()

    return out

def main():
    # resolve paths from project root so it works no matter where you run it
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    csv_in  = os.path.join(root, 'data', 'procurement_orders.csv')
    pretty_out = os.path.join(root, 'data', 'supplier_monthly_kpis_pretty.csv')

    if not os.path.exists(csv_in):
        raise FileNotFoundError(f"{csv_in} not found. Place your Kaggle CSV there.")

    raw = pd.read_csv(csv_in)
    df = prepare(raw)
    monthly = kpis_by_supplier_month(df)

    # # ----- RAW (rounded) -----
    # monthly_rounded = monthly.copy().round({
    #     'on_time_rate': 3,
    #     'avg_lead_time_days': 1,
    #     'delivery_completion_rate': 3,
    #     'avg_savings_rate': 3,
    #     'defect_rate': 3,
    #     'compliance_rate': 3
    # })
    # monthly_rounded.to_csv(raw_out, index=False)

    # ----- HUMAN-FRIENDLY (percent columns 0–100, 1 decimal) -----
    pretty = monthly.copy()
    pct_cols = [
        'on_time_rate',
        'delivery_completion_rate',
        'avg_savings_rate',
        'defect_rate',
        'compliance_rate'
    ]
    for c in pct_cols:
        pretty[c + '_pct'] = (pretty[c] * 100).round(1)

    pretty = pretty.drop(columns=pct_cols)  # keep only *_pct for clarity
    # Round lead time a bit as well
    pretty['avg_lead_time_days'] = pretty['avg_lead_time_days'].round(1)

    # Reorder for readability
    cols = [
        'supplier', 'month', 'orders',
        'on_time_rate_pct', 'delivery_completion_rate_pct',
        'avg_savings_rate_pct', 'defect_rate_pct', 'compliance_rate_pct',
        'avg_lead_time_days'
    ]
    pretty = pretty[cols]
    pretty.to_csv(pretty_out, index=False)

    # print(f"Wrote {raw_out}")
    print(f"Wrote {pretty_out}")

if __name__ == '__main__':
    main()
