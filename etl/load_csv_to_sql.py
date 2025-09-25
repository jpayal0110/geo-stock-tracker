
# etl/load_csv_to_sql.py
import sys, os, pandas as pd
from sqlalchemy import create_engine, text

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    out = df.rename(columns={
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
    out['order_date'] = pd.to_datetime(out['order_date'], errors='coerce').dt.date
    out['delivery_date'] = pd.to_datetime(out['delivery_date'], errors='coerce').dt.date
    out['quantity'] = pd.to_numeric(out['quantity'], errors='coerce').fillna(0).astype(int)
    out['defective_units'] = pd.to_numeric(out['defective_units'], errors='coerce').round().astype('Int64')
    out['unit_price'] = pd.to_numeric(out['unit_price'], errors='coerce')
    out['negotiated_price'] = pd.to_numeric(out['negotiated_price'], errors='coerce')
    out['compliance'] = out['compliance'].fillna('Unknown')
    return out

def main():
    if len(sys.argv) < 2:
        print("Usage: python etl/load_csv_to_sql.py <csv_path>")
        sys.exit(1)
    csv_path = sys.argv[1]
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("Set DATABASE_URL, e.g. postgresql+psycopg2://user:pass@host:5432/db")
        sys.exit(1)

    engine = create_engine(db_url)
    df = pd.read_csv(csv_path)
    df = normalize(df)

    with engine.begin() as conn:
        schema_path = os.path.join('sql','schema_postgres.sql')
        with open(schema_path,'r') as f: conn.execute(text(f.read()))
        conn.execute(text("DELETE FROM procurement_orders;"))
        df.to_sql('procurement_orders', con=conn, if_exists='append', index=False)
    print(f"Loaded {len(df)} rows.")

if __name__ == "__main__":
    main()
