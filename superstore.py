import pandas as pd
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
import os
from datetime import datetime
import numpy as np

load_dotenv()
postgres_user = os.getenv('PG_USER')
postgres_pass = os.getenv('PG_PASS')

def most_frequent(series):
    mode = series.mode()
    if not mode.empty:
        return mode.iloc[0]   # return the most common value
    else:
        return series.iloc[0] # fallback: just return the first one

def transform(df):
    missing_vals = ["", " ", "NULL", "null", "Null", "N/A", "n/a", "na", "NaN", "nan", "?", "none", "None"]

    try:
        df.columns = df.columns.str.strip().str.lower().str.replace(r'[\s\-]+','_', regex=True)
        df = df.drop_duplicates()
        df = df.dropna(axis=1, how='all')
        df = df.dropna(axis=0, how='all')
        df = df.replace(missing_vals, pd.NA)

        print("DF INFO\n", df.info())
        print("DF NULL VALUES\n", df.isnull().sum())
        print("DF TYPES\n", df.dtypes)


        for col in df.columns:
            if 'date' in col:
                #df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                df[col] = pd.to_datetime(df[col], errors='coerce')

            if df[col].dtype == 'object' or df[col].dtype == 'string':
                cleaned = df[col].astype(str).str.strip().str.replace(r'[\$,\s]','', regex = True)
                converted = pd.to_numeric(cleaned, errors='coerce')
                if converted.notna().sum() > 0.9*len(df):
                    df[col] = converted
                else:
                    df[col] = df[col].astype(str).str.strip()
            
        df['delivery_days'] = (df['ship_date'] - df['order_date']).dt.days
        df['delivery_status'] = np.where(df['delivery_days'] > 5, 'Late', 'On-Time')

        df['profit_margin'] = ((df['profit'] / df['sales'])*100).round(0)
        df['discount_impact'] = (df['sales'] * df['discount']).round(2)

        df['price_bucket'] = pd.cut(
            df['profit_margin'],
            bins=[-float("inf"), 0, 10, 30, float("inf")],
            labels=['Loss', 'Low', 'Moderate', 'High']
        )

        title_case_cols = ['ship_mode','customer_name','segment','country','city','state','region','category','sub_category']
        for col in title_case_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).map(str.title)

        customer_value = (df.groupby('customer_id')[['sales','profit']].sum().reset_index())
        customer_value['clv'] = customer_value['sales'] + customer_value["profit"]
        customer_value['customer_lifetime_value'] = pd.qcut(customer_value['clv'],3,labels=['Low','Medium','High'])

        dim_product = (
            df.groupby('product_id')
            .agg({
                'category': 'first',  
                'sub_category': 'first',
                'product_name': most_frequent
            })
            .reset_index()
        )

        dim_date = df[['order_date']].drop_duplicates().copy()
        dim_date['year'] = dim_date['order_date'].dt.year
        dim_date['quarter'] = dim_date['order_date'].dt.quarter
        dim_date['month'] = dim_date['order_date'].dt.month
        dim_date['week'] = dim_date['order_date'].dt.isocalendar().week
        dim_date['weekday'] = dim_date['order_date'].dt.day_name()
        dim_date["weekend_flag"] = dim_date["order_date"].dt.weekday >= 5

        return df, dim_date, customer_value, dim_product
    except Exception as e:
        raise

def load_to_pg(df,dim_date_val,customer_val, dim_product_val):
    conn = None
    cur = None
    try: 
        dim_location_cols = df[['country','city','state','postal_code','region']].drop_duplicates()
        dim_location_cols['location_id'] = dim_location_cols.index + 1

        customer_attributes = df[['customer_id', 'customer_name', 'segment']].drop_duplicates()
        dim_customer_cols = (customer_val[['customer_id','clv','customer_lifetime_value']]
                             .merge(customer_attributes, on='customer_id', how='left')
        )

        dim_date_cols = dim_date_val
        dim_product_cols = dim_product_val
        #dim_product_cols = df[['product_id','category','sub_category','product_name']].drop_duplicates()

        df = df.merge(dim_location_cols, on=['country','city','state','postal_code','region'], how='left')
        fact_order_cols = (df[['order_id','order_date','customer_id','location_id','product_id','sales',
            'quantity','discount','profit','profit_margin','price_bucket','discount_impact', 'delivery_days',
            'delivery_status']].drop_duplicates()
        )
        
        conn = psycopg2.connect(
            host = 'localhost',
            dbname = 'superstore',
            user = postgres_user,
            password = postgres_pass,
            port = 5432
        )

        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_location(
            location_id INT PRIMARY KEY,
            country TEXT, city TEXT, 
            state TEXT, postal_code TEXT,
            region TEXT, loaded_at TIMESTAMP
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_customer(
                customer_id TEXT PRIMARY KEY,
                customer_name TEXT,
                segment TEXT,
                clv NUMERIC,
                customer_lifetime_value TEXT,
                loaded_at TIMESTAMP
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_date(
                order_date DATE PRIMARY KEY,
                year INT,
                quarter INT,
                month INT,
                week INT,
                weekday TEXT,
                weekend_flag BOOLEAN,
                loaded_at TIMESTAMP
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_product(
                product_id TEXT PRIMARY KEY,
                category TEXT,
                sub_category TEXT,
                product_name TEXT,
                loaded_at TIMESTAMP
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_order(
                row_id SERIAL PRIMARY KEY,
                order_id TEXT,
                order_date DATE REFERENCES dim_date(order_date),
                customer_id TEXT REFERENCES dim_customer(customer_id),
                location_id INT REFERENCES dim_location(location_id),
                product_id TEXT REFERENCES dim_product(product_id),
                sales NUMERIC, quantity INT,
                discount NUMERIC, profit NUMERIC,
                profit_margin INT, price_bucket TEXT,
                discount_impact NUMERIC, delivery_days NUMERIC,
                delivery_status TEXT, loaded_at TIMESTAMP
            );
        """)

        load_time = datetime.now()

        if not dim_location_cols.empty:
            dim_location_cols['loaded_at'] = load_time
            columns = dim_location_cols.columns.tolist()

            values = [tuple(row) for row in dim_location_cols[columns].to_numpy()]
            execute_values(
                cur,
                f"""
                INSERT INTO dim_location ({','.join(columns)})
                VALUES %s;
                """,
                values
            )

        if not dim_customer_cols.empty:
            dim_customer_cols['loaded_at'] = load_time
            columns = dim_customer_cols.columns.tolist()

            values = [tuple(row) for row in dim_customer_cols[columns].to_numpy()]
            execute_values(
                cur,
                f"""
                INSERT INTO dim_customer ({','.join(columns)})
                VALUES %s;
                """,
                values
            )
        
        if not dim_date_cols.empty:
            dim_date_cols['loaded_at'] = load_time
            columns = dim_date_cols.columns.tolist()
            
            values = [tuple(row) for row in dim_date_cols[columns].to_numpy()]
            execute_values(
                cur,
                f"""
                INSERT INTO dim_date ({','.join(columns)})
                VALUES %s;
                """,
                values
            )

        if not dim_product_cols.empty:
            dim_product_cols['loaded_at'] = load_time
            columns = dim_product_cols.columns.tolist()

            values = [tuple(row) for row in dim_product_cols[columns].to_numpy()]
            execute_values(
                cur,
                f"""
                INSERT INTO dim_product({','.join(columns)})
                VALUES %s
                ON CONFLICT (product_id) DO UPDATE
                SET category = EXCLUDED.category,
                    sub_category = EXCLUDED.sub_category,
                    product_name = EXCLUDED.product_name,
                    loaded_at = EXCLUDED.loaded_at;
                """,
                values
            )

        if not fact_order_cols.empty:
            fact_order_cols['loaded_at'] = load_time
            columns = fact_order_cols.columns.tolist()

            values = [tuple(row) for row in fact_order_cols[columns].to_numpy()]
            execute_values(
                cur,
                f"""
                INSERT INTO fact_order ({','.join(columns)})
                VALUES %s;
                """,
                values
            )
        conn.commit()
        #logger.info("All data successfully loaded to PostgreSQL.")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error loading to PostgreSQL: {e}")
        #logger.error(f"Error loading to PostgreSQL: {e}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def main():
    try:
        filepath = r'extracted\Sample - Superstore.csv'
        df = pd.read_csv(filepath)
        cleaned, dim_date, customer_val, product_val = transform(df)

        load_to_pg(cleaned, dim_date, customer_val, product_val)
        cleaned.to_csv(r'cleaned\superstore.csv', date_format = '%Y-%m-%d')
        dim_date.to_csv(r'cleaned\superstore_date.csv', date_format = '%Y-%m-%d')
        customer_val.to_csv(r'cleaned\superstore_customer.csv')

    except Exception as e:
        raise
if __name__=='__main__':
    main()