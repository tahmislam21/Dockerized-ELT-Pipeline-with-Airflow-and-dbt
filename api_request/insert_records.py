import psycopg2
from api_request import fetch_data

def connect_to_db():
    print("Connecting to the PostgreSQL database...")
    try:
        conn = psycopg2.connect(
            host='db',## when running isolated scale, convert to "localhost"
            port = 5432, ## when running isolated scale, convert to 5000
            dbname = 'db',
            user = 'db_user',
            password = 'db_password'
        )
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise

def create_table(conn):
    print("Create table if not exists...")
    try:
        cursor = conn.cursor()         
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS dev.raw_sales_data (
                id SERIAL PRIMARY KEY,
                order_id              INTEGER,
                order_date            DATE,
                customer_id           INTEGER,
                customer_name         TEXT,
                city                  TEXT,
                state                 TEXT,
                country_region        TEXT,
                salesperson           TEXT,
                region                TEXT,
                shipped_date          DATE,
                shipper_name          TEXT,
                ship_name             TEXT,
                ship_address          TEXT,
                ship_city             TEXT,
                ship_state            TEXT,
                ship_country_region   TEXT,
                payment_type          TEXT,
                product_name          TEXT,
                category              TEXT,
                unit_price            NUMERIC(10,2),
                quantity              FLOAT,
                revenue               FLOAT,
                shipping_fee          FLOAT,
                revenue_bins          FLOAT,
                inserted_at TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        print("Table was created")

    except psycopg2.Error as e:
        print(f"Failed to create table: {e}")
        raise

def insert_records(conn, df):
    print("Inserting sales data into the database")
    try:
        cursor = conn.cursor()
        for i, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO dev.raw_sales_data (
                    order_id,
                    order_date,
                    customer_id,
                    customer_name,
                    city,
                    state,
                    country_region,
                    salesperson,
                    region,
                    shipped_date,
                    shipper_name,
                    ship_name,
                    ship_address,
                    ship_city,
                    ship_state,
                    ship_country_region,
                    payment_type,
                    product_name,
                    category,
                    unit_price,
                    quantity,
                    revenue,
                    shipping_fee,
                    revenue_bins,
                    inserted_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                """,
                (
                    row['Order ID'],
                    row['Order Date'],
                    row['Customer ID'],
                    row['Customer Name'],
                    row['City'],
                    row['State'],
                    row['Country/Region'],
                    row['Salesperson'],
                    row['Region'],
                    row['Shipped Date'],
                    row['Shipper Name'],
                    row['Ship Name'],
                    row['Ship Address'],
                    row['Ship City'],
                    row['Ship State'],
                    row['Ship Country/Region'],
                    row['Payment Type'],
                    row['Product Name'],
                    row['Category'],
                    row['Unit Price'],
                    row['Quantity'],
                    row['Revenue'],
                    row['Shipping Fee'],
                    row['Revenue (Bins)']
                )
        )
        conn.commit()
        print("Data Successfully inserted")

    except psycopg2.Error as e:
        print(f"Error inserting data into the database: {e}")
        print(f"Row {i} failed: {e}")
        print(row)
        raise

def main():
    try:
        df = fetch_data()
        conn = connect_to_db()
        create_table(conn)
        insert_records(conn, df)
    except Exception as e:
        print(f"An error occured during execution: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")
