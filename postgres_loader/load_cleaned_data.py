from sqlalchemy import create_engine
import pandas as pd
import glob


DATABASE_URL = "postgresql://admin:admin123@postgres:5432/retail_db"

engine = create_engine(DATABASE_URL)


orders_file = glob.glob(
    "data/processed/cleaned_orders/part-*.csv"
)[0]

customers_file = glob.glob(
    "data/processed/cleaned_customers/part-*.csv"
)[0]


orders_df = pd.read_csv(orders_file)

customers_df = pd.read_csv(customers_file)


print("Loading orders table...")

orders_df.to_sql(
    "orders",
    engine,
    if_exists="replace",
    index=False
)


print("Loading customers table...")

customers_df.to_sql(
    "customers",
    engine,
    if_exists="replace",
    index=False
)


print("PostgreSQL loading completed.")