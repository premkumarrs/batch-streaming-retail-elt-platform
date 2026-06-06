from sqlalchemy import create_engine, text
import pandas as pd
import glob


DATABASE_URL = "postgresql://admin:admin123@postgres:5432/retail_db"

engine = create_engine(DATABASE_URL)


def _find_part_csv(directory: str) -> str:
    matches = glob.glob(f"{directory}/part-*.csv")
    if not matches:
        raise FileNotFoundError(
            f"No Spark output CSV found in {directory}. Run spark_cleaning first."
        )
    return matches[0]


def load_dataframe(df: pd.DataFrame, table_name: str) -> None:
    with engine.begin() as conn:
        table_exists = conn.execute(
            text("SELECT to_regclass(:table_ref) IS NOT NULL"),
            {"table_ref": f"public.{table_name}"},
        ).scalar()

        if table_exists:
            conn.execute(text(f'TRUNCATE TABLE "{table_name}"'))
            df.to_sql(table_name, conn, if_exists="append", index=False)
        else:
            df.to_sql(table_name, conn, if_exists="replace", index=False)


orders_file = _find_part_csv("data/processed/cleaned_orders")
customers_file = _find_part_csv("data/processed/cleaned_customers")

orders_df = pd.read_csv(orders_file)
customers_df = pd.read_csv(customers_file)

print("Loading orders table...")
load_dataframe(orders_df, "orders")

print("Loading customers table...")
load_dataframe(customers_df, "customers")

print("PostgreSQL loading completed.")
