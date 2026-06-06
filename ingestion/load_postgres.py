from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine


RAW_DATA_PATH = Path("data/raw")

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "retail_db",
    "user": "admin",
    "password": "admin123"
}


engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)


def load_table(file_name: str, table_name: str):

    file_path = RAW_DATA_PATH / file_name

    print(f"\nLoading {file_name}...")

    df = pd.read_csv(file_path)

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    print(f"Rows: {len(df)}")

    df.to_sql(
        table_name,
        engine,
        if_exists="replace",
        index=False
    )

    print(f"Loaded table: {table_name}")


if __name__ == "__main__":

    tables = [
        ("olist_orders_dataset.csv", "orders"),
        ("olist_customers_dataset.csv", "customers")
    ]

    for file_name, table_name in tables:
        load_table(file_name, table_name)

    print("\nPostgreSQL load completed.")