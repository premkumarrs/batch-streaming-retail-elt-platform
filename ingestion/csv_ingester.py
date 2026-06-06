from pathlib import Path
import pandas as pd


RAW_DATA_PATH = Path("data/raw")
PROCESSED_DATA_PATH = Path("data/processed")


def process_csv(file_name: str):
    file_path = RAW_DATA_PATH / file_name

    print(f"\nReading {file_name}...")

    df = pd.read_csv(file_path)

    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")

    # clean column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # remove duplicates
    df = df.drop_duplicates()

    output_name = file_name.replace(".csv", ".parquet")

    output_path = PROCESSED_DATA_PATH / output_name

    print(f"Writing parquet file: {output_name}")

    df.to_parquet(output_path, index=False)

    print("Finished.")


if __name__ == "__main__":

    PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)

    csv_files = [
        "olist_orders_dataset.csv",
        "olist_customers_dataset.csv",
        "olist_products_dataset.csv"
    ]

    for file in csv_files:
        process_csv(file)