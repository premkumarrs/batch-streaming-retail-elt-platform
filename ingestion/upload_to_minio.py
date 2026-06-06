from pathlib import Path
from minio import Minio


MINIO_CLIENT = Minio(
    "localhost:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)

BUCKET_NAME = "retail-data"

PROCESSED_DATA_PATH = Path("data/processed")


def upload_file(file_path: Path):

    object_name = f"raw/{file_path.name}"

    print(f"Uploading {file_path.name}...")

    MINIO_CLIENT.fput_object(
        BUCKET_NAME,
        object_name,
        str(file_path)
    )

    print(f"Uploaded to MinIO: {object_name}")


if __name__ == "__main__":

    parquet_files = list(PROCESSED_DATA_PATH.glob("*.parquet"))

    print(f"Found {len(parquet_files)} parquet files")

    for file in parquet_files:
        upload_file(file)

    print("\nAll uploads completed.")