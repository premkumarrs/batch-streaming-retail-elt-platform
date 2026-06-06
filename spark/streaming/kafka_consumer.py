import os

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

CHECKPOINT_PATH = "data/checkpoints/retail_orders"
KAFKA_BOOTSTRAP = "kafka:9092"
POSTGRES_CONFIG = {
    "host": "postgres",
    "port": "5432",
    "database": "retail_db",
    "user": "admin",
    "password": "admin123",
}

ORDER_SCHEMA = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_status", StringType(), True),
])


def ensure_streaming_orders_table():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS streaming_orders (
            order_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            order_status VARCHAR(50) NOT NULL
        )
        """
    )
    conn.commit()
    cursor.close()
    conn.close()


def write_to_postgres(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    for row in rows:
        cursor.execute(
            """
            INSERT INTO streaming_orders
                (order_id, customer_id, order_status)
            VALUES (%s, %s, %s)
            """,
            (row.order_id, row.customer_id, row.order_status),
        )

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Batch {batch_id}: wrote {len(rows)} row(s) to streaming_orders")


os.makedirs(CHECKPOINT_PATH, exist_ok=True)
ensure_streaming_orders_table()

spark = (
    SparkSession.builder
    .appName("KafkaRetailConsumer")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    )
    .getOrCreate()
)

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", "retail_orders")
    .load()
)

parsed_df = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), ORDER_SCHEMA).alias("data"))
    .select("data.*")
    .dropna(subset=["order_id", "customer_id", "order_status"])
)

query = (
    parsed_df.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

query.awaitTermination()
