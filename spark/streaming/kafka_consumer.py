from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType
)

import psycopg2


spark = (
    SparkSession.builder
    .appName("KafkaRetailConsumer")
    .master("local[*]")
    .getOrCreate()
)

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("product", StringType()),
    StructField("category", StringType()),
    StructField("price", FloatType()),
    StructField("quantity", IntegerType()),
    StructField("order_date", StringType())
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "retail_kafka:9092")
    .option("subscribe", "retail_orders")
    .load()
)

parsed_df = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)


def write_to_postgres(batch_df, batch_id):

    rows = batch_df.collect()

    conn = psycopg2.connect(
        host="retail_postgres",
        port="5432",
        database="retail_db",
        user="admin",
        password="admin123"
    )

    cursor = conn.cursor()

    for row in rows:
        cursor.execute(
            """
            INSERT INTO streaming_orders
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                row.order_id,
                row.product,
                row.category,
                row.price,
                row.quantity,
                row.order_date
            )
        )

    conn.commit()

    cursor.close()
    conn.close()

    print(f"Batch {batch_id} written to PostgreSQL")


query = (
    parsed_df.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .start()
)

query.awaitTermination()