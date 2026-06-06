import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# Remove old output folders if they exist
if os.path.exists("data/processed/cleaned_orders"):
    shutil.rmtree("data/processed/cleaned_orders")

if os.path.exists("data/processed/cleaned_customers"):
    shutil.rmtree("data/processed/cleaned_customers")


spark = (
    SparkSession.builder
    .appName("RetailDataCleaning")
    .master("local[*]")
    .config("spark.hadoop.io.native.lib.available", "false")
    .config(
        "spark.hadoop.fs.file.impl",
        "org.apache.hadoop.fs.LocalFileSystem"
    )
    .config(
        "spark.hadoop.fs.file.impl.disable.cache",
        "true"
    )
    .getOrCreate()
)


orders_df = spark.read.parquet(
    "data/processed/olist_orders_dataset.parquet"
)

customers_df = spark.read.parquet(
    "data/processed/olist_customers_dataset.parquet"
)


orders_df = orders_df.dropDuplicates()

customers_df = customers_df.dropDuplicates()


orders_df = orders_df.filter(
    col("order_id").isNotNull()
)

customers_df = customers_df.filter(
    col("customer_id").isNotNull()
)


print("\nOrders Count:")
print(orders_df.count())

print("\nCustomers Count:")
print(customers_df.count())


orders_df.coalesce(1).write.mode("overwrite").csv(
    "data/processed/cleaned_orders",
    header=True
)

customers_df.coalesce(1).write.mode("overwrite").csv(
    "data/processed/cleaned_customers",
    header=True
)


print("\nCleaning completed.")

spark.stop()