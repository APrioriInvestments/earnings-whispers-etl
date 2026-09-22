# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Ingest Products
# MAGIC
# MAGIC Loads a small batch of example product rows into the bronze layer.

# COMMAND ----------

from pyspark.sql import functions as F

from earnings_whispers_etl.pins import pins
from earnings_whispers_etl.util.delta_upsert import upsert_table
from earnings_whispers_etl.util.runtime import configure_notebook_runtime, record_notebook_pin_usage

# COMMAND ----------

runtime = configure_notebook_runtime()
BRONZE_TABLE = pins.table("bronze.products_raw", output=True)

# COMMAND ----------

sample_rows = [
    {
        "product_id": "P001",
        "name": "Laptop",
        "category": "Electronics",
        "price": 1200.0,
        "stock": 15,
        "source_batch": "seed_batch",
    },
    {
        "product_id": "P002",
        "name": "Desk Chair",
        "category": "Furniture",
        "price": 350.0,
        "stock": 30,
        "source_batch": "seed_batch",
    },
    {
        "product_id": "P003",
        "name": "Monitor",
        "category": "Electronics",
        "price": 400.0,
        "stock": 25,
        "source_batch": "seed_batch",
    },
    {
        "product_id": "P004",
        "name": "Keyboard",
        "category": "Electronics",
        "price": 75.0,
        "stock": 80,
        "source_batch": "seed_batch",
    },
    {
        "product_id": "P005",
        "name": "Standing Desk",
        "category": "Furniture",
        "price": 800.0,
        "stock": 12,
        "source_batch": "seed_batch",
    },
]

# COMMAND ----------

bronze_df = (
    spark.createDataFrame(sample_rows)
    .withColumn("ingested_at", F.current_timestamp())
)

upsert_table(
    df=bronze_df,
    table_name=BRONZE_TABLE,
    key_columns=["product_id"],
    full_refresh=runtime.full_refresh,
)

print(f"Wrote bronze table: {BRONZE_TABLE}")
print(f"Row count: {spark.table(BRONZE_TABLE).count()}")
record_notebook_pin_usage(spark=spark, runtime=runtime)

# COMMAND ----------

display(spark.table(BRONZE_TABLE).orderBy("product_id"))
