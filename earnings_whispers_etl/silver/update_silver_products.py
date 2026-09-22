# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Curate Products
# MAGIC
# MAGIC Applies light business logic to the bronze rows and writes a curated silver table.

# COMMAND ----------

from pyspark.sql import functions as F

from earnings_whispers_etl.pins import pins
from earnings_whispers_etl.util.delta_upsert import upsert_table
from earnings_whispers_etl.util.runtime import configure_notebook_runtime, record_notebook_pin_usage

# COMMAND ----------

runtime = configure_notebook_runtime()
BRONZE_TABLE = pins.table("bronze.products_raw", input=True)
SILVER_TABLE = pins.table("silver.products_curated", output=True)

# COMMAND ----------

silver_df = (
    spark.table(BRONZE_TABLE)
    .withColumn("inventory_value", F.round(F.col("price") * F.col("stock"), 2))
    .withColumn("is_premium", F.col("price") >= F.lit(250.0))
    .withColumn("updated_at", F.current_timestamp())
)

upsert_table(
    df=silver_df,
    table_name=SILVER_TABLE,
    key_columns=["product_id"],
    full_refresh=runtime.full_refresh,
)

print(f"Wrote silver table: {SILVER_TABLE}")
print(f"Row count: {spark.table(SILVER_TABLE).count()}")
record_notebook_pin_usage(spark=spark, runtime=runtime)

# COMMAND ----------

display(spark.table(SILVER_TABLE).orderBy("product_id"))
