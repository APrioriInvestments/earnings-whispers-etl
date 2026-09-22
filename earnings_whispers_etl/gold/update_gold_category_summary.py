# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Category Summary
# MAGIC
# MAGIC Produces a simple category-level summary from the silver table.

# COMMAND ----------

from pyspark.sql import functions as F

from earnings_whispers_etl.pins import pins
from earnings_whispers_etl.util.delta_upsert import upsert_table
from earnings_whispers_etl.util.runtime import configure_notebook_runtime, record_notebook_pin_usage

# COMMAND ----------

runtime = configure_notebook_runtime()
SILVER_TABLE = pins.table("silver.products_curated", input=True)
GOLD_TABLE = pins.table("gold.category_summary", output=True)

# COMMAND ----------

gold_df = (
    spark.table(SILVER_TABLE)
    .groupBy("category")
    .agg(
        F.count("*").alias("product_count"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.sum("stock").alias("total_stock"),
        F.round(F.sum("inventory_value"), 2).alias("inventory_value"),
        F.sum(F.when(F.col("is_premium"), 1).otherwise(0)).alias("premium_products"),
    )
    .withColumn("updated_at", F.current_timestamp())
)

upsert_table(
    df=gold_df,
    table_name=GOLD_TABLE,
    key_columns=["category"],
    full_refresh=runtime.full_refresh,
)

print(f"Wrote gold table: {GOLD_TABLE}")
print(f"Row count: {spark.table(GOLD_TABLE).count()}")
record_notebook_pin_usage(spark=spark, runtime=runtime)

# COMMAND ----------

display(spark.table(GOLD_TABLE).orderBy("category"))
