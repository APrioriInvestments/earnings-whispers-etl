# Databricks notebook source
# MAGIC %md
# MAGIC # Decommission Template Resources
# MAGIC
# MAGIC Drops the example tables and removes their checkpoint directories.

# COMMAND ----------

from earnings_whispers_etl.pins import pins

# COMMAND ----------

TABLE_PINS = (
    "gold.category_summary",
    "silver.products_curated",
    "bronze.products_raw",
)
CHECKPOINT_PINS = (
    "silver_products_curated",
    "bronze_products_raw",
)

# COMMAND ----------

for pin_id in TABLE_PINS:
    table_name = pins.table(pin_id)
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    print(f"Dropped table: {table_name}")

# COMMAND ----------

for pin_id in CHECKPOINT_PINS:
    checkpoint_path = pins.checkpoint(pin_id)
    try:
        dbutils.fs.rm(checkpoint_path, True)
        print(f"Removed checkpoint path: {checkpoint_path}")
    except Exception as exc:
        print(f"Could not remove checkpoint path {checkpoint_path}: {exc}")

# COMMAND ----------

print("Decommissioning complete.")
