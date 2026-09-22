# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Whisper Numbers
# MAGIC
# MAGIC The presentation cut of `silver.whisper_numbers`: the same rows, without the
# MAGIC columns a downstream consumer does not use.
# MAGIC
# MAGIC Dropped, and what each cost:
# MAGIC   * `ticker` — superseded by `ap_name`, which is the stable key. A ticker is
# MAGIC     recycled between companies; `ap_name` (gvkey_iid) is not.
# MAGIC   * `release_time` — the vendor's pre-open/post-close code, present only on rows
# MAGIC     sourced from the live API. Every backhistory row is null.
# MAGIC   * `confirmed` — likewise API-only.
# MAGIC   * `source` — which bronze table a row came from; lineage, not signal.
# MAGIC
# MAGIC A full overwrite, like silver: gold is derived, and rebuilding keeps it honest
# MAGIC if a silver rule changes.

# COMMAND ----------

from earnings_whispers_etl.pins import pins
from earnings_whispers_etl.util.runtime import (
    configure_notebook_runtime,
    record_notebook_pin_usage,
)

# COMMAND ----------

runtime = configure_notebook_runtime()

SILVER_TABLE = pins.table("silver.whisper_numbers")
GOLD_TABLE = pins.table("gold.whisper_numbers", output=True)

# COMMAND ----------

out = spark.table(SILVER_TABLE).select(  # noqa: F821
    "ap_name",
    "timestamp",
    "earnings_announcement_date",
    "quarter_end_date",
    "whisper_number",
    "sentiment",
)

out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_TABLE)
spark.sql(  # noqa: F821
    f"ALTER TABLE {GOLD_TABLE} CLUSTER BY (ap_name, earnings_announcement_date)"
)

print(f"rebuilt {GOLD_TABLE}: {spark.table(GOLD_TABLE).count()} rows")  # noqa: F821

# COMMAND ----------

record_notebook_pin_usage(spark=spark, runtime=runtime)  # noqa: F821
