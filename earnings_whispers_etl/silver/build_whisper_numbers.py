# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: Whisper Numbers
# MAGIC
# MAGIC Rebuilds `silver.whisper_numbers` from the three bronze sources. A full rebuild,
# MAGIC not incremental — bronze is small (~75k rows) and rebuilding keeps the dedupe and
# MAGIC join rules re-runnable rather than baked into whatever order things landed.
# MAGIC
# MAGIC The backhistory extracts overlap: a chunk's final day holds reports that had not
# MAGIC happened at extract time, dated one day early, which the next chunk restates with
# MAGIC the real date (same whisper, same `WhisperDate`, `Date` +1). The later extract wins
# MAGIC per `(ticker, quarter_end_date)`.
# MAGIC
# MAGIC API rows are never deduped — one row per pull is the revision path, a different
# MAGIC grain from the backhistory's one-final-value-per-quarter.

# COMMAND ----------

from databricks_etl_utils import read_parquet
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from earnings_whispers_etl.pins import pins
from earnings_whispers_etl.util.runtime import (
    configure_notebook_runtime,
    record_notebook_pin_usage,
)

# COMMAND ----------

runtime = configure_notebook_runtime()

BACKHISTORY_1 = pins.table("bronze.whisper_backhistory_1")
BACKHISTORY_2 = pins.table("bronze.whisper_backhistory_2")
WHISPER_API = pins.table("bronze.whisper_api")
SILVER_TABLE = pins.table("silver.whisper_numbers", output=True)

SEC_IDHIST = "/Volumes/ap_equities_prod/bronze/snowflake/sec_idhist/all.parquet"

# COMMAND ----------


def backhistory(table: str, extract_rank: int):
    """One vendor xlsx extract, normalised to the silver column set."""
    return spark.table(table).select(  # noqa: F821
        F.col("Ticker").alias("ticker"),
        # WhisperDate is TIMESTAMP_NTZ holding a US Eastern wall clock, and the session
        # runs in UTC — a plain cast would read 07:49 as 07:49Z and shift every row 4-5h,
        # DST-dependent. to_utc_timestamp reads it as Eastern and yields the right instant.
        F.to_utc_timestamp(F.col("WhisperDate"), "America/New_York").alias("timestamp"),
        F.col("Date").alias("earnings_announcement_date"),
        F.col("QuarterDate").alias("quarter_end_date"),
        F.col("Whisper").cast("double").alias("whisper_number"),
        F.col("Sentiment").cast("double").alias("sentiment"),
        F.lit(None).cast("int").alias("release_time"),
        F.lit(None).cast("boolean").alias("confirmed"),
        F.lit(table.rsplit(".", 1)[-1]).alias("source"),
        F.lit(extract_rank).alias("_rank"),
    )


hist = backhistory(BACKHISTORY_1, 1).unionByName(backhistory(BACKHISTORY_2, 2))

# later extract wins per company-quarter
newest = Window.partitionBy("ticker", "quarter_end_date").orderBy(F.col("_rank").desc())
hist = hist.withColumn("_rn", F.row_number().over(newest)).filter("_rn = 1").drop("_rn", "_rank")

# a whisper recorded after its own report date is lookahead leakage
hist = hist.filter(
    F.col("timestamp").isNull()
    | (F.to_date("timestamp") <= F.col("earnings_announcement_date"))
)

# COMMAND ----------

api = spark.table(WHISPER_API).select(  # noqa: F821
    "ticker",
    "timestamp",
    "earnings_announcement_date",
    # the whisper feed carries no quarter identifier, unlike the xlsx extracts
    F.lit(None).cast("date").alias("quarter_end_date"),
    "whisper_number",
    "sentiment",
    "release_time",
    "confirmed",
    F.lit("whisper_api").alias("source"),
)

combined = hist.unionByName(api)
n_in = combined.count()  # no .cache(): PERSIST TABLE is unsupported on serverless

# COMMAND ----------

# read_parquet rather than spark.read.parquet: this is a prod volume, and its producer
# can rewrite the file mid-read and hand back partial data. read_parquet snapshots to the
# driver first. Small enough to broadcast at the join.
tickers = (
    read_parquet(SEC_IDHIST)
    .filter(F.col("ITEM") == "TIC")
    .select(
        F.col("ITEMVALUE").alias("tic"),
        F.to_date("EFFFROM").alias("eff_from"),
        F.to_date("EFFTHRU").alias("eff_thru"),
        F.concat_ws("_", "GVKEY", "IID").alias("ap_name"),
    )
)

# ap_name = gvkey_iid, matching ap_equities.wsh.wsh_earnings_utilities: join on ticker
# gated by the effective window, so a recycled ticker resolves to the security that held
# it on the report date. Inner: a ticker that does not resolve is not usable downstream.
out = (
    combined.join(
        F.broadcast(tickers),
        (combined.ticker == tickers.tic)
        & (combined.earnings_announcement_date >= tickers.eff_from)
        & (combined.earnings_announcement_date <= tickers.eff_thru),
        "inner",
    ).select(
        "ap_name",
        "ticker",
        "timestamp",
        "earnings_announcement_date",
        "quarter_end_date",
        "whisper_number",
        "sentiment",
        "release_time",
        "confirmed",
        "source",
    )
)

# COMMAND ----------

out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLE)
spark.sql(  # noqa: F821
    f"ALTER TABLE {SILVER_TABLE} CLUSTER BY (ap_name, earnings_announcement_date)"
)

# COMMAND ----------

n = spark.table(SILVER_TABLE).count()  # noqa: F821
dupes = spark.sql(  # noqa: F821
    f"""SELECT count(*) c FROM (
          SELECT ap_name, quarter_end_date FROM {SILVER_TABLE}
          WHERE source <> 'whisper_api'
          GROUP BY 1,2 HAVING count(*) > 1)"""
).collect()[0]["c"]
assert dupes == 0, f"{dupes} duplicate (ap_name, quarter_end_date) in backhistory"

# the inner join silently discards unresolvable tickers, so say how many
print(f"rebuilt {SILVER_TABLE}: {n} rows, 0 duplicate backhistory keys, "
      f"{n_in - n} rows dropped with no ap_name")

# COMMAND ----------

record_notebook_pin_usage(runtime)
