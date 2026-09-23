# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Ingest Whispers
# MAGIC
# MAGIC Appends today's raw whisper + sentiment snapshot to `bronze.whisper_api`.
# MAGIC
# MAGIC Raw landing only — no `ap_name`, no dedupe. Everything derived happens in
# MAGIC `silver/build_whisper_numbers`, so the same rules apply to the API and to the
# MAGIC backhistory extracts.
# MAGIC
# MAGIC The API is snapshot-only with no history endpoint, so a day not pulled is gone
# MAGIC for good. Each run appends, which gives the whisper's revision path as it moves
# MAGIC ahead of the report — a finer grain than the backhistory's one value per quarter.

# COMMAND ----------

import urllib.parse
from datetime import datetime, timezone

import requests

from earnings_whispers_etl.pins import pins
from earnings_whispers_etl.util.runtime import (
    configure_notebook_runtime,
    record_notebook_pin_usage,
)

# COMMAND ----------

runtime = configure_notebook_runtime()
BRONZE_TABLE = pins.table("bronze.whisper_api", output=True)
SENTIMENT_TABLE = pins.table("bronze.sentiment_api", output=True)

API_URL = "https://www.earningswhispers.com/api/data"
SECRET_SCOPE = "earnings-whispers-etl"

# COMMAND ----------

key = dbutils.secrets.get(scope=SECRET_SCOPE, key="api-key")  # noqa: F821
# the key is licensed percent-encoded, as it appears in the feed URLs
key = urllib.parse.unquote(key) if "%" in key else key


def _d(s):
    """Vendor ISO date string -> date. Absent and empty both mean no value."""
    return datetime.fromisoformat(s).date() if s else None


def fetch(dataset: str) -> list[dict]:
    r = requests.get(
        API_URL,
        params={"d": dataset, "q": key},
        timeout=60,
        allow_redirects=False,
    )
    # a bad key redirects to the sign-up page as HTML with status 200, so refusing
    # redirects is what turns a silent junk write into a failure
    if r.is_redirect:
        raise RuntimeError(f"d={dataset} redirected to {r.headers.get('Location')}")
    r.raise_for_status()
    return r.json()


# COMMAND ----------

pull_ts = datetime.now(timezone.utc)

# One fetch, two tables. `whisper_api` keeps only the companies with an upcoming
# report; `sentiment_api` keeps the whole ~3.2k-ticker panel. Sharing the fetch means
# both tables describe the same snapshot rather than two calls minutes apart.
sentiment_rows = fetch("sentiment")
sentiment = {r["ticker"]: r["sentiment"] for r in sentiment_rows}
rows = [
    (
        r["ticker"],
        pull_ts,
        datetime.fromisoformat(r["nextEPSDate"]).date(),
        r["whisper"],
        sentiment.get(r["ticker"]),
        r["releaseTime"],
        r["confirmed"] == "Yes",
    )
    for r in fetch("whisper")
]
if not rows:
    raise RuntimeError(f"whisper feed was empty at {pull_ts.isoformat()}")

# COMMAND ----------

(
    spark.createDataFrame(  # noqa: F821
        rows,
        "ticker string, timestamp timestamp, earnings_announcement_date date, "
        "whisper_number double, sentiment double, release_time int, confirmed boolean",
    )
    .write.mode("append")
    .saveAsTable(BRONZE_TABLE)
)

print(f"{pull_ts.isoformat()}: appended {len(rows)} rows to {BRONZE_TABLE}")

# COMMAND ----------

# The full sentiment panel. `whisper_api` keeps sentiment only for the handful of
# tickers reporting soon, which discards a daily reading for ~3.2k others -- and the
# feed is snapshot-only with no history endpoint, so a day not landed is gone. Measured
# over the first two weeks: whisper_number never changed across pulls (0 of 63 reports)
# while sentiment changed for 52 of them, so this is the half that actually moves.
#
# Every vendor field is kept: they are all part of the snapshot and none can be
# recovered later. `currentDate` becomes `as_of_date` -- `current_date` is a Spark
# builtin and an awkward column name to quote everywhere downstream.
panel = [
    (
        r["ticker"],
        pull_ts,
        _d(r.get("currentDate")),
        _d(r.get("firstDate")),
        _d(r.get("lastDate")),
        r.get("sentiment"),
        r.get("sentChange"),
        r.get("avgSent"),
        r.get("total"),
        r.get("avgMonthlyGain"),
        r.get("avgQuarterlyGain"),
        r.get("monthlySuccess"),
        r.get("quarterlySuccess"),
        r.get("sentChangeTotal"),
        r.get("sentChangeMonthlyGain"),
        r.get("sentChangeQuarterlyGain"),
        r.get("sentChangeMonthlySuccess"),
        r.get("sentChangeQuarterlySuccess"),
        r.get("combinedTotal"),
        r.get("combinedMonthlyGain"),
        r.get("combinedQuarterlyGain"),
        r.get("combinedMonthlySuccess"),
        r.get("combinedQuarterlySuccess"),
    )
    for r in sentiment_rows
]
if not panel:
    raise RuntimeError(f"sentiment feed was empty at {pull_ts.isoformat()}")

(
    spark.createDataFrame(  # noqa: F821
        panel,
        "ticker string, timestamp timestamp, as_of_date date, first_date date, "
        "last_date date, sentiment double, sent_change double, avg_sent double, "
        "total int, avg_monthly_gain double, avg_quarterly_gain double, "
        "monthly_success double, quarterly_success double, sent_change_total int, "
        "sent_change_monthly_gain double, sent_change_quarterly_gain double, "
        "sent_change_monthly_success double, sent_change_quarterly_success double, "
        "combined_total int, combined_monthly_gain double, "
        "combined_quarterly_gain double, combined_monthly_success double, "
        "combined_quarterly_success double",
    )
    .write.mode("append")
    .saveAsTable(SENTIMENT_TABLE)
)

print(f"{pull_ts.isoformat()}: appended {len(panel)} rows to {SENTIMENT_TABLE}")

# COMMAND ----------

record_notebook_pin_usage(spark=spark, runtime=runtime)  # noqa: F821
