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

API_URL = "https://www.earningswhispers.com/api/data"
SECRET_SCOPE = "earnings-whispers-etl"

# COMMAND ----------

key = dbutils.secrets.get(scope=SECRET_SCOPE, key="api-key")  # noqa: F821
# the key is licensed percent-encoded, as it appears in the feed URLs
key = urllib.parse.unquote(key) if "%" in key else key


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

# sentiment covers the whole universe (~3k tickers); whisper covers only the companies
# with an upcoming report, so sentiment is the lookup and whisper is the driver
sentiment = {r["ticker"]: r["sentiment"] for r in fetch("sentiment")}
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

record_notebook_pin_usage(runtime)
