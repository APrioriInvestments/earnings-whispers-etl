# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Matlabify the Whisper Numbers
# MAGIC
# MAGIC Exports `gold.whisper_numbers` to a single MAT v5 file, one struct per `ap_name`
# MAGIC (`XX__<GVKEY>_<IID>`) holding that security's full history:
# MAGIC `/Volumes/{catalog}/matlab/exports/whisper_numbers/all.mat`.
# MAGIC
# MAGIC Serialization uses the gs implementation, ported into
# MAGIC `util/matlab_export.py`, rather than the newer shared
# MAGIC `databricks_export_matlab` library — so this dataset's `.mat` matches what the
# MAGIC MATLAB side already loads for gs and jpm.
# MAGIC
# MAGIC **Timezone.** Everything upstream is stored UTC. `datenumify_pandas` converts the
# MAGIC instant column to Eastern wall-clock datenums here, at the export boundary, which
# MAGIC is the only place a timezone belongs. The two `date` columns go through
# MAGIC `datenumify_dates` instead and are *not* shifted: a date is a label, not an
# MAGIC instant, and routing it through UTC->Eastern would land each one at 19:00 the
# MAGIC previous day.
# MAGIC
# MAGIC **Pre-2011 rows have no time component.** The vendor recorded a date only, so
# MAGIC those timestamps are midnight ET — absence of data rather than an observed time.
# MAGIC Roughly 12,400 of the exported rows are affected, all backhistory. Rows that do
# MAGIC carry a time cluster at 07:00-09:00 ET with a second bump at 16:00-18:00.
# MAGIC
# MAGIC **Grain.** One row per `(ap_name, timestamp)`, not per report. Rows sourced from
# MAGIC the live API are snapshots: a company with a report three weeks out appears once
# MAGIC per daily pull, so its whisper's revision path is visible. Rows from the vendor's
# MAGIC backhistory are a single final value per quarter.

# COMMAND ----------

from earnings_whispers_etl.pins import pins
from earnings_whispers_etl.util.matlab_export import export_mat, verify_mat
from earnings_whispers_etl.util.runtime import (
    configure_notebook_runtime,
    record_notebook_pin_usage,
)

# COMMAND ----------

runtime = configure_notebook_runtime()

GOLD_TABLE = pins.table("gold.whisper_numbers")
EXPORT_DIR = pins.volume("matlab.exports")
EXPORT_PATH = f"{EXPORT_DIR}/whisper_numbers/all.mat"

DATE_COLS = frozenset({"earnings_announcement_date", "quarter_end_date"})

# COMMAND ----------

df = spark.table(GOLD_TABLE)  # noqa: F821
n_structs = export_mat(df, EXPORT_PATH, date_cols=DATE_COLS)
print(f"wrote {EXPORT_PATH}  ({df.count():,} rows, {n_structs:,} ap_names)")

# COMMAND ----------

# round-trip the file rather than trusting the write; whisper_number is the payload,
# so its absence means the export is useless even though savemat succeeded
n_verified = verify_mat(EXPORT_PATH, required_fields=("timestamp", "whisper_number"))
print(f"verified {EXPORT_PATH}: {n_verified:,} structs, required fields present")

# COMMAND ----------

record_notebook_pin_usage(spark=spark, runtime=runtime)  # noqa: F821
