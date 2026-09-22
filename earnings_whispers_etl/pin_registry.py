"""Central pin registration for the Earnings Whispers ETL."""

from earnings_whispers_etl.constants import DEFAULT_CATALOG
from earnings_whispers_etl.pins import pins


# One bronze table per raw source, verbatim and immutable. The two backhistory tables are
# the vendor's xlsx extracts, loaded once each; whisper_api is appended daily from the feed.
pins.register_table(
    "bronze.whisper_backhistory_1",
    f"{DEFAULT_CATALOG}.bronze.whisper_backhistory_1",
)
pins.register_table(
    "bronze.whisper_backhistory_2",
    f"{DEFAULT_CATALOG}.bronze.whisper_backhistory_2",
)
pins.register_table("bronze.whisper_api", f"{DEFAULT_CATALOG}.bronze.whisper_api")

# Derived, and rebuilt in full on every run so the dedupe and join rules stay re-runnable
# rather than baked into whatever order things happened to land.
pins.register_table(
    "silver.whisper_numbers",
    f"{DEFAULT_CATALOG}.silver.whisper_numbers",
)

# Gold: the presentation cut -- silver without the columns a consumer of the MATLAB
# export does not use (ticker, release_time, confirmed, source).
pins.register_table("gold.whisper_numbers", f"{DEFAULT_CATALOG}.gold.whisper_numbers")

pins.register_table(
    "eng.pin_audit",
    f"{DEFAULT_CATALOG}.eng.pin_audit",
)

# No checkpoints: neither task streams. Both are batch, and silver is a full rebuild.
pins.register_volume("eng.config", f"/Volumes/{DEFAULT_CATALOG}/eng/config")
pins.register_volume("eng.logs", f"/Volumes/{DEFAULT_CATALOG}/eng/logs")
pins.register_volume("matlab.exports", f"/Volumes/{DEFAULT_CATALOG}/matlab/exports")
