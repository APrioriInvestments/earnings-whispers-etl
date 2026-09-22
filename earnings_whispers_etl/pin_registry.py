"""Central pin registration for the template ETL."""

from earnings_whispers_etl.constants import CHECKPOINT_BASE, DEFAULT_CATALOG
from earnings_whispers_etl.pins import pins


pins.register_table("bronze.products_raw", f"{DEFAULT_CATALOG}.bronze.products_raw")
pins.register_table(
    "silver.products_curated",
    f"{DEFAULT_CATALOG}.silver.products_curated",
)
pins.register_table(
    "gold.category_summary",
    f"{DEFAULT_CATALOG}.gold.category_summary",
)
pins.register_table(
    "eng.pin_audit",
    f"{DEFAULT_CATALOG}.eng.pin_audit",
)

pins.register_checkpoint(
    "bronze_products_raw",
    f"{CHECKPOINT_BASE}/bronze_products_raw",
)
pins.register_checkpoint(
    "silver_products_curated",
    f"{CHECKPOINT_BASE}/silver_products_curated",
)

pins.register_volume("eng.config", f"/Volumes/{DEFAULT_CATALOG}/eng/config")
pins.register_volume("eng.logs", f"/Volumes/{DEFAULT_CATALOG}/eng/logs")
pins.register_volume("eng.checkpoints", f"/Volumes/{DEFAULT_CATALOG}/eng/checkpoints")
