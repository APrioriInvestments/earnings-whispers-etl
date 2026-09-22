"""Template package for Databricks ETL projects."""

from earnings_whispers_etl import pin_registry as _pin_registry
from earnings_whispers_etl.pins import pins

__all__ = ["pins", "_pin_registry"]
