"""Monitoring metrics for the template ETL.

Loaded by the shared ``databricks-monitoring`` ``collect_metrics`` notebook, which imports
this module and reads ``DATASETS``. The environment comes from ``DEPLOY_ENV`` on the shared
monitoring cluster, so the monitoring job watches whatever environment it runs in.
"""

import os

from databricks_monitoring import (
    DatabricksTableTimestampGetter,
    Dataset,
    FreshnessChecker,
    Gauge,
)

# Duplicated from `earnings_whispers_etl.constants`, not imported: the shared monitoring cluster does
# not install this project's wheel.
CATALOG_BASE = "earnings_whispers"

# databricks-monitoring's environment name for each deploy environment.
_ENVIRONMENTS = {"dev": "DEV", "stg": "STAGING", "prod": "PROD"}


def _deploy_env() -> str:
    """The environment this cluster runs in. Set on every cluster the platform plane owns."""
    env = os.environ.get("DEPLOY_ENV")
    if env not in _ENVIRONMENTS:
        raise ValueError(
            f"DEPLOY_ENV is {env!r}; expected one of {sorted(_ENVIRONMENTS)}. "
            "The shared monitoring cluster sets it."
        )
    return env


_ENV = _deploy_env()
CATALOG = f"{CATALOG_BASE}_{_ENV}"
ENVIRONMENT = _ENVIRONMENTS[_ENV]

EARNINGS_WHISPERS_ETL_METRICS = [
    Gauge(
        getter=DatabricksTableTimestampGetter(
            table_path=f"{CATALOG}.gold.category_summary",
            timestamp_column="updated_at",
        ),
        range_checker=FreshnessChecker(
            should_be_seconds=24 * 3600,
            must_be_seconds=48 * 3600,
        ),
        frequency="DAILY",
        data_format="DELTA_LAKE",
        medallion_layer="GOLD",
    ),
]

DATASETS = [Dataset(name="earnings-whispers-etl", environment=ENVIRONMENT, metrics=EARNINGS_WHISPERS_ETL_METRICS)]
