"""Shared constants for the template ETL."""

import os


PROJECT_NAME = "earnings-whispers-etl"
PACKAGE_NAME = "earnings_whispers_etl"

CATALOG_BASE = "earnings_whispers"

SCHEMA_ENG = "eng"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

_ENVIRONMENTS = ("dev", "stg", "prod")


def _deploy_env() -> str:
    """The environment this cluster runs in.

    Set by the bundle as `DEPLOY_ENV: ${bundle.target}` on every cluster it defines, and by
    the platform plane on the shared clusters. Environment is a property of the workspace,
    so one value is correct everywhere in it.
    """
    env = os.environ.get("DEPLOY_ENV")
    if env not in _ENVIRONMENTS:
        raise ValueError(f"DEPLOY_ENV is {env!r}; expected one of {list(_ENVIRONMENTS)}.")
    return env


# DATABRICKS_CATALOG points one run at another catalog. It is not how environment is
# resolved: being per project, it cannot serve a shared cluster.
DEFAULT_CATALOG = os.environ.get("DATABRICKS_CATALOG") or f"{CATALOG_BASE}_{_deploy_env()}"
# The bundle syncs `pins/pins.<target>.yaml` and points EARNINGS_WHISPERS_ETL_PINS_CONFIG at it, so
# nothing has to upload a file to a volume. Only a notebook run outside a job falls back.
DEFAULT_PINS_CONFIG_PATH = f"/Volumes/{DEFAULT_CATALOG}/{SCHEMA_ENG}/config/pins.yaml"
CHECKPOINT_BASE = f"/Volumes/{DEFAULT_CATALOG}/{SCHEMA_ENG}/checkpoints"
