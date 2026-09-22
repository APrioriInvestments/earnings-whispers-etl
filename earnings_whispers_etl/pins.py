"""Repo-local adapter for the shared datapins package."""

from __future__ import annotations

from datapins import PinConventions, PinResolver, build_env_config_loader

from earnings_whispers_etl.constants import (
    CHECKPOINT_BASE,
    DEFAULT_CATALOG,
    DEFAULT_PINS_CONFIG_PATH,
)


pins = PinResolver(
    conventions=PinConventions.for_catalog(
        DEFAULT_CATALOG,
        checkpoint_base=CHECKPOINT_BASE,
    ),
    config_loader=build_env_config_loader(
        env_var="EARNINGS_WHISPERS_ETL_PINS_CONFIG",
        default_path=DEFAULT_PINS_CONFIG_PATH,
    ),
)
