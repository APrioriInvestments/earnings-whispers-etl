"""Notebook runtime helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from datapins import PinAuditMetadata

from earnings_whispers_etl.constants import DEFAULT_CATALOG, PROJECT_NAME
from earnings_whispers_etl.pins import pins


dbutils: Any


@dataclass(frozen=True)
class NotebookRuntime:
    full_refresh: bool
    pin_suffix: str
    audit_task_key: str
    audit_notebook_path: str
    audit_pipeline: str


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "t", "yes", "y"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", ""}:
        return False
    return default


def _set_widget_defaults() -> None:
    try:
        dbutils.widgets.text("pin_suffix", "", "Pin Suffix")
        dbutils.widgets.dropdown(
            "full_refresh", "false", ["false", "true"], "Full Refresh"
        )
        dbutils.widgets.text("audit_task_key", "", "Audit Task Key")
        dbutils.widgets.text("audit_notebook_path", "", "Audit Notebook Path")
        dbutils.widgets.text("audit_pipeline", "", "Audit Pipeline")
    except Exception:
        return


def _get_param(name: str, default: str = "") -> str:
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default


def configure_notebook_runtime() -> NotebookRuntime:
    """Create widgets and apply pin overrides for notebook execution."""
    _set_widget_defaults()
    pins.clear_dereferences()

    pin_suffix = _get_param("pin_suffix").strip()
    full_refresh = _parse_bool(_get_param("full_refresh", "false"), default=False)
    audit_task_key = _get_param("audit_task_key").strip()
    audit_notebook_path = _get_param("audit_notebook_path").strip()
    audit_pipeline = _get_param("audit_pipeline").strip()

    if pin_suffix:
        pins.apply_suffix(pin_suffix)

    return NotebookRuntime(
        full_refresh=full_refresh,
        pin_suffix=pin_suffix,
        audit_task_key=audit_task_key,
        audit_notebook_path=audit_notebook_path,
        audit_pipeline=audit_pipeline,
    )


def build_pin_audit_metadata(runtime: NotebookRuntime) -> PinAuditMetadata:
    """Build explicit audit metadata to merge with Databricks runtime inference."""
    extra_metadata = {
        "project_name": PROJECT_NAME,
        "catalog": DEFAULT_CATALOG,
        "pin_suffix": runtime.pin_suffix or None,
        "full_refresh": runtime.full_refresh,
    }
    if runtime.audit_pipeline:
        extra_metadata["pipeline_name"] = runtime.audit_pipeline

    extra_metadata = {
        key: value for key, value in extra_metadata.items() if value not in ("", None)
    }

    return PinAuditMetadata(
        notebook_path=runtime.audit_notebook_path or None,
        task_key=runtime.audit_task_key or None,
        extra_metadata=extra_metadata,
    )


def record_notebook_pin_usage(
    *,
    spark: Any,
    runtime: NotebookRuntime,
    audit_table_pin_id: str = "eng.pin_audit",
) -> int:
    """Flush tracked pin dereferences to the configured audit table."""
    written = pins.record_pin_usage(
        audit_table_pin_id,
        spark=spark,
        metadata=build_pin_audit_metadata(runtime),
    )
    if written:
        print(
            json.dumps(
                {
                    "pin_usage_rows_written": written,
                    "audit_table_pin_id": audit_table_pin_id,
                    "task_key": runtime.audit_task_key or None,
                },
                sort_keys=True,
            )
        )
    return written
