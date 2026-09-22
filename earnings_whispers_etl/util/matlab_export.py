"""MAT v5 export, ported from `gs_etl/gold_lib.py`.

`make_matlab_name`, `datenumify_pandas`, `export_mat` and `verify_mat` are the gs
implementations, kept so this dataset's `.mat` matches what the MATLAB side already
loads for gs and jpm rather than the newer shared `databricks_export_matlab` library.

One addition: gs exports a single `timestamp` column, and this dataset also carries
two plain `date` columns. Those are serialized by `datenumify_dates`, which does NOT
go through a timezone — a date is a label, not an instant, and pushing it through
UTC->Eastern would shift every one back a day.
"""

from __future__ import annotations

import io
import os


def make_matlab_name(name):
    """Convert a name to a valid MATLAB variable name."""
    if not isinstance(name, str):
        name = str(name)

    if name == "":
        return "XX__"

    name = (
        name.replace(" ", "_")
        .replace("@", "ATSIGN")
        .replace("-", "MNS")
        .replace("/", "SLASH")
        .replace(".", "DOT")
        .replace(",", "COMMA")
        .replace("(", "PAREN")
        .replace(")", "PAREN")
        .replace(">", "GT")
        .replace("<", "LT")
        .replace("&", "AND")
    )

    if name[:1].isdigit():
        name = "XX__" + name

    for i in range(len(name)):
        if not name[i].isalnum():
            name = name[:i] + "_" + name[i + 1 :]

    if len(name) > 63:
        name = name[:63]

    return name


def datenumify_pandas(series):
    """Pandas datetimes -> MATLAB datenum in Eastern Time (naive; datenum has
    no timezone — values are wall-clock America/New_York)."""
    import pandas as pd

    if series.dt.tz is None:
        series = series.dt.tz_localize("UTC")
    series = series.dt.tz_convert("America/New_York")
    series = series.dt.tz_localize(None)
    epoch = pd.Timestamp("1970-01-01")
    days_since_epoch = (series - epoch).dt.total_seconds() / 86400
    return days_since_epoch + 719529


def datenumify_dates(series):
    """Plain dates -> MATLAB datenum, with no timezone conversion.

    Not in gs, which exports no date columns. A date is a label rather than an
    instant: routing it through UTC->Eastern like `datenumify_pandas` does would
    land every value at 19:00 the previous day.
    """
    import pandas as pd

    series = pd.to_datetime(series)
    if getattr(series.dt, "tz", None) is not None:
        series = series.dt.tz_localize(None)
    return (series - pd.Timestamp("1970-01-01")).dt.days.astype("float64") + 719529


def export_mat(gold_df, out_path: str, string_cols=frozenset(), date_cols=frozenset()):
    """Gold spark frame -> one .mat of per-ap_name structs at out_path.

    string_cols are kept as cell-array fields (excluded from the numeric
    fillna/double coercion); their NULLs become "" (savemat cannot serialize
    None inside an object array — the gold table keeps the proper NULLs).
    date_cols are serialized as unshifted datenums; see `datenumify_dates`.
    Returns the number of ap_name structs written.
    """
    import numpy as np
    import pandas as pd
    import scipy.io

    pdf = gold_df.toPandas()
    pdf["timestamp"] = datenumify_pandas(pd.to_datetime(pdf["timestamp"]))
    for col in date_cols:
        if col in pdf.columns:
            pdf[col] = datenumify_dates(pdf[col])

    for col in pdf.columns:
        if col in ("timestamp", "ap_name") or col in date_cols:
            continue
        if col in string_cols:
            pdf[col] = pdf[col].fillna("")
        else:
            pdf[col] = pdf[col].fillna(np.nan)

    matlab_dict = {}
    pdf = pdf.sort_values(["ap_name", "timestamp"])
    for ap_name, group in pdf.groupby("ap_name"):
        group_data = group.drop(columns=["ap_name"])
        struct_dict = {}
        for col in group_data.columns:
            arr = group_data[col].to_numpy()
            if "int" in str(arr.dtype) or "float" in str(arr.dtype):
                arr = arr.astype("double")
            struct_dict[col] = arr
        matlab_dict[make_matlab_name(f"XX__{ap_name}")] = struct_dict

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    buf = io.BytesIO()
    scipy.io.savemat(buf, matlab_dict, long_field_names=True, oned_as="column")
    with open(out_path, "wb") as f:
        f.write(buf.getvalue())
    return len(matlab_dict)


def verify_mat(out_path: str, required_fields=()):
    """Round-trip the written .mat; assert it is non-empty and that every
    required field survived."""
    import scipy.io

    verification = scipy.io.loadmat(out_path)
    keys = [k for k in verification.keys() if not k.startswith("__")]
    assert keys, f"{out_path}: .mat file is empty"
    sample_fields = verification[keys[0]].dtype.names or ()
    for field in required_fields:
        assert field in sample_fields, f"{out_path}: {field} missing from .mat"
    return len(keys)
