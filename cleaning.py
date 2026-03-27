"""
cleaning.py — Generic, project-agnostic data-cleaning engine for Decipher
(Forsta) survey data.  All business rules are driven by a config dict
supplied at runtime.  No hardcoded variable names or thresholds.

Dependencies: requests, pandas, numpy, openpyxl
"""

import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows


# ──────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────

def _safe_list(val):
    """Return val if it is a list, otherwise return [].
    Prevents 'NoneType is not iterable' when a config array is null."""
    return val if isinstance(val, list) else []


def _ensure_dict(config):
    """Return config as a dict.
    Guards against the config being passed as a JSON string."""
    if isinstance(config, str):
        return json.loads(config)
    if config is None:
        return {}
    return config


def clean_cast_column(series: pd.Series) -> pd.Series:
    """Cast a column to the tightest appropriate dtype.
    Strings that look numeric become int64 or float64; everything else
    stays as nullable string.  Sentinel strings are normalised to pd.NA.
    """
    series = series.replace(["<NA>", "nan", "NaN", "None", ""], pd.NA)
    numeric = pd.to_numeric(series, errors="coerce")
    non_numeric_mask = series.notna() & numeric.isna()

    if non_numeric_mask.any():
        return series.astype("string")

    if numeric.notna().any():
        is_all_whole = (numeric.dropna() == numeric.dropna().astype(int)).all()
        has_nulls = numeric.isna().any()
        if is_all_whole and not has_nulls:
            return numeric.astype("int64")
        else:
            return numeric.astype("float64")

    return series.astype("string")


def ensure_columns(df: pd.DataFrame, cols: list, fill_value=np.nan) -> pd.DataFrame:
    """Guarantee that every column in *cols* exists in *df*.
    Missing columns are created and filled with *fill_value*.
    Decipher API omits columns that have zero responses, so this guard
    is essential before any column-level operations.
    """
    for c in [col for col in cols if col not in df.columns]:
        df[c] = fill_value
    return df


def _resolve_grid_vars(grid: dict) -> list:
    """Resolve a grid's variable list.
    Uses explicit 'vars' list if provided and non-empty.
    Falls back to generating from 'prefix' + 'max_idx' if vars is null/[].
    Returns [] if neither is available.
    """
    vars_list = _safe_list(grid.get("vars"))
    if vars_list:
        return vars_list
    prefix  = grid.get("prefix") or ""
    max_idx = grid.get("max_idx")
    if prefix and max_idx:
        return [f"{prefix}{i}" for i in range(1, int(max_idx) + 1)]
    return []


# ──────────────────────────────────────────────────────────────────────
# Data pull
# ──────────────────────────────────────────────────────────────────────

def pull_data(
    survey_id: str,
    client_id: str,
    api_key: str,
    start_date: str = None,
) -> pd.DataFrame:
    """Fetch qualified (status == 3) responses from Decipher and return a
    type-cast DataFrame.  Optionally filter to responses on or after
    *start_date* (ISO-8601 string).
    """
    headers = {"x-apikey": api_key, "Accept": "application/json"}
    url = (
        f"https://sw2.decipherinc.com/api/v1/surveys/selfserve/"
        f"{client_id}/{survey_id}/data?format=json"
    )

    response = requests.get(url, headers=headers, timeout=120)

    if response.status_code == 401:
        raise PermissionError("Could not authenticate. Check the API key.")
    if response.status_code == 404:
        raise FileNotFoundError(f"Survey '{survey_id}' not found in Decipher.")
    response.raise_for_status()

    data = response.json()
    if not data:
        raise ValueError("API returned an empty dataset.")

    df = pd.json_normalize(data)
    df = df.apply(clean_cast_column)

    df = ensure_columns(df, ["status"])
    df = df[df["status"] == 3].copy()
    df = df.apply(clean_cast_column)

    if df.empty:
        raise ValueError("No qualified (status = 3) responses in this survey.")

    if start_date:
        df = ensure_columns(df, ["start_date"])
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
        df = df[df["start_date"] >= pd.to_datetime(start_date)].copy()
        if df.empty:
            raise ValueError(
                f"No qualified responses found from {start_date} onwards."
            )

    df.reset_index(drop=True, inplace=True)
    return df


# ──────────────────────────────────────────────────────────────────────
# Flag engine
# ──────────────────────────────────────────────────────────────────────

def apply_standard_flags(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Apply every configured flag family and return the augmented DataFrame."""
    config = _ensure_dict(config)

    # ── LOI / Speeding ────────────────────────────────────────────────
    loi_var      = config.get("loi_variable", "qtime")
    loi_pct      = config.get("loi_min_threshold_pct", 0.33)
    exclude_if_oe = config.get("loi_exclude_if_quality_oe", False)
    oe_cols      = [v["var"] for v in _safe_list(config.get("oe_variables"))]

    df = ensure_columns(df, [loi_var] + oe_cols)

    loi_numeric = pd.to_numeric(df[loi_var], errors="coerce")
    median_loi  = loi_numeric.median()

    df["flag_speeder"] = 0.0
    df.loc[loi_numeric < (median_loi * loi_pct), "flag_speeder"] = 1.0

    df["flag_lagger"] = 0.0
    df.loc[loi_numeric > (median_loi * 3), "flag_lagger"] = 0.5

    if exclude_if_oe and oe_cols:
        existing_oe = [c for c in oe_cols if c in df.columns]
        if existing_oe:
            has_oe = (
                df[existing_oe].notna().any(axis=1) &
                df[existing_oe].astype(str).replace("", pd.NA).notna().any(axis=1)
            )
            df.loc[(df["flag_speeder"] == 1.0) & has_oe, "flag_speeder"] = 0.5

    # ── Straightlining ────────────────────────────────────────────────
    sl_flag_cols = []

    for grid in _safe_list(config.get("grid_variables")):
        grid_vars = _resolve_grid_vars(grid)
        if not grid_vars:
            continue

        label     = grid.get("label", "unknown")
        min_items = int(grid.get("min_straightline_items") or 3)
        col_name  = f"flag_SL_{label}"
        sl_flag_cols.append(col_name)

        df = ensure_columns(df, grid_vars)

        def _check_sl(row, gv=grid_vars, mi=min_items):
            vals         = row[gv].dropna()
            vals_numeric = pd.to_numeric(vals, errors="coerce")
            use          = vals_numeric if vals_numeric.notna().all() else vals
            if len(use) < mi:
                return 0.0
            return 0.5 if use.nunique() == 1 else 0.0

        df[col_name] = df.apply(_check_sl, axis=1)

    df["SL_Count"] = (
        (df[sl_flag_cols] != 0).sum(axis=1) if sl_flag_cols else 0
    )

    # ── Inconsistency checks ─────────────────────────────────────────
    for chk in _safe_list(config.get("inconsistency_checks")):
        # Skip manual-only checks — they require human review
        if chk.get("manual_check", False):
            continue

        cid          = chk.get("id", "unknown")
        if_var       = chk.get("if_var", "")
        if_val       = chk.get("if_val")
        then_var     = chk.get("then_var", "")
        invalid_vals = _safe_list(chk.get("invalid_vals"))
        col_name     = f"flag_{cid}"

        if not if_var or not then_var or not invalid_vals:
            # Not enough info to evaluate — skip silently
            df[col_name] = 0.0
            continue

        df = ensure_columns(df, [if_var, then_var])

        def _check_inc(row, iv=if_var, ivl=if_val, tv=then_var, bad=invalid_vals):
            if pd.isna(row[iv]) or pd.isna(row[tv]):
                return 0.0
            try:
                row_iv = int(float(row[iv]))
                row_tv = int(float(row[tv]))
            except (ValueError, TypeError):
                row_iv = row[iv]
                row_tv = row[tv]
            if ivl is None:
                return 0.0
            if row_iv == ivl and row_tv in bad:
                return 1.0
            return 0.0

        df[col_name] = df.apply(_check_inc, axis=1)

    # ── Numeric variable review ───────────────────────────────────────
    for entry in _safe_list(config.get("numeric_variables")):
        var      = entry.get("var", "")
        rmin     = entry.get("review_min")
        rmax     = entry.get("review_max")
        col_name = f"flag_{var}"

        if not var:
            continue

        df = ensure_columns(df, [var])
        numeric_vals = pd.to_numeric(df[var], errors="coerce")

        conditions = []
        if rmin is not None:
            conditions.append(numeric_vals < rmin)
        if rmax is not None:
            conditions.append(numeric_vals > rmax)

        if conditions:
            outside = conditions[0]
            for c in conditions[1:]:
                outside = outside | c
            df[col_name] = np.where(numeric_vals.isna(), 0.0,
                                    np.where(outside, 0.5, 0.0))
        else:
            df[col_name] = 0.0

    # ── MaxDiff timer check ───────────────────────────────────────────
    df["flag_maxdiff_speed"] = 0.0
    for sec in _safe_list(config.get("maxdiff_sections")):
        timer_var = sec.get("timer_var", "")
        min_sec   = sec.get("min_seconds")
        if not timer_var or min_sec is None:
            continue
        df = ensure_columns(df, [timer_var])
        timer_vals = pd.to_numeric(df[timer_var], errors="coerce")
        df.loc[timer_vals < min_sec, "flag_maxdiff_speed"] = 1.0

    return df


# ──────────────────────────────────────────────────────────────────────
# Summary columns
# ──────────────────────────────────────────────────────────────────────

def build_summary_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add total_flags, flag_remove, flag_review, affected_questions,
    reason, and action columns.
    """
    flag_cols    = [c for c in df.columns if c.startswith("flag_")]
    sl_flag_cols = [c for c in df.columns if c.startswith("flag_SL_")]

    df["total_flags"] = df[flag_cols].sum(axis=1) if flag_cols else 0.0

    if sl_flag_cols:
        df["SL_Count"] = (df[sl_flag_cols] != 0).sum(axis=1)
    elif "SL_Count" not in df.columns:
        df["SL_Count"] = 0

    df["flag_remove"] = 0
    if "flag_speeder" in df.columns:
        df.loc[df["flag_speeder"] == 1.0, "flag_remove"] = 1
    if "flag_maxdiff_speed" in df.columns:
        df.loc[df["flag_maxdiff_speed"] == 1.0, "flag_remove"] = 1

    df["flag_review"] = np.where(
        (df["total_flags"] > 0) & (df["flag_remove"] == 0), 1, 0
    )

    def _build_affected(row):
        parts        = []
        detail_parts = []

        if row.get("flag_speeder", 0) > 0:
            parts.append("Speeders")
            detail_parts.append("Speeders")

        if row.get("flag_lagger", 0) > 0:
            parts.append("Laggers")
            detail_parts.append("Laggers")

        inc_only = []
        num_only = []
        for f in flag_cols:
            if f in ("flag_speeder", "flag_lagger", "flag_maxdiff_speed",
                     "flag_remove", "flag_review"):
                continue
            if f.startswith("flag_SL_"):
                continue
            val = row.get(f, 0)
            if val <= 0:
                continue
            if val == 0.5:
                num_only.append(f)
            else:
                inc_only.append(f)

        if inc_only:
            names = ", ".join(inc_only)
            parts.append(f"Inconsistent/Illogical Response ({names})")
            detail_parts.append("Inconsistent/Illogical Response")

        if num_only:
            names = ", ".join(num_only)
            parts.append(f"Numeric Out-of-Range ({names})")
            detail_parts.append("Numeric Out-of-Range")

        fired_sl = [
            c.replace("flag_SL_", "")
            for c in sl_flag_cols
            if row.get(c, 0) > 0
        ]
        if fired_sl:
            parts.append(f"Straightliner ({', '.join(fired_sl)})")
            detail_parts.append("Straightliner")

        if row.get("flag_maxdiff_speed", 0) > 0:
            parts.append("MaxDiff Speeder")
            detail_parts.append("MaxDiff Speeder")

        return ("; ".join(parts), "; ".join(detail_parts))

    affected = df.apply(_build_affected, axis=1, result_type="expand")
    df["affected_questions"] = affected[0].fillna("")
    df["reason"]             = affected[1].fillna("")
    df["action"]             = np.where(df["flag_remove"] == 1, "Remove", "")

    return df


# ──────────────────────────────────────────────────────────────────────
# OE extraction
# ──────────────────────────────────────────────────────────────────────

def build_oe_dataframe(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Return a separate DataFrame with uuid + all configured OE columns.
    This feeds the OE Review sheet which the OE Review artifact reads.
    """
    config  = _ensure_dict(config)
    oe_vars = [v["var"] for v in _safe_list(config.get("oe_variables"))]
    oe_labels = {
        v["var"]: v.get("label", v["var"])
        for v in _safe_list(config.get("oe_variables"))
    }
    keep = ["uuid"] + oe_vars
    df   = ensure_columns(df, keep)
    return df[keep].copy(), oe_labels


# ──────────────────────────────────────────────────────────────────────
# Excel export
# ──────────────────────────────────────────────────────────────────────

def export_to_excel(
    df: pd.DataFrame,
    oe_df_and_labels,
    config: dict,
    survey_id: str,
    output_dir: str = ".",
) -> str:
    """Write three-sheet Excel workbook and return the filepath string.

    Sheets:
      A1           – Full dataset sorted by action desc, total_flags desc
      OE Review    – Two header rows (var names + question labels) then data
      Flagged Only – Rows where total_flags > 0
    """
    config = _ensure_dict(config)

    # Unpack oe_df and labels (build_oe_dataframe returns a tuple)
    if isinstance(oe_df_and_labels, tuple):
        oe_df, oe_labels = oe_df_and_labels
    else:
        # Fallback: no labels available
        oe_df     = oe_df_and_labels
        oe_labels = {}

    today    = datetime.now().strftime("%Y-%m-%d")
    filename = f"{survey_id}_data_cleaning_{today}.xlsx"
    filepath = str(Path(output_dir) / filename)

    # ── Sort ──────────────────────────────────────────────────────────
    sort_cols = [c for c in ["action", "total_flags"] if c in df.columns]
    df_sorted = (
        df.sort_values(sort_cols, ascending=[False] * len(sort_cols)).copy()
        if sort_cols else df.copy()
    )
    flagged = (
        df_sorted[df_sorted["total_flags"] > 0].copy()
        if "total_flags" in df_sorted.columns
        else pd.DataFrame()
    )

    # ── Build OE Review two-row header ───────────────────────────────
    oe_entries      = _safe_list(config.get("oe_variables"))
    var_name_row    = ["uuid"] + [v["var"]               for v in oe_entries]
    question_label_row = ["ID"] + [v.get("label", v["var"]) for v in oe_entries]

    # ── Write workbook with openpyxl directly ────────────────────────
    wb = Workbook()

    # Sheet 1: A1 — full dataset
    ws_a1       = wb.active
    ws_a1.title = "A1"
    for r in dataframe_to_rows(df_sorted, index=False, header=True):
        ws_a1.append(r)

    # Sheet 2: OE Review — two-row header then data
    ws_oe = wb.create_sheet("OE Review")
    ws_oe.append(var_name_row)
    ws_oe.append(question_label_row)
    for row in oe_df.itertuples(index=False):
        ws_oe.append(list(row))

    # Sheet 3: Flagged Only
    ws_flag       = wb.create_sheet("Flagged Only")
    for r in dataframe_to_rows(flagged, index=False, header=True):
        ws_flag.append(r)

    wb.save(filepath)
    return filepath
