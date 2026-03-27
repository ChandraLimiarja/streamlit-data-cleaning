"""
cleaning.py — Generic, project-agnostic data-cleaning engine for Decipher
(Forsta) survey data.  All business rules are driven by a config dict
supplied at runtime.  No hardcoded variable names or thresholds.

Dependencies: requests, pandas, numpy, openpyxl
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

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
    """
    for c in cols:
        if c not in df.columns:
            df[c] = fill_value
    return df


# ──────────────────────────────────────────────────────────────────────
# Data pull
# ──────────────────────────────────────────────────────────────────────

def pull_data(
    survey_id: str,
    client_id: str,
    api_key: str,
    start_date: str | None = None,
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

    # Surface clear errors for the UI layer to catch
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

    # Keep qualified respondents only
    df = ensure_columns(df, ["status"])
    df = df[df["status"] == 3].copy()
    df = df.apply(clean_cast_column)

    if df.empty:
        raise ValueError("No qualified (status = 3) responses in this survey.")

    # Optional start-date filter
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

    # ── LOI / Speeding ────────────────────────────────────────────────
    loi_var = config.get("loi_variable", "LOI")
    loi_pct = config.get("loi_min_threshold_pct", 0.35)
    exclude_if_oe = config.get("loi_exclude_if_quality_oe", False)
    oe_cols = [v["var"] for v in config.get("oe_variables", [])]

    df = ensure_columns(df, [loi_var] + oe_cols)

    median_loi = pd.to_numeric(df[loi_var], errors="coerce").median()

    # Vectorised speeder / lagger calculation
    loi_numeric = pd.to_numeric(df[loi_var], errors="coerce")
    df["flag_speeder"] = 0.0
    # Speeder: LOI below median * threshold percentage
    df.loc[loi_numeric < (median_loi * loi_pct), "flag_speeder"] = 1.0
    # Lagger: LOI above median * 3 — worth reviewing, not removing
    df.loc[loi_numeric > (median_loi * 3), "flag_lagger"] = 0.5
    if "flag_lagger" not in df.columns:
        df["flag_lagger"] = 0.0
    df["flag_lagger"] = df["flag_lagger"].fillna(0.0)

    # Downgrade speeders who gave quality OE responses
    if exclude_if_oe and oe_cols:
        existing_oe = [c for c in oe_cols if c in df.columns]
        if existing_oe:
            has_oe = df[existing_oe].notna().any(axis=1) & (
                df[existing_oe].astype(str).replace("", pd.NA).notna().any(axis=1)
            )
            # Downgrade from Remove (1) to Review (0.5) if respondent wrote OEs
            df.loc[(df["flag_speeder"] == 1.0) & has_oe, "flag_speeder"] = 0.5

    # ── Straightlining ────────────────────────────────────────────────
    grid_cfgs = config.get("grid_variables", [])
    sl_flag_cols: list[str] = []

    for grid in grid_cfgs:
        grid_vars: list[str] = grid.get("vars") or []
        if not grid_vars:
            prefix = grid.get("prefix", "")
            max_idx = grid.get("max_idx")
            if prefix and max_idx:
                grid_vars = [f"{prefix}{i}" for i in range(1, max_idx + 1)]
        label: str = grid["label"]
        min_items: int = grid.get("min_straightline_items", 3)
        col_name = f"flag_SL_{label}"
        sl_flag_cols.append(col_name)

        df = ensure_columns(df, grid_vars)

        def _check_sl(row, gv=grid_vars, mi=min_items):
            vals = row[gv].dropna()
            # Convert to numeric where possible for comparison
            vals_numeric = pd.to_numeric(vals, errors="coerce")
            use = vals_numeric if vals_numeric.notna().all() else vals
            if len(use) < mi:
                return 0.0  # Not enough items to judge
            return 0.5 if use.nunique() == 1 else 0.0

        df[col_name] = df.apply(_check_sl, axis=1)

    # Count of grids that triggered straightlining
    if sl_flag_cols:
        df["SL_Count"] = (df[sl_flag_cols] != 0).sum(axis=1)
    else:
        df["SL_Count"] = 0

    # ── Inconsistency checks ─────────────────────────────────────────
    for chk in config.get("inconsistency_checks", []):
        cid = chk["id"]
        if_var = chk["if_var"]
        if_val = chk["if_val"]
        then_var = chk["then_var"]
        invalid_vals = chk["invalid_vals"]
        col_name = f"flag_{cid}"

        df = ensure_columns(df, [if_var, then_var])

        def _check_inc(row, iv=if_var, ivl=if_val, tv=then_var, bad=invalid_vals):
            # Null-safe: if either variable is null, do not flag
            if pd.isna(row[iv]) or pd.isna(row[tv]):
                return 0.0
            try:
                row_iv = int(float(row[iv]))
                row_tv = int(float(row[tv]))
            except (ValueError, TypeError):
                row_iv = row[iv]
                row_tv = row[tv]
            if row_iv == ivl and row_tv in bad:
                return 1.0
            return 0.0

        df[col_name] = df.apply(_check_inc, axis=1)

    # ── Numeric variable review ───────────────────────────────────────
    for entry in config.get("numeric_variables", []):
        var = entry["var"]
        rmin = entry["review_min"]
        rmax = entry["review_max"]
        col_name = f"flag_{var}"

        df = ensure_columns(df, [var])

        numeric_vals = pd.to_numeric(df[var], errors="coerce")
        # Flag 0.5 (review) if outside range; null values → 0
        df[col_name] = np.where(
            numeric_vals.isna(),
            0.0,
            np.where((numeric_vals < rmin) | (numeric_vals > rmax), 0.5, 0.0),
        )

    # ── MaxDiff timer check ───────────────────────────────────────────
    df["flag_maxdiff_speed"] = 0.0
    for sec in config.get("maxdiff_sections", []):
        timer_var = sec["timer_var"]
        min_sec = sec["min_seconds"]
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
    flag_cols = [c for c in df.columns if c.startswith("flag_")]
    sl_flag_cols = [c for c in df.columns if c.startswith("flag_SL_")]

    # Total flags: sum of every flag_* column
    df["total_flags"] = df[flag_cols].sum(axis=1) if flag_cols else 0.0

    # SL_Count may already exist; recalculate to be safe
    if sl_flag_cols:
        df["SL_Count"] = (df[sl_flag_cols] != 0).sum(axis=1)
    elif "SL_Count" not in df.columns:
        df["SL_Count"] = 0

    # flag_remove: hard-remove if speeder == 1 OR maxdiff_speed == 1
    df["flag_remove"] = 0
    if "flag_speeder" in df.columns:
        df.loc[df["flag_speeder"] == 1.0, "flag_remove"] = 1
    if "flag_maxdiff_speed" in df.columns:
        df.loc[df["flag_maxdiff_speed"] == 1.0, "flag_remove"] = 1

    # flag_review: any flags but not at remove level
    df["flag_review"] = np.where(
        (df["total_flags"] > 0) & (df["flag_remove"] == 0), 1, 0
    )

    # ── Build human-readable affected_questions / reason ──────────────
    def _build_affected(row):
        parts: list[str] = []
        detail_parts: list[str] = []

        # Speeders
        if row.get("flag_speeder", 0) > 0:
            parts.append("Speeders")
            detail_parts.append("Speeders")

        # Laggers
        if row.get("flag_lagger", 0) > 0:
            parts.append("Laggers")
            detail_parts.append("Laggers")

        # Inconsistency flags (flag_* that are not SL, speeder, lagger,
        # maxdiff, remove, review, numeric-review, or summary cols)
        inc_flags = [
            c for c in flag_cols
            if c.startswith("flag_")
            and c not in (
                "flag_speeder", "flag_lagger", "flag_maxdiff_speed",
                "flag_remove", "flag_review",
            )
            and not c.startswith("flag_SL_")
            and row.get(c, 0) > 0
        ]
        # Separate inconsistency vs numeric-review flags
        # Numeric-review flags match config["numeric_variables"][*]["var"]
        numeric_flag_names = set()
        # (We detect them by pattern: they don't match any inconsistency id)
        inc_only = []
        num_only = []
        for f in inc_flags:
            # If value is exactly 0.5 it's likely a numeric-review flag
            # If value is 1.0 it's an inconsistency flag
            val = row.get(f, 0)
            if val == 0.5 and not f.startswith("flag_SL_"):
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

        # Straightlining
        fired_sl = [
            c.replace("flag_SL_", "")
            for c in sl_flag_cols
            if row.get(c, 0) > 0
        ]
        if fired_sl:
            labels = ", ".join(fired_sl)
            parts.append(f"Straightliner ({labels})")
            detail_parts.append("Straightliner")

        # MaxDiff speed
        if row.get("flag_maxdiff_speed", 0) > 0:
            parts.append("MaxDiff Speeder")
            detail_parts.append("MaxDiff Speeder")

        return ("; ".join(parts), "; ".join(detail_parts))

    affected = df.apply(_build_affected, axis=1, result_type="expand")
    df["affected_questions"] = affected[0].fillna("")
    df["reason"] = affected[1].fillna("")

    # action: only mark Remove for hard-fail flags
    df["action"] = np.where(df["flag_remove"] == 1, "Remove", "")

    return df


# ──────────────────────────────────────────────────────────────────────
# OE extraction
# ──────────────────────────────────────────────────────────────────────

def build_oe_dataframe(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Return a separate DataFrame with uuid + all configured OE columns."""
    oe_vars = [v["var"] for v in config.get("oe_variables", [])]
    keep = ["uuid"] + oe_vars
    df = ensure_columns(df, keep)
    return df[keep].copy()


# ──────────────────────────────────────────────────────────────────────
# Excel export
# ──────────────────────────────────────────────────────────────────────

def export_to_excel(
    df: pd.DataFrame,
    oe_df: pd.DataFrame,
    survey_id: str,
    config: dict,
    output_dir: str = ".",
) -> str:
    """Write three-sheet Excel workbook and return the filepath string.

    Sheets:
      A1           – Full dataset sorted by action desc, total_flags desc
      OE Review    – uuid + OE text columns (keeps original var names)
      Flagged Only – Rows where total_flags > 0

    Flag columns are renamed to human-readable labels derived from
    *config* before writing.  OE Review is intentionally left untouched
    so the OE artifact can match columns back correctly.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"{survey_id}_data_cleaning_{today}.xlsx"
    filepath = str(Path(output_dir) / filename)

    # Sort: Remove rows first, then by total_flags descending
    sort_cols = []
    if "action" in df.columns:
        sort_cols.append("action")
    if "total_flags" in df.columns:
        sort_cols.append("total_flags")

    df_sorted = df.sort_values(
        sort_cols, ascending=[False] * len(sort_cols)
    ).copy() if sort_cols else df.copy()

    flagged = df_sorted[df_sorted["total_flags"] > 0].copy() if "total_flags" in df_sorted.columns else pd.DataFrame()

    # ── Rename flag columns to human-readable labels ──────────────────
    rename_map: dict[str, str] = {}

    # Inconsistency checks → use label field (skip manual-check entries)
    for chk in config.get("inconsistency_checks", []):
        if not chk.get("manual_check", False) and chk.get("label"):
            rename_map[f"flag_{chk['id']}"] = chk["label"]

    # Numeric variables → use label field
    for entry in config.get("numeric_variables", []):
        if entry.get("label"):
            rename_map[f"flag_{entry['var']}"] = entry["label"]

    # Straightlining → prefix with "Straightliner – "
    for grid in config.get("grid_variables", []):
        if grid.get("label"):
            rename_map[f"flag_SL_{grid['label']}"] = f"Straightliner – {grid['label']}"

    # Apply rename to the two data sheets (NOT oe_df)
    df_sorted = df_sorted.rename(columns=rename_map)
    flagged = flagged.rename(columns=rename_map)

    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        df_sorted.to_excel(writer, sheet_name="A1", index=False)
        oe_df.to_excel(writer, sheet_name="OE Review", index=False)
        flagged.to_excel(writer, sheet_name="Flagged Only", index=False)

    return filepath
