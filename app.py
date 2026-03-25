"""
app.py — Streamlit UI for the Decipher survey data-cleaning pipeline.

Company-level constants (CLIENT_ID, API_KEY) live here.
All project-specific cleaning rules come from the uploaded rules_config.json.

Run:  streamlit run app.py
"""

import json
import traceback
import streamlit as st
import pandas as pd
from cleaning import pull_data, apply_standard_flags, build_summary_columns, build_oe_dataframe, export_to_excel

# ──────────────────────────────────────────────────────────────────────
# Company-level constants — same across every project
# ──────────────────────────────────────────────────────────────────────
CLIENT_ID = "4475"                # Decipher company client ID
API_KEY   = "z8ajshbwdkzwb5qms48ty5w85p8h4wvn4e2mytrh258n7hwwe3a6bm5mcxg806nv"          # Decipher API key — replace before use

# ──────────────────────────────────────────────────────────────────────
# Page config
# ──────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Survey Data Cleaning", layout="wide")
st.title("Survey Data Cleaning Tool")
st.caption("Generic cleaning pipeline for Decipher (Forsta) survey data")

# ──────────────────────────────────────────────────────────────────────
# Sidebar inputs
# ──────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Configuration")

    survey_id = st.text_input(
        "Survey ID (Decipher)",
        placeholder="e.g. 240100",
        help="The numeric survey ID from the Decipher platform.",
    )

    config_file = st.file_uploader(
        "rules_config.json",
        type=["json"],
        help="Upload the project-specific rules configuration file.",
    )

    start_date = st.date_input(
        "Start Date (optional)",
        value=None,
        help="If set, only responses on or after this date are included.",
    )

# ──────────────────────────────────────────────────────────────────────
# Config validation
# ──────────────────────────────────────────────────────────────────────
REQUIRED_KEYS = {"loi_variable", "loi_min_threshold_pct", "oe_variables"}

config: dict | None = None

if config_file is not None:
    try:
        raw = config_file.read().decode("utf-8")
        config = json.loads(raw)

        missing = REQUIRED_KEYS - set(config.keys())
        if missing:
            st.sidebar.error(f"Config is missing required keys: {', '.join(sorted(missing))}")
            config = None
        else:
            n_oe    = len(config.get("oe_variables", []))
            n_grids = len(config.get("grid_variables", []))
            n_inc   = len(config.get("inconsistency_checks", []))
            loi_var = config.get("loi_variable", "—")
            st.sidebar.success(
                f"✅ Config loaded: {n_oe} OE variables, {n_grids} grids, "
                f"{n_inc} inconsistency checks, LOI variable: {loi_var}"
            )
    except json.JSONDecodeError as exc:
        st.sidebar.error(f"Invalid JSON: {exc}")
    except Exception as exc:
        st.sidebar.error(f"Error reading config: {exc}")

# ──────────────────────────────────────────────────────────────────────
# Run button
# ──────────────────────────────────────────────────────────────────────
ready = bool(survey_id and config)

run_clicked = st.sidebar.button(
    "Run Cleaning Pipeline",
    disabled=not ready,
    type="primary",
    use_container_width=True,
)

if not ready:
    st.info("Provide a **Survey ID** and upload a valid **rules_config.json** in the sidebar to get started.")
    st.stop()

if not run_clicked:
    st.stop()

# ──────────────────────────────────────────────────────────────────────
# Pipeline execution
# ──────────────────────────────────────────────────────────────────────
progress = st.progress(0)
status   = st.empty()

try:
    # Step 1 — Pull data
    status.text("Connecting to Decipher API...")
    progress.progress(5)

    status.text(f"Pulling data for survey {survey_id}...")
    progress.progress(10)

    sd = start_date.isoformat() if start_date else None
    df = pull_data(survey_id, CLIENT_ID, API_KEY, start_date=sd)
    progress.progress(30)

    # Step 2 — LOI & speeding
    status.text("Applying LOI and speeding flags...")
    progress.progress(40)

    # Step 3 — Straightlining
    status.text("Applying straightlining checks...")
    progress.progress(50)

    # Step 4 — Inconsistency & numeric
    status.text("Applying inconsistency and numeric flags...")
    df = apply_standard_flags(df, config)
    progress.progress(65)

    # Step 5 — Summary
    status.text("Building summary columns...")
    df = build_summary_columns(df)
    progress.progress(75)

    # Step 6 — OE extraction
    oe_df = build_oe_dataframe(df, config)
    progress.progress(80)

    # Step 7 — Excel export
    status.text("Exporting Excel file...")
    filepath = export_to_excel(df, oe_df, survey_id, output_dir="/tmp")
    progress.progress(100)
    status.text("✅ Pipeline complete!")

except PermissionError as exc:
    progress.empty()
    status.empty()
    st.error(str(exc))
    st.stop()
except FileNotFoundError as exc:
    progress.empty()
    status.empty()
    st.error(str(exc))
    st.stop()
except ValueError as exc:
    progress.empty()
    status.empty()
    st.error(str(exc))
    st.stop()
except Exception as exc:
    progress.empty()
    status.empty()
    st.error(f"An unexpected error occurred: {exc}")
    traceback.print_exc()          # full traceback in server console only
    st.stop()

# ──────────────────────────────────────────────────────────────────────
# Results display
# ──────────────────────────────────────────────────────────────────────
st.divider()
st.subheader("Summary")

total       = len(df)
flagged     = int((df["total_flags"] > 0).sum())
remove_lvl  = int((df["flag_remove"] == 1).sum())
review_lvl  = int((df["flag_review"] == 1).sum())

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Responses", total)
col2.metric("Flagged", flagged)
col3.metric("Remove-level", remove_lvl)
col4.metric("Review-level", review_lvl)

# Preview table of flagged rows
st.subheader("Flagged Rows (top 20)")

preview_cols = ["uuid", "reason", "total_flags", "action"]
# Include transid if present
if "transid" in df.columns:
    preview_cols.insert(1, "transid")

flagged_df = df[df["total_flags"] > 0].sort_values("total_flags", ascending=False)
st.dataframe(flagged_df[preview_cols].head(20), use_container_width=True, hide_index=True)

# Download
with open(filepath, "rb") as f:
    st.download_button(
        label="Download Excel Report",
        data=f,
        file_name=filepath.split("/")[-1],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
    )
