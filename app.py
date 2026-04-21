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
from cleaning import (
    pull_data,
    apply_standard_flags,
    build_summary_columns,
    build_oe_dataframe,
    export_to_excel,
    run_ip_check,
    merge_ip_results,
    ensure_geoip_db,
)

# ──────────────────────────────────────────────────────────────────────
# Company-level constants — same across every project
# Load from Streamlit secrets if available, fall back to hardcoded
# ──────────────────────────────────────────────────────────────────────
CLIENT_ID = "4475"
API_KEY   = st.secrets.get("DECIPHER_API_KEY", "z8ajshbwdkzwb5qms48ty5w85p8h4wvn4e2mytrh258n7hwwe3a6bm5mcxg806nv")

# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

def ensure_config_dict(config):
    """Ensure config is a parsed dict — handles the case where it may
    have been serialised back to a JSON string somewhere in transit."""
    if isinstance(config, str):
        return json.loads(config)
    return config


def safe_list(val):
    """Return val if it is a list; return [] if None or missing."""
    return val if isinstance(val, list) else []


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
        placeholder="e.g. 260306",
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

    st.divider()
    ip_check_enabled = st.toggle(
        "Enable IP Risk Check",
        value=False,
        help="Run Scamalytics IP check on respondent IPs. Adds an IP Check sheet to the output.",
    )

    scam_username = ""
    scam_api_key = ""
    maxmind_key = ""
    if ip_check_enabled:
        scam_username = st.text_input(
            "Scamalytics Username",
            value=st.secrets.get("SCAM_USERNAME", ""),
            help="Your Scamalytics API username.",
        )
        scam_api_key = st.text_input(
            "Scamalytics API Key",
            type="password",
            value=st.secrets.get("SCAM_API_KEY", ""),
            help="Your Scamalytics API key.",
        )
        maxmind_key = st.text_input(
            "MaxMind License Key (optional, for geo)",
            type="password",
            value=st.secrets.get("MAXMIND_LICENSE_KEY", ""),
            help="Free MaxMind license key for IP geolocation. "
                 "Get one at maxmind.com. Leave blank to skip geo columns.",
        )

# ──────────────────────────────────────────────────────────────────────
# Config parsing and validation
# ──────────────────────────────────────────────────────────────────────
REQUIRED_KEYS = {"loi_variable", "loi_min_threshold_pct", "oe_variables"}

config = None

if config_file is not None:
    try:
        raw = config_file.read().decode("utf-8")
        config = json.loads(raw)
        config = ensure_config_dict(config)

        missing = REQUIRED_KEYS - set(config.keys())
        if missing:
            st.sidebar.error(
                f"Config is missing required keys: {', '.join(sorted(missing))}"
            )
            config = None
        else:
            n_oe    = len(safe_list(config.get("oe_variables")))
            n_grids = len(safe_list(config.get("grid_variables")))
            n_inc   = len(safe_list(config.get("inconsistency_checks")))
            loi_var = config.get("loi_variable", "—")
            st.sidebar.success(
                f"✅ Config loaded: {n_oe} OE variables · {n_grids} grids · "
                f"{n_inc} inconsistency checks · LOI: {loi_var}"
            )

    except json.JSONDecodeError as exc:
        st.sidebar.error(f"Invalid JSON: {exc}")
    except Exception as exc:
        st.sidebar.error(f"Error reading config: {exc}")

# ──────────────────────────────────────────────────────────────────────
# Run button — disabled until both required inputs are present
# ──────────────────────────────────────────────────────────────────────
ready = bool(survey_id.strip() and config is not None)

run_clicked = st.sidebar.button(
    "▶ Run Cleaning Pipeline",
    disabled=not ready,
    type="primary",
    use_container_width=True,
)

if not ready:
    what_missing = []
    if not survey_id.strip():
        what_missing.append("**Survey ID**")
    if config is None:
        what_missing.append("**rules_config.json**")
    st.info(f"Provide {' and '.join(what_missing)} in the sidebar to get started.")
    st.stop()

if not run_clicked:
    st.stop()

# ──────────────────────────────────────────────────────────────────────
# Pipeline execution
# ──────────────────────────────────────────────────────────────────────
progress = st.progress(0)
status   = st.empty()

try:
    # Step 1 — Pull data from Decipher
    status.text("Connecting to Decipher API...")
    progress.progress(5)

    sd = start_date.isoformat() if start_date else None
    status.text(f"Pulling data for survey {survey_id.strip()}...")
    progress.progress(10)

    df = pull_data(survey_id.strip(), CLIENT_ID, API_KEY, start_date=sd)
    progress.progress(30)

    # Step 2–4 — Apply all flag families
    status.text("Applying LOI, speeding and straightlining flags...")
    progress.progress(40)

    status.text("Applying inconsistency and numeric flags...")
    df = apply_standard_flags(df, config)
    progress.progress(65)

    # Step 5 — Build summary columns
    status.text("Building summary columns...")
    df = build_summary_columns(df, config)
    progress.progress(70)

    # Step 6 — IP risk check (optional)
    ip_df = None
    if ip_check_enabled and scam_username and scam_api_key:
        n_ips = df["ipAddress"].dropna().nunique() if "ipAddress" in df.columns else 0
        status.text(f"Running IP risk check on {n_ips} unique IPs...")

        # Resolve GeoLite2 database (download if needed)
        geoip_path = ensure_geoip_db(
            license_key=maxmind_key or None,
            local_path="GeoLite2-City.mmdb",
        )
        if geoip_path:
            status.text(f"GeoLite2 database ready. Checking {n_ips} IPs...")
        else:
            status.text(f"No GeoLite2 database — skipping geo columns. Checking {n_ips} IPs...")

        ip_progress = st.progress(0, text="IP check progress")
        def _ip_cb(frac):
            ip_progress.progress(frac, text=f"IP check: {int(frac*100)}%")
        ip_df = run_ip_check(df, scam_username, scam_api_key,
                             geoip_db_path=geoip_path or None,
                             progress_callback=_ip_cb)
        ip_progress.empty()
        df = merge_ip_results(df, ip_df)
        status.text(f"IP check complete — {len(ip_df)} IPs checked.")
    progress.progress(80)

    # Step 7 — Extract OE dataframe for the OE Review sheet
    oe_df = build_oe_dataframe(df, config)
    progress.progress(85)

    # Step 8 — Export to Excel
    status.text("Exporting Excel file...")
    filepath = export_to_excel(df, oe_df, config, survey_id.strip(), output_dir="/tmp", ip_df=ip_df)
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
    traceback.print_exc()   # full traceback in server console only
    st.stop()

# ──────────────────────────────────────────────────────────────────────
# Results display
# ──────────────────────────────────────────────────────────────────────
st.divider()
st.subheader("Summary")

total      = len(df)
flagged    = int((df["total_flags"] > 0).sum())
remove_lvl = int((df.get("flag_remove", pd.Series(0)) == 1).sum())
review_lvl = int((df.get("flag_review", pd.Series(0)) == 1).sum())

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Responses", total)
col2.metric("Flagged",         flagged)
col3.metric("Remove-level",    remove_lvl)
col4.metric("Review-level",    review_lvl)

# Preview table — include only columns that actually exist
st.subheader("Flagged Rows (top 20)")

candidate_cols = ["uuid", "transid", "reason", "total_flags", "SL_Count"]
preview_cols   = [c for c in candidate_cols if c in df.columns]

flagged_df = df[df["total_flags"] > 0].sort_values("total_flags", ascending=False)

if flagged_df.empty:
    st.info("No flagged rows found.")
else:
    st.dataframe(
        flagged_df[preview_cols].head(20),
        use_container_width=True,
        hide_index=True,
    )

# Download button
st.divider()
with open(filepath, "rb") as f:
    st.download_button(
        label="⬇ Download Excel Report",
        data=f,
        file_name=filepath.split("/")[-1],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
    )
