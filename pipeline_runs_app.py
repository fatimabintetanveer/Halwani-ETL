"""Small read-only Streamlit viewer for pipeline stage runs."""

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st
import yaml


PROJECT_DIR = Path(__file__).resolve().parent
SETTINGS_PATH = PROJECT_DIR / "config" / "settings.yaml"


def get_database_path() -> Path:
    with SETTINGS_PATH.open(encoding="utf-8") as settings_file:
        settings = yaml.safe_load(settings_file)

    configured_path = Path(settings["duckdb"]["path"]).expanduser()
    if not configured_path.is_absolute():
        configured_path = PROJECT_DIR / configured_path
    return configured_path


def load_pipeline_runs(database_path: Path) -> pd.DataFrame:
    if not database_path.is_file():
        return pd.DataFrame()

    with duckdb.connect(str(database_path), read_only=True) as connection:
        table_exists = connection.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = 'pipeline_runs'
            """
        ).fetchone()[0]
        if not table_exists:
            return pd.DataFrame()

        return connection.execute(
            """
            SELECT
                run_id,
                retailer,
                year,
                month,
                stage,
                status,
                started_at,
                completed_at,
                rows_processed,
                file_path,
                error_message
            FROM pipeline_runs
            ORDER BY started_at DESC, run_id DESC
            LIMIT 500
            """
        ).df()


st.set_page_config(page_title="Pipeline Runs", layout="wide")
st.title("Pipeline Runs")
st.caption("Recent pipeline stage statuses from the configured DuckDB database.")
st.caption("This page checks for updates every 0.1 seconds.")


@st.fragment(run_every=0.1)
def show_pipeline_runs() -> None:
    if st.button("Refresh"):
        st.rerun()

    try:
        db_path = get_database_path()
        runs = load_pipeline_runs(db_path)
    except Exception as exc:
        st.error(f"Could not load pipeline runs: {exc}")
        return

    st.caption(f"Database: `{db_path}`")

    if runs.empty:
        st.info("No pipeline run records found. Run the pipeline first.")
        return

    filter_columns = st.columns(4)
    retailers = sorted(runs["retailer"].dropna().astype(str).unique())
    selected_retailers = filter_columns[0].multiselect("Retailer", retailers)
    stages = sorted(runs["stage"].dropna().astype(str).unique())
    selected_stages = filter_columns[1].multiselect("Stage", stages)
    statuses = sorted(runs["status"].dropna().astype(str).unique())
    selected_statuses = filter_columns[2].multiselect("Status", statuses)
    selected_years = filter_columns[3].multiselect(
        "Year", sorted(runs["year"].dropna().unique(), reverse=True)
    )

    filtered_runs = runs.copy()
    if selected_retailers:
        filtered_runs = filtered_runs[filtered_runs["retailer"].isin(selected_retailers)]
    if selected_stages:
        filtered_runs = filtered_runs[filtered_runs["stage"].isin(selected_stages)]
    if selected_statuses:
        filtered_runs = filtered_runs[filtered_runs["status"].isin(selected_statuses)]
    if selected_years:
        filtered_runs = filtered_runs[filtered_runs["year"].isin(selected_years)]

    st.dataframe(filtered_runs, use_container_width=True, hide_index=True)


show_pipeline_runs()
