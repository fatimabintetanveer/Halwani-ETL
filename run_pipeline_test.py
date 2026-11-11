"""Test runner for one month: process Silver, mapping, dimensions, and facts."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import yaml


PROJECT_DIR = Path(__file__).resolve().parent
DBT_DIR = PROJECT_DIR / "dbt"
DBT_PROFILE_DIR = DBT_DIR / ".dbt"
DBT_PROFILE_NAME = "halwani_mdm"
DBT_TARGET = "dev"
DBT_MAPPING_MODEL = "mdm_all"
DBT_MAPPING_STAGE = "dbt_mapping"
DBT_DIMENSION_MODELS = ("dim_product", "dim_store")
DBT_DIMENSION_STAGE = "dimension_modeling"
DBT_FACT_MODELS = ("fct_sales",)
DBT_FACT_STAGE = "fact_modeling"

# Add the project root to Python's import path so the retailer processors load.
sys.path.insert(0, str(PROJECT_DIR))

from silver.othaim_process import process as othaim_process
from silver.tamimi_process import process as tamimi_process
from silver.panda_process import process as panda_process
from silver.lulu_process import process as lulu_process
from utils.io import (
    log_pipeline_run,
    update_pipeline_run,
)
from utils.views import refresh_silver_views, build_unified_silver


ADAPTERS = {
    "Othaim": othaim_process,
    "Tamimi": tamimi_process,
    "Panda": panda_process,
    "Lulu": lulu_process,
}


def resolve_project_path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (PROJECT_DIR / path).resolve()


def read_latest_pipeline_run_id(db_path: Path) -> int:
    if not db_path.is_file():
        return 0

    with duckdb.connect(str(db_path), read_only=True) as connection:
        exists = connection.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name = 'pipeline_runs'
            """
        ).fetchone()[0]
        if not exists:
            return 0
        return connection.execute(
            "SELECT COALESCE(MAX(run_id), 0) FROM pipeline_runs"
        ).fetchone()[0]


def silver_transform_succeeded(
    db_path: Path,
    previous_max_run_id: int,
    retailer: str,
    year: int,
    month: str,
) -> tuple[bool, str | None]:
    if not db_path.is_file():
        return False, None

    with duckdb.connect(str(db_path), read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT stage, status, source_file_name
            FROM pipeline_runs
            WHERE run_id > ?
              AND retailer = ?
              AND year = ?
              AND month = ?
              AND stage IN ('discover_file', 'silver_transform')
            ORDER BY run_id DESC
            """,
            [previous_max_run_id, retailer, year, month],
        ).fetchall()

    transform_row = next((row for row in rows if row[0] == "silver_transform"), None)
    source_file_name = next((row[2] for row in rows if row[2]), None)
    transform_succeeded = transform_row is not None and transform_row[1] == "success"
    return transform_succeeded, source_file_name


def get_dbt_target_database(target: str) -> Path:
    profile_path = DBT_PROFILE_DIR / "profiles.yml"
    if not profile_path.is_file():
        raise FileNotFoundError(f"dbt profile not found: {profile_path}")

    with profile_path.open(encoding="utf-8") as profile_file:
        profile = yaml.safe_load(profile_file)

    try:
        database_path = profile[DBT_PROFILE_NAME]["outputs"][target]["path"]
    except (KeyError, TypeError) as exc:
        raise ValueError(
            f"dbt target {target!r} is not configured in {profile_path}"
        ) from exc

    path = Path(database_path).expanduser()
    return path.resolve() if path.is_absolute() else (DBT_DIR / path).resolve()


def has_mdm_all_source_retailer_column(database_path: Path) -> bool:
    if not database_path.is_file():
        return False

    with duckdb.connect(str(database_path), read_only=True) as connection:
        return connection.execute(
            """
            SELECT COUNT(*) > 0
            FROM information_schema.columns
            WHERE table_name = 'harmonized_mapping'
              AND column_name = 'Retailer'
            """
        ).fetchone()[0]


def collect_dbt_output(command: list[str]) -> tuple[int, list[str]]:
    """Run dbt, printing its output as it arrives and retaining it for errors."""
    lines: list[str] = []
    with subprocess.Popen(
        command,
        cwd=DBT_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    ) as process:
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="", flush=True)
            lines.append(line.rstrip())
        return_code = process.wait()
    return return_code, lines


def run_dbt_model_stage(
    *,
    log_db_path: Path,
    retailer: str,
    year: int,
    month: str,
    model_name: str,
    stage_name: str,
    source_file_name: str | None = None,
    dbt_vars: dict | None = None,
) -> bool:
    run_id = log_pipeline_run(
        str(log_db_path),
        retailer,
        year,
        month,
        stage=stage_name,
        status="running",
        dbt_model_name=model_name,
        source_file_name=source_file_name,
    )
    started = time.perf_counter()
    invocation_id: str | None = None

    try:
        dbt_database = get_dbt_target_database(DBT_TARGET)
        if dbt_database != log_db_path:
            raise FileNotFoundError(
                "Python and dbt must use the same database for this runner. "
                f"Python uses {log_db_path}; dbt target '{DBT_TARGET}' uses {dbt_database}."
            )

        if model_name == DBT_MAPPING_MODEL and not has_mdm_all_source_retailer_column(dbt_database):
            raise RuntimeError(
                "The dev harmonized_mapping table does not have Source_Retailer. "
                "Run the one-time dbt full refresh on dev before the monthly run."
            )

        dbt_executable = shutil.which("dbt")
        if dbt_executable is None:
            fallback = PROJECT_DIR / "venv" / "Scripts" / "dbt.exe"
            dbt_executable = str(fallback) if fallback.is_file() else "dbt"

        command = [
            dbt_executable,
            "build",
            "--profiles-dir", str(DBT_PROFILE_DIR),
            "--target", DBT_TARGET,
            "--select", model_name,
        ]
        if dbt_vars is not None:
            command.extend(["--vars", json.dumps(dbt_vars)])

        results_path = DBT_DIR / "target" / "run_results.json"
        previous_artifact_mtime = (
            results_path.stat().st_mtime_ns if results_path.exists() else None
        )
        return_code, output_lines = collect_dbt_output(command)
        duration = time.perf_counter() - started

        artifact: dict = {}
        if results_path.exists():
            current_mtime = results_path.stat().st_mtime_ns
            if previous_artifact_mtime is None or current_mtime > previous_artifact_mtime:
                artifact = json.loads(results_path.read_text(encoding="utf-8"))

        metadata = artifact.get("metadata", {})
        invocation_id = metadata.get("invocation_id")
        results = artifact.get("results", [])
        model_result = next(
            (
                item for item in results
                if item.get("unique_id", "").endswith(f".{model_name}")
            ),
            None,
        )
        failures = [
            item for item in results
            if str(item.get("status", "")).lower() in {"error", "fail"}
        ]

        if return_code == 0 and model_result and model_result.get("status") == "success" and not failures:
            update_pipeline_run(
                str(log_db_path),
                run_id,
                status="success",
                dbt_invocation_id=invocation_id,
                dbt_model_name=model_name,
                duration_seconds=duration,
            )
            print(f"[DBT DONE] {model_name} succeeded in {duration:.2f}s")
            return True

        failure_messages = [
            f"{item.get('unique_id', 'dbt result')}: {item['message']}"
            for item in failures
            if item.get("message")
        ]
        if not failure_messages and model_result:
            failure_messages.append(
                f"{model_name} status: {model_result.get('status', 'unknown')}"
            )
        if not failure_messages:
            failure_messages = output_lines[-30:]
        error_message = " | ".join(failure_messages)[-4000:]
        if not error_message:
            error_message = f"dbt build failed with exit code {return_code}"

        update_pipeline_run(
            str(log_db_path),
            run_id,
            status="failed",
            error_msg=error_message,
            dbt_invocation_id=invocation_id,
            dbt_model_name=model_name,
            duration_seconds=duration,
        )
        print(f"[DBT FAIL] {error_message}")
        return False

    except Exception as exc:
        duration = time.perf_counter() - started
        update_pipeline_run(
            str(log_db_path),
            run_id,
            status="failed",
            error_msg=f"{type(exc).__name__}: {exc}",
            dbt_invocation_id=invocation_id,
            dbt_model_name=model_name,
            duration_seconds=duration,
        )
        print(f"[DBT FAIL] {type(exc).__name__}: {exc}")
        return False


def log_skipped_dbt_model(
    *,
    log_db_path: Path,
    retailer: str,
    year: int,
    month: str,
    model_name: str,
    stage_name: str,
    reason: str,
    source_file_name: str | None = None,
) -> None:
    run_id = log_pipeline_run(
        str(log_db_path),
        retailer,
        year,
        month,
        stage=stage_name,
        status="running",
        dbt_model_name=model_name,
        source_file_name=source_file_name,
    )
    update_pipeline_run(
        str(log_db_path),
        run_id,
        status="skipped",
        error_msg=reason,
        dbt_model_name=model_name,
    )


def log_skipped_dbt_models(
    *,
    log_db_path: Path,
    retailer: str,
    year: int,
    month: str,
    models: tuple[str, ...],
    stage_name: str,
    reason: str,
) -> None:
    for model_name in models:
        log_skipped_dbt_model(
            log_db_path=log_db_path,
            retailer=retailer,
            year=year,
            month=month,
            model_name=model_name,
            stage_name=stage_name,
            reason=reason,
        )


def main() -> int:
    os.chdir(PROJECT_DIR)
    parser = argparse.ArgumentParser(description="Test pipeline with one dbt month run")
    parser.add_argument("--retailer", required=True, choices=list(ADAPTERS.keys()))
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=str, required=True)
    parser.add_argument("--config", default="config/schemas")
    parser.add_argument("--settings", default="config/settings.yaml")
    args = parser.parse_args()

    settings_path = resolve_project_path(args.settings)
    config_path = resolve_project_path(args.config)
    with settings_path.open(encoding="utf-8") as settings_file:
        settings = yaml.safe_load(settings_file)

    process_fn = ADAPTERS[args.retailer]
    db_path = resolve_project_path(settings["duckdb"]["path"])
    silver_dir = resolve_project_path(settings["output"]["silver"])

    print(f"Processing {args.retailer} {args.year} - {args.month}")
    previous_max_run_id = read_latest_pipeline_run_id(db_path)
    process_fn(args.year, args.month, str(config_path), str(settings_path))

    # Refresh the regular pipeline database, as the original runner does.
    refresh_silver_views(str(db_path), str(silver_dir), args.retailer)
    build_unified_silver(str(db_path))

    transform_succeeded, source_file_name = silver_transform_succeeded(
        db_path, previous_max_run_id, args.retailer, args.year, args.month
    )
    if not transform_succeeded:
        reason = "Skipped because this month's Silver transform did not succeed"
        log_skipped_dbt_model(
            log_db_path=db_path,
            retailer=args.retailer,
            year=args.year,
            month=args.month,
            model_name=DBT_MAPPING_MODEL,
            stage_name=DBT_MAPPING_STAGE,
            reason=reason,
            source_file_name=source_file_name,
        )
        log_skipped_dbt_models(
            log_db_path=db_path,
            retailer=args.retailer,
            year=args.year,
            month=args.month,
            models=DBT_DIMENSION_MODELS,
            stage_name=DBT_DIMENSION_STAGE,
            reason="Skipped because the mapping model did not run",
        )
        log_skipped_dbt_models(
            log_db_path=db_path,
            retailer=args.retailer,
            year=args.year,
            month=args.month,
            models=DBT_FACT_MODELS,
            stage_name=DBT_FACT_STAGE,
            reason="Skipped because the mapping model did not run",
        )
        print(f"[DBT SKIP] {reason}")
        return 1

    mapping_ok = run_dbt_model_stage(
        log_db_path=db_path,
        retailer=args.retailer,
        year=args.year,
        month=args.month,
        model_name=DBT_MAPPING_MODEL,
        stage_name=DBT_MAPPING_STAGE,
        source_file_name=source_file_name,
        dbt_vars={
            "month": args.month,
            "year": args.year,
            "retailer": args.retailer,
        },
    )
    if not mapping_ok:
        reason = "Skipped because the mapping model failed"
        log_skipped_dbt_models(
            log_db_path=db_path,
            retailer=args.retailer,
            year=args.year,
            month=args.month,
            models=DBT_DIMENSION_MODELS,
            stage_name=DBT_DIMENSION_STAGE,
            reason=reason,
        )
        log_skipped_dbt_models(
            log_db_path=db_path,
            retailer=args.retailer,
            year=args.year,
            month=args.month,
            models=DBT_FACT_MODELS,
            stage_name=DBT_FACT_STAGE,
            reason=reason,
        )
        return 1

    for index, model_name in enumerate(DBT_DIMENSION_MODELS):
        dimension_ok = run_dbt_model_stage(
            log_db_path=db_path,
            retailer=args.retailer,
            year=args.year,
            month=args.month,
            model_name=model_name,
            stage_name=DBT_DIMENSION_STAGE,
        )
        if not dimension_ok:
            reason = f"Skipped because dimension model {model_name} failed"
            log_skipped_dbt_models(
                log_db_path=db_path,
                retailer=args.retailer,
                year=args.year,
                month=args.month,
                models=DBT_DIMENSION_MODELS[index + 1 :],
                stage_name=DBT_DIMENSION_STAGE,
                reason=reason,
            )
            log_skipped_dbt_models(
                log_db_path=db_path,
                retailer=args.retailer,
                year=args.year,
                month=args.month,
                models=DBT_FACT_MODELS,
                stage_name=DBT_FACT_STAGE,
                reason=reason,
            )
            return 1

    for index, model_name in enumerate(DBT_FACT_MODELS):
        fact_ok = run_dbt_model_stage(
            log_db_path=db_path,
            retailer=args.retailer,
            year=args.year,
            month=args.month,
            model_name=model_name,
            stage_name=DBT_FACT_STAGE,
        )
        if not fact_ok:
            log_skipped_dbt_models(
                log_db_path=db_path,
                retailer=args.retailer,
                year=args.year,
                month=args.month,
                models=DBT_FACT_MODELS[index + 1 :],
                stage_name=DBT_FACT_STAGE,
                reason=f"Skipped because fact model {model_name} failed",
            )
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
