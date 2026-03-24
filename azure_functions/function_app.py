"""
Azure Functions App — QMS ETL Pipeline
Function App:  func-qms-etl-prod

Schedule (all times UTC):
  SAP exports run at 06:00 UTC (05:00 CET) Mon-Fri
  Azure Functions ingest at 07:30 UTC Mon-Fri  (90-min buffer after SAP)
  Cold extract SAP runs 1st of month at 06:00 UTC
  Cold extract Azure ingests 1st of month at 09:00 UTC (3-hr buffer, larger query)
  Power BI refreshes at 08:00 UTC daily

Functions:
  1.  cold_extract_timer        — Timer (1st of month 09:00 UTC): Cold_Extract Email → Bronze
  2.  cold_extract_http         — HTTP manual trigger
  3.  fact_sales_daily_timer    — Timer (Mon-Fri 07:30 UTC): FACT_SALES_DAILY_INCREMENTAL → Bronze
  4.  fact_sales_daily_http     — HTTP manual trigger
  5.  dim_tables_timer         — Timer (Mon-Fri 07:30 UTC): dim_tables Email → Bronze Blob
  6.  dim_tables_http           — HTTP manual trigger
  7.  parquet_dim_tables_timer  — Timer (Mon-Fri 07:35 UTC): Dim CSVs → Parquet
  8.  parquet_dim_tables_http   — HTTP manual trigger
  9.  parquet_cold_timer        — Timer (1st of month 09:05 UTC): Cold CSV → Parquet
  10. parquet_cold_http         — HTTP manual trigger
  11. parquet_daily_timer       — Timer (Mon-Fri 07:35 UTC): Daily CSV → Parquet
  12. parquet_daily_http        — HTTP manual trigger
  13. parquet_dim_tables_timer  — Timer (Mon-Fri 07:35 UTC): All Dim CSVs → Parquet
  14. parquet_dim_tables_http   — HTTP manual trigger
  15. health                    — HTTP GET: Health check / status

Architecture:
  SAP B1 (CRON) → Email → Graph API → Bronze (CSV) → Silver (Parquet) → Power BI
  State tracking via blob: bronze/state/{pipeline}_state.json
  Fact sales daily uses 30-day rolling UpdateDate window to bridge monthly cold extracts.

Deployment:
  az functionapp publish func-qms-etl-prod --python
  — or GitHub Actions (.github/workflows/azure-functions-deploy.yml)
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone

import azure.functions as func

# ─────────────────────────────────────────────
# Path setup — so we can import src.core / src.pipelines / src.transforms
# In deployment: src/ is copied alongside function_app.py
# In local dev: src/ lives in ../src/
# ─────────────────────────────────────────────
_FUNC_ROOT = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_FUNC_ROOT)

# Add both locations — whichever contains the modules wins
for path in [_FUNC_ROOT, _PROJECT_ROOT]:
    if path not in sys.path:
        sys.path.insert(0, path)

app = func.FunctionApp()
log = logging.getLogger("qms-etl")


# ═════════════════════════════════════════════
# HELPERS — run pipelines
# ═════════════════════════════════════════════

def _send_alert_if_needed(pipeline_name: str, result: dict) -> None:
    from src.core.alerting import send_failure_alert, should_send_failure_alert
    from src.pipelines.config import get_pipeline

    if not should_send_failure_alert(result):
        return

    cfg = get_pipeline(pipeline_name)
    try:
        send_failure_alert(
            cfg,
            pipeline_name=pipeline_name,
            result=result,
            environment=os.getenv("ENVIRONMENT", "prod"),
            run_date=result.get("date"),
        )
    except Exception as exc:
        log.warning("%s alert send failed: %s", pipeline_name, exc)


def _run_ingest(pipeline_name: str, include_processed: bool = False,
                auto_transform: bool = True) -> dict:
    """Run an email → blob ingest pipeline by name.

    When auto_transform=True (default), the corresponding Bronze → Silver
    transform runs automatically after new files are uploaded.
    """
    from src.pipelines.config import get_pipeline
    from src.core.pipeline_runner import process_emails

    cfg = get_pipeline(pipeline_name)
    result = process_emails(cfg, dry_run=False, include_processed=include_processed,
                            auto_transform=auto_transform)
    _send_alert_if_needed(pipeline_name, result)
    return result


def _run_transform(pipeline_name: str, date: str | None = None) -> dict:
    """Run a Bronze → Silver transform by name."""
    from src.transforms import cold_extract_to_parquet
    from src.transforms import dim_tables_to_parquet
    from src.transforms import fact_sales_daily_to_parquet

    transform_map = {
        "cold_extract":     cold_extract_to_parquet.transform,
        "fact_sales_daily": fact_sales_daily_to_parquet.transform,
        "dim_tables":       dim_tables_to_parquet.transform,
    }
    fn = transform_map[pipeline_name]
    result = fn(date=date)
    _send_alert_if_needed(pipeline_name, result)
    return result


# ═════════════════════════════════════════════
# INGEST: Cold Extract
# ═════════════════════════════════════════════

@app.timer_trigger(schedule="0 0 9 1 * *", arg_name="timer", run_on_startup=False)
def cold_extract_timer(timer: func.TimerRequest) -> None:
    """Cold_Extract emails → Bronze blob (timer, 1st of month 09:00 UTC)."""
    log.info("cold_extract_timer — START (past_due=%s)", timer.past_due)
    try:
        stats = _run_ingest("cold_extract")
        log.info("cold_extract_timer — DONE: %d emails, %d files, %d errors",
                 stats["emails_processed"], stats["files_uploaded"], len(stats["errors"]))
    except Exception as e:
        log.exception("cold_extract_timer — FAILED: %s", e)
        raise


@app.route(route="cold-extract", methods=["POST", "GET"], auth_level=func.AuthLevel.FUNCTION)
def cold_extract_http(req: func.HttpRequest) -> func.HttpResponse:
    """Cold_Extract emails → Bronze blob (HTTP manual)."""
    include_all = req.params.get("all", "").lower() in ("true", "1", "yes")
    try:
        stats = _run_ingest("cold_extract", include_processed=include_all)
        return func.HttpResponse(json.dumps(stats, indent=2, default=str),
                                 mimetype="application/json", status_code=200)
    except Exception as e:
        log.exception("cold_extract_http — FAILED: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}),
                                 mimetype="application/json", status_code=500)


# ═════════════════════════════════════════════
# INGEST: Fact Sales Daily
# ═════════════════════════════════════════════

@app.timer_trigger(schedule="0 30 7 * * 1-5", arg_name="timer", run_on_startup=False)
def fact_sales_daily_timer(timer: func.TimerRequest) -> None:
    """FACT_SALES_DAILY_INCREMENTAL emails → Bronze blob (timer, Mon-Fri 07:30 UTC)."""
    log.info("fact_sales_daily_timer — START (past_due=%s)", timer.past_due)
    try:
        stats = _run_ingest("fact_sales_daily")
        log.info("fact_sales_daily_timer — DONE: %d emails, %d files, %d errors",
                 stats["emails_processed"], stats["files_uploaded"], len(stats["errors"]))
    except Exception as e:
        log.exception("fact_sales_daily_timer — FAILED: %s", e)
        raise


@app.route(route="fact-sales-daily", methods=["POST", "GET"], auth_level=func.AuthLevel.FUNCTION)
def fact_sales_daily_http(req: func.HttpRequest) -> func.HttpResponse:
    """FACT_SALES_DAILY_INCREMENTAL emails → Bronze blob (HTTP manual)."""
    include_all = req.params.get("all", "").lower() in ("true", "1", "yes")
    try:
        stats = _run_ingest("fact_sales_daily", include_processed=include_all)
        return func.HttpResponse(json.dumps(stats, indent=2, default=str),
                                 mimetype="application/json", status_code=200)
    except Exception as e:
        log.exception("fact_sales_daily_http — FAILED: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}),
                                 mimetype="application/json", status_code=500)


# ═════════════════════════════════════════════
# INGEST: Dim Customer
# ═════════════════════════════════════════════

@app.timer_trigger(schedule="0 30 7 * * 1-5", arg_name="timer", run_on_startup=False)
def dim_customer_timer(timer: func.TimerRequest) -> None:
    """Dim_Customer_Extract emails → Bronze blob (timer, Mon-Fri 07:30 UTC)."""
    log.info("dim_customer_timer — START (past_due=%s)", timer.past_due)
    try:
        stats = _run_ingest("dim_customer")
        log.info("dim_customer_timer — DONE: %d emails, %d files, %d errors",
                 stats["emails_processed"], stats["files_uploaded"], len(stats["errors"]))
    except Exception as e:
        log.exception("dim_customer_timer — FAILED: %s", e)
        raise


@app.route(route="dim-customer", methods=["POST", "GET"], auth_level=func.AuthLevel.FUNCTION)
def dim_customer_http(req: func.HttpRequest) -> func.HttpResponse:
    """Dim_Customer_Extract emails → Bronze blob (HTTP manual)."""
    include_all = req.params.get("all", "").lower() in ("true", "1", "yes")
    try:
        stats = _run_ingest("dim_customer", include_processed=include_all)
        return func.HttpResponse(json.dumps(stats, indent=2, default=str),
                                 mimetype="application/json", status_code=200)
    except Exception as e:
        log.exception("dim_customer_http — FAILED: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}),
                                 mimetype="application/json", status_code=500)


# ═════════════════════════════════════════════
# INGEST: Dim Product
# ═════════════════════════════════════════════

@app.timer_trigger(schedule="0 30 7 * * 1-5", arg_name="timer", run_on_startup=False)
def dim_product_timer(timer: func.TimerRequest) -> None:
    """dim_product_master emails → Bronze blob (timer, Mon-Fri 07:30 UTC)."""
    log.info("dim_product_timer — START (past_due=%s)", timer.past_due)
    try:
        stats = _run_ingest("dim_product")
        log.info("dim_product_timer — DONE: %d emails, %d files, %d errors",
                 stats["emails_processed"], stats["files_uploaded"], len(stats["errors"]))
    except Exception as e:
        log.exception("dim_product_timer — FAILED: %s", e)
        raise


@app.route(route="dim-product", methods=["POST", "GET"], auth_level=func.AuthLevel.FUNCTION)
def dim_product_http(req: func.HttpRequest) -> func.HttpResponse:
    """dim_product_master emails → Bronze blob (HTTP manual)."""
    include_all = req.params.get("all", "").lower() in ("true", "1", "yes")
    try:
        stats = _run_ingest("dim_product", include_processed=include_all)
        return func.HttpResponse(json.dumps(stats, indent=2, default=str),
                                 mimetype="application/json", status_code=200)
    except Exception as e:
        log.exception("dim_product_http — FAILED: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}),
                                 mimetype="application/json", status_code=500)


# ═════════════════════════════════════════════
# TRANSFORM: Cold Extract → Parquet
# ═════════════════════════════════════════════

@app.timer_trigger(schedule="0 5 9 1 * *", arg_name="timer", run_on_startup=False)
def parquet_cold_timer(timer: func.TimerRequest) -> None:
    """Sales CSV → Silver Parquet (timer, 1st of month 09:05 UTC — safety net after ingest)."""
    log.info("parquet_cold_timer — START (past_due=%s)", timer.past_due)
    try:
        result = _run_transform("cold_extract")
        log.info("parquet_cold_timer — DONE: %s, %d files, %d rows",
                 result["status"], result.get("files_converted", 0), result.get("total_rows", 0))
    except Exception as e:
        log.exception("parquet_cold_timer — FAILED: %s", e)
        raise


@app.route(route="parquet-cold", methods=["POST", "GET"], auth_level=func.AuthLevel.FUNCTION)
def parquet_cold_http(req: func.HttpRequest) -> func.HttpResponse:
    """Sales CSV → Silver Parquet (HTTP manual)."""
    date = req.params.get("date")
    try:
        result = _run_transform("cold_extract", date=date)
        return func.HttpResponse(json.dumps(result, indent=2, default=str),
                                 mimetype="application/json", status_code=200)
    except Exception as e:
        log.exception("parquet_cold_http — FAILED: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}),
                                 mimetype="application/json", status_code=500)


# ═════════════════════════════════════════════
# TRANSFORM: Fact Sales Daily → Parquet
# ═════════════════════════════════════════════

@app.timer_trigger(schedule="0 35 7 * * 1-5", arg_name="timer", run_on_startup=False)
def parquet_daily_timer(timer: func.TimerRequest) -> None:
    """Daily sales CSV → Silver Parquet (timer, Mon-Fri 07:35 UTC — safety net after ingest)."""
    log.info("parquet_daily_timer — START (past_due=%s)", timer.past_due)
    try:
        result = _run_transform("fact_sales_daily")
        log.info("parquet_daily_timer — DONE: %s, %d files, %d rows",
                 result["status"], result.get("files_converted", 0), result.get("total_rows", 0))
    except Exception as e:
        log.exception("parquet_daily_timer — FAILED: %s", e)
        raise


@app.route(route="parquet-daily", methods=["POST", "GET"], auth_level=func.AuthLevel.FUNCTION)
def parquet_daily_http(req: func.HttpRequest) -> func.HttpResponse:
    """Daily sales CSV → Silver Parquet (HTTP manual)."""
    date = req.params.get("date")
    try:
        result = _run_transform("fact_sales_daily", date=date)
        return func.HttpResponse(json.dumps(result, indent=2, default=str),
                                 mimetype="application/json", status_code=200)
    except Exception as e:
        log.exception("parquet_daily_http — FAILED: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}),
                                 mimetype="application/json", status_code=500)


# ═════════════════════════════════════════════
# TRANSFORM: Dim Customer → Parquet
# ═════════════════════════════════════════════

@app.timer_trigger(schedule="0 35 7 * * 1-5", arg_name="timer", run_on_startup=False)
def parquet_dim_customer_timer(timer: func.TimerRequest) -> None:
    """Customer CSV → Silver Parquet (timer, Mon-Fri 07:35 UTC — safety net after ingest)."""
    log.info("parquet_dim_customer_timer — START (past_due=%s)", timer.past_due)
    try:
        result = _run_transform("dim_customer")
        log.info("parquet_dim_customer_timer — DONE: %s, %d files, %d rows",
                 result["status"], result.get("files_converted", 0), result.get("total_rows", 0))
    except Exception as e:
        log.exception("parquet_dim_customer_timer — FAILED: %s", e)
        raise


@app.route(route="parquet-dim-customer", methods=["POST", "GET"], auth_level=func.AuthLevel.FUNCTION)
def parquet_dim_customer_http(req: func.HttpRequest) -> func.HttpResponse:
    """Customer CSV → Silver Parquet (HTTP manual)."""
    date = req.params.get("date")
    try:
        result = _run_transform("dim_customer", date=date)
        return func.HttpResponse(json.dumps(result, indent=2, default=str),
                                 mimetype="application/json", status_code=200)
    except Exception as e:
        log.exception("parquet_dim_customer_http — FAILED: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}),
                                 mimetype="application/json", status_code=500)


# ═════════════════════════════════════════════
# TRANSFORM: Dim Product → Parquet
# ═════════════════════════════════════════════

@app.timer_trigger(schedule="0 35 7 * * 1-5", arg_name="timer", run_on_startup=False)
def parquet_dim_product_timer(timer: func.TimerRequest) -> None:
    """Product CSV → Silver Parquet (timer, Mon-Fri 07:35 UTC — safety net after ingest)."""
    log.info("parquet_dim_product_timer — START (past_due=%s)", timer.past_due)
    try:
        result = _run_transform("dim_product")
        log.info("parquet_dim_product_timer — DONE: %s, %d files, %d rows",
                 result["status"], result.get("files_converted", 0), result.get("total_rows", 0))
    except Exception as e:
        log.exception("parquet_dim_product_timer — FAILED: %s", e)
        raise


@app.route(route="parquet-dim-product", methods=["POST", "GET"], auth_level=func.AuthLevel.FUNCTION)
def parquet_dim_product_http(req: func.HttpRequest) -> func.HttpResponse:
    """Product CSV → Silver Parquet (HTTP manual)."""
    date = req.params.get("date")
    try:
        result = _run_transform("dim_product", date=date)
        return func.HttpResponse(json.dumps(result, indent=2, default=str),
                                 mimetype="application/json", status_code=200)
    except Exception as e:
        log.exception("parquet_dim_product_http — FAILED: %s", e)
        return func.HttpResponse(json.dumps({"error": str(e)}),
                                 mimetype="application/json", status_code=500)


# ═════════════════════════════════════════════
# HEALTH CHECK
# ═════════════════════════════════════════════

@app.route(route="health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def health(req: func.HttpRequest) -> func.HttpResponse:
    """Health check — returns pipeline status and config summary."""
    from src.pipelines.config import PIPELINES, get_pipeline
    from src.core.blob_client import load_state

    # Check which env vars are set (don't leak values)
    env_check = {}
    for var in [
        "GRAPH_TENANT_ID", "GRAPH_CLIENT_ID", "GRAPH_CLIENT_SECRET",
        "PA_MAILBOX_ADDRESS", "DATALAKE_ACCOUNT_NAME", "DATALAKE_ACCOUNT_KEY",
    ]:
        env_check[var] = "✅ set" if os.environ.get(var, "") else "❌ missing"

    # Pipeline state from blob
    state_info = {}
    for name in ["cold_extract", "fact_sales_daily", "dim_tables"]:
        try:
            cfg = get_pipeline(name)
            state = load_state(cfg)
            state_info[name] = {
                "processed_ids": len(state.get("processed_ids", [])),
                "last_run": state.get("last_run"),
            }
        except Exception as e:
            state_info[name] = {"error": str(e)}

    body = {
        "status": "healthy",
        "function_app": "func-qms-etl-prod",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "python_version": sys.version,
        "functions": 13,
        "environment_variables": env_check,
        "state": state_info,
        "pipelines": {
            name: {
                "description": cfg.get("description", ""),
                "blob_prefix": cfg.get("blob_prefix", ""),
                "silver_prefix": cfg.get("silver_prefix", ""),
            }
            for name, cfg in PIPELINES.items()
        },
    }
    return func.HttpResponse(
        json.dumps(body, indent=2, default=str),
        mimetype="application/json", status_code=200,
    )
