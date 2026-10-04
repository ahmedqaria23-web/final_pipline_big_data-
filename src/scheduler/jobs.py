"""
Scheduled Jobs Service & Background Scheduler.
Implements:
1. refresh_materialized_views: Incremental refresh of daily_sales_summary & top_products_summary.
2. generate_periodic_report: Generates executive analytics summary and saves to reports/.

Features:
- Configurable intervals and timezone.
- Identical execution path for manual and scheduled triggers.
- Detailed audit logging to MongoDB and local JSON report.
- Explicit failure reporting (no silent failures).
"""

import sys
import json
import time
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Callable
from pathlib import Path

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pymongo import DESCENDING
from pymongo.database import Database
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config.settings import (
    REPORT_DIR,
    COLLECTION_JOB_LOGS,
    SCHEDULER_TIMEZONE,
    JOB_REFRESH_MV_INTERVAL_MINUTES,
    JOB_REPORT_INTERVAL_MINUTES
)
from src.mongodb.mongo_setup import get_mongo_db
from src.views.materialized_views import refresh_materialized_views
from src.analytics.reports import (
    report_sales_by_city,
    report_top_products,
    report_orders_by_status
)
from src.analytics.queries import json_safe

logger = logging.getLogger(__name__)

# Global BackgroundScheduler singleton
_scheduler: Optional[BackgroundScheduler] = None


def _resolve_db(db: Optional[Database]) -> Database:
    if db is not None:
        return db
    return get_mongo_db()


# ─────────────────────────────────────────────────────────────
# 1. AUDIT LOGGING SERVICE
# ─────────────────────────────────────────────────────────────
def log_job_execution(
    db: Database,
    log_doc: Dict[str, Any]
) -> None:
    """
    Persists structured job execution details into MongoDB 'scheduled_job_logs'
    and appends to 'reports/job_logs.json'.
    """
    # 1. MongoDB collection insert
    try:
        coll = db[COLLECTION_JOB_LOGS]
        coll.insert_one(dict(log_doc))
    except Exception as e:
        logger.error(f"Failed to write job log to MongoDB: {e}")

    # 2. Local JSON log append
    try:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        file_path = REPORT_DIR / "job_logs.json"
        existing_logs = []
        if file_path.exists():
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    existing_logs = json.load(f)
            except Exception:
                existing_logs = []

        existing_logs.append(json_safe(log_doc))
        # Keep last 100 executions in file
        if len(existing_logs) > 100:
            existing_logs = existing_logs[-100:]

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(existing_logs, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Failed to append to job_logs.json: {e}")


def get_job_logs(
    limit: int = 20,
    job_name: Optional[str] = None,
    db: Optional[Database] = None
) -> List[Dict[str, Any]]:
    """Retrieves recent job execution audit records from MongoDB."""
    target_db = _resolve_db(db)
    coll = target_db[COLLECTION_JOB_LOGS]
    query = {"job_name": job_name} if job_name else {}
    cursor = coll.find(query).sort("start_time", DESCENDING).limit(limit)
    return [json_safe(doc) for doc in cursor]


# ─────────────────────────────────────────────────────────────
# 2. JOB 1: refresh_materialized_views
# ─────────────────────────────────────────────────────────────
def job_refresh_materialized_views(db: Optional[Database] = None) -> Dict[str, Any]:
    """
    Scheduled Job 1: Refreshes daily_sales_summary and top_products_summary
    using the incremental watermark mechanism.
    """
    target_db = _resolve_db(db)
    # Uses incremental mode by default
    result = refresh_materialized_views(full_refresh=False, db=target_db)
    return result


# ─────────────────────────────────────────────────────────────
# 3. JOB 2: generate_periodic_report
# ─────────────────────────────────────────────────────────────
def job_generate_periodic_report(db: Optional[Database] = None) -> Dict[str, Any]:
    """
    Scheduled Job 2: Aggregates high-level business KPIs, executes top reports,
    and produces an executive summary saved to reports/scheduled_periodic_report.json.
    """
    target_db = _resolve_db(db)
    coll_val = target_db["orders_validated"]
    total_validated = coll_val.count_documents({})

    # Run key analytics reports
    city_report = report_sales_by_city(limit=5, db=target_db)
    products_report = report_top_products(limit=5, db=target_db)
    status_report = report_orders_by_status(db=target_db)

    # Calculate overall KPIs
    total_revenue = sum(r.get("total_revenue", 0.0) for r in city_report)
    top_city = city_report[0]["city"] if city_report else "N/A"
    top_product = products_report[0]["product_name"] if products_report else "N/A"

    report_payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "database": target_db.name,
        "kpis": {
            "total_validated_orders": total_validated,
            "top_city_by_revenue": top_city,
            "top_product": top_product
        },
        "top_5_cities": city_report,
        "top_5_products": products_report,
        "status_distribution": status_report
    }

    # Persist report file
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_file = REPORT_DIR / "scheduled_periodic_report.json"
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report_payload, f, indent=2, ensure_ascii=False)

    return {
        "report_file": str(report_file),
        "total_validated_orders": total_validated,
        "top_city": top_city,
        "top_product": top_product
    }


# ─────────────────────────────────────────────────────────────
# 4. JOB REGISTRY & UNIFIED EXECUTION RUNNER
# ─────────────────────────────────────────────────────────────
REGISTERED_JOBS: Dict[str, Dict[str, Any]] = {
    "refresh_materialized_views": {
        "name": "refresh_materialized_views",
        "description": "Performs incremental $merge refresh of daily_sales_summary and top_products_summary.",
        "func": job_refresh_materialized_views,
        "default_interval_minutes": JOB_REFRESH_MV_INTERVAL_MINUTES
    },
    "generate_periodic_report": {
        "name": "generate_periodic_report",
        "description": "Computes executive KPIs and exports consolidated periodic report to reports/.",
        "func": job_generate_periodic_report,
        "default_interval_minutes": JOB_REPORT_INTERVAL_MINUTES
    }
}


def run_job(
    job_name: str,
    trigger_type: str = "manual",
    db: Optional[Database] = None
) -> Dict[str, Any]:
    """
    Executes a registered job function (used identically by manual CLI and scheduled triggers).
    Logs execution, timing, and errors to MongoDB.
    Raises RuntimeError on failure (never fails silently).
    """
    if job_name not in REGISTERED_JOBS:
        raise KeyError(f"Job '{job_name}' is not registered. Available jobs: {list(REGISTERED_JOBS.keys())}")

    target_db = _resolve_db(db)
    job_info = REGISTERED_JOBS[job_name]
    job_func = job_info["func"]

    start_time = datetime.now(timezone.utc)
    start_perf = time.perf_counter()
    status = "SUCCESS"
    error_msg = None
    output_details = None

    try:
        output_details = job_func(db=target_db)
    except Exception as e:
        status = "FAILED"
        error_msg = str(e)
        logger.error(f"Job '{job_name}' execution failed: {e}", exc_info=True)
    finally:
        end_time = datetime.now(timezone.utc)
        duration_sec = round(time.perf_counter() - start_perf, 4)

        log_payload = {
            "job_name": job_name,
            "trigger_type": trigger_type,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration_sec,
            "status": status,
            "details": output_details,
            "error": error_msg
        }

        log_job_execution(target_db, log_payload)

    if status == "FAILED":
        raise RuntimeError(f"Job '{job_name}' failed with error: {error_msg}")

    return log_payload


# ─────────────────────────────────────────────────────────────
# 5. APSCHEDULER LIFECYCLE MANAGEMENT
# ─────────────────────────────────────────────────────────────
def get_scheduler() -> BackgroundScheduler:
    """Returns or instantiates the singleton APScheduler instance."""
    global _scheduler
    if _scheduler is None:
        _scheduler = BackgroundScheduler(timezone=SCHEDULER_TIMEZONE)
    return _scheduler


def start_scheduler(db: Optional[Database] = None) -> BackgroundScheduler:
    """
    Configures and starts the background job scheduler.
    Registers both jobs with their configured intervals.
    """
    sched = get_scheduler()
    target_db = _resolve_db(db)

    # Register Job 1: refresh_materialized_views
    if not sched.get_job("refresh_materialized_views"):
        sched.add_job(
            func=run_job,
            trigger=IntervalTrigger(minutes=JOB_REFRESH_MV_INTERVAL_MINUTES),
            args=["refresh_materialized_views", "scheduled", target_db],
            id="refresh_materialized_views",
            name="Refresh Materialized Views",
            replace_existing=True
        )

    # Register Job 2: generate_periodic_report
    if not sched.get_job("generate_periodic_report"):
        sched.add_job(
            func=run_job,
            trigger=IntervalTrigger(minutes=JOB_REPORT_INTERVAL_MINUTES),
            args=["generate_periodic_report", "scheduled", target_db],
            id="generate_periodic_report",
            name="Generate Periodic Analytics Report",
            replace_existing=True
        )

    if not sched.running:
        sched.start()
        logger.info(f"APScheduler started successfully with timezone '{SCHEDULER_TIMEZONE}'.")

    return sched


def stop_scheduler(wait: bool = False) -> None:
    """Shuts down the background scheduler if running."""
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        _scheduler.shutdown(wait=wait)
        logger.info("APScheduler stopped.")
        _scheduler = None


def list_jobs(db: Optional[Database] = None) -> List[Dict[str, Any]]:
    """
    Lists all registered jobs with their schedule, active status,
    next run time, and last execution status.
    """
    target_db = _resolve_db(db)
    sched = get_scheduler()
    is_running = sched.running if sched else False

    results = []
    for name, info in REGISTERED_JOBS.items():
        scheduled_job = sched.get_job(name) if sched and is_running else None
        next_run = scheduled_job.next_run_time.isoformat() if scheduled_job and scheduled_job.next_run_time else None

        # Fetch last log from MongoDB
        last_log = target_db[COLLECTION_JOB_LOGS].find_one(
            {"job_name": name},
            sort=[("start_time", DESCENDING)]
        )

        results.append({
            "name": name,
            "description": info["description"],
            "interval_minutes": info["default_interval_minutes"],
            "is_scheduled": scheduled_job is not None,
            "next_run_time": next_run,
            "last_run_time": last_log.get("start_time") if last_log else None,
            "last_status": last_log.get("status") if last_log else "NEVER_RUN"
        })

    return results
