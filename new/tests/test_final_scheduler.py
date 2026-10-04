"""
Test Suite for Final Project Phase 5: Scheduled Jobs.
Verifies:
1. Job discovery and registry listing.
2. Manual execution of Job 1 (refresh_materialized_views).
3. Manual execution of Job 2 (generate_periodic_report).
4. Audit logging in MongoDB and local JSON file.
5. Error handling and non-silent failure.
6. Scheduler lifecycle (start and stop).
"""

import json
import pytest
from pathlib import Path
from src.mongodb.mongo_setup import initialize_database
from config.settings import (
    COLLECTION_VALIDATED,
    COLLECTION_JOB_LOGS,
    COLLECTION_DAILY_SALES,
    REPORT_DIR
)
from src.scheduler.jobs import (
    REGISTERED_JOBS,
    list_jobs,
    run_job,
    get_job_logs,
    start_scheduler,
    stop_scheduler,
    job_refresh_materialized_views,
    job_generate_periodic_report
)


@pytest.fixture
def scheduler_test_db():
    """Populates an isolated test database with order records for job testing."""
    db_name = "test_scheduler_phase5_db"
    db = initialize_database(db_name=db_name)
    coll = db[COLLECTION_VALIDATED]

    sample_orders = [
        {
            "id_order": "ORD-SCHED-001",
            "order_date": "2025-01-10T10:00:00Z",
            "updated_at": "2025-01-10T10:00:00Z",
            "status": "مؤكد",
            "customer": {"customer_id": "CUS-1", "name": "Ali", "phone": "967771234567", "email": "a@ex.com", "address": {"city": "صنعاء", "district": "حدة"}},
            "items": [{"sku": "SKU-A", "name": "Item A", "qty": 2, "unit_price": 500.0, "total": 1000.0}],
            "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "YER", "amount": 1000.0},
            "total_amount": 1000.0,
            "quality_status": "valid"
        },
        {
            "id_order": "ORD-SCHED-002",
            "order_date": "2025-01-15T12:00:00Z",
            "updated_at": "2025-01-15T12:00:00Z",
            "status": "تم التسليم",
            "customer": {"customer_id": "CUS-2", "name": "Sara", "phone": "967731234567", "email": "s@ex.com", "address": {"city": "عدن", "district": "صيرة"}},
            "items": [{"sku": "SKU-B", "name": "Item B", "qty": 1, "unit_price": 200.0, "total": 200.0}],
            "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "YER", "amount": 200.0},
            "total_amount": 200.0,
            "quality_status": "valid"
        }
    ]

    coll.insert_many(sample_orders)
    yield db
    stop_scheduler()
    db.client.drop_database(db_name)


# ─────────────────────────────────────────────────────────────
# 1. Job Discovery Tests
# ─────────────────────────────────────────────────────────────
def test_list_jobs_returns_at_least_2_jobs(scheduler_test_db):
    jobs = list_jobs(db=scheduler_test_db)
    assert len(jobs) >= 2, f"Expected at least 2 jobs, got {len(jobs)}"

    job_names = [j["name"] for j in jobs]
    assert "refresh_materialized_views" in job_names
    assert "generate_periodic_report" in job_names

    for j in jobs:
        assert "interval_minutes" in j
        assert j["interval_minutes"] > 0
        assert "description" in j


# ─────────────────────────────────────────────────────────────
# 2. Manual Execution of Job 1: refresh_materialized_views
# ─────────────────────────────────────────────────────────────
def test_execute_job_refresh_materialized_views(scheduler_test_db):
    db = scheduler_test_db
    res = run_job("refresh_materialized_views", trigger_type="manual", db=db)

    assert res["status"] == "SUCCESS"
    assert res["job_name"] == "refresh_materialized_views"
    assert res["trigger_type"] == "manual"
    assert res["duration_seconds"] >= 0.0

    # Verify materialized views were populated in MongoDB
    assert db[COLLECTION_DAILY_SALES].count_documents({}) >= 1

    # Verify audit log in MongoDB
    logs = get_job_logs(limit=5, job_name="refresh_materialized_views", db=db)
    assert len(logs) >= 1
    assert logs[0]["status"] == "SUCCESS"
    assert "start_time" in logs[0]
    assert "end_time" in logs[0]


# ─────────────────────────────────────────────────────────────
# 3. Manual Execution of Job 2: generate_periodic_report
# ─────────────────────────────────────────────────────────────
def test_execute_job_generate_periodic_report(scheduler_test_db):
    db = scheduler_test_db
    res = run_job("generate_periodic_report", trigger_type="manual", db=db)

    assert res["status"] == "SUCCESS"
    assert res["job_name"] == "generate_periodic_report"

    # Verify report file was created
    report_file = REPORT_DIR / "scheduled_periodic_report.json"
    assert report_file.exists()

    with open(report_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        assert "kpis" in data
        assert data["kpis"]["total_validated_orders"] == 2
        assert "top_5_cities" in data

    # Verify audit log in MongoDB
    logs = get_job_logs(limit=5, job_name="generate_periodic_report", db=db)
    assert len(logs) >= 1
    assert logs[0]["status"] == "SUCCESS"


# ─────────────────────────────────────────────────────────────
# 4. Error Handling & Non-Silent Failure
# ─────────────────────────────────────────────────────────────
def test_run_unregistered_job_raises_key_error(scheduler_test_db):
    with pytest.raises(KeyError, match="not registered"):
        run_job("non_existent_job", db=scheduler_test_db)


def test_failing_job_does_not_fail_silently(scheduler_test_db, monkeypatch):
    db = scheduler_test_db

    # Force a failure inside the job function
    def faulty_job(db=None):
        raise ValueError("Simulated database connection failure")

    monkeypatch.setitem(REGISTERED_JOBS, "faulty_test_job", {
        "name": "faulty_test_job",
        "description": "Faulty job for testing error capturing",
        "func": faulty_job,
        "default_interval_minutes": 10
    })

    # Execution must raise RuntimeError and NOT silently succeed
    with pytest.raises(RuntimeError, match="Simulated database connection failure"):
        run_job("faulty_test_job", db=db)

    # Verify that the failure was explicitly audited in MongoDB
    logs = get_job_logs(limit=1, job_name="faulty_test_job", db=db)
    assert len(logs) == 1
    assert logs[0]["status"] == "FAILED"
    assert "Simulated database connection failure" in logs[0]["error"]


# ─────────────────────────────────────────────────────────────
# 5. APScheduler Lifecycle Management
# ─────────────────────────────────────────────────────────────
def test_scheduler_start_and_stop_lifecycle(scheduler_test_db):
    sched = start_scheduler(db=scheduler_test_db)
    assert sched.running is True

    # Verify both jobs are scheduled
    assert sched.get_job("refresh_materialized_views") is not None
    assert sched.get_job("generate_periodic_report") is not None

    # Clean shutdown
    stop_scheduler()
    assert sched.running is False
