"""
Comprehensive Verification Suite for Final Project Phase 6: FastAPI Execution API.
Tests every required endpoint, error handling, parameter validation, and status codes.
"""

import csv
import json
import pytest
from fastapi.testclient import TestClient
from pathlib import Path

from src.mongodb.mongo_setup import initialize_database
from config.settings import COLLECTION_VALIDATED
from src.api.main import app


@pytest.fixture(scope="module")
def api_client():
    """Provides a TestClient instance without launching a real background server."""
    with TestClient(app) as client:
        yield client


@pytest.fixture(scope="module")
def api_test_data():
    """Populates test data into database to verify query and aggregation endpoints."""
    db_name = "ecommerce_store"  # Default test db
    db = initialize_database(db_name=db_name)
    return db


# ─────────────────────────────────────────────────────────────
# 1. System & OpenAPI Documentation Tests
# ─────────────────────────────────────────────────────────────
def test_get_health(api_client):
    response = api_client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["database"] == "connected"
    assert "timestamp_utc" in data


def test_openapi_docs_available(api_client):
    response = api_client.get("/docs")
    assert response.status_code == 200

    response_json = api_client.get("/openapi.json")
    assert response_json.status_code == 200
    schema = response_json.json()
    paths = schema.get("paths", {})
    assert "/health" in paths
    assert "/ingest" in paths
    assert "/indexes" in paths
    assert "/queries" in paths
    assert "/queries/{name}" in paths
    assert "/aggregations" in paths
    assert "/aggregations/{name}" in paths
    assert "/refresh-mv" in paths
    assert "/jobs" in paths
    assert "/jobs/{name}/run" in paths


# ─────────────────────────────────────────────────────────────
# 2. Indexes Endpoint Tests
# ─────────────────────────────────────────────────────────────
def test_post_indexes(api_client):
    response = api_client.post("/indexes")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "SUCCESS"
    assert "created_indexes" in data
    assert data["active_indexes_count"] >= 3


# ─────────────────────────────────────────────────────────────
# 3. Queries Endpoints Tests
# ─────────────────────────────────────────────────────────────
def test_get_queries_list(api_client):
    response = api_client.get("/queries")
    assert response.status_code == 200
    queries = response.json()
    assert len(queries) >= 5
    names = [q["name"] for q in queries]
    assert "orders_by_customer" in names
    assert "orders_by_city" in names
    assert "high_value_orders" in names


def test_execute_query_valid(api_client):
    # Query high value orders with limit=2
    response = api_client.get("/queries/high_value_orders?min_amount=100.0&limit=2")
    assert response.status_code == 200
    data = response.json()
    assert data["query_name"] == "high_value_orders"
    assert "data" in data
    assert len(data["data"]) <= 2


def test_execute_query_not_found(api_client):
    response = api_client.get("/queries/unknown_query_name")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_execute_query_malformed_parameter(api_client):
    # Pass non-numeric value where float is expected
    response = api_client.get("/queries/high_value_orders?min_amount=invalid_number")
    assert response.status_code == 400
    assert "Invalid parameter type" in response.json()["detail"]


def test_execute_query_validation_error(api_client):
    # Negative min_amount triggers ValueError in query logic
    response = api_client.get("/queries/high_value_orders?min_amount=-50")
    assert response.status_code == 400
    assert "negative" in response.json()["detail"]


# ─────────────────────────────────────────────────────────────
# 4. Aggregations Endpoints Tests
# ─────────────────────────────────────────────────────────────
def test_get_aggregations_list(api_client):
    response = api_client.get("/aggregations")
    assert response.status_code == 200
    aggs = response.json()
    assert len(aggs) >= 5
    names = [a["name"] for a in aggs]
    assert "sales_by_city" in names
    assert "top_products" in names
    assert "orders_by_status" in names


def test_execute_aggregation_valid(api_client):
    response = api_client.get("/aggregations/sales_by_city?limit=3")
    assert response.status_code == 200
    data = response.json()
    assert data["aggregation_name"] == "sales_by_city"
    assert "data" in data
    assert len(data["data"]) <= 3


def test_execute_aggregation_not_found(api_client):
    response = api_client.get("/aggregations/unknown_aggregation")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_execute_aggregation_malformed_param(api_client):
    response = api_client.get("/aggregations/sales_by_period?period=invalid_period")
    assert response.status_code == 400
    assert "daily" in response.json()["detail"]


# ─────────────────────────────────────────────────────────────
# 5. Materialized Views Endpoint Tests
# ─────────────────────────────────────────────────────────────
def test_post_refresh_mv(api_client):
    response = api_client.post("/refresh-mv", json={"full_refresh": False})
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "COMPLETED"
    assert "views" in data
    assert "daily_sales_summary" in data["views"]
    assert "top_products_summary" in data["views"]


# ─────────────────────────────────────────────────────────────
# 6. Scheduled Jobs Endpoints Tests
# ─────────────────────────────────────────────────────────────
def test_get_jobs_list(api_client):
    response = api_client.get("/jobs")
    assert response.status_code == 200
    jobs = response.json()
    assert len(jobs) >= 2
    names = [j["name"] for j in jobs]
    assert "refresh_materialized_views" in names
    assert "generate_periodic_report" in names


def test_post_run_job_valid(api_client):
    response = api_client.post("/jobs/refresh_materialized_views/run")
    assert response.status_code == 200
    data = response.json()
    assert data["job_name"] == "refresh_materialized_views"
    assert data["status"] == "SUCCESS"
    assert data["trigger_type"] == "manual"
    assert data["duration_seconds"] >= 0.0


def test_post_run_job_not_found(api_client):
    response = api_client.post("/jobs/unregistered_job_name/run")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


# ─────────────────────────────────────────────────────────────
# 7. Ingestion Endpoint Tests
# ─────────────────────────────────────────────────────────────
def test_post_ingest_file_not_found(api_client):
    response = api_client.post("/ingest", json={"file_path": "non_existent_file.csv"})
    assert response.status_code == 400
    assert "not found" in response.json()["detail"].lower()


def test_post_ingest_valid_file(api_client, tmp_path):
    # Create small valid test CSV
    csv_file = tmp_path / "api_test_orders.csv"
    items_json = json.dumps([{"sku": "SKU-API-1", "name": "API Item", "qty": 1, "unit_price": 500.0, "total": 500.0}], ensure_ascii=False)
    
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id_order", "order_date", "status", "customer_id", "customer_name", "phone", "email", "shipping_city", "district", "items", "payment_method", "total_amount"])
        writer.writerow(["ORD-API-001", "2025-01-31T10:00:00Z", "مؤكد", "CUS-API-1", "Saleh", "967771234567", "saleh@ex.com", "صنعاء", "حدة", items_json, "بطاقة", "500.0"])

    response = api_client.post("/ingest", json={
        "file_path": str(csv_file),
        "threshold_mb": 200.0,
        "engine": "python_batch"
    })
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "SUCCESS"
    assert "metrics" in data
    assert data["metrics"]["read_rows"] >= 1


def test_end_to_end_mv_refresh_via_api_ingest(api_client, tmp_path):
    """
    End-to-End Test for Materialized Views Lifecycle via /ingest and /refresh-mv API.
    Verifies that:
    1. Orders ingested via POST /ingest populate orders_validated with updated_at.
    2. POST /refresh-mv creates baseline materialized summaries.
    3. Re-ingesting modified orders (changed date, changed SKU, changed amount) via POST /ingest
       updates orders_validated and automatically advances updated_at.
    4. Incremental POST /refresh-mv correctly detects both OLD and NEW keys via delta queries.
    5. Old date and SKU are purged (when 0 orders remain) and new date and SKU are populated.
    6. Re-ingesting unchanged data results in idempotent up_to_date MV status.
    """
    from src.mongodb.mongo_setup import get_mongo_db
    from config.settings import (
        COLLECTION_DAILY_SALES,
        COLLECTION_TOP_PRODUCTS,
        COLLECTION_VALIDATED,
        COLLECTION_RAW
    )
    from src.views.materialized_views import save_mv_watermark
    from datetime import datetime, timezone, timedelta

    db = get_mongo_db()
    coll_daily = db[COLLECTION_DAILY_SALES]
    coll_prod = db[COLLECTION_TOP_PRODUCTS]
    coll_val = db[COLLECTION_VALIDATED]
    coll_raw = db[COLLECTION_RAW]

    test_order_ids = ["ORD-E2E-001", "ORD-E2E-002"]
    test_dates = ["2024-01-01", "2024-01-02", "2024-01-05"]
    test_skus = ["SKU-E2E-A", "SKU-E2E-B", "SKU-E2E-C"]

    def cleanup():
        coll_val.delete_many({"id_order": {"$in": test_order_ids}})
        coll_raw.delete_many({"id_order": {"$in": test_order_ids}})
        coll_daily.delete_many({"_id": {"$in": test_dates}})
        coll_prod.delete_many({"_id": {"$in": test_skus}})

    cleanup()

    try:
        # Set watermark baseline right before ingest so refresh-mv operates incrementally
        watermark_baseline = (datetime.now(timezone.utc) - timedelta(seconds=2)).isoformat()
        save_mv_watermark(db, watermark=watermark_baseline, view_name=COLLECTION_DAILY_SALES)
        save_mv_watermark(db, watermark=watermark_baseline, view_name=COLLECTION_TOP_PRODUCTS)

        # Step 1: Ingest Initial Batch via POST /ingest
        csv_file_v1 = tmp_path / "orders_batch_v1.csv"
        items_1 = json.dumps([{"sku": "SKU-E2E-A", "name": "E2E Item A", "qty": 1, "unit_price": 100.0, "total": 100.0}], ensure_ascii=False)
        items_2 = json.dumps([{"sku": "SKU-E2E-B", "name": "E2E Item B", "qty": 2, "unit_price": 100.0, "total": 200.0}], ensure_ascii=False)

        with open(csv_file_v1, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["id_order", "order_date", "status", "customer_id", "customer_name", "phone", "email", "shipping_city", "district", "items", "payment_method", "total_amount"])
            writer.writerow(["ORD-E2E-001", "2024-01-01T10:00:00Z", "مؤكد", "CUS-E2E-1", "Ali", "967771234567", "a@ex.com", "صنعاء", "حدة", items_1, "بطاقة", "100.0"])
            writer.writerow(["ORD-E2E-002", "2024-01-02T10:00:00Z", "مؤكد", "CUS-E2E-2", "Sara", "967731234567", "s@ex.com", "عدن", "كريتر", items_2, "بطاقة", "200.0"])

        ingest_res1 = api_client.post("/ingest", json={
            "file_path": str(csv_file_v1),
            "threshold_mb": 200.0,
            "engine": "python_batch"
        })
        assert ingest_res1.status_code == 201
        assert ingest_res1.json()["metrics"]["count_inserted"] >= 2

        # Step 2: Refresh Materialized Views via POST /refresh-mv (incremental)
        mv_res1 = api_client.post("/refresh-mv", json={"full_refresh": False})
        assert mv_res1.status_code == 200
        assert mv_res1.json()["status"] == "COMPLETED"

        doc_date1 = coll_daily.find_one({"_id": "2024-01-01"})
        assert doc_date1 is not None
        assert doc_date1["total_revenue"] == 100.0

        doc_sku_a = coll_prod.find_one({"_id": "SKU-E2E-A"})
        assert doc_sku_a is not None
        assert doc_sku_a["total_revenue"] == 100.0

        # Step 3: Re-ingest Updated Order via POST /ingest
        # Move ORD-E2E-001:
        # - date: 2024-01-01 -> 2024-01-05
        # - sku: SKU-E2E-A -> SKU-E2E-C
        # - total: 100.0 -> 150.0
        csv_file_v2 = tmp_path / "orders_batch_v2.csv"
        items_updated = json.dumps([{"sku": "SKU-E2E-C", "name": "E2E Item C", "qty": 1, "unit_price": 150.0, "total": 150.0}], ensure_ascii=False)

        with open(csv_file_v2, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["id_order", "order_date", "status", "customer_id", "customer_name", "phone", "email", "shipping_city", "district", "items", "payment_method", "total_amount"])
            writer.writerow(["ORD-E2E-001", "2024-01-05T12:00:00Z", "مؤكد", "CUS-E2E-1", "Ali", "967771234567", "a@ex.com", "صنعاء", "حدة", items_updated, "بطاقة", "150.0"])

        ingest_res2 = api_client.post("/ingest", json={
            "file_path": str(csv_file_v2),
            "threshold_mb": 200.0,
            "engine": "python_batch"
        })
        assert ingest_res2.status_code == 201
        assert ingest_res2.json()["metrics"]["count_updated"] == 1

        # Step 4: Incremental Refresh via POST /refresh-mv
        mv_res2 = api_client.post("/refresh-mv", json={"full_refresh": False})
        assert mv_res2.status_code == 200
        mv_data = mv_res2.json()

        # Verify both old and new dates were affected
        affected_dates = mv_data["views"][COLLECTION_DAILY_SALES]["affected_periods"]
        assert "2024-01-01" in affected_dates
        assert "2024-01-05" in affected_dates

        # Old date purged because 0 orders remain on 2024-01-01
        assert coll_daily.find_one({"_id": "2024-01-01"}) is None

        # New date has correct revenue
        doc_date5 = coll_daily.find_one({"_id": "2024-01-05"})
        assert doc_date5 is not None
        assert doc_date5["total_revenue"] == 150.0

        # Verify both old and new SKUs were affected
        affected_skus = mv_data["views"][COLLECTION_TOP_PRODUCTS]["affected_skus"]
        assert "SKU-E2E-A" in affected_skus
        assert "SKU-E2E-C" in affected_skus

        # Old SKU purged because 0 orders remain for SKU-E2E-A
        assert coll_prod.find_one({"_id": "SKU-E2E-A"}) is None

        # New SKU has correct revenue
        doc_sku_c = coll_prod.find_one({"_id": "SKU-E2E-C"})
        assert doc_sku_c is not None
        assert doc_sku_c["total_revenue"] == 150.0

        # Unrelated order on 2024-01-02 with SKU-E2E-B is untouched
        assert coll_daily.find_one({"_id": "2024-01-02"})["total_revenue"] == 200.0
        assert coll_prod.find_one({"_id": "SKU-E2E-B"})["total_revenue"] == 200.0

        # Step 5: Idempotency Re-ingest unchanged batch
        ingest_res3 = api_client.post("/ingest", json={
            "file_path": str(csv_file_v2),
            "threshold_mb": 200.0,
            "engine": "python_batch"
        })
        assert ingest_res3.status_code == 201
        assert ingest_res3.json()["metrics"]["count_unchanged"] == 1

        mv_res3 = api_client.post("/refresh-mv", json={"full_refresh": False})
        assert mv_res3.json()["views"][COLLECTION_DAILY_SALES]["status"] == "up_to_date"
        assert mv_res3.json()["views"][COLLECTION_TOP_PRODUCTS]["status"] == "up_to_date"
    finally:
        cleanup()

