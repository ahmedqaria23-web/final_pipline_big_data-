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
