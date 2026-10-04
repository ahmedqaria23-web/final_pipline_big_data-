"""
Verification Suite for Final Project Phase 7: Generalization for Unknown Datasets.
Tests all queries, aggregations, materialized views, scheduled jobs, and ingestion
against completely disparate synthetic datasets (different cities, products, dates,
customers, counts, and statuses) to guarantee zero hard-coded assumptions.
"""

import csv
import json
import pytest
from datetime import datetime, timezone
from pathlib import Path

from src.mongodb.mongo_setup import initialize_database
from config.settings import (
    COLLECTION_VALIDATED,
    COLLECTION_DAILY_SALES,
    COLLECTION_TOP_PRODUCTS,
    COLLECTION_JOB_LOGS
)
from src.analytics.queries import (
    get_orders_by_customer,
    get_orders_by_city,
    get_orders_by_status,
    get_orders_by_date_range,
    get_high_value_orders,
    get_orders_by_city_and_status
)
from src.analytics.reports import (
    report_sales_by_city,
    report_top_products,
    report_top_customers,
    report_sales_by_period,
    report_orders_by_status
)
from src.views.materialized_views import (
    refresh_materialized_views,
    get_materialized_view_data
)
from src.scheduler.jobs import (
    run_job,
    job_generate_periodic_report
)


@pytest.fixture
def synthetic_isolated_db():
    """Creates a temporary, isolated MongoDB database for generalization testing."""
    test_db_name = "test_generalization_isolated_db"
    db = initialize_database(db_name=test_db_name)
    # Clean collections before test
    db[COLLECTION_VALIDATED].drop()
    db[COLLECTION_DAILY_SALES].drop()
    db[COLLECTION_TOP_PRODUCTS].drop()
    db[COLLECTION_JOB_LOGS].drop()

    yield db

    # Cleanup after test
    db.client.drop_database(test_db_name)


def generate_synthetic_orders(
    dataset_prefix: str,
    cities: list,
    products: list,
    customers: list,
    statuses: list,
    date_prefix: str,
    count: int
):
    """Generates synthetic order documents tailored to completely distinct scenarios."""
    docs = []
    for i in range(1, count + 1):
        city = cities[(i - 1) % len(cities)]
        prod = products[(i - 1) % len(products)]
        cust = customers[(i - 1) % len(customers)]
        status = statuses[(i - 1) % len(statuses)]
        order_date = f"{date_prefix}-{i:02d}T10:00:00Z"
        qty = (i % 3) + 1
        unit_price = prod["price"]
        line_total = qty * unit_price

        doc = {
            "id_order": f"{dataset_prefix}-ORD-{i:04d}",
            "order_date": order_date,
            "updated_at": order_date,
            "status": status,
            "version": 1,
            "customer": {
                "customer_id": cust["id"],
                "name": cust["name"],
                "phone": f"00{i:09d}",
                "email": f"{cust['name'].lower()}@domain.test",
                "address": {
                    "city": city,
                    "district": f"District-{i}"
                }
            },
            "items": [
                {
                    "sku": prod["sku"],
                    "name": prod["name"],
                    "qty": qty,
                    "unit_price": unit_price,
                    "total": line_total
                }
            ],
            "payment": {
                "method": "credit_card",
                "status": "paid",
                "currency": "EUR" if "EU" in dataset_prefix else "USD",
                "amount": line_total
            },
            "total_amount": line_total,
            "quality_status": "valid"
        }
        docs.append(doc)
    return docs


# ─────────────────────────────────────────────────────────────
# 1. TEST: Dynamic Queries Across Disparate Datasets
# ─────────────────────────────────────────────────────────────
def test_queries_generalization_european_dataset(synthetic_isolated_db):
    """
    Tests queries against European Tech Store dataset:
    Cities: Berlin, Paris, Amsterdam
    Statuses: processing, shipped, delivered, returned
    """
    db = synthetic_isolated_db
    coll = db[COLLECTION_VALIDATED]

    eu_cities = ["Berlin", "Paris", "Amsterdam"]
    eu_products = [
        {"sku": "SKU-TECH-01", "name": "Laptop Pro", "price": 1200.0},
        {"sku": "SKU-TECH-02", "name": "Wireless Mouse", "price": 40.0}
    ]
    eu_customers = [
        {"id": "CUS-EU-01", "name": "Alice Muller"},
        {"id": "CUS-EU-02", "name": "Jean Dupont"}
    ]
    eu_statuses = ["processing", "shipped", "delivered", "returned"]

    docs = generate_synthetic_orders("EU", eu_cities, eu_products, eu_customers, eu_statuses, "2024-04", 10)
    coll.insert_many(docs)

    # 1. Query by Customer
    res_cust = get_orders_by_customer("CUS-EU-01", db=db)
    assert len(res_cust) == 5
    assert all(r["customer"]["customer_id"] == "CUS-EU-01" for r in res_cust)

    # 2. Query by City
    res_berlin = get_orders_by_city("Berlin", db=db)
    assert len(res_berlin) >= 1
    assert all(r["customer"]["address"]["city"] == "Berlin" for r in res_berlin)

    # 3. Query by Status
    res_shipped = get_orders_by_status("shipped", db=db)
    assert len(res_shipped) >= 1
    assert all(r["status"] == "shipped" for r in res_shipped)

    # 4. Query by Date Range
    res_dates = get_orders_by_date_range("2024-04-01T00:00:00Z", "2024-04-05T23:59:59Z", db=db)
    assert len(res_dates) == 5

    # 5. High Value Orders
    res_high = get_high_value_orders(min_amount=1000.0, db=db)
    assert len(res_high) > 0
    assert all(r["total_amount"] >= 1000.0 for r in res_high)


def test_queries_generalization_asian_dataset(synthetic_isolated_db):
    """
    Tests queries against Asian Fashion Retail dataset:
    Cities: Tokyo, Seoul, Singapore
    Statuses: received, packed, dispatched, completed
    """
    db = synthetic_isolated_db
    coll = db[COLLECTION_VALIDATED]

    asia_cities = ["Tokyo", "Seoul", "Singapore"]
    asia_products = [
        {"sku": "SKU-FASH-01", "name": "Silk Kimono", "price": 250.0},
        {"sku": "SKU-FASH-02", "name": "Leather Boots", "price": 180.0}
    ]
    asia_customers = [
        {"id": "CUS-ASIA-01", "name": "Kenji Sato"},
        {"id": "CUS-ASIA-02", "name": "Min-seo Park"}
    ]
    asia_statuses = ["received", "packed", "dispatched", "completed"]

    docs = generate_synthetic_orders("ASIA", asia_cities, asia_products, asia_customers, asia_statuses, "2026-11", 6)
    coll.insert_many(docs)

    # 1. Query by Customer
    res_cust = get_orders_by_customer("CUS-ASIA-02", db=db)
    assert len(res_cust) == 3
    assert all(r["customer"]["customer_id"] == "CUS-ASIA-02" for r in res_cust)

    # 2. Query by City
    res_tokyo = get_orders_by_city("Tokyo", db=db)
    assert len(res_tokyo) == 2
    assert all(r["customer"]["address"]["city"] == "Tokyo" for r in res_tokyo)

    # 3. Query by Status
    res_packed = get_orders_by_status("packed", db=db)
    assert len(res_packed) >= 1
    assert all(r["status"] == "packed" for r in res_packed)

    # 4. Compound Query: City & Status
    res_compound = get_orders_by_city_and_status("Seoul", "packed", db=db)
    for r in res_compound:
        assert r["customer"]["address"]["city"] == "Seoul"
        assert r["status"] == "packed"


# ─────────────────────────────────────────────────────────────
# 2. TEST: Dynamic Aggregations Across Disparate Datasets
# ─────────────────────────────────────────────────────────────
def test_aggregations_generalization(synthetic_isolated_db):
    """
    Verifies that aggregation reports dynamically discover actual data groups
    without hard-coding city names, product names, or date structures.
    """
    db = synthetic_isolated_db
    coll = db[COLLECTION_VALIDATED]

    cities = ["Toronto", "Vancouver", "Montreal"]
    products = [
        {"sku": "SKU-CAN-A", "name": "Maple Syrup Barrel", "price": 80.0},
        {"sku": "SKU-CAN-B", "name": "Winter Parka", "price": 450.0}
    ]
    customers = [
        {"id": "CUS-CAN-1", "name": "David Tremblay"},
        {"id": "CUS-CAN-2", "name": "Emma Roy"}
    ]
    statuses = ["placed", "in_transit", "delivered"]

    docs = generate_synthetic_orders("CAN", cities, products, customers, statuses, "2025-06", 9)
    coll.insert_many(docs)

    # 1. Report: sales_by_city
    city_report = report_sales_by_city(limit=10, db=db)
    discovered_cities = {r["city"] for r in city_report}
    assert discovered_cities == {"Toronto", "Vancouver", "Montreal"}
    assert all("total_revenue" in r and "total_orders" in r for r in city_report)

    # 2. Report: top_products
    prod_report = report_top_products(limit=10, db=db)
    discovered_skus = {r["sku"] for r in prod_report}
    assert discovered_skus == {"SKU-CAN-A", "SKU-CAN-B"}

    # 3. Report: top_customers
    cust_report = report_top_customers(limit=10, db=db)
    discovered_custs = {r["customer_id"] for r in cust_report}
    assert discovered_custs == {"CUS-CAN-1", "CUS-CAN-2"}

    # 4. Report: sales_by_period (daily)
    period_report = report_sales_by_period(period="daily", db=db)
    assert len(period_report) == 9
    assert all(r["period"].startswith("2025-06-") for r in period_report)

    # 5. Report: orders_by_status
    status_report = report_orders_by_status(db=db)
    discovered_statuses = {r["status"] for r in status_report}
    assert discovered_statuses == {"placed", "in_transit", "delivered"}


# ─────────────────────────────────────────────────────────────
# 3. TEST: Dynamic Materialized Views & Refresh Lifecycle
# ─────────────────────────────────────────────────────────────
def test_materialized_views_generalization(synthetic_isolated_db):
    """
    Verifies that Materialized Views derive data strictly from the actual records
    in MongoDB and adapt correctly to multiple new synthetic batches.
    """
    db = synthetic_isolated_db
    coll = db[COLLECTION_VALIDATED]

    cities = ["Sydney", "Melbourne"]
    products = [
        {"sku": "SKU-AU-1", "name": "Surfboard", "price": 600.0},
        {"sku": "SKU-AU-2", "name": "Sunscreen 50+", "price": 25.0}
    ]
    customers = [{"id": "CUS-AU-1", "name": "Jack Connor"}]
    statuses = ["fulfilled"]

    # Initial batch: 4 orders
    batch_1 = generate_synthetic_orders("AU", cities, products, customers, statuses, "2026-01", 4)
    coll.insert_many(batch_1)

    # Run initial refresh
    res_init = refresh_materialized_views(full_refresh=True, db=db)
    assert res_init["status"] == "COMPLETED"

    daily_mv = get_materialized_view_data("daily_sales_summary", db=db)
    top_prod_mv = get_materialized_view_data("top_products_summary", db=db)

    assert len(daily_mv) == 4
    discovered_skus = {p["sku"] for p in top_prod_mv}
    assert discovered_skus == {"SKU-AU-1", "SKU-AU-2"}

    # Add second batch on new dates and new product
    new_products = [{"sku": "SKU-AU-3", "name": "Boomerang", "price": 45.0}]
    batch_2 = generate_synthetic_orders("AU2", cities, new_products, customers, statuses, "2026-02", 2)
    coll.insert_many(batch_2)

    # Incremental refresh
    res_inc = refresh_materialized_views(full_refresh=False, db=db)
    assert res_inc["status"] == "COMPLETED"

    top_prod_mv_after = get_materialized_view_data("top_products_summary", db=db)
    discovered_skus_after = {p["sku"] for p in top_prod_mv_after}
    assert "SKU-AU-3" in discovered_skus_after


# ─────────────────────────────────────────────────────────────
# 4. TEST: Dynamic Scheduled Jobs Operating on Actual State
# ─────────────────────────────────────────────────────────────
def test_scheduled_jobs_generalization(synthetic_isolated_db):
    """
    Verifies that scheduled jobs execute against the actual database state
    and generate dynamic audit logs and executive summaries.
    """
    db = synthetic_isolated_db
    coll = db[COLLECTION_VALIDATED]

    cities = ["Oslo"]
    products = [{"sku": "SKU-NO-1", "name": "Wool Sweater", "price": 150.0}]
    customers = [{"id": "CUS-NO-1", "name": "Lars Hansen"}]
    statuses = ["dispatched"]

    docs = generate_synthetic_orders("NO", cities, products, customers, statuses, "2026-05", 3)
    coll.insert_many(docs)

    # Execute periodic report job
    job_res = run_job("generate_periodic_report", trigger_type="manual", db=db)
    assert job_res["status"] == "SUCCESS"
    assert job_res["details"]["total_validated_orders"] == 3
    assert job_res["details"]["top_city"] == "Oslo"
    assert job_res["details"]["top_product"] == "Wool Sweater"

    # Verify audit log recorded in MongoDB
    logs = list(db[COLLECTION_JOB_LOGS].find({"job_name": "generate_periodic_report"}))
    assert len(logs) == 1
    assert logs[0]["status"] == "SUCCESS"


# ─────────────────────────────────────────────────────────────
# 5. TEST: Ingestion Generalization with Arbitrary CSV File
# ─────────────────────────────────────────────────────────────
def test_ingest_generalization_synthetic_file(tmp_path):
    """
    Verifies that the /ingest endpoint processes arbitrary datasets
    with unknown filenames, customer names, and cities using the router.
    """
    from fastapi.testclient import TestClient
    from src.api.main import app

    arbitrary_csv = tmp_path / "evaluator_test_dataset_random.csv"
    items_json = json.dumps([{"sku": "SKU-GEN-99", "name": "Generic Item", "qty": 2, "unit_price": 75.0, "total": 150.0}])

    with open(arbitrary_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id_order", "order_date", "status", "customer_id", "customer_name", "phone", "email", "shipping_city", "district", "items", "payment_method", "total_amount"])
        writer.writerow(["ORD-EVAL-01", "2026-10-04T12:00:00Z", "مؤكد", "CUS-EVAL-1", "Evaluator Name", "967771234567", "eval@example.com", "London", "Mayfair", items_json, "credit_card", "150.0"])

    with TestClient(app) as client:
        response = client.post("/ingest", json={
            "file_path": str(arbitrary_csv),
            "threshold_mb": 100.0,
            "engine": "python_batch"
        })
        assert response.status_code == 201
        data = response.json()
        assert data["status"] == "SUCCESS"
        assert data["metrics"]["read_rows"] == 1

