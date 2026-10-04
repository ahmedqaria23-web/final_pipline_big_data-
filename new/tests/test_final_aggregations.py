"""
Test Suite for Final Project Phase 3:
- 5 Independent Aggregation Reports
- Aggregation Registry and Discovery
- Parameter Validation and Error Handling
- Safe JSON Serialization
"""

import json
import pytest
from src.mongodb.mongo_setup import initialize_database
from config.settings import COLLECTION_VALIDATED
from src.analytics.reports import (
    AGGREGATION_REGISTRY,
    list_registered_aggregations,
    get_aggregation_descriptor,
    execute_registered_aggregation,
    report_sales_by_city,
    report_top_products,
    report_top_customers,
    report_sales_by_period,
    report_orders_by_status
)


@pytest.fixture
def agg_test_db():
    """Populates an isolated test database with controlled order records."""
    db_name = "test_agg_phase3_db"
    db = initialize_database(db_name=db_name)
    coll = db[COLLECTION_VALIDATED]

    sample_orders = [
        # Customer 1 - Sanaa - 2 orders
        {
            "id_order": "ORD-AGG-001",
            "order_date": "2025-01-10T10:00:00Z",
            "status": "تم التسليم",
            "customer": {
                "customer_id": "CUS-001",
                "name": "Ahmad",
                "phone": "967771234567",
                "email": "ahmad@example.com",
                "address": {"city": "صنعاء", "district": "حدة"}
            },
            "items": [
                {"sku": "SKU-A", "name": "Laptop", "qty": 1, "unit_price": 1000.0, "total": 1000.0},
                {"sku": "SKU-B", "name": "Mouse", "qty": 2, "unit_price": 50.0, "total": 100.0}
            ],
            "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "YER", "amount": 1100.0},
            "total_amount": 1100.0,
            "quality_status": "valid"
        },
        {
            "id_order": "ORD-AGG-002",
            "order_date": "2025-01-15T12:00:00Z",
            "status": "مؤكد",
            "customer": {
                "customer_id": "CUS-001",
                "name": "Ahmad",
                "phone": "967771234567",
                "email": "ahmad@example.com",
                "address": {"city": "صنعاء", "district": "حدة"}
            },
            "items": [
                {"sku": "SKU-B", "name": "Mouse", "qty": 1, "unit_price": 50.0, "total": 50.0}
            ],
            "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "YER", "amount": 50.0},
            "total_amount": 50.0,
            "quality_status": "valid"
        },
        # Customer 2 - Aden - 1 order
        {
            "id_order": "ORD-AGG-003",
            "order_date": "2025-01-20T14:00:00Z",
            "status": "تم التسليم",
            "customer": {
                "customer_id": "CUS-002",
                "name": "Sara",
                "phone": "967731234567",
                "email": "sara@example.com",
                "address": {"city": "عدن", "district": "المعلا"}
            },
            "items": [
                {"sku": "SKU-A", "name": "Laptop", "qty": 2, "unit_price": 1000.0, "total": 2000.0}
            ],
            "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "YER", "amount": 2000.0},
            "total_amount": 2000.0,
            "quality_status": "valid"
        },
        # Customer 3 - Taiz - 1 order
        {
            "id_order": "ORD-AGG-004",
            "order_date": "2025-02-05T09:00:00Z",
            "status": "ملغي",
            "customer": {
                "customer_id": "CUS-003",
                "name": "Ali",
                "phone": "967711234567",
                "email": "ali@example.com",
                "address": {"city": "تعز", "district": "صالة"}
            },
            "items": [
                {"sku": "SKU-C", "name": "Keyboard", "qty": 1, "unit_price": 80.0, "total": 80.0}
            ],
            "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "YER", "amount": 80.0},
            "total_amount": 80.0,
            "quality_status": "valid"
        }
    ]

    coll.insert_many(sample_orders)
    yield db
    db.client.drop_database(db_name)


# ─────────────────────────────────────────────────────────────
# 1. Registry & Discovery Tests
# ─────────────────────────────────────────────────────────────
def test_aggregation_registry_contains_at_least_5_reports():
    reports = list_registered_aggregations()
    assert len(reports) >= 5, f"Expected at least 5 reports in registry, found {len(reports)}"

    names = [r["name"] for r in reports]
    assert "sales_by_city" in names
    assert "top_products" in names
    assert "top_customers" in names
    assert "sales_by_period" in names
    assert "orders_by_status" in names


def test_get_aggregation_descriptor():
    desc = get_aggregation_descriptor("sales_by_city")
    assert desc is not None
    assert desc["name"] == "sales_by_city"
    assert len(desc["parameters"]) >= 1

    assert get_aggregation_descriptor("non_existent_report") is None


# ─────────────────────────────────────────────────────────────
# 2. Aggregation Execution & Math Verification
# ─────────────────────────────────────────────────────────────
def test_report_sales_by_city(agg_test_db):
    results = report_sales_by_city(db=agg_test_db)
    assert len(results) == 3  # Sanaa, Aden, Taiz

    # Sanaa has 2 orders, total 1150.0
    sanaa = next(r for r in results if r["city"] == "صنعاء")
    assert sanaa["total_orders"] == 2
    assert sanaa["total_revenue"] == 1150.0
    assert sanaa["avg_order_value"] == 575.0

    # Aden has 1 order, total 2000.0 (top revenue)
    assert results[0]["city"] == "عدن"
    assert results[0]["total_revenue"] == 2000.0


def test_report_top_products(agg_test_db):
    results = report_top_products(db=agg_test_db)
    assert len(results) == 3  # SKU-A, SKU-B, SKU-C

    # SKU-A: 3 total units (1 in ORD-001 + 2 in ORD-003), total revenue 3000.0
    sku_a = next(r for r in results if r["sku"] == "SKU-A")
    assert sku_a["total_quantity_sold"] == 3
    assert sku_a["total_revenue"] == 3000.0
    assert sku_a["order_occurrences"] == 2

    # SKU-B: 3 total units (2 in ORD-001 + 1 in ORD-002), total revenue 150.0
    sku_b = next(r for r in results if r["sku"] == "SKU-B")
    assert sku_b["total_quantity_sold"] == 3
    assert sku_b["total_revenue"] == 150.0


def test_report_top_customers(agg_test_db):
    results = report_top_customers(db=agg_test_db)
    assert len(results) == 3

    # Top customer by spend is Sara (CUS-002: 2000.0)
    assert results[0]["customer_id"] == "CUS-002"
    assert results[0]["total_spend"] == 2000.0
    assert results[0]["total_orders"] == 1

    # Second is Ahmad (CUS-001: 1150.0, 2 orders)
    ahmad = next(r for r in results if r["customer_id"] == "CUS-001")
    assert ahmad["total_orders"] == 2
    assert ahmad["total_spend"] == 1150.0


def test_report_sales_by_period_daily_and_monthly(agg_test_db):
    # Daily aggregation
    daily = report_sales_by_period(period="daily", db=agg_test_db)
    assert len(daily) == 4  # 4 distinct days

    # Monthly aggregation
    monthly = report_sales_by_period(period="monthly", db=agg_test_db)
    assert len(monthly) == 2  # 2025-01 and 2025-02

    jan = next(m for m in monthly if m["period"] == "2025-01")
    assert jan["total_orders"] == 3
    assert jan["total_revenue"] == 3150.0

    feb = next(m for m in monthly if m["period"] == "2025-02")
    assert feb["total_orders"] == 1
    assert feb["total_revenue"] == 80.0


def test_report_orders_by_status(agg_test_db):
    results = report_orders_by_status(db=agg_test_db)
    assert len(results) == 3  # تم التسليم (2), مؤكد (1), ملغي (1)

    delivered = next(r for r in results if r["status"] == "تم التسليم")
    assert delivered["count_orders"] == 2
    assert delivered["total_value"] == 3100.0


# ─────────────────────────────────────────────────────────────
# 3. Dynamic Execution & Parameter Validation
# ─────────────────────────────────────────────────────────────
def test_execute_registered_aggregation(agg_test_db):
    res = execute_registered_aggregation("sales_by_city", {"limit": 1}, db=agg_test_db)
    assert len(res) == 1
    assert res[0]["city"] == "عدن"

    with pytest.raises(KeyError):
        execute_registered_aggregation("unregistered_report")


def test_aggregation_parameter_validation(agg_test_db):
    with pytest.raises(ValueError, match="positive integer"):
        report_sales_by_city(limit=0, db=agg_test_db)

    with pytest.raises(ValueError, match="non-negative"):
        report_top_customers(min_spend=-5.0, db=agg_test_db)

    with pytest.raises(ValueError, match="daily"):
        report_sales_by_period(period="hourly", db=agg_test_db)

    with pytest.raises(ValueError, match="cannot be greater"):
        report_sales_by_period(start_date="2025-02-01", end_date="2025-01-01", db=agg_test_db)


def test_aggregation_json_serialization(agg_test_db):
    reports = list_registered_aggregations()
    for r in reports:
        output = execute_registered_aggregation(r["name"], db=agg_test_db)
        json_str = json.dumps(output, ensure_ascii=False)
        assert isinstance(json_str, str)
        assert len(json_str) > 0


def test_aggregation_empty_collection():
    db = initialize_database(db_name="test_empty_agg_db")
    try:
        # Running against completely empty collection returns empty list without error
        assert report_sales_by_city(db=db) == []
        assert report_top_products(db=db) == []
        assert report_top_customers(db=db) == []
        assert report_sales_by_period(db=db) == []
        assert report_orders_by_status(db=db) == []
    finally:
        db.client.drop_database("test_empty_agg_db")
