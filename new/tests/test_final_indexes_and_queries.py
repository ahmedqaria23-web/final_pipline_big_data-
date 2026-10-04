"""
Verification Suite for Final Project Phase 2:
- Queries, Query Registry, and Parameter Validation
- Indexes Creation and Compound Index Verification
- Preservation of Midterm Indexes
- Explain('executionStats') Experiments & Before/After Validation
- JSON-Safe Serialization
"""

import json
import pytest
from datetime import datetime, timezone
from pymongo.database import Database

from src.mongodb.mongo_setup import initialize_database
from config.settings import COLLECTION_VALIDATED
from src.analytics.queries import (
    QUERY_REGISTRY,
    list_registered_queries,
    get_query_descriptor,
    execute_registered_query,
    get_orders_by_customer,
    get_orders_by_city,
    get_orders_by_status,
    get_orders_by_date_range,
    get_high_value_orders,
    get_orders_by_city_and_status,
    json_safe
)
from src.analytics.indexes import (
    ANALYTICS_INDEX_DEFINITIONS,
    PROTECTED_MIDTERM_INDEXES,
    create_analytics_indexes,
    drop_analytics_indexes,
    list_indexes,
    run_explain_experiment
)


@pytest.fixture
def analytics_test_db():
    """Isolated test database populated with clean sample orders."""
    db_name = "test_analytics_phase2_db"
    db = initialize_database(db_name=db_name)
    coll = db[COLLECTION_VALIDATED]

    # Insert 100 sample documents for realistic querying and explain experiments
    sample_orders = []
    cities = ["صنعاء", "عدن", "تعز", "الحديدة", "إب"]
    statuses = ["مؤكد", "تم التسليم", "قيد الانتظار", "قيد الشحن"]

    for i in range(1, 101):
        city = cities[i % len(cities)]
        status = statuses[i % len(statuses)]
        amount = float(i * 100)  # 100 to 10000
        cus_id = f"CUS-{(i % 10) + 1:03d}"  # 10 unique customers
        sample_orders.append({
            "id_order": f"ORD-P2-{i:04d}",
            "order_date": f"2025-01-{(i % 28) + 1:02d}T10:00:00Z",
            "status": status,
            "version": 1,
            "customer": {
                "customer_id": cus_id,
                "name": f"Customer {cus_id}",
                "phone": "967771234567",
                "email": f"customer_{cus_id}@example.com",
                "address": {
                    "city": city,
                    "district": "التحرير"
                }
            },
            "items": [
                {
                    "sku": f"SKU-{i:03d}",
                    "name": f"Item {i}",
                    "qty": 1,
                    "unit_price": amount,
                    "total": amount
                }
            ],
            "payment": {
                "method": "بطاقة",
                "status": "تم الدفع",
                "currency": "YER",
                "amount": amount
            },
            "total_amount": amount,
            "quality_status": "valid"
        })

    coll.insert_many(sample_orders)
    yield db
    db.client.drop_database(db_name)


# ─────────────────────────────────────────────────────────────
# 1. Query Registry Verification
# ─────────────────────────────────────────────────────────────
def test_query_registry_contains_at_least_5_queries():
    queries = list_registered_queries()
    assert len(queries) >= 5, f"Expected at least 5 queries in registry, found {len(queries)}"
    
    names = [q["name"] for q in queries]
    assert "orders_by_customer" in names
    assert "orders_by_city" in names
    assert "orders_by_status" in names
    assert "orders_by_date_range" in names
    assert "high_value_orders" in names
    assert "orders_by_city_and_status" in names


def test_query_descriptors():
    desc = get_query_descriptor("orders_by_customer")
    assert desc is not None
    assert desc["name"] == "orders_by_customer"
    assert any(p["name"] == "customer_id" for p in desc["parameters"])

    assert get_query_descriptor("non_existent_query") is None


# ─────────────────────────────────────────────────────────────
# 2. Query Execution & Results Verification
# ─────────────────────────────────────────────────────────────
def test_query_orders_by_customer(analytics_test_db):
    results = get_orders_by_customer("CUS-001", db=analytics_test_db)
    assert len(results) == 10
    for r in results:
        assert r["customer"]["customer_id"] == "CUS-001"
        assert isinstance(r["id_order"], str)


def test_query_orders_by_city(analytics_test_db):
    results = get_orders_by_city("صنعاء", db=analytics_test_db)
    assert len(results) > 0
    for r in results:
        assert r["customer"]["address"]["city"] == "صنعاء"


def test_query_orders_by_status(analytics_test_db):
    results = get_orders_by_status("مؤكد", db=analytics_test_db)
    assert len(results) > 0
    for r in results:
        assert r["status"] == "مؤكد"


def test_query_orders_by_date_range(analytics_test_db):
    results = get_orders_by_date_range("2025-01-05T00:00:00Z", "2025-01-10T23:59:59Z", db=analytics_test_db)
    assert len(results) > 0
    for r in results:
        assert "2025-01-05" <= r["order_date"] <= "2025-01-10T23:59:59Z"


def test_query_high_value_orders(analytics_test_db):
    results = get_high_value_orders(min_amount=5000.0, db=analytics_test_db)
    assert len(results) > 0
    for r in results:
        assert float(r["total_amount"]) >= 5000.0

    # Verify descending sort
    amounts = [r["total_amount"] for r in results]
    assert amounts == sorted(amounts, reverse=True)


def test_query_orders_by_city_and_status(analytics_test_db):
    results = get_orders_by_city_and_status("صنعاء", "مؤكد", db=analytics_test_db)
    for r in results:
        assert r["customer"]["address"]["city"] == "صنعاء"
        assert r["status"] == "مؤكد"


# ─────────────────────────────────────────────────────────────
# 3. Empty Results & Parameter Validation
# ─────────────────────────────────────────────────────────────
def test_queries_handle_empty_results_safely(analytics_test_db):
    # Non-existent customer returns empty list without error
    res_cus = get_orders_by_customer("CUS-NON-EXISTENT", db=analytics_test_db)
    assert res_cus == []

    # Non-existent city returns empty list
    res_city = get_orders_by_city("باريس", db=analytics_test_db)
    assert res_city == []

    # Extremely high amount returns empty list
    res_amt = get_high_value_orders(999999999.0, db=analytics_test_db)
    assert res_amt == []


def test_query_parameter_validation():
    # Empty customer ID
    with pytest.raises(ValueError, match="customer_id"):
        get_orders_by_customer("")

    # Empty city
    with pytest.raises(ValueError, match="city"):
        get_orders_by_city("   ")

    # Inverted date range
    with pytest.raises(ValueError, match="cannot be greater than end_date"):
        get_orders_by_date_range("2025-02-01", "2025-01-01")

    # Negative amount
    with pytest.raises(ValueError, match="negative"):
        get_high_value_orders(-100.0)

    # Invalid pagination limit
    with pytest.raises(ValueError, match="limit"):
        get_orders_by_customer("CUS-001", limit=0)


def test_json_safe_serialization(analytics_test_db):
    results = get_orders_by_customer("CUS-001", db=analytics_test_db)
    # Must serialize to pure JSON string with standard library json without error
    json_str = json.dumps(results, ensure_ascii=False)
    assert isinstance(json_str, str)
    assert len(json_str) > 0


# ─────────────────────────────────────────────────────────────
# 4. Indexes & Compound Index Verification
# ─────────────────────────────────────────────────────────────
def test_indexes_implementation_and_compound_requirement(analytics_test_db):
    # Verify definition count
    assert len(ANALYTICS_INDEX_DEFINITIONS) >= 3

    # Verify at least one compound index
    compound_indexes = [idx for idx in ANALYTICS_INDEX_DEFINITIONS if idx.get("compound") is True]
    assert len(compound_indexes) >= 1
    assert compound_indexes[0]["name"] == "idx_city_status"
    assert len(compound_indexes[0]["keys"]) == 2

    # Create indexes in test DB
    created = create_analytics_indexes(analytics_test_db)
    assert "idx_customer_id" in created
    assert "idx_city_status" in created
    assert "idx_total_amount" in created

    # List indexes and verify properties
    active_indexes = list_indexes(analytics_test_db)
    active_names = [idx["name"] for idx in active_indexes]

    assert "idx_customer_id" in active_names
    assert "idx_city_status" in active_names
    assert "idx_total_amount" in active_names

    # Midterm unique index must be strictly preserved
    assert "ux_id_order" in active_names
    ux_idx = next(idx for idx in active_indexes if idx["name"] == "ux_id_order")
    assert ux_idx["unique"] is True


def test_drop_analytics_preserves_midterm_indexes(analytics_test_db):
    create_analytics_indexes(analytics_test_db)
    dropped = drop_analytics_indexes(analytics_test_db)

    assert "idx_customer_id" in dropped
    assert "idx_city_status" in dropped
    assert "idx_total_amount" in dropped

    # Ensure Midterm indexes are STILL THERE
    remaining_indexes = list_indexes(analytics_test_db)
    remaining_names = [idx["name"] for idx in remaining_indexes]

    for protected in PROTECTED_MIDTERM_INDEXES:
        assert protected in remaining_names, f"Protected Midterm index '{protected}' was accidentally removed!"


# ─────────────────────────────────────────────────────────────
# 5. Explain('executionStats') Before and After Experiment
# ─────────────────────────────────────────────────────────────
def test_explain_experiment_before_and_after(analytics_test_db):
    """
    Executes the full Before/After explain experiment.
    Verifies that:
    1. Before state used COLLSCAN (or scanned total documents).
    2. After state used IXSCAN (or examined significantly fewer documents).
    3. Quantitative metrics are captured with real numbers.
    """
    experiment_report = run_explain_experiment(analytics_test_db, save_reports=False)

    assert "experiments" in experiment_report
    assert len(experiment_report["experiments"]) == 3

    for exp in experiment_report["experiments"]:
        b = exp["before"]
        a = exp["after"]
        diff = exp["difference"]

        # Validate that executionStats were collected
        assert "executionTimeMillis" in b
        assert "totalDocsExamined" in b
        assert "winningPlanStage" in b

        assert "executionTimeMillis" in a
        assert "totalDocsExamined" in a
        assert "winningPlanStage" in a

        # After index, docs examined should be <= before index
        assert a["totalDocsExamined"] <= b["totalDocsExamined"], (
            f"Query '{exp['query_name']}' examined more docs after index: "
            f"before={b['totalDocsExamined']}, after={a['totalDocsExamined']}"
        )

        # In winning plan, after stage should contain IXSCAN
        assert "IXSCAN" in a["winningPlanStage"], (
            f"Expected IXSCAN in winning plan for query '{exp['query_name']}', got: {a['winningPlanStage']}"
        )
