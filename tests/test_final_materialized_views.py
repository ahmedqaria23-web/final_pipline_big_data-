"""
Test Suite for Final Project Phase 4: Materialized Views.
Verifies:
1. Initial refresh builds both materialized views.
2. Incremental refresh identifies only affected periods and products.
3. Unaffected summaries remain completely intact.
4. Idempotency: Re-running refresh produces no duplicate records or drifted counts.
5. Direct retrieval from materialized view collections.
"""

import pytest
from datetime import datetime, timezone
from pymongo.database import Database

from src.mongodb.mongo_setup import initialize_database
from config.settings import (
    COLLECTION_VALIDATED,
    COLLECTION_DAILY_SALES,
    COLLECTION_TOP_PRODUCTS
)
from src.views.materialized_views import (
    refresh_daily_sales_summary,
    refresh_top_products_summary,
    refresh_materialized_views,
    get_materialized_view_data,
    get_mv_watermark
)


@pytest.fixture
def mv_test_db():
    """Initializes an isolated test database with baseline order records."""
    db_name = "test_mv_phase4_db"
    db = initialize_database(db_name=db_name)
    coll = db[COLLECTION_VALIDATED]

    # Baseline: 3 orders on 2 dates (2025-01-10 and 2025-01-20)
    baseline_orders = [
        {
            "id_order": "ORD-MV-001",
            "order_date": "2025-01-10T10:00:00Z",
            "updated_at": "2025-01-10T10:00:00Z",
            "status": "تم التسليم",
            "customer": {"customer_id": "CUS-1", "name": "Ali", "phone": "967771234567", "email": "a@ex.com", "address": {"city": "صنعاء", "district": "حدة"}},
            "items": [
                {"sku": "SKU-LAPTOP", "name": "Laptop", "qty": 1, "unit_price": 1000.0, "total": 1000.0},
                {"sku": "SKU-MOUSE", "name": "Mouse", "qty": 2, "unit_price": 25.0, "total": 50.0}
            ],
            "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "YER", "amount": 1050.0},
            "total_amount": 1050.0,
            "quality_status": "valid"
        },
        {
            "id_order": "ORD-MV-002",
            "order_date": "2025-01-10T14:00:00Z",
            "updated_at": "2025-01-10T14:00:00Z",
            "status": "مؤكد",
            "customer": {"customer_id": "CUS-2", "name": "Sara", "phone": "967731234567", "email": "s@ex.com", "address": {"city": "عدن", "district": "كريتر"}},
            "items": [
                {"sku": "SKU-MOUSE", "name": "Mouse", "qty": 1, "unit_price": 25.0, "total": 25.0}
            ],
            "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "YER", "amount": 25.0},
            "total_amount": 25.0,
            "quality_status": "valid"
        },
        {
            "id_order": "ORD-MV-003",
            "order_date": "2025-01-20T11:00:00Z",
            "updated_at": "2025-01-20T11:00:00Z",
            "status": "تم التسليم",
            "customer": {"customer_id": "CUS-3", "name": "Omar", "phone": "967711234567", "email": "o@ex.com", "address": {"city": "تعز", "district": "المظفر"}},
            "items": [
                {"sku": "SKU-KEYBOARD", "name": "Keyboard", "qty": 1, "unit_price": 80.0, "total": 80.0}
            ],
            "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "YER", "amount": 80.0},
            "total_amount": 80.0,
            "quality_status": "valid"
        }
    ]

    coll.insert_many(baseline_orders)
    yield db
    db.client.drop_database(db_name)


# ─────────────────────────────────────────────────────────────
# 1. Initial Refresh Tests
# ─────────────────────────────────────────────────────────────
def test_initial_refresh_materialized_views(mv_test_db):
    db = mv_test_db
    res = refresh_materialized_views(full_refresh=True, db=db)

    assert res["status"] == "COMPLETED"
    assert COLLECTION_DAILY_SALES in res["views"]
    assert COLLECTION_TOP_PRODUCTS in res["views"]

    # Verify daily_sales_summary collection
    coll_daily = db[COLLECTION_DAILY_SALES]
    assert coll_daily.count_documents({}) == 2  # 2025-01-10 and 2025-01-20

    doc_10 = coll_daily.find_one({"_id": "2025-01-10"})
    assert doc_10 is not None
    assert doc_10["total_orders"] == 2
    assert doc_10["total_revenue"] == 1075.0  # 1050 + 25

    doc_20 = coll_daily.find_one({"_id": "2025-01-20"})
    assert doc_20 is not None
    assert doc_20["total_orders"] == 1
    assert doc_20["total_revenue"] == 80.0

    # Verify top_products_summary collection
    coll_prod = db[COLLECTION_TOP_PRODUCTS]
    assert coll_prod.count_documents({}) == 3  # SKU-LAPTOP, SKU-MOUSE, SKU-KEYBOARD

    mouse = coll_prod.find_one({"_id": "SKU-MOUSE"})
    assert mouse is not None
    assert mouse["total_quantity_sold"] == 3  # 2 + 1
    assert mouse["total_revenue"] == 75.0     # 50 + 25
    assert mouse["order_occurrences"] == 2


# ─────────────────────────────────────────────────────────────
# 2. Incremental Refresh & Delta Detection Tests
# ─────────────────────────────────────────────────────────────
def test_incremental_refresh_lifecycle(mv_test_db):
    db = mv_test_db

    # Step 1: Initial full refresh
    refresh_materialized_views(full_refresh=True, db=db)
    init_watermark = get_mv_watermark(db)
    assert init_watermark is not None

    # Step 2: Add a new order on a new date (2025-02-01) with SKU-MOUSE and SKU-NEW
    new_order = {
        "id_order": "ORD-MV-004",
        "order_date": "2025-02-01T09:00:00Z",
        "updated_at": "2025-02-01T09:00:00Z",
        "status": "مؤكد",
        "customer": {"customer_id": "CUS-4", "name": "Mona", "phone": "967771122334", "email": "m@ex.com", "address": {"city": "إب", "district": "الظهار"}},
        "items": [
            {"sku": "SKU-MOUSE", "name": "Mouse", "qty": 5, "unit_price": 25.0, "total": 125.0},
            {"sku": "SKU-NEW", "name": "USB Cable", "qty": 10, "unit_price": 5.0, "total": 50.0}
        ],
        "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "YER", "amount": 175.0},
        "total_amount": 175.0,
        "quality_status": "valid"
    }
    db[COLLECTION_VALIDATED].insert_one(new_order)

    # Step 3: Run INCREMENTAL refresh (full_refresh=False)
    inc_res = refresh_materialized_views(full_refresh=False, db=db)
    assert inc_res["status"] == "COMPLETED"

    daily_res = inc_res["views"][COLLECTION_DAILY_SALES]
    prod_res = inc_res["views"][COLLECTION_TOP_PRODUCTS]

    assert daily_res["mode"] == "incremental"
    assert "2025-02-01" in daily_res["affected_periods"]
    assert "2025-01-10" not in daily_res["affected_periods"]  # Unaffected date not recalculated

    assert prod_res["mode"] == "incremental"
    assert "SKU-NEW" in prod_res["affected_skus"]
    assert "SKU-MOUSE" in prod_res["affected_skus"]
    assert "SKU-LAPTOP" not in prod_res["affected_skus"]  # Unaffected product not recalculated

    # Step 4: Verify affected daily summary
    doc_feb1 = db[COLLECTION_DAILY_SALES].find_one({"_id": "2025-02-01"})
    assert doc_feb1 is not None
    assert doc_feb1["total_orders"] == 1
    assert doc_feb1["total_revenue"] == 175.0

    # Step 5: Verify unaffected daily summaries remain identical
    doc_jan10 = db[COLLECTION_DAILY_SALES].find_one({"_id": "2025-01-10"})
    assert doc_jan10["total_orders"] == 2
    assert doc_jan10["total_revenue"] == 1075.0

    # Step 6: Verify affected products summary
    mouse = db[COLLECTION_TOP_PRODUCTS].find_one({"_id": "SKU-MOUSE"})
    assert mouse["total_quantity_sold"] == 8  # 3 prior + 5 new
    assert mouse["total_revenue"] == 200.0    # 75 + 125

    usb = db[COLLECTION_TOP_PRODUCTS].find_one({"_id": "SKU-NEW"})
    assert usb["total_quantity_sold"] == 10
    assert usb["total_revenue"] == 50.0

    # Step 7: Verify unaffected product remains identical
    laptop = db[COLLECTION_TOP_PRODUCTS].find_one({"_id": "SKU-LAPTOP"})
    assert laptop["total_quantity_sold"] == 1
    assert laptop["total_revenue"] == 1000.0


# ─────────────────────────────────────────────────────────────
# 3. Idempotency & Replay Tests
# ─────────────────────────────────────────────────────────────
def test_refresh_idempotency_without_new_data(mv_test_db):
    db = mv_test_db

    # Run initial refresh
    refresh_materialized_views(full_refresh=True, db=db)
    count_daily_1 = db[COLLECTION_DAILY_SALES].count_documents({})
    count_prod_1 = db[COLLECTION_TOP_PRODUCTS].count_documents({})

    # Run refresh AGAIN without adding any records
    replay_res = refresh_materialized_views(full_refresh=False, db=db)
    daily_res = replay_res["views"][COLLECTION_DAILY_SALES]
    prod_res = replay_res["views"][COLLECTION_TOP_PRODUCTS]

    assert daily_res["status"] == "up_to_date"
    assert daily_res["affected_periods_count"] == 0
    assert prod_res["status"] == "up_to_date"
    assert prod_res["affected_products_count"] == 0

    count_daily_2 = db[COLLECTION_DAILY_SALES].count_documents({})
    count_prod_2 = db[COLLECTION_TOP_PRODUCTS].count_documents({})

    # Absolutely no duplicate rows created
    assert count_daily_1 == count_daily_2
    assert count_prod_1 == count_prod_2


def test_get_materialized_view_data(mv_test_db):
    db = mv_test_db
    refresh_materialized_views(full_refresh=True, db=db)

    data_daily = get_materialized_view_data(COLLECTION_DAILY_SALES, limit=10, db=db)
    assert len(data_daily) == 2
    assert "date" in data_daily[0]
    assert "total_revenue" in data_daily[0]

    data_prod = get_materialized_view_data(COLLECTION_TOP_PRODUCTS, limit=10, db=db)
    assert len(data_prod) == 3
    assert "sku" in data_prod[0]
    assert "total_quantity_sold" in data_prod[0]

    with pytest.raises(ValueError):
        get_materialized_view_data("invalid_view_name", db=db)
