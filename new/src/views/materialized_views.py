"""
Materialized Views & Incremental $merge Refresh Engine.
Implements:
1. daily_sales_summary (Key: date)
2. top_products_summary (Key: sku)

Integrates directly with the project's incremental watermark mechanism.
Recalculates ONLY affected dates/products during delta loads instead of rebuilding
the entire collection from scratch.
 """

import sys
import time
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from pathlib import Path

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pymongo import ASCENDING, DESCENDING
from pymongo.database import Database
from config.settings import (
    COLLECTION_VALIDATED,
    COLLECTION_META_STATE,
    COLLECTION_DAILY_SALES,
    COLLECTION_TOP_PRODUCTS
)
from src.mongodb.mongo_setup import get_mongo_db
from src.analytics.queries import json_safe

logger = logging.getLogger(__name__)

MV_PIPELINE_NAME = "materialized_views_refresh"


def _resolve_db(db: Optional[Database]) -> Database:
    if db is not None:
        return db
    return get_mongo_db()


def get_mv_watermark(db: Database, view_name: str = COLLECTION_DAILY_SALES) -> Optional[str]:
    """Retrieves the last committed watermark timestamp for a specific Materialized View."""
    pipeline_key = f"{MV_PIPELINE_NAME}_{view_name}"
    state = db[COLLECTION_META_STATE].find_one({"pipeline": pipeline_key})
    if state and "last_watermark" in state:
        return state["last_watermark"]
    return None


def save_mv_watermark(
    db: Database,
    watermark: str,
    view_name: str = COLLECTION_DAILY_SALES,
    meta: Optional[Dict[str, Any]] = None
):
    """Saves the updated watermark timestamp and refresh metadata for a specific Materialized View."""
    pipeline_key = f"{MV_PIPELINE_NAME}_{view_name}"
    payload = {
        "pipeline": pipeline_key,
        "view_name": view_name,
        "last_watermark": watermark,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    if meta:
        payload.update(meta)
    db[COLLECTION_META_STATE].update_one(
        {"pipeline": pipeline_key},
        {"$set": payload},
        upsert=True
    )


# ─────────────────────────────────────────────────────────────
# 1. MATERIALIZED VIEW: daily_sales_summary
# ─────────────────────────────────────────────────────────────
def refresh_daily_sales_summary(
    full_refresh: bool = False,
    db: Optional[Database] = None
) -> Dict[str, Any]:
    """
    Refreshes the 'daily_sales_summary' Materialized View in MongoDB.
    - If full_refresh=True or initial run: aggregates all records and writes via $merge.
    - If incremental (full_refresh=False): identifies only the dates affected by new/updated
      orders since the last watermark, and updates only those specific daily documents.
    """
    start_time = time.perf_counter()
    target_db = _resolve_db(db)
    coll_val = target_db[COLLECTION_VALIDATED]
    coll_mv = target_db[COLLECTION_DAILY_SALES]

    # Ensure unique index on date (_id is default unique)
    coll_mv.create_index([("date", ASCENDING)], unique=True)

    watermark = get_mv_watermark(target_db, view_name=COLLECTION_DAILY_SALES)
    is_initial = (watermark is None) or (coll_mv.count_documents({}) == 0)

    now_iso = datetime.now(timezone.utc).isoformat()
    mode = "full" if (full_refresh or is_initial) else "incremental"

    max_watermark = watermark or "1970-01-01T00:00:00Z"
    affected_dates: List[str] = []

    if mode == "full":
        # Full recalculation across all validated orders
        match_stage = {"order_date": {"$exists": True, "$type": "string", "$ne": ""}}
    else:
        # Incremental: discover records updated or inserted after the last watermark
        delta_query = {
            "$or": [
                {"updated_at": {"$gt": watermark}},
                {"order_date": {"$gt": watermark}}
            ]
        }
        delta_cursor = coll_val.find(delta_query, {"order_date": 1, "updated_at": 1})
        delta_dates_set = set()

        for doc in delta_cursor:
            od = doc.get("order_date")
            if od and isinstance(od, str) and len(od) >= 10:
                delta_dates_set.add(od[:10])
            ts = doc.get("updated_at") or doc.get("order_date")
            if ts and ts > max_watermark:
                max_watermark = ts

        if not delta_dates_set:
            # Zero records changed -> perfectly idempotent no-op
            elapsed = time.perf_counter() - start_time
            return {
                "view_name": COLLECTION_DAILY_SALES,
                "mode": "incremental",
                "status": "up_to_date",
                "affected_periods_count": 0,
                "affected_periods": [],
                "total_documents_in_view": coll_mv.count_documents({}),
                "refreshed_at": now_iso,
                "elapsed_seconds": round(elapsed, 4)
            }

        affected_dates = sorted(list(delta_dates_set))
        # Regex matching for only the affected dates: e.g. "^(2025-01-10|2025-01-15)"
        date_pattern = "^(" + "|".join(affected_dates) + ")"
        match_stage = {"order_date": {"$regex": date_pattern}}

    pipeline = [
        {"$match": match_stage},
        {
            "$project": {
                "date": {"$substrCP": ["$order_date", 0, 10]},
                "total_amount": 1
            }
        },
        {
            "$group": {
                "_id": "$date",
                "date": {"$first": "$date"},
                "total_revenue": {"$sum": "$total_amount"},
                "total_orders": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"}
            }
        },
        {
            "$project": {
                "_id": "$_id",
                "date": "$date",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "total_orders": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "last_refreshed_at": {"$literal": now_iso}
            }
        },
        {
            "$merge": {
                "into": COLLECTION_DAILY_SALES,
                "on": "_id",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]

    coll_val.aggregate(pipeline, allowDiskUse=True)

    # In full refresh mode, advance watermark to latest timestamp in collection
    if mode == "full":
        latest_doc = coll_val.find_one(
            sort=[("updated_at", DESCENDING), ("order_date", DESCENDING)],
            projection={"updated_at": 1, "order_date": 1}
        )
        if latest_doc:
            max_watermark = latest_doc.get("updated_at") or latest_doc.get("order_date") or max_watermark

    save_mv_watermark(target_db, max_watermark, view_name=COLLECTION_DAILY_SALES, meta={
        "daily_sales_refreshed_at": now_iso,
        "daily_sales_mode": mode
    })

    elapsed = time.perf_counter() - start_time
    total_docs = coll_mv.count_documents({})

    return {
        "view_name": COLLECTION_DAILY_SALES,
        "mode": mode,
        "status": "refreshed",
        "affected_periods_count": len(affected_dates) if mode == "incremental" else total_docs,
        "affected_periods": affected_dates,
        "total_documents_in_view": total_docs,
        "refreshed_at": now_iso,
        "elapsed_seconds": round(elapsed, 4)
    }


# ─────────────────────────────────────────────────────────────
# 2. MATERIALIZED VIEW: top_products_summary
# ─────────────────────────────────────────────────────────────
def refresh_top_products_summary(
    full_refresh: bool = False,
    db: Optional[Database] = None
) -> Dict[str, Any]:
    """
    Refreshes the 'top_products_summary' Materialized View in MongoDB.
    - If full_refresh=True or initial run: aggregates all product items and writes via $merge.
    - If incremental: identifies only the product SKUs affected by new/updated orders
      since the last watermark, and updates only those specific product documents.
    """
    start_time = time.perf_counter()
    target_db = _resolve_db(db)
    coll_val = target_db[COLLECTION_VALIDATED]
    coll_mv = target_db[COLLECTION_TOP_PRODUCTS]

    # Ensure unique index on sku
    coll_mv.create_index([("sku", ASCENDING)], unique=True)

    watermark = get_mv_watermark(target_db, view_name=COLLECTION_TOP_PRODUCTS)
    is_initial = (watermark is None) or (coll_mv.count_documents({}) == 0)

    now_iso = datetime.now(timezone.utc).isoformat()
    mode = "full" if (full_refresh or is_initial) else "incremental"

    max_watermark = watermark or "1970-01-01T00:00:00Z"
    affected_skus: List[str] = []

    if mode == "full":
        # Full recalculation across all validated orders
        match_initial = {"items": {"$exists": True, "$type": "array", "$ne": []}}
        unwind_match = {"items.sku": {"$exists": True, "$ne": None}}
    else:
        # Incremental: discover SKUs in orders updated or inserted after the watermark
        delta_query = {
            "$or": [
                {"updated_at": {"$gt": watermark}},
                {"order_date": {"$gt": watermark}}
            ]
        }
        delta_cursor = coll_val.find(delta_query, {"items": 1, "updated_at": 1, "order_date": 1})
        delta_skus_set = set()

        for doc in delta_cursor:
            items = doc.get("items") or []
            for itm in items:
                sku = itm.get("sku")
                if sku:
                    delta_skus_set.add(str(sku).strip())
            ts = doc.get("updated_at") or doc.get("order_date")
            if ts and ts > max_watermark:
                max_watermark = ts

        if not delta_skus_set:
            # Zero products changed -> perfectly idempotent no-op
            elapsed = time.perf_counter() - start_time
            return {
                "view_name": COLLECTION_TOP_PRODUCTS,
                "mode": "incremental",
                "status": "up_to_date",
                "affected_products_count": 0,
                "affected_skus": [],
                "total_documents_in_view": coll_mv.count_documents({}),
                "refreshed_at": now_iso,
                "elapsed_seconds": round(elapsed, 4)
            }

        affected_skus = sorted(list(delta_skus_set))
        # Match orders containing any of the affected SKUs
        match_initial = {"items.sku": {"$in": affected_skus}}
        unwind_match = {"items.sku": {"$in": affected_skus}}

    pipeline = [
        {"$match": match_initial},
        {"$unwind": "$items"},
        {"$match": unwind_match},
        {
            "$group": {
                "_id": "$items.sku",
                "sku": {"$first": "$items.sku"},
                "product_name": {"$first": "$items.name"},
                "total_quantity_sold": {"$sum": "$items.qty"},
                "total_revenue": {"$sum": "$items.total"},
                "order_occurrences": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": "$_id",
                "sku": "$sku",
                "product_name": "$product_name",
                "total_quantity_sold": 1,
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "order_occurrences": 1,
                "last_refreshed_at": {"$literal": now_iso}
            }
        },
        {
            "$merge": {
                "into": COLLECTION_TOP_PRODUCTS,
                "on": "_id",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]

    coll_val.aggregate(pipeline, allowDiskUse=True)

    # Advance watermark
    if mode == "full":
        latest_doc = coll_val.find_one(
            sort=[("updated_at", DESCENDING), ("order_date", DESCENDING)],
            projection={"updated_at": 1, "order_date": 1}
        )
        if latest_doc:
            max_watermark = latest_doc.get("updated_at") or latest_doc.get("order_date") or max_watermark

    save_mv_watermark(target_db, max_watermark, view_name=COLLECTION_TOP_PRODUCTS, meta={
        "top_products_refreshed_at": now_iso,
        "top_products_mode": mode
    })

    elapsed = time.perf_counter() - start_time
    total_docs = coll_mv.count_documents({})

    return {
        "view_name": COLLECTION_TOP_PRODUCTS,
        "mode": mode,
        "status": "refreshed",
        "affected_products_count": len(affected_skus) if mode == "incremental" else total_docs,
        "affected_skus": affected_skus,
        "total_documents_in_view": total_docs,
        "refreshed_at": now_iso,
        "elapsed_seconds": round(elapsed, 4)
    }


# ─────────────────────────────────────────────────────────────
# 3. UNIFIED REFRESH ENTRY POINT
# ─────────────────────────────────────────────────────────────
def refresh_materialized_views(
    full_refresh: bool = False,
    db: Optional[Database] = None
) -> Dict[str, Any]:
    """
    Unified execution function to refresh all Materialized Views.
    Idempotent and supports incremental updates based on watermarks.
    """
    start_total = time.perf_counter()
    target_db = _resolve_db(db)

    res_daily = refresh_daily_sales_summary(full_refresh=full_refresh, db=target_db)
    res_products = refresh_top_products_summary(full_refresh=full_refresh, db=target_db)

    elapsed_total = round(time.perf_counter() - start_total, 4)

    return {
        "status": "COMPLETED",
        "refreshed_at": datetime.now(timezone.utc).isoformat(),
        "total_elapsed_seconds": elapsed_total,
        "views": {
            COLLECTION_DAILY_SALES: res_daily,
            COLLECTION_TOP_PRODUCTS: res_products
        }
    }


def get_materialized_view_data(
    view_name: str,
    limit: int = 50,
    skip: int = 0,
    db: Optional[Database] = None
) -> List[Dict[str, Any]]:
    """
    Retrieves stored aggregated documents directly from a materialized view collection.
    Instant O(1) reads without recalculating raw data.
    """
    target_db = _resolve_db(db)
    if view_name not in (COLLECTION_DAILY_SALES, COLLECTION_TOP_PRODUCTS):
        raise ValueError(f"Unknown materialized view: '{view_name}'. Valid: [{COLLECTION_DAILY_SALES}, {COLLECTION_TOP_PRODUCTS}]")

    coll = target_db[view_name]
    cursor = coll.find({}).sort("_id", -1).skip(skip).limit(limit)
    return [json_safe(doc) for doc in cursor]
