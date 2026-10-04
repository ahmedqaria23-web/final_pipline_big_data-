"""
Aggregation Reports & Analytics Aggregation Registry.
Provides at least 5 independent, production-grade MongoDB Aggregation Pipelines
for ecommerce_store data analysis, fully compatible with future FastAPI integration.
"""

import sys
import logging
from typing import Dict, Any, List, Optional
from pathlib import Path

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pymongo.database import Database
from config.settings import COLLECTION_VALIDATED
from src.mongodb.mongo_setup import get_mongo_db
from src.analytics.queries import json_safe

logger = logging.getLogger(__name__)


def _resolve_db(db: Optional[Database]) -> Database:
    if db is not None:
        return db
    return get_mongo_db()


def _validate_positive_int(value: Any, name: str, default: int = 10) -> int:
    if value is None:
        return default
    try:
        val = int(value)
        if val <= 0:
            raise ValueError
        return val
    except (ValueError, TypeError):
        raise ValueError(f"Parameter '{name}' must be a positive integer, got: {value}")


# ─────────────────────────────────────────────────────────────
# 1. Report: sales_by_city
# ─────────────────────────────────────────────────────────────
def report_sales_by_city(
    limit: int = 20,
    min_orders: int = 1,
    db: Optional[Database] = None
) -> List[Dict[str, Any]]:
    """
    Groups validated orders by shipping destination city.
    Computes total revenue, total orders, and average order value.
    """
    limit_val = _validate_positive_int(limit, "limit", default=20)
    min_orders_val = _validate_positive_int(min_orders, "min_orders", default=1)

    pipeline = [
        {
            "$match": {
                "customer.address.city": {"$exists": True, "$ne": None, "$nin": ["", "null"]}
            }
        },
        {
            "$group": {
                "_id": "$customer.address.city",
                "total_revenue": {"$sum": "$total_amount"},
                "total_orders": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"}
            }
        },
        {
            "$match": {
                "total_orders": {"$gte": min_orders_val}
            }
        },
        {
            "$project": {
                "_id": 0,
                "city": "$_id",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "total_orders": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]}
            }
        },
        {"$sort": {"total_revenue": -1}},
        {"$limit": limit_val}
    ]

    target_db = _resolve_db(db)
    coll = target_db[COLLECTION_VALIDATED]
    cursor = coll.aggregate(pipeline, allowDiskUse=True)
    return [json_safe(doc) for doc in cursor]


# ─────────────────────────────────────────────────────────────
# 2. Report: top_products
# ─────────────────────────────────────────────────────────────
def report_top_products(
    limit: int = 10,
    min_qty: int = 1,
    db: Optional[Database] = None
) -> List[Dict[str, Any]]:
    """
    Unwinds the items array in validated orders and aggregates sales per SKU/product.
    Computes total units sold, total product revenue, and order occurrences.
    """
    limit_val = _validate_positive_int(limit, "limit", default=10)
    min_qty_val = _validate_positive_int(min_qty, "min_qty", default=1)

    pipeline = [
        {
            "$match": {
                "items": {"$exists": True, "$type": "array", "$ne": []}
            }
        },
        {"$unwind": "$items"},
        {
            "$match": {
                "items.sku": {"$exists": True, "$ne": None}
            }
        },
        {
            "$group": {
                "_id": {
                    "sku": "$items.sku",
                    "name": "$items.name"
                },
                "total_quantity_sold": {"$sum": "$items.qty"},
                "total_revenue": {"$sum": "$items.total"},
                "order_occurrences": {"$sum": 1}
            }
        },
        {
            "$match": {
                "total_quantity_sold": {"$gte": min_qty_val}
            }
        },
        {
            "$project": {
                "_id": 0,
                "sku": "$_id.sku",
                "product_name": "$_id.name",
                "total_quantity_sold": 1,
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "order_occurrences": 1
            }
        },
        {"$sort": {"total_revenue": -1}},
        {"$limit": limit_val}
    ]

    target_db = _resolve_db(db)
    coll = target_db[COLLECTION_VALIDATED]
    cursor = coll.aggregate(pipeline, allowDiskUse=True)
    return [json_safe(doc) for doc in cursor]


# ─────────────────────────────────────────────────────────────
# 3. Report: top_customers
# ─────────────────────────────────────────────────────────────
def report_top_customers(
    limit: int = 10,
    min_spend: float = 0.0,
    db: Optional[Database] = None
) -> List[Dict[str, Any]]:
    """
    Aggregates orders per unique customer.
    Computes total expenditure, total orders count, average spend, and latest purchase date.
    """
    limit_val = _validate_positive_int(limit, "limit", default=10)
    try:
        min_spend_val = float(min_spend)
        if min_spend_val < 0:
            raise ValueError
    except (ValueError, TypeError):
        raise ValueError(f"Parameter 'min_spend' must be a non-negative number, got: {min_spend}")

    pipeline = [
        {
            "$match": {
                "customer.customer_id": {"$exists": True, "$ne": None, "$nin": ["", "null"]}
            }
        },
        {
            "$group": {
                "_id": "$customer.customer_id",
                "customer_name": {"$first": "$customer.name"},
                "total_spend": {"$sum": "$total_amount"},
                "total_orders": {"$sum": 1},
                "avg_order_spend": {"$avg": "$total_amount"},
                "last_order_date": {"$max": "$order_date"}
            }
        },
        {
            "$match": {
                "total_spend": {"$gte": min_spend_val}
            }
        },
        {
            "$project": {
                "_id": 0,
                "customer_id": "$_id",
                "customer_name": 1,
                "total_spend": {"$round": ["$total_spend", 2]},
                "total_orders": 1,
                "avg_order_spend": {"$round": ["$avg_order_spend", 2]},
                "last_order_date": 1
            }
        },
        {"$sort": {"total_spend": -1}},
        {"$limit": limit_val}
    ]

    target_db = _resolve_db(db)
    coll = target_db[COLLECTION_VALIDATED]
    cursor = coll.aggregate(pipeline, allowDiskUse=True)
    return [json_safe(doc) for doc in cursor]


# ─────────────────────────────────────────────────────────────
# 4. Report: sales_by_period
# ─────────────────────────────────────────────────────────────
def report_sales_by_period(
    period: str = "daily",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 30,
    db: Optional[Database] = None
) -> List[Dict[str, Any]]:
    """
    Aggregates revenue and order volume by date periods ('daily' -> YYYY-MM-DD, 'monthly' -> YYYY-MM).
    Supports optional date-range filtering.
    """
    limit_val = _validate_positive_int(limit, "limit", default=30)
    period_clean = (period or "daily").lower().strip()
    if period_clean not in ("daily", "monthly"):
        raise ValueError(f"Parameter 'period' must be 'daily' or 'monthly', got: '{period}'")

    substr_length = 10 if period_clean == "daily" else 7  # YYYY-MM-DD or YYYY-MM

    match_filter: Dict[str, Any] = {
        "order_date": {"$exists": True, "$type": "string", "$ne": ""}
    }

    if start_date and end_date:
        if start_date.strip() > end_date.strip():
            raise ValueError(f"start_date ({start_date}) cannot be greater than end_date ({end_date})")
        match_filter["order_date"] = {
            "$gte": start_date.strip(),
            "$lte": end_date.strip()
        }
    elif start_date:
        match_filter["order_date"] = {"$gte": start_date.strip()}
    elif end_date:
        match_filter["order_date"] = {"$lte": end_date.strip()}

    pipeline = [
        {"$match": match_filter},
        {
            "$project": {
                "period_label": {"$substrCP": ["$order_date", 0, substr_length]},
                "total_amount": 1
            }
        },
        {
            "$group": {
                "_id": "$period_label",
                "total_revenue": {"$sum": "$total_amount"},
                "total_orders": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "period": "$_id",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "total_orders": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]}
            }
        },
        {"$sort": {"period": -1}},
        {"$limit": limit_val}
    ]

    target_db = _resolve_db(db)
    coll = target_db[COLLECTION_VALIDATED]
    cursor = coll.aggregate(pipeline, allowDiskUse=True)
    return [json_safe(doc) for doc in cursor]


# ─────────────────────────────────────────────────────────────
# 5. Report: orders_by_status
# ─────────────────────────────────────────────────────────────
def report_orders_by_status(
    min_orders: int = 1,
    db: Optional[Database] = None
) -> List[Dict[str, Any]]:
    """
    Groups orders by fulfillment status.
    Computes total orders count, total volume revenue, and percentage of overall business orders.
    """
    min_orders_val = _validate_positive_int(min_orders, "min_orders", default=1)

    pipeline = [
        {
            "$match": {
                "status": {"$exists": True, "$ne": None, "$nin": ["", "null"]}
            }
        },
        {
            "$group": {
                "_id": "$status",
                "count_orders": {"$sum": 1},
                "total_value": {"$sum": "$total_amount"},
                "avg_value": {"$avg": "$total_amount"}
            }
        },
        {
            "$match": {
                "count_orders": {"$gte": min_orders_val}
            }
        },
        {
            "$project": {
                "_id": 0,
                "status": "$_id",
                "count_orders": 1,
                "total_value": {"$round": ["$total_value", 2]},
                "avg_value": {"$round": ["$avg_value", 2]}
            }
        },
        {"$sort": {"count_orders": -1}}
    ]

    target_db = _resolve_db(db)
    coll = target_db[COLLECTION_VALIDATED]
    cursor = coll.aggregate(pipeline, allowDiskUse=True)
    return [json_safe(doc) for doc in cursor]


# ─────────────────────────────────────────────────────────────
# AGGREGATION REGISTRY
# ─────────────────────────────────────────────────────────────
AGGREGATION_REGISTRY: Dict[str, Dict[str, Any]] = {
    "sales_by_city": {
        "name": "sales_by_city",
        "description": "Aggregates revenue and order volume by shipping city.",
        "func": report_sales_by_city,
        "parameters": [
            {"name": "limit", "type": "int", "required": False, "default": 20, "description": "Maximum cities to return"},
            {"name": "min_orders", "type": "int", "required": False, "default": 1, "description": "Minimum orders threshold"}
        ]
    },
    "top_products": {
        "name": "top_products",
        "description": "Unwinds items and aggregates sales, units sold, and revenue per product/SKU.",
        "func": report_top_products,
        "parameters": [
            {"name": "limit", "type": "int", "required": False, "default": 10, "description": "Maximum top products to return"},
            {"name": "min_qty", "type": "int", "required": False, "default": 1, "description": "Minimum total quantity sold threshold"}
        ]
    },
    "top_customers": {
        "name": "top_customers",
        "description": "Aggregates customer spend, order counts, and identifies high-value accounts.",
        "func": report_top_customers,
        "parameters": [
            {"name": "limit", "type": "int", "required": False, "default": 10, "description": "Maximum top customers to return"},
            {"name": "min_spend", "type": "float", "required": False, "default": 0.0, "description": "Minimum total expenditure threshold"}
        ]
    },
    "sales_by_period": {
        "name": "sales_by_period",
        "description": "Aggregates sales trends over time grouped by daily or monthly timeline periods.",
        "func": report_sales_by_period,
        "parameters": [
            {"name": "period", "type": "str", "required": False, "default": "daily", "description": "'daily' or 'monthly'"},
            {"name": "start_date", "type": "str", "required": False, "default": None, "description": "Optional ISO start date"},
            {"name": "end_date", "type": "str", "required": False, "default": None, "description": "Optional ISO end date"},
            {"name": "limit", "type": "int", "required": False, "default": 30, "description": "Maximum periods to return"}
        ]
    },
    "orders_by_status": {
        "name": "orders_by_status",
        "description": "Aggregates order distribution, total financial value, and volume across fulfillment statuses.",
        "func": report_orders_by_status,
        "parameters": [
            {"name": "min_orders", "type": "int", "required": False, "default": 1, "description": "Minimum orders per status threshold"}
        ]
    }
}


def list_registered_aggregations() -> List[Dict[str, Any]]:
    """Returns list of all available aggregation report descriptors."""
    return [
        {
            "name": agg["name"],
            "description": agg["description"],
            "parameters": agg["parameters"]
        }
        for agg in AGGREGATION_REGISTRY.values()
    ]


def get_aggregation_descriptor(name: str) -> Optional[Dict[str, Any]]:
    """Returns descriptor for a specific aggregation report."""
    agg = AGGREGATION_REGISTRY.get(name)
    if not agg:
        return None
    return {
        "name": agg["name"],
        "description": agg["description"],
        "parameters": agg["parameters"]
    }


def execute_registered_aggregation(
    name: str,
    params: Optional[Dict[str, Any]] = None,
    db: Optional[Database] = None
) -> List[Dict[str, Any]]:
    """
    Executes an aggregation report by name with dynamic parameters.
    """
    if name not in AGGREGATION_REGISTRY:
        raise KeyError(f"Aggregation report '{name}' not found. Available: {list(AGGREGATION_REGISTRY.keys())}")

    func = AGGREGATION_REGISTRY[name]["func"]
    kwargs = dict(params or {})
    kwargs["db"] = db
    return func(**kwargs)
