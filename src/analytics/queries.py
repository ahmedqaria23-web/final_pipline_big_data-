"""
Query Service & Query Registry for orders_validated.
Provides at least 5 practical, dynamic queries with parameter validation,
safe JSON serialization, and dynamic execution registry.
"""

import sys
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, Any, List, Optional, Callable
from pathlib import Path

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bson import ObjectId
from pymongo.database import Database
from config.settings import COLLECTION_VALIDATED
from src.mongodb.mongo_setup import get_mongo_db


def json_safe(val: Any) -> Any:
    """
    Recursively converts MongoDB-specific types (ObjectId, datetime, Decimal, etc.)
    into JSON-serializable primitives.
    """
    if isinstance(val, dict):
        return {k: json_safe(v) for k, v in val.items()}
    elif isinstance(val, (list, tuple, set)):
        return [json_safe(item) for item in val]
    elif isinstance(val, ObjectId):
        return str(val)
    elif isinstance(val, (datetime, date)):
        return val.isoformat()
    elif isinstance(val, Decimal):
        return float(val)
    return val


def _resolve_db(db: Optional[Database]) -> Database:
    if db is not None:
        return db
    return get_mongo_db()


def _validate_pagination(limit: int, skip: int) -> None:
    if not isinstance(limit, int) or limit <= 0:
        raise ValueError(f"Parameter 'limit' must be a positive integer, got: {limit}")
    if not isinstance(skip, int) or skip < 0:
        raise ValueError(f"Parameter 'skip' must be a non-negative integer, got: {skip}")


# ─────────────────────────────────────────────────────────────
# 1. Query: Orders by Customer
# ─────────────────────────────────────────────────────────────
def get_orders_by_customer(
    customer_id: str,
    limit: int = 50,
    skip: int = 0,
    db: Optional[Database] = None
) -> List[Dict[str, Any]]:
    """
    Find orders placed by a specific customer ID.
    Uses 'customer.customer_id' field.
    """
    if not customer_id or not isinstance(customer_id, str) or not customer_id.strip():
        raise ValueError("Parameter 'customer_id' must be a non-empty string.")
    _validate_pagination(limit, skip)

    target_db = _resolve_db(db)
    coll = target_db[COLLECTION_VALIDATED]
    query_filter = {"customer.customer_id": customer_id.strip()}

    cursor = coll.find(query_filter).skip(skip).limit(limit)
    return [json_safe(doc) for doc in cursor]


# ─────────────────────────────────────────────────────────────
# 2. Query: Orders by City
# ─────────────────────────────────────────────────────────────
def get_orders_by_city(
    city: str,
    limit: int = 50,
    skip: int = 0,
    db: Optional[Database] = None
) -> List[Dict[str, Any]]:
    """
    Find orders destination city.
    Uses 'customer.address.city' field.
    """
    if not city or not isinstance(city, str) or not city.strip():
        raise ValueError("Parameter 'city' must be a non-empty string.")
    _validate_pagination(limit, skip)

    target_db = _resolve_db(db)
    coll = target_db[COLLECTION_VALIDATED]
    query_filter = {"customer.address.city": city.strip()}

    cursor = coll.find(query_filter).skip(skip).limit(limit)
    return [json_safe(doc) for doc in cursor]


# ─────────────────────────────────────────────────────────────
# 3. Query: Orders by Status
# ─────────────────────────────────────────────────────────────
def get_orders_by_status(
    status: str,
    limit: int = 50,
    skip: int = 0,
    db: Optional[Database] = None
) -> List[Dict[str, Any]]:
    """
    Find orders by fulfillment status.
    Uses 'status' field.
    """
    if not status or not isinstance(status, str) or not status.strip():
        raise ValueError("Parameter 'status' must be a non-empty string.")
    _validate_pagination(limit, skip)

    target_db = _resolve_db(db)
    coll = target_db[COLLECTION_VALIDATED]
    query_filter = {"status": status.strip()}

    cursor = coll.find(query_filter).skip(skip).limit(limit)
    return [json_safe(doc) for doc in cursor]


# ─────────────────────────────────────────────────────────────
# 4. Query: Orders within Date Range
# ─────────────────────────────────────────────────────────────
def get_orders_by_date_range(
    start_date: str,
    end_date: str,
    limit: int = 50,
    skip: int = 0,
    db: Optional[Database] = None
) -> List[Dict[str, Any]]:
    """
    Find orders placed within an ISO8601 date range [start_date, end_date].
    Uses 'order_date' field.
    """
    if not start_date or not isinstance(start_date, str) or not start_date.strip():
        raise ValueError("Parameter 'start_date' must be a non-empty string.")
    if not end_date or not isinstance(end_date, str) or not end_date.strip():
        raise ValueError("Parameter 'end_date' must be a non-empty string.")
    if start_date.strip() > end_date.strip():
        raise ValueError(f"start_date ({start_date}) cannot be greater than end_date ({end_date}).")
    _validate_pagination(limit, skip)

    target_db = _resolve_db(db)
    coll = target_db[COLLECTION_VALIDATED]
    query_filter = {
        "order_date": {
            "$gte": start_date.strip(),
            "$lte": end_date.strip()
        }
    }

    cursor = coll.find(query_filter).sort("order_date", 1).skip(skip).limit(limit)
    return [json_safe(doc) for doc in cursor]


# ─────────────────────────────────────────────────────────────
# 5. Query: High-Value Orders
# ─────────────────────────────────────────────────────────────
def get_high_value_orders(
    min_amount: float,
    limit: int = 50,
    skip: int = 0,
    db: Optional[Database] = None
) -> List[Dict[str, Any]]:
    """
    Find orders with total_amount >= min_amount, sorted descending by total_amount.
    Uses 'total_amount' field.
    """
    try:
        val_amount = float(min_amount)
    except (ValueError, TypeError):
        raise ValueError(f"Parameter 'min_amount' must be a valid number, got: {min_amount}")

    if val_amount < 0:
        raise ValueError(f"Parameter 'min_amount' cannot be negative, got: {min_amount}")
    _validate_pagination(limit, skip)

    target_db = _resolve_db(db)
    coll = target_db[COLLECTION_VALIDATED]
    query_filter = {"total_amount": {"$gte": val_amount}}

    cursor = coll.find(query_filter).sort("total_amount", -1).skip(skip).limit(limit)
    return [json_safe(doc) for doc in cursor]


# ─────────────────────────────────────────────────────────────
# 6. Combined Query: Orders by City and Status
# ─────────────────────────────────────────────────────────────
def get_orders_by_city_and_status(
    city: str,
    status: str,
    limit: int = 50,
    skip: int = 0,
    db: Optional[Database] = None
) -> List[Dict[str, Any]]:
    """
    Find orders by destination city and status simultaneously.
    Designed specifically to leverage the compound index: (customer.address.city, status).
    """
    if not city or not isinstance(city, str) or not city.strip():
        raise ValueError("Parameter 'city' must be a non-empty string.")
    if not status or not isinstance(status, str) or not status.strip():
        raise ValueError("Parameter 'status' must be a non-empty string.")
    _validate_pagination(limit, skip)

    target_db = _resolve_db(db)
    coll = target_db[COLLECTION_VALIDATED]
    query_filter = {
        "customer.address.city": city.strip(),
        "status": status.strip()
    }

    cursor = coll.find(query_filter).skip(skip).limit(limit)
    return [json_safe(doc) for doc in cursor]


# ─────────────────────────────────────────────────────────────
# QUERY REGISTRY
# ─────────────────────────────────────────────────────────────
QUERY_REGISTRY: Dict[str, Dict[str, Any]] = {
    "orders_by_customer": {
        "name": "orders_by_customer",
        "description": "Retrieve orders placed by a specific customer identifier.",
        "func": get_orders_by_customer,
        "parameters": [
            {"name": "customer_id", "type": "str", "required": True, "description": "Unique customer ID (e.g. CUS-1001)"},
            {"name": "limit", "type": "int", "required": False, "default": 50, "description": "Maximum records to return"},
            {"name": "skip", "type": "int", "required": False, "default": 0, "description": "Records offset"}
        ]
    },
    "orders_by_city": {
        "name": "orders_by_city",
        "description": "Retrieve orders filtered by shipping destination city.",
        "func": get_orders_by_city,
        "parameters": [
            {"name": "city", "type": "str", "required": True, "description": "City name (e.g. Tokyo, London, Paris, etc.)"},
            {"name": "limit", "type": "int", "required": False, "default": 50, "description": "Maximum records to return"},
            {"name": "skip", "type": "int", "required": False, "default": 0, "description": "Records offset"}
        ]
    },
    "orders_by_status": {
        "name": "orders_by_status",
        "description": "Retrieve orders filtered by current business fulfillment status.",
        "func": get_orders_by_status,
        "parameters": [
            {"name": "status", "type": "str", "required": True, "description": "Order status (e.g. pending, confirmed, delivered, etc.)"},
            {"name": "limit", "type": "int", "required": False, "default": 50, "description": "Maximum records to return"},
            {"name": "skip", "type": "int", "required": False, "default": 0, "description": "Records offset"}
        ]
    },
    "orders_by_date_range": {
        "name": "orders_by_date_range",
        "description": "Retrieve orders placed within an inclusive ISO date window.",
        "func": get_orders_by_date_range,
        "parameters": [
            {"name": "start_date", "type": "str", "required": True, "description": "ISO format start timestamp"},
            {"name": "end_date", "type": "str", "required": True, "description": "ISO format end timestamp"},
            {"name": "limit", "type": "int", "required": False, "default": 50, "description": "Maximum records to return"},
            {"name": "skip", "type": "int", "required": False, "default": 0, "description": "Records offset"}
        ]
    },
    "high_value_orders": {
        "name": "high_value_orders",
        "description": "Retrieve orders with total_amount greater than or equal to min_amount, sorted descending.",
        "func": get_high_value_orders,
        "parameters": [
            {"name": "min_amount", "type": "float", "required": True, "description": "Threshold order total amount"},
            {"name": "limit", "type": "int", "required": False, "default": 50, "description": "Maximum records to return"},
            {"name": "skip", "type": "int", "required": False, "default": 0, "description": "Records offset"}
        ]
    },
    "orders_by_city_and_status": {
        "name": "orders_by_city_and_status",
        "description": "Retrieve orders matching both shipping city and status simultaneously using compound index.",
        "func": get_orders_by_city_and_status,
        "parameters": [
            {"name": "city", "type": "str", "required": True, "description": "Shipping city"},
            {"name": "status", "type": "str", "required": True, "description": "Order status"},
            {"name": "limit", "type": "int", "required": False, "default": 50, "description": "Maximum records to return"},
            {"name": "skip", "type": "int", "required": False, "default": 0, "description": "Records offset"}
        ]
    }
}


def list_registered_queries() -> List[Dict[str, Any]]:
    """Returns list of query descriptors for registry discovery."""
    return [
        {
            "name": q["name"],
            "description": q["description"],
            "parameters": q["parameters"]
        }
        for q in QUERY_REGISTRY.values()
    ]


def get_query_descriptor(query_name: str) -> Optional[Dict[str, Any]]:
    """Returns a specific query descriptor from the registry."""
    q = QUERY_REGISTRY.get(query_name)
    if not q:
        return None
    return {
        "name": q["name"],
        "description": q["description"],
        "parameters": q["parameters"]
    }


def execute_registered_query(query_name: str, params: Dict[str, Any], db: Optional[Database] = None) -> List[Dict[str, Any]]:
    """
    Executes a query by registered name with arbitrary keyword arguments.
    """
    if query_name not in QUERY_REGISTRY:
        raise KeyError(f"Query '{query_name}' is not registered. Available queries: {list(QUERY_REGISTRY.keys())}")
    
    func = QUERY_REGISTRY[query_name]["func"]
    kwargs = dict(params or {})
    kwargs["db"] = db
    return func(**kwargs)
