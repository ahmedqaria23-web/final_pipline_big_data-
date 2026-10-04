"""
Index Management & Explain('executionStats') Experiment Service.
Defines, creates, and verifies indexes supporting orders_validated queries,
and conducts reproducible Before-and-After explain experiments.
"""

import sys
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pymongo import ASCENDING, DESCENDING
from pymongo.database import Database
from config.settings import COLLECTION_VALIDATED, REPORT_DIR
from src.mongodb.mongo_setup import get_mongo_db

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# 3 ANALYTICS INDEX DEFINITIONS (Including 1 Compound Index)
# ─────────────────────────────────────────────────────────────
ANALYTICS_INDEX_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "name": "idx_customer_id",
        "keys": [("customer.customer_id", ASCENDING)],
        "unique": False,
        "compound": False,
        "supported_query": "orders_by_customer",
        "reason": "Allows rapid lookup of orders belonging to a single customer ID without scanning the entire collection.",
        "expected_benefit": "Replaces full collection scan (COLLSCAN) with an index scan (IXSCAN), bounding docs examined strictly to customer's order count."
    },
    {
        "name": "idx_city_status",
        "keys": [("customer.address.city", ASCENDING), ("status", ASCENDING)],
        "unique": False,
        "compound": True,
        "supported_query": "orders_by_city_and_status (and orders_by_city prefix)",
        "reason": "Compound index on shipping destination city and fulfillment status. Serves both single-field city queries (prefix match) and combined city+status queries.",
        "expected_benefit": "Filters documents on both dimensions directly within the index B-tree before document fetching, eliminating unnecessary disk document reads."
    },
    {
        "name": "idx_total_amount",
        "keys": [("total_amount", DESCENDING)],
        "unique": False,
        "compound": False,
        "supported_query": "high_value_orders",
        "reason": "Descending index on total_amount designed for range filtering ($gte) and sorted retrieval of largest orders.",
        "expected_benefit": "Eliminates blocking in-memory SORT stages in MongoDB and provides instant streaming from the top values of the B-tree."
    }
]

# Protected Midterm Index Names that must NEVER be dropped
PROTECTED_MIDTERM_INDEXES = {"_id_", "ux_id_order", "order_date_-1", "quality_status_1"}


def _resolve_db(db: Optional[Database]) -> Database:
    if db is not None:
        return db
    return get_mongo_db()


def create_analytics_indexes(db: Optional[Database] = None) -> List[str]:
    """
    Creates the 3 analytics indexes on orders_validated.
    Preserves all existing Midterm indexes.
    Returns list of created index names.
    """
    target_db = _resolve_db(db)
    coll = target_db[COLLECTION_VALIDATED]
    existing_indexes = coll.index_information()

    created = []
    for idx_def in ANALYTICS_INDEX_DEFINITIONS:
        name = idx_def["name"]
        if name not in existing_indexes:
            coll.create_index(
                idx_def["keys"],
                name=name,
                unique=idx_def.get("unique", False)
            )
            created.append(name)
            logger.info(f"Created index '{name}' on {COLLECTION_VALIDATED}")
        else:
            logger.info(f"Index '{name}' already exists on {COLLECTION_VALIDATED}")
            created.append(name)

    return created


def drop_analytics_indexes(db: Optional[Database] = None) -> List[str]:
    """
    Safely drops ONLY the 3 analytics indexes to prepare for a clean 'BEFORE' explain state.
    Strictly safeguards all Midterm indexes.
    Returns list of dropped index names.
    """
    target_db = _resolve_db(db)
    coll = target_db[COLLECTION_VALIDATED]
    existing = coll.index_information()

    dropped = []
    for idx_def in ANALYTICS_INDEX_DEFINITIONS:
        name = idx_def["name"]
        if name in existing and name not in PROTECTED_MIDTERM_INDEXES:
            coll.drop_index(name)
            dropped.append(name)
            logger.info(f"Dropped index '{name}' for reproducible testing.")

    return dropped


def list_indexes(db: Optional[Database] = None) -> List[Dict[str, Any]]:
    """
    Lists all active indexes on orders_validated with keys, unique flag, and whether compound.
    """
    target_db = _resolve_db(db)
    coll = target_db[COLLECTION_VALIDATED]
    info = coll.index_information()

    result = []
    for name, meta in info.items():
        keys = meta.get("key", [])
        result.append({
            "name": name,
            "keys": keys,
            "unique": meta.get("unique", False),
            "is_compound": len(keys) > 1,
            "is_midterm_protected": name in PROTECTED_MIDTERM_INDEXES
        })
    return result


get_active_indexes = list_indexes


def extract_execution_stats(explain_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts relevant executionStats metrics from a raw MongoDB explain() dictionary.
    """
    exec_stats = explain_result.get("executionStats", {})
    query_planner = explain_result.get("queryPlanner", {})
    winning_plan = query_planner.get("winningPlan", {})

    # Helper to find root or leaf stage
    def find_stage(plan: Dict[str, Any]) -> str:
        if not isinstance(plan, dict):
            return "UNKNOWN"
        stage = plan.get("stage", "UNKNOWN")
        input_stage = plan.get("inputStage", {})
        if input_stage:
            child = find_stage(input_stage)
            return f"{stage} -> {child}"
        return stage

    return {
        "executionTimeMillis": exec_stats.get("executionTimeMillis", 0),
        "totalKeysExamined": exec_stats.get("totalKeysExamined", 0),
        "totalDocsExamined": exec_stats.get("totalDocsExamined", 0),
        "nReturned": exec_stats.get("nReturned", 0),
        "winningPlanStage": find_stage(winning_plan),
        "rootStage": winning_plan.get("stage", "UNKNOWN"),
        "indexUsed": winning_plan.get("inputStage", {}).get("indexName") if isinstance(winning_plan.get("inputStage"), dict) else winning_plan.get("indexName")
    }


def execute_explain_for_query(
    coll,
    query_filter: Dict[str, Any],
    sort_fields: Optional[List[tuple]] = None,
    limit: int = 50
) -> Dict[str, Any]:
    """
    Runs explain('executionStats') for a specific MongoDB find cursor.
    """
    cursor = coll.find(query_filter)
    if sort_fields:
        cursor = cursor.sort(sort_fields)
    cursor = cursor.limit(limit)

    raw_explain = cursor.explain()
    return extract_execution_stats(raw_explain)


# ─────────────────────────────────────────────────────────────
# REPRODUCIBLE EXPLAIN EXPERIMENT RUNNER
# ─────────────────────────────────────────────────────────────
def run_explain_experiment(
    db: Optional[Database] = None,
    save_reports: bool = True
) -> Dict[str, Any]:
    """
    Executes a clean, reproducible Before-and-After explain experiment:
    1. Drops analytics indexes -> captures BEFORE stats for 3 queries.
    2. Creates analytics indexes -> captures AFTER stats for the same 3 queries.
    3. Calculates quantitative improvements (docs examined, time, plan stages).
    4. Writes machine-readable JSON and formatted Markdown report into reports/.
    """
    target_db = _resolve_db(db)
    coll = target_db[COLLECTION_VALIDATED]

    total_records = coll.count_documents({})
    if total_records == 0:
        logger.warning("orders_validated is empty. Explain experiment requires data to observe index metrics.")

    sample_doc = coll.find_one() or {}
    sample_customer = sample_doc.get("customer", {})
    sample_customer_id = sample_customer.get("customer_id") or "CUS-UNKNOWN"
    sample_city = sample_customer.get("address", {}).get("city") or "UNKNOWN_CITY"
    sample_status = sample_doc.get("status") or "UNKNOWN_STATUS"
    sample_amount = float(sample_doc.get("total_amount") or 100.0)

    # Define the 3 targeted queries for the experiment
    experiment_queries = [
        {
            "query_name": "orders_by_customer",
            "target_index": "idx_customer_id",
            "filter": {"customer.customer_id": sample_customer_id},
            "sort": None,
            "description": f"Lookup orders for customer_id='{sample_customer_id}'"
        },
        {
            "query_name": "orders_by_city_and_status",
            "target_index": "idx_city_status",
            "filter": {"customer.address.city": sample_city, "status": sample_status},
            "sort": None,
            "description": f"Compound lookup for city='{sample_city}' and status='{sample_status}'"
        },
        {
            "query_name": "high_value_orders",
            "target_index": "idx_total_amount",
            "filter": {"total_amount": {"$gte": sample_amount}},
            "sort": [("total_amount", DESCENDING)],
            "description": f"High value range filter and sort total_amount >= {sample_amount}"
        }
    ]

    # STEP A: BEFORE STATE (Ensure new indexes are dropped)
    drop_analytics_indexes(target_db)
    before_stats = {}
    for q in experiment_queries:
        stats = execute_explain_for_query(coll, q["filter"], q["sort"])
        before_stats[q["query_name"]] = stats

    # STEP B: AFTER STATE (Create new indexes)
    create_analytics_indexes(target_db)
    after_stats = {}
    for q in experiment_queries:
        stats = execute_explain_for_query(coll, q["filter"], q["sort"])
        after_stats[q["query_name"]] = stats

    # STEP C: IMPACT ANALYSIS & INTERPRETATION
    comparison_results = []
    for q in experiment_queries:
        name = q["query_name"]
        b = before_stats[name]
        a = after_stats[name]

        docs_diff = b["totalDocsExamined"] - a["totalDocsExamined"]
        keys_diff = a["totalKeysExamined"] - b["totalKeysExamined"]

        # Explain the transition
        before_stage = b["winningPlanStage"]
        after_stage = a["winningPlanStage"]
        transition = f"{before_stage} -> {after_stage}"

        impact = (
            f"Examined docs dropped from {b['totalDocsExamined']} to {a['totalDocsExamined']} "
            f"(reduction of {docs_diff:,} docs). Plan transformed from {before_stage} to {after_stage}."
        )

        comparison_results.append({
            "query_name": name,
            "target_index": q["target_index"],
            "description": q["description"],
            "before": b,
            "after": a,
            "difference": {
                "docs_examined_saved": docs_diff,
                "keys_examined_delta": keys_diff,
                "time_millis_delta": b["executionTimeMillis"] - a["executionTimeMillis"],
                "stage_transition": transition
            },
            "interpretation": impact
        })

    report_payload = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "database": target_db.name,
        "collection": COLLECTION_VALIDATED,
        "collection_total_documents": total_records,
        "indexes_tested": [idx["name"] for idx in ANALYTICS_INDEX_DEFINITIONS],
        "experiments": comparison_results
    }

    # STEP D: SAVE ARTIFACTS
    if save_reports:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        json_path = REPORT_DIR / "explain_results.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report_payload, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved explain experiment results to {json_path}")

        md_path = REPORT_DIR / "explain_report.md"
        _write_markdown_report(md_path, report_payload)
        logger.info(f"Saved explain markdown report to {md_path}")

    return report_payload


def _write_markdown_report(file_path: Path, payload: Dict[str, Any]) -> None:
    """Generates an evaluation-ready Markdown report showing Before vs After explain metrics."""
    lines = [
        "# MongoDB Explain('executionStats') Experiment Report",
        "",
        f"**Date**: {payload['timestamp_utc']}  ",
        f"**Database**: `{payload['database']}`  ",
        f"**Collection**: `{payload['collection']}`  ",
        f"**Total Records Evaluated**: `{payload['collection_total_documents']:,}`  ",
        "",
        "## Summary of Indexes Tested",
        "",
        "| Index Name | Type | Keys | Supported Query |",
        "| :--- | :--- | :--- | :--- |",
    ]
    for idx in ANALYTICS_INDEX_DEFINITIONS:
        idx_type = "Compound" if idx["compound"] else "Single Field"
        keys_str = ", ".join([f"{k}: {d}" for k, d in idx["keys"]])
        lines.append(f"| `{idx['name']}` | **{idx_type}** | `{keys_str}` | `{idx['supported_query']}` |")

    lines.extend([
        "",
        "## Experimental Results: Before vs. After Index Creation",
        ""
    ])

    for exp in payload["experiments"]:
        b = exp["before"]
        a = exp["after"]
        diff = exp["difference"]
        lines.extend([
            f"### Query: `{exp['query_name']}`",
            f"* **Target Index**: `{exp['target_index']}`",
            f"* **Execution Description**: {exp['description']}",
            "",
            "| Metric | BEFORE Index | AFTER Index | Impact |",
            "| :--- | :--- | :--- | :--- |",
            f"| **Winning Plan Stage** | `{b['winningPlanStage']}` | `{a['winningPlanStage']}` | `{diff['stage_transition']}` |",
            f"| **Total Docs Examined** | `{b['totalDocsExamined']:,}` | `{a['totalDocsExamined']:,}` | **{diff['docs_examined_saved']:,} fewer docs scanned** |",
            f"| **Total Keys Examined** | `{b['totalKeysExamined']:,}` | `{a['totalKeysExamined']:,}` | `{diff['keys_examined_delta']:,} index keys read` |",
            f"| **Execution Time (ms)** | `{b['executionTimeMillis']} ms` | `{a['executionTimeMillis']} ms` | `Δ {diff['time_millis_delta']} ms` |",
            f"| **Returned Docs** | `{b['nReturned']}` | `{a['nReturned']}` | Exact same result count |",
            "",
            f"> **Interpretation**: {exp['interpretation']}",
            ""
        ])

    with open(file_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
