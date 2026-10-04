"""
CLI Script to execute and verify the 5 MongoDB Aggregation Reports against live database.
Saves real aggregation results into reports/aggregation_results.json.
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timezone

# Add project root to sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# Ensure stdout handles UTF-8 strings gracefully on Windows
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
    except Exception:
        pass

from config.settings import REPORT_DIR
from src.mongodb.mongo_setup import get_mongo_db
from src.analytics.reports import (
    AGGREGATION_REGISTRY,
    execute_registered_aggregation,
    list_registered_aggregations
)


def main():
    print("=" * 65)
    print("MongoDB Aggregation Reports Runner (Phase 3)")
    print("=" * 65)

    db = get_mongo_db()
    total_docs = db["orders_validated"].count_documents({})
    print(f"Connected to database: '{db.name}'")
    print(f"Collection 'orders_validated' contains: {total_docs:,} documents")

    reports = list_registered_aggregations()
    print(f"\nDiscovered {len(reports)} registered aggregation reports:")
    for r in reports:
        print(f" - {r['name']}: {r['description']}")

    print("\n" + "=" * 65)
    print("EXECUTING EACH REPORT AGAINST REAL DATA")
    print("=" * 65)

    full_results = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "database": db.name,
        "collection": "orders_validated",
        "total_documents": total_docs,
        "reports": {}
    }

    for r in reports:
        name = r["name"]
        print(f"\n>>> Running Aggregation Report: '{name}' ...")
        try:
            results = execute_registered_aggregation(name, db=db)
            full_results["reports"][name] = {
                "description": r["description"],
                "total_rows": len(results),
                "data": results
            }
            print(f"SUCCESS: Returned {len(results)} rows. Sample row:")
            if results:
                sample_str = json.dumps(results[0], ensure_ascii=False)
                # Print safe version for terminal
                print(f"   {sample_str[:120]}...")
        except Exception as e:
            print(f"ERROR executing '{name}': {e}")

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_file = REPORT_DIR / "aggregation_results.json"
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(full_results, f, indent=2, ensure_ascii=False)

    print("\n" + "=" * 65)
    print(f"All reports finished! Detailed results saved to: {report_file}")
    print("=" * 65)


if __name__ == "__main__":
    main()
