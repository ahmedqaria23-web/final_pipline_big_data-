"""
CLI Script to execute Materialized View refresh (Full or Incremental) and view stored summaries.
"""

import sys
import json
import argparse
from pathlib import Path

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

from src.mongodb.mongo_setup import get_mongo_db
from src.views.materialized_views import (
    refresh_materialized_views,
    get_materialized_view_data
)
from config.settings import COLLECTION_DAILY_SALES, COLLECTION_TOP_PRODUCTS


def main():
    parser = argparse.ArgumentParser(description="Refresh and inspect MongoDB Materialized Views.")
    parser.add_argument("--full", action="store_true", help="Force a full recalculation instead of incremental delta.")
    args = parser.parse_args()

    print("=" * 65)
    print("MongoDB Materialized Views Refresh Runner")
    print(f"Mode: {'FULL REFRESH' if args.full else 'INCREMENTAL (WATERMARK-DRIVEN)'}")
    print("=" * 65)

    db = get_mongo_db()
    total_docs = db["orders_validated"].count_documents({})
    print(f"Connected to database: '{db.name}' ({total_docs:,} validated orders)")

    result = refresh_materialized_views(full_refresh=args.full, db=db)
    print("\nRefresh Results:")
    print(json.dumps(result, indent=2, ensure_ascii=False))

    print("\n" + "=" * 65)
    print(f"Sample data from '{COLLECTION_DAILY_SALES}':")
    daily_sample = get_materialized_view_data(COLLECTION_DAILY_SALES, limit=3, db=db)
    for row in daily_sample:
        print("  ", json.dumps(row, ensure_ascii=False))

    print(f"\nSample data from '{COLLECTION_TOP_PRODUCTS}':")
    products_sample = get_materialized_view_data(COLLECTION_TOP_PRODUCTS, limit=3, db=db)
    for row in products_sample:
        print("  ", json.dumps(row, ensure_ascii=False))
    print("=" * 65)


if __name__ == "__main__":
    main()
