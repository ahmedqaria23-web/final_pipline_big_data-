"""
CLI Script to execute the reproducible MongoDB explain('executionStats') experiment.
Collects actual before/after execution statistics, evaluates index efficiency,
and produces reports/explain_results.json and reports/explain_report.md.
"""

import sys
import json
from pathlib import Path

# Add project root to sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.mongodb.mongo_setup import get_mongo_db
from src.analytics.indexes import run_explain_experiment, create_analytics_indexes, list_indexes


def main():
    print("=" * 60)
    print("MongoDB Explain('executionStats') Experiment Runner")
    print("=" * 60)

    db = get_mongo_db()
    total_docs = db["orders_validated"].count_documents({})
    print(f"Connected to database: '{db.name}'")
    print(f"Collection 'orders_validated' contains: {total_docs:,} documents")

    if total_docs == 0:
        print("\n[NOTE] 'orders_validated' is currently empty in live database.")
        print("Populating test sample data into temporary collection or loading records...")

    print("\nRunning Before & After Explain Experiment on 3 queries...")
    results = run_explain_experiment(db=db, save_reports=True)

    print("\n" + "=" * 60)
    print("EXPERIMENT RESULTS SUMMARY")
    print("=" * 60)

    for exp in results["experiments"]:
        q_name = exp["query_name"]
        idx = exp["target_index"]
        b = exp["before"]
        a = exp["after"]
        diff = exp["difference"]

        print(f"\n--- [Query: {q_name}] ---")
        print(f"Target Index        : {idx}")
        print(f"Stage Transition    : {diff['stage_transition']}")
        print(f"Docs Examined Before: {b['totalDocsExamined']:,}")
        print(f"Docs Examined After : {a['totalDocsExamined']:,} (Saved {diff['docs_examined_saved']:,} docs)")
        print(f"Keys Examined       : {a['totalKeysExamined']:,}")
        print(f"Execution Time (ms) : Before={b['executionTimeMillis']}ms | After={a['executionTimeMillis']}ms")
        print(f"Interpretation      : {exp['interpretation']}")

    # Finalize indexes on the live collection
    create_analytics_indexes(db)
    print("\nActive indexes on 'orders_validated':")
    for idx in list_indexes(db):
        idx_type = "Compound" if idx["is_compound"] else "Single"
        print(f" - {idx['name']} ({idx_type}): {idx['keys']}")

    print("\nReports successfully written to:")
    print(" - reports/explain_results.json")
    print(" - reports/explain_report.md")


if __name__ == "__main__":
    main()
