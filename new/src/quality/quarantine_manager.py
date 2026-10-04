from typing import List, Dict, Any
from pymongo.database import Database
from src.mongodb.repositories import find_quarantine_sample, count_quarantine, upsert_validated_batch
from src.quality.classifier import classify_record
from config.settings import COLLECTION_QUARANTINE


def get_quarantine_summary(db: Database) -> List[Dict[str, Any]]:
    pipeline = [
        {"$unwind": "$codes_error"},
        {"$group": {"_id": "$codes_error", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    results = list(db[COLLECTION_QUARANTINE].aggregate(pipeline))
    return [{"code": r["_id"], "count": r["count"]} for r in results]


def revalidate_quarantine_records(db: Database) -> Dict[str, Any]:
    quarantined_items = list(db[COLLECTION_QUARANTINE].find())
    reprocessed_valid = []
    remained_quarantined_ids = []

    for item in quarantined_items:
        outcome, payload = classify_record(item)
        if outcome in ["VALID", "CORRECTED"]:
            reprocessed_valid.append(payload)
            db[COLLECTION_QUARANTINE].delete_one({"_id": item["_id"]})
        else:
            remained_quarantined_ids.append(item["_id"])

    inserted, updated, unchanged = (0, 0, 0)
    if reprocessed_valid:
        inserted, updated, unchanged = upsert_validated_batch(db, reprocessed_valid)

    return {
        "total_examined": len(quarantined_items),
        "recovered_count": len(reprocessed_valid),
        "remaining_quarantine": len(remained_quarantined_ids),
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged
    }
