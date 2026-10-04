"""
Incremental Loader — Path B (Change Data Capture / Delta Engine)

Architecture & Specifications:
1. High-Watermark Metric:
   - Primary metric: `updated_at` (ISO-8601 Timestamp).
   - Fallback metric: `order_date` for initial records lacking explicit `updated_at`.
   - Watermark state is persisted in MongoDB `meta_state` collection (`pipeline: path_b_incremental`).

2. Change Discovery & Delta Filtering:
   - Initial Load: Loads baseline dataset and initializes the Watermark.
   - Delta Load: Filters incoming stream for records where `updated_at > watermark`.

3. Version Conflict Resolution (Latest-Wins & Stale Rejection):
   - Compares incoming `version` against stored `version` in `orders_validated`.
   - If `incoming_version >= stored_version`: Record is accepted for upsert.
   - If `incoming_version < stored_version`: Stale update is rejected (`conflicts_rejected`).

4. Atomic Upsert & True Idempotency:
   - Integrates with `upsert_validated_batch` to execute atomic `ReplaceOne(..., upsert=True)`.
   - Accurately tracks:
     * `count_inserted`: Truly new records.
     * `count_updated`: Existing records with modified business state.
     * `count_unchanged`: Existing records whose business state is identical (on re-runs).
   - Re-running the same Delta batch is 100% idempotent.
"""

from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from pymongo.database import Database

from config.settings import (
    COLLECTION_META_STATE,
    COLLECTION_VALIDATED,
    COLLECTION_PROCESSED_EVENTS
)
from src.mongodb.repositories import upsert_validated_batch


def get_watermark(db: Database, pipeline_name: str = "path_b_incremental") -> str:
    """Retrieves the last committed watermark timestamp from meta_state collection."""
    state = db[COLLECTION_META_STATE].find_one({"pipeline": pipeline_name})
    if state and "last_watermark" in state:
        return state["last_watermark"]
    return "1970-01-01T00:00:00Z"


def save_watermark(
    db: Database,
    new_watermark: str,
    pipeline_name: str = "path_b_incremental",
    additional_meta: Optional[Dict[str, Any]] = None
):
    """Persists the updated watermark timestamp into meta_state collection."""
    payload = {
        "last_watermark": new_watermark,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    if additional_meta:
        payload.update(additional_meta)

    db[COLLECTION_META_STATE].update_one(
        {"pipeline": pipeline_name},
        {"$set": payload},
        upsert=True
    )


def initial_load(
    db: Database,
    records: List[Dict[str, Any]],
    pipeline_name: str = "path_b_incremental"
) -> Dict[str, Any]:
    """
    Executes an Initial Baseline Load for Path B.
    Inserts initial records, resets watermark, and initializes tracking state.
    """
    if not records:
        return {
            "mode": "initial_load",
            "records_count": 0,
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
            "count_inserted": 0,
            "count_updated": 0,
            "count_unchanged": 0,
            "watermark": "1970-01-01T00:00:00Z"
        }

    max_ts = "1970-01-01T00:00:00Z"
    for r in records:
        ts = r.get("updated_at") or r.get("order_date") or "1970-01-01T00:00:00Z"
        if ts > max_ts:
            max_ts = ts
        event_id = r.get("event_id")
        if event_id:
            try:
                db[COLLECTION_PROCESSED_EVENTS].update_one(
                    {"event_id": event_id},
                    {"$set": {"event_id": event_id, "id_order": r.get("id_order"), "processed_at": datetime.now(timezone.utc).isoformat()}},
                    upsert=True
                )
            except Exception:
                pass

    inserted, updated, unchanged = upsert_validated_batch(db, records)
    save_watermark(db, max_ts, pipeline_name, {"mode": "initial_load", "baseline_records": len(records)})

    return {
        "mode": "initial_load",
        "records_count": len(records),
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "count_inserted": inserted,
        "count_updated": updated,
        "count_unchanged": unchanged,
        "watermark": max_ts
    }


def process_delta_batch(
    db: Database,
    records: List[Dict[str, Any]],
    pipeline_name: str = "path_b_incremental",
    force_process: bool = False,
    replay_mode: bool = False
) -> Dict[str, Any]:
    """
    Processes an Incremental Delta Batch containing new and modified records.
    Filters by Watermark (`updated_at > watermark`), enforces version conflict resolution,
    and performs idempotent upsert into `orders_validated`.

    Watermark Semantics:
    - Normal CDC mode (replay_mode=False, force_process=False):
      Records with updated_at <= watermark are classified as 'filtered_by_watermark' (already committed).
      They do NOT reach the Upsert layer, so they are not artificially claimed as 'unchanged'.
    - Replay mode (replay_mode=True or force_process=True):
      Records are explicitly evaluated through the Upsert layer to verify business state idempotency.
      Identical records are detected by is_business_state_equal() and counted as 'unchanged'.
    """
    watermark = get_watermark(db, pipeline_name)

    # 1. Discover Delta Records vs Filtered Stale Records
    delta_records = []
    filtered_stale = []

    if force_process or replay_mode:
        delta_records = records
    else:
        for r in records:
            rec_ts = r.get("updated_at") or r.get("order_date") or "9999-12-31T23:59:59Z"
            if rec_ts > watermark:
                delta_records.append(r)
            else:
                filtered_stale.append(r)

    if not delta_records:
        return {
            "mode": "delta_load",
            "watermark_used": watermark,
            "delta_records_read": len(records),
            "filtered_by_watermark": len(filtered_stale),
            "processed_count": 0,
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
            "count_inserted": 0,
            "count_updated": 0,
            "count_unchanged": 0,
            "conflicts_rejected": 0,
            "events_skipped": 0,
            "new_watermark": watermark
        }

    # 2. Conflict Resolution (Latest-Wins & Version Check & Cumulative Events)
    id_orders = [
        str(r.get("id_order") or r.get("order_id")).strip()
        for r in delta_records
        if (r.get("id_order") or r.get("order_id"))
    ]
    existing_docs = {
        doc["id_order"]: doc
        for doc in db[COLLECTION_VALIDATED].find({"id_order": {"$in": id_orders}})
    }

    eligible_records = []
    conflicts_rejected = 0
    events_skipped = 0
    max_watermark = watermark

    for rec in delta_records:
        raw_id = rec.get("id_order") or rec.get("order_id")
        if not raw_id:
            continue
        id_order = str(raw_id).strip()
        rec["id_order"] = id_order

        # Cumulative Operation Protection: check event_id deduplication
        event_id = rec.get("event_id")
        if event_id:
            existing_event = db[COLLECTION_PROCESSED_EVENTS].find_one({"event_id": event_id})
            if existing_event:
                events_skipped += 1
                continue

        incoming_ver = rec.get("version", 1)
        rec_ts = rec.get("updated_at") or rec.get("order_date") or watermark

        if id_order in existing_docs:
            stored_doc = existing_docs[id_order]
            stored_ver = stored_doc.get("version", 0)
            stored_ts = stored_doc.get("updated_at") or stored_doc.get("order_date") or "1970-01-01T00:00:00Z"

            # Version/Timestamp Conflict Handling
            if incoming_ver > stored_ver or (incoming_ver == stored_ver and rec_ts >= stored_ts):
                eligible_records.append(rec)
            else:
                conflicts_rejected += 1
        else:
            eligible_records.append(rec)

        if rec_ts > max_watermark:
            max_watermark = rec_ts

    # 3. Perform atomic upsert for eligible delta records
    inserted, updated, unchanged = upsert_validated_batch(db, eligible_records)

    # Persist processed events to prevent replaying cumulative side effects
    for rec in eligible_records:
        e_id = rec.get("event_id")
        if e_id:
            try:
                db[COLLECTION_PROCESSED_EVENTS].update_one(
                    {"event_id": e_id},
                    {"$set": {"event_id": e_id, "id_order": rec.get("id_order"), "processed_at": datetime.now(timezone.utc).isoformat()}},
                    upsert=True
                )
            except Exception:
                pass

    save_watermark(db, max_watermark, pipeline_name, {
        "mode": "delta_load",
        "last_delta_size": len(eligible_records)
    })

    return {
        "mode": "delta_load",
        "watermark_used": watermark,
        "delta_records_read": len(records),
        "filtered_by_watermark": len(filtered_stale),
        "processed_count": len(eligible_records),
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "count_inserted": inserted,
        "count_updated": updated,
        "count_unchanged": unchanged,
        "conflicts_rejected": conflicts_rejected,
        "events_skipped": events_skipped,
        "new_watermark": max_watermark
    }
