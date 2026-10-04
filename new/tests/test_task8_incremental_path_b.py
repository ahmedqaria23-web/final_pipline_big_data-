import pytest
from datetime import datetime, timezone
from src.mongodb.mongo_setup import initialize_database
from src.incremental.incremental_loader import (
    get_watermark,
    save_watermark,
    initial_load,
    process_delta_batch
)
from config.settings import COLLECTION_VALIDATED, COLLECTION_PROCESSED_EVENTS


@pytest.fixture
def inc_test_db():
    db = initialize_database(db_name="test_inc_path_b_db")
    yield db
    db.client.drop_database("test_inc_path_b_db")


def make_order(id_order: str, status: str, order_date: str, updated_at: str, version: int = 1, amount: float = 1000.0, event_id: str = None) -> dict:
    doc = {
        "id_order": id_order,
        "order_date": order_date,
        "updated_at": updated_at,
        "status": status,
        "version": version,
        "customer": {
            "customer_id": f"CUS-{id_order}",
            "name": f"Customer {id_order}",
            "phone": "967771234567",
            "email": f"user_{id_order}@example.com",
            "address": {"city": "صنعاء", "district": "حدة"}
        },
        "items": [{"sku": f"SKU-{id_order}", "name": "Item", "qty": 1, "unit_price": amount, "total": amount}],
        "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "YER", "amount": amount},
        "total_amount": amount,
        "quality_status": "valid"
    }
    if event_id:
        doc["event_id"] = event_id
    return doc


def test_task8_incremental_lifecycle(inc_test_db):
    """
    Reproducible End-to-End Verification for Task 8:
    1. INITIAL LOAD: 3 baseline records
    2. DELTA 1: 1 UPDATE + 1 INSERT
    3. REPLAY: Same Delta 1 re-run (verifies zero duplicates and explicit semantics)
    4. DELTA 2: Another UPDATE + 1 INSERT
    """
    db = inc_test_db
    pipe = "test_inc_pipeline"

    # ─────────────────────────────────────────────────────────────
    # STEP 1: INITIAL LOAD
    # ─────────────────────────────────────────────────────────────
    initial_records = [
        make_order("ORD-INC-001", "مؤكد", "2025-01-31T10:00:00Z", "2025-01-31T10:00:00Z", version=1),
        make_order("ORD-INC-002", "قيد الانتظار", "2025-01-31T10:30:00Z", "2025-01-31T10:30:00Z", version=1),
        make_order("ORD-INC-003", "مؤكد", "2025-01-31T11:00:00Z", "2025-01-31T11:00:00Z", version=1),
    ]

    res_init = initial_load(db, initial_records, pipeline_name=pipe)
    print(f"\n[Initial Load] inserted={res_init['count_inserted']}, updated={res_init['count_updated']}, unchanged={res_init['count_unchanged']}, watermark={res_init['watermark']}")

    assert res_init["count_inserted"] == 3
    assert res_init["count_updated"] == 0
    assert res_init["count_unchanged"] == 0
    assert res_init["watermark"] == "2025-01-31T11:00:00Z"
    assert db[COLLECTION_VALIDATED].count_documents({}) == 3

    # ─────────────────────────────────────────────────────────────
    # STEP 2: DELTA 1 (1 UPDATE + 1 INSERT)
    # ─────────────────────────────────────────────────────────────
    # Update ORD-INC-002 (status changed to "تم التسليم", version=2, updated_at="2025-02-01T12:00:00Z")
    # Insert ORD-INC-004 (new record, version=1, updated_at="2025-02-01T12:00:00Z")
    delta_1 = [
        make_order("ORD-INC-002", "تم التسليم", "2025-01-31T10:30:00Z", "2025-02-01T12:00:00Z", version=2),
        make_order("ORD-INC-004", "مؤكد", "2025-02-01T12:00:00Z", "2025-02-01T12:00:00Z", version=1),
    ]

    res_d1 = process_delta_batch(db, delta_1, pipeline_name=pipe)
    print(f"[Delta 1] inserted={res_d1['count_inserted']}, updated={res_d1['count_updated']}, unchanged={res_d1['count_unchanged']}, watermark={res_d1['new_watermark']}")

    assert res_d1["count_inserted"] == 1, f"Expected 1 insert in Delta 1, got {res_d1['count_inserted']}"
    assert res_d1["count_updated"] == 1, f"Expected 1 update in Delta 1, got {res_d1['count_updated']}"
    assert res_d1["count_unchanged"] == 0
    assert res_d1["new_watermark"] == "2025-02-01T12:00:00Z"
    assert db[COLLECTION_VALIDATED].count_documents({}) == 4

    doc_002 = db[COLLECTION_VALIDATED].find_one({"id_order": "ORD-INC-002"})
    assert doc_002["status"] == "تم التسليم"
    assert doc_002["version"] == 2

    # ─────────────────────────────────────────────────────────────
    # STEP 3A: REPLAY SAME DELTA 1 (Streaming CDC Mode: Filtered by Watermark)
    # ─────────────────────────────────────────────────────────────
    # When replayed in standard streaming mode, records with updated_at <= watermark
    # are recognized as already committed up to this watermark:
    res_replay_stream = process_delta_batch(db, delta_1, pipeline_name=pipe, replay_mode=False)
    print(f"[Replay Stream] filtered_by_watermark={res_replay_stream['filtered_by_watermark']}, inserted={res_replay_stream['count_inserted']}, updated={res_replay_stream['count_updated']}, unchanged={res_replay_stream['count_unchanged']}")

    assert res_replay_stream["filtered_by_watermark"] == 2, "Both records must be identified as filtered by watermark"
    assert res_replay_stream["count_inserted"] == 0
    assert res_replay_stream["count_updated"] == 0
    assert res_replay_stream["count_unchanged"] == 0
    assert db[COLLECTION_VALIDATED].count_documents({}) == 4, "No duplicate documents created"

    # ─────────────────────────────────────────────────────────────
    # STEP 3B: REPLAY SAME DELTA 1 (Replay Mode: Evaluated by Upsert Layer)
    # ─────────────────────────────────────────────────────────────
    # When explicitly evaluated through the Upsert layer (replay_mode=True),
    # records pass to upsert_validated_batch, match existing documents,
    # and are verified as unchanged:
    res_replay_upsert = process_delta_batch(db, delta_1, pipeline_name=pipe, replay_mode=True)
    print(f"[Replay Upsert] inserted={res_replay_upsert['count_inserted']}, updated={res_replay_upsert['count_updated']}, unchanged={res_replay_upsert['count_unchanged']}")

    assert res_replay_upsert["count_inserted"] == 0
    assert res_replay_upsert["count_updated"] == 0
    assert res_replay_upsert["count_unchanged"] == 2, "Both records verified identical by Upsert layer"
    assert db[COLLECTION_VALIDATED].count_documents({}) == 4, "Validated count must NOT increase"

    # ─────────────────────────────────────────────────────────────
    # STEP 4: DELTA 2 (Another UPDATE + INSERT)
    # ─────────────────────────────────────────────────────────────
    # Update ORD-INC-001 (status changed to "ملغي", version=2, updated_at="2025-02-02T15:00:00Z")
    # Insert ORD-INC-005 (new record, version=1, updated_at="2025-02-02T15:00:00Z")
    delta_2 = [
        make_order("ORD-INC-001", "ملغي", "2025-01-31T10:00:00Z", "2025-02-02T15:00:00Z", version=2),
        make_order("ORD-INC-005", "مؤكد", "2025-02-02T15:00:00Z", "2025-02-02T15:00:00Z", version=1),
    ]

    res_d2 = process_delta_batch(db, delta_2, pipeline_name=pipe)
    print(f"[Delta 2] inserted={res_d2['count_inserted']}, updated={res_d2['count_updated']}, unchanged={res_d2['count_unchanged']}, watermark={res_d2['new_watermark']}")

    assert res_d2["count_inserted"] == 1, f"Expected 1 insert in Delta 2, got {res_d2['count_inserted']}"
    assert res_d2["count_updated"] == 1, f"Expected 1 update in Delta 2, got {res_d2['count_updated']}"
    assert res_d2["count_unchanged"] == 0
    assert res_d2["new_watermark"] == "2025-02-02T15:00:00Z"
    assert db[COLLECTION_VALIDATED].count_documents({}) == 5

    doc_001 = db[COLLECTION_VALIDATED].find_one({"id_order": "ORD-INC-001"})
    assert doc_001["status"] == "ملغي"
    assert doc_001["version"] == 2


def test_stale_version_conflict_resolution(inc_test_db):
    """
    Requirement 8: Version/updated_at conflict handling is explicit.
    Stale records (incoming_version < stored_version) must be rejected.
    """
    db = inc_test_db
    pipe = "test_conflict_pipe"

    init = [make_order("ORD-VER-001", "مؤكد", "2025-01-31T10:00:00Z", "2025-01-31T10:00:00Z", version=3)]
    initial_load(db, init, pipeline_name=pipe)

    # Attempt to apply a stale update (version=2 < stored version=3) with a newer timestamp
    stale_delta = [
        make_order("ORD-VER-001", "مرتجع", "2025-01-31T10:00:00Z", "2025-02-05T10:00:00Z", version=2)
    ]
    res = process_delta_batch(db, stale_delta, pipeline_name=pipe)
    assert res["conflicts_rejected"] == 1
    assert res["count_updated"] == 0
    assert res["count_inserted"] == 0

    # Verify the document in MongoDB remained at version 3 and status "مؤكد"
    doc = db[COLLECTION_VALIDATED].find_one({"id_order": "ORD-VER-001"})
    assert doc["version"] == 3
    assert doc["status"] == "مؤكد"


def test_cumulative_operation_idempotency(inc_test_db):
    """
    Requirement 7: Replaying a cumulative operation does not apply its effect twice.
    Verifies event_id deduplication via processed_events.
    """
    db = inc_test_db
    pipe = "test_cumul_pipe"

    # Initial order
    init = [make_order("ORD-CUM-001", "مؤكد", "2025-01-31T10:00:00Z", "2025-01-31T10:00:00Z", version=1, amount=1000.0, event_id="EVT-INIT-001")]
    initial_load(db, init, pipeline_name=pipe)

    # Delta: Apply payment top-up operation with unique event_id
    delta_op = [
        make_order("ORD-CUM-001", "مؤكد", "2025-01-31T10:00:00Z", "2025-02-01T12:00:00Z", version=2, amount=2000.0, event_id="EVT-TOPUP-002")
    ]
    res1 = process_delta_batch(db, delta_op, pipeline_name=pipe)
    assert res1["count_updated"] == 1
    assert res1["events_skipped"] == 0
    assert db[COLLECTION_VALIDATED].find_one({"id_order": "ORD-CUM-001"})["total_amount"] == 2000.0

    # Replay same cumulative event in force/replay mode
    res2 = process_delta_batch(db, delta_op, pipeline_name=pipe, force_process=True)
    assert res2["events_skipped"] == 1, "Cumulative event must be skipped from re-execution"
    assert res2["count_updated"] == 0
    # Amount must remain 2000.0 and NOT be added or doubled
    assert db[COLLECTION_VALIDATED].find_one({"id_order": "ORD-CUM-001"})["total_amount"] == 2000.0
