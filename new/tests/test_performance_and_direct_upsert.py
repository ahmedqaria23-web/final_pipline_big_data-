import pytest
import os
import json
import time
from pathlib import Path

from src.mongodb.mongo_setup import initialize_database
from src.mongodb.repositories import (
    upsert_validated_batch,
    count_validated,
    count_quarantine,
    find_validated_sample
)
from src.quality.classifier import classify_record
from src.pipeline.elt_pipeline import run_elt_pipeline, classify_worker, _stream_cursor_chunks


@pytest.fixture
def test_db():
    db = initialize_database(db_name="test_opt_db")
    yield db
    db.client.drop_database("test_opt_db")


def make_valid_order_doc(id_order: str, status: str = "مؤكد", total_amount: float = 1000.0) -> dict:
    return {
        "id_order": id_order,
        "order_date": "2025-01-31T10:00:00Z",
        "status": status,
        "customer": {
            "customer_id": "CUS-1",
            "name": "Test User",
            "phone": "967771234567",
            "email": "test@example.com",
            "address": {"city": "صنعاء", "district": "حدة"}
        },
        "items": [{"sku": "SKU-1", "name": "Item 1", "qty": 1, "unit_price": total_amount, "total": total_amount}],
        "payment": {"method": "بطاقة", "status": "تم الدفع", "amount": total_amount, "currency": "YER"},
        "total_amount": total_amount,
        "quality_status": "valid"
    }


def test_empty_batch(test_db):
    ins, upd, unc = upsert_validated_batch(test_db, [])
    assert (ins, upd, unc) == (0, 0, 0)


def test_requirement_11_nonexistent_document_upsert(test_db):
    """Requirement 11: Non-existent document -> upserted = 1 (inserted = 1)"""
    rec = make_valid_order_doc("REQ-11-NEW-001")
    ins, upd, unc = upsert_validated_batch(test_db, [rec])
    assert ins == 1, f"Expected inserted=1, got {ins}"
    assert upd == 0, f"Expected updated=0, got {upd}"
    assert unc == 0, f"Expected unchanged=0, got {unc}"
    assert count_validated(test_db, {"id_order": "REQ-11-NEW-001"}) == 1


def test_requirement_9_identical_document_upsert(test_db):
    """Requirement 9: existing document == new document -> matched = 1, ins = 0, zero duplicates created"""
    rec = make_valid_order_doc("REQ-9-SAME-001")
    # Initial insert
    ins1, upd1, unc1 = upsert_validated_batch(test_db, [rec])
    assert ins1 == 1 and count_validated(test_db, {"id_order": "REQ-9-SAME-001"}) == 1

    # Re-insert exact same record
    ins2, upd2, unc2 = upsert_validated_batch(test_db, [rec])
    assert ins2 == 0, f"Expected inserted=0 on re-run, got {ins2}"
    # Verify zero duplicate creation in orders_validated
    assert count_validated(test_db, {"id_order": "REQ-9-SAME-001"}) == 1



def test_requirement_10_modified_document_upsert(test_db):
    """Requirement 10: existing document != new document -> matched = 1, modified = 1 (updated = 1)"""
    rec = make_valid_order_doc("REQ-10-MOD-001", status="قيد الانتظار")
    # Initial insert
    upsert_validated_batch(test_db, [rec])

    # Re-insert modified record
    rec_mod = make_valid_order_doc("REQ-10-MOD-001", status="تم التسليم")
    ins, upd, unc = upsert_validated_batch(test_db, [rec_mod])
    assert ins == 0, f"Expected inserted=0, got {ins}"
    assert upd == 1, f"Expected updated=1 (modified=1), got {upd}"
    assert unc == 0, f"Expected unchanged=0, got {unc}"
    
    doc = test_db["orders_validated"].find_one({"id_order": "REQ-10-MOD-001"})
    assert doc["status"] == "تم التسليم"
    assert count_validated(test_db, {"id_order": "REQ-10-MOD-001"}) == 1


def test_upsert_policy_test_a_b_c_lifecycle(test_db):
    """
    Task 7 End-to-End Idempotency & Upsert Lifecycle:
    - Verifies unique index on id_order in orders_validated
    - TEST A: First Run -> inserted = N, updated = 0, unchanged = 0
    - TEST B: Replay -> inserted = 0, updated = 0, unchanged = N (count does not increase)
    - TEST C: Update -> inserted = 0, updated = 1 (no duplicates created)
    """
    val_coll = test_db["orders_validated"]

    # Verify unique index
    indexes = val_coll.index_information()
    has_unique = any(
        "id_order" in [k[0] for k in idx.get("key", [])] and idx.get("unique")
        for idx in indexes.values()
    )
    assert has_unique is True, "Unique index on id_order must exist in orders_validated"

    # Prepare 5 records
    orders = [make_valid_order_doc(f"ORD-LIFE-{i:03d}") for i in range(1, 6)]

    # TEST A: First Run
    ins_a, upd_a, unc_a = upsert_validated_batch(test_db, [dict(o) for o in orders])
    assert ins_a == 5
    assert upd_a == 0
    assert unc_a == 0
    assert val_coll.count_documents({}) == 5

    # TEST B: Replay identical records with updated runtime tracking metadata
    replay_orders = []
    for o in orders:
        r = dict(o)
        r["id_run"] = "replay_run_002"
        r["processed_at"] = "2026-09-02T23:59:59Z"
        replay_orders.append(r)

    ins_b, upd_b, unc_b = upsert_validated_batch(test_db, replay_orders)
    assert ins_b == 0, f"Expected 0 inserted, got {ins_b}"
    assert upd_b == 0, f"Expected 0 updated, got {upd_b}"
    assert unc_b == 5, f"Expected 5 unchanged, got {unc_b}"
    assert val_coll.count_documents({}) == 5, "Validated count must NOT increase on replay"

    for o in orders:
        assert val_coll.count_documents({"id_order": o["id_order"]}) == 1

    # TEST C: Update single record
    mod_order = dict(orders[0])
    mod_order["status"] = "تم التسليم"
    ins_c, upd_c, unc_c = upsert_validated_batch(test_db, [mod_order])
    assert ins_c == 0
    assert upd_c == 1
    assert unc_c == 0
    assert val_coll.count_documents({}) == 5
    assert val_coll.count_documents({"id_order": orders[0]["id_order"]}) == 1
    assert val_coll.find_one({"id_order": orders[0]["id_order"]})["status"] == "تم التسليم"


def test_requirement_13_parallel_vs_sequential_identity():
    """Requirement 13: Parallel classification produces 100% identical outputs to sequential classification."""
    raw_docs = [
        {
            "id_run": "test_run",
            "number_row_source": idx,
            "record_raw": {
                "id_order": f"ORD-SEQ-PAR-{idx}",
                "order_date": "2025/01/31",
                "status": " مؤكد ",
                "customer": {"customer_id": f"CUS-{idx}", "name": "Test", "phone": "967771234567", "email": "a@b.com", "address": {"city": "صنعاء", "district": "حدة"}},
                "items": [{"sku": "S1", "name": "Item", "qty": 1, "unit_price": 1000.0, "total": 1000.0}],
                "payment": {"method": "بطاقة", "status": "تم الدفع", "amount": 1000.0, "currency": "YER"},
                "total_amount": 1000.0
            }
        }
        for idx in range(1, 11)
    ]

    seq_results = [classify_record(doc) for doc in raw_docs]
    par_results = [classify_worker(doc) for doc in raw_docs]

    assert len(seq_results) == len(par_results)
    from src.mongodb.repositories import is_business_state_equal
    for (seq_out, seq_pay), (par_out, par_pay) in zip(seq_results, par_results):
        assert seq_out == par_out
        assert is_business_state_equal(seq_pay, par_pay) is True


def test_elt_pipeline_end_to_end(tmp_path, test_db):
    sample_records = []
    for i in range(1, 16):
        sample_records.append({
            "id_order": f"ORD-PERF-{i:03d}",
            "order_date": "2025-01-31T10:00:00Z",
            "status": "مؤكد",
            "customer": {"customer_id": f"CUS-{i}", "name": f"User {i}", "phone": "967771234567", "email": f"u{i}@test.com", "address": {"city": "صنعاء", "district": "حدة"}},
            "items": [{"sku": f"SKU-{i}", "name": f"Item {i}", "qty": 1, "unit_price": 1000.0, "total": 1000.0}],
            "payment": {"method": "بطاقة", "status": "تم الدفع", "amount": 1000.0, "currency": "YER"},
            "total_amount": 1000.0
        })

    for j in range(1, 6):
        sample_records.append({
            "id_order": "",
            "status": "غير معروف",
            "customer": {"name": f"Invalid User {j}"}
        })

    sample_file = tmp_path / "perf_sample.jsonl"
    with open(sample_file, "w", encoding="utf-8") as f:
        for item in sample_records:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    metrics = run_elt_pipeline(str(sample_file), db=test_db)

    assert metrics["read_rows"] == 20
    assert metrics["count_valid"] + metrics["count_corrected"] == 15
    assert metrics["count_quarantine"] == 5
    assert count_validated(test_db) == 15
    assert count_quarantine(test_db) == 5

