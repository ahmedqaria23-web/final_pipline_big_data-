import sys
import json
from pathlib import Path
from datetime import datetime, timezone

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from src.mongodb.mongo_setup import initialize_database, get_mongo_db
from src.mongodb.repositories import (
    upsert_validated_batch,
    count_validated,
    is_business_state_equal
)
from src.monitoring.metrics import save_results


def run_experiments():
    print("=" * 65)
    print("  TASK 7: MONGODB UPSERT AND IDEMPOTENCY EXPERIMENTS")
    print("=" * 65)

    db_name = "ecommerce_store_idempotency_audit"
    db = initialize_database(db_name=db_name)
    val_coll = db["orders_validated"]

    # 1. Verify Unique Index
    indexes = val_coll.index_information()
    print("\n[1] Verifying Unique Index on 'id_order':")
    has_unique_id_order = False
    for idx_name, idx_spec in indexes.items():
        keys = [k[0] for k in idx_spec.get("key", [])]
        is_unique = idx_spec.get("unique", False)
        print(f"  - Index '{idx_name}': keys={keys}, unique={is_unique}")
        if "id_order" in keys and is_unique:
            has_unique_id_order = True

    assert has_unique_id_order, "CRITICAL: Unique index on 'id_order' not found!"
    print("  -> UNIQUE INDEX ON 'id_order' CONFIRMED [PASS]\n")

    # Clean test collection
    val_coll.delete_many({})

    # 2. Prepare Controlled Test Dataset (10 distinct orders)
    test_orders = []
    for i in range(1, 11):
        order_id = f"ORD-IDEM-{i:03d}"
        test_orders.append({
            "id_order": order_id,
            "order_date": "2025-01-31T10:00:00Z",
            "status": "مؤكد",
            "customer": {
                "customer_id": f"CUS-{i:03d}",
                "name": f"Customer {i}",
                "phone": "967771234567",
                "email": f"cust{i}@example.com",
                "address": {"city": "صنعاء", "district": "حدة"}
            },
            "items": [{"sku": f"SKU-{i:03d}", "name": f"Product {i}", "qty": 1, "unit_price": 5000.0, "total": 5000.0}],
            "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "YER", "amount": 5000.0},
            "total_amount": 5000.0,
            "quality_status": "valid",
            "id_run": "run_test_a",
            "at_ingested": datetime.now(timezone.utc).isoformat(),
            "processed_at": datetime.now(timezone.utc).isoformat()
        })

    # ─────────────────────────────────────────────────────────
    # TEST A: FIRST RUN
    # ─────────────────────────────────────────────────────────
    print("[2] Running TEST A — FIRST RUN (10 fresh orders)...")
    ins_a, upd_a, unc_a = upsert_validated_batch(db, [dict(rec) for rec in test_orders])
    total_val_a = val_coll.count_documents({})

    print(f"  - Inserted:        {ins_a}")
    print(f"  - Updated:         {upd_a}")
    print(f"  - Unchanged:       {unc_a}")
    print(f"  - Validated Total: {total_val_a}")

    assert ins_a == 10, f"Expected 10 inserted, got {ins_a}"
    assert upd_a == 0, f"Expected 0 updated, got {upd_a}"
    assert unc_a == 0, f"Expected 0 unchanged, got {unc_a}"
    assert total_val_a == 10, f"Expected 10 in collection, got {total_val_a}"
    print("  -> TEST A PASSED [PASS]\n")

    # ─────────────────────────────────────────────────────────
    # TEST B: REPLAY (Exact same dataset again with new run ID)
    # ─────────────────────────────────────────────────────────
    print("[3] Running TEST B — REPLAY (Same 10 orders with new id_run / timestamps)...")
    replay_orders = []
    for rec in test_orders:
        r = dict(rec)
        r["id_run"] = "run_test_b_replay"
        r["processed_at"] = datetime.now(timezone.utc).isoformat()
        replay_orders.append(r)

    ins_b, upd_b, unc_b = upsert_validated_batch(db, replay_orders)
    total_val_b = val_coll.count_documents({})

    print(f"  - Inserted:        {ins_b}")
    print(f"  - Updated:         {upd_b}")
    print(f"  - Unchanged:       {unc_b}")
    print(f"  - Validated Total: {total_val_b}")

    assert ins_b == 0, f"Expected 0 inserted on replay, got {ins_b}"
    assert upd_b == 0, f"Expected 0 updated on replay, got {upd_b}"
    assert unc_b == 10, f"Expected 10 unchanged on replay, got {unc_b}"
    assert total_val_b == 10, f"Validated count must NOT increase (expected 10, got {total_val_b})"

    # Verify each individual id_order count
    for rec in test_orders:
        cnt = val_coll.count_documents({"id_order": rec["id_order"]})
        assert cnt == 1, f"Expected exactly 1 document for {rec['id_order']}, got {cnt}"

    print("  -> TEST B PASSED (Zero duplicates, 10 unchanged, count untouched) [PASS]\n")

    # ─────────────────────────────────────────────────────────
    # TEST C: UPDATE (Modify one existing record: status changed)
    # ─────────────────────────────────────────────────────────
    print("[4] Running TEST C — UPDATE (Modify 1 order: ORD-IDEM-001 status changed to 'تم التسليم')...")
    modified_orders = []
    for rec in test_orders:
        r = dict(rec)
        r["id_run"] = "run_test_c_update"
        if r["id_order"] == "ORD-IDEM-001":
            r["status"] = "تم التسليم"
            modified_orders.append(r)

    ins_c, upd_c, unc_c = upsert_validated_batch(db, modified_orders)
    total_val_c = val_coll.count_documents({})

    print(f"  - Inserted:        {ins_c}")
    print(f"  - Updated:         {upd_c}")
    print(f"  - Unchanged:       {unc_c}")
    print(f"  - Validated Total: {total_val_c}")

    assert ins_c == 0, f"Expected 0 inserted, got {ins_c}"
    assert upd_c == 1, f"Expected 1 updated, got {upd_c}"
    assert total_val_c == 10, f"Validated total must remain 10, got {total_val_c}"

    # Verify updated record content and duplicate freedom
    updated_doc = val_coll.find_one({"id_order": "ORD-IDEM-001"})
    assert updated_doc["status"] == "تم التسليم", f"Expected updated status 'تم التسليم', got {updated_doc['status']}"
    cnt_updated = val_coll.count_documents({"id_order": "ORD-IDEM-001"})
    assert cnt_updated == 1, f"Duplicate detected for ORD-IDEM-001: count is {cnt_updated}"

    print("  -> TEST C PASSED (1 updated, 0 inserted, exactly 1 doc per id_order) [PASS]\n")

    # Record verified experiments in reports/results.json
    experiment_metrics = {
        "id_run": "audit_task_7_upsert_idempotency",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "experiment": "Task 7 - MongoDB Upsert and Idempotency Verification",
        "business_key": "id_order",
        "unique_index_confirmed": True,
        "test_a_first_run": {
            "input_records": 10,
            "inserted": ins_a,
            "updated": upd_a,
            "unchanged": unc_a,
            "validated_count": total_val_a
        },
        "test_b_replay": {
            "input_records": 10,
            "inserted": ins_b,
            "updated": upd_b,
            "unchanged": unc_b,
            "validated_count": total_val_b,
            "idempotency_verified": True
        },
        "test_c_update": {
            "modified_record": "ORD-IDEM-001",
            "modified_field": "status",
            "new_value": "تم التسليم",
            "inserted": ins_c,
            "updated": upd_c,
            "unchanged": unc_c,
            "validated_count": total_val_c,
            "duplicate_count_for_modified_key": cnt_updated
        },
        "upsert_policy": "ReplaceOne with upsert=True matching by unique business key id_order. Business state equality comparison ignores execution metadata (id_run, timestamps) to achieve true idempotency (unchanged=N, updated=0, inserted=0 on replay)."
    }

    save_results(experiment_metrics)
    print("  -> Experiment metrics appended to reports/results.json [OK]")

    # Cleanup test db
    db.client.drop_database(db_name)
    print("\n" + "=" * 65)
    print("  ALL TASK 7 EXPERIMENTS COMPLETED AND VERIFIED AGAINST MONGODB")
    print("=" * 65)


if __name__ == "__main__":
    run_experiments()
