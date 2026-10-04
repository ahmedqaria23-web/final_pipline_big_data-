import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.mongodb.mongo_setup import initialize_database
from src.mongodb.repositories import (
    get_ingestion_checkpoint,
    count_raw,
    insert_raw_batch,
    COLLECTION_RAW,
    COLLECTION_META_STATE
)
from src.ingestion.batch_loader import load_batch_to_raw


@pytest.fixture
def mock_csv_file(tmp_path):
    """Creates a 6-row CSV file for testing 2-row batches."""
    f = tmp_path / "test_batches.csv"
    content = (
        "order_id,order_date,status,customer_id,customer_name,customer_phone,customer_email,city,district,delivery_cost,payment_method,payment_status,payment_amount,currency,total_amount,items_json\n"
        "ORD-001,2025-01-12T00:13:00,قيد الانتظار,CUS-1,Name1,714876334,user1@example.com,لحج,القاهرة,2000.0,محفظة إلكترونية,بانتظار الدفع,546500.0,YER,546500.0,\"[{sku:1}]\"\n"
        "ORD-002,2025-01-12T00:13:00,قيد الانتظار,CUS-2,Name2,714876334,user2@example.com,لحج,القاهرة,2000.0,محفظة إلكترونية,بانتظار الدفع,546500.0,YER,546500.0,\"[{sku:2}]\"\n"
        "ORD-003,2025-01-12T00:13:00,قيد الانتظار,CUS-3,Name3,714876334,user3@example.com,لحج,القاهرة,2000.0,محفظة إلكترونية,بانتظار الدفع,546500.0,YER,546500.0,\"[{sku:3}]\"\n"
        "ORD-004,2025-01-12T00:13:00,قيد الانتظار,CUS-4,Name4,714876334,user4@example.com,لحج,القاهرة,2000.0,محفظة إلكترونية,بانتظار الدفع,546500.0,YER,546500.0,\"[{sku:4}]\"\n"
        "ORD-005,2025-01-12T00:13:00,قيد الانتظار,CUS-5,Name5,714876334,user5@example.com,لحج,القاهرة,2000.0,محفظة إلكترونية,بانتظار الدفع,546500.0,YER,546500.0,\"[{sku:5}]\"\n"
        "ORD-006,2025-01-12T00:13:00,قيد الانتظار,CUS-6,Name6,714876334,user6@example.com,لحج,القاهرة,2000.0,محفظة إلكترونية,بانتظار الدفع,546500.0,YER,546500.0,\"[{sku:6}]\"\n"
    )
    f.write_text(content, encoding="utf-8")
    return f


@pytest.fixture
def test_db():
    db = initialize_database(db_name="test_batch_failure_db")
    yield db
    db.client.drop_database("test_batch_failure_db")


def test_1_successful_batch(mock_csv_file, test_db):
    """
    Test 1 — Successful batch:
    Mongo write = SUCCESS
    Expected: batch = successful, checkpoint = advanced, run finishes COMPLETED.
    """
    res = load_batch_to_raw(
        str(mock_csv_file),
        id_run="run_success_1",
        db=test_db,
        batch_size=2,
        max_retries=3,
        retry_delay_seconds=0.01
    )

    assert res["status"] == "COMPLETED"
    assert res["loaded_raw"] == 6
    assert res["batch_count"] == 3
    assert res["errors"] == 0

    chk = get_ingestion_checkpoint(test_db, res["file_source"], id_run="run_success_1")
    if not chk:
        chk = test_db[COLLECTION_META_STATE].find_one({"id_run": "run_success_1"})
    assert chk is not None
    assert chk["status"] == "COMPLETED"
    assert chk["processed_rows"] == 6
    assert chk["last_completed_batch"] == 3


def test_2_temporary_failure_then_success(mock_csv_file, test_db):
    """
    Test 2 — Temporary Mongo failure followed by success:
    Attempt 1 -> FAIL, Attempt 2 -> SUCCESS
    Expected: batch = successful, checkpoint = advanced only after SUCCESS, run continues normally.
    Verify checkpoint was not advanced after attempt 1.
    """
    real_insert = insert_raw_batch
    call_count = {"count": 0}

    def flaky_bulk_write(db, batch):
        call_count["count"] += 1
        if call_count["count"] == 1:
            # First attempt fails
            raise RuntimeError("Simulated transient MongoDB network failure on attempt 1")
        return real_insert(db, batch)

    with patch("src.ingestion.batch_loader.insert_raw_batch", side_effect=flaky_bulk_write):
        res = load_batch_to_raw(
            str(mock_csv_file),
            id_run="run_retry_success",
            db=test_db,
            batch_size=2,
            max_retries=3,
            retry_delay_seconds=0.01
        )

    # First batch succeeded on attempt 2, remaining 2 batches succeeded on attempt 1
    # Total calls: 1 (fail) + 1 (success batch 1) + 1 (success batch 2) + 1 (success batch 3) = 4
    assert call_count["count"] == 4
    assert res["status"] == "COMPLETED"
    assert res["loaded_raw"] == 6

    chk = test_db[COLLECTION_META_STATE].find_one({"id_run": "run_retry_success"})
    assert chk["status"] == "COMPLETED"
    assert chk["processed_rows"] == 6


def test_3_permanent_failure(mock_csv_file, test_db):
    """
    Test 3 — Permanent Mongo failure:
    All retries fail.
    Expected: batch = FAILED, checkpoint = NOT advanced past failed batch, run = FAILED.
    Failed batch is NOT silently discarded.
    """
    with patch("src.ingestion.batch_loader.insert_raw_batch", side_effect=RuntimeError("Permanent connection error")):
        with pytest.raises(RuntimeError) as exc_info:
            load_batch_to_raw(
                str(mock_csv_file),
                id_run="run_perm_fail",
                db=test_db,
                batch_size=2,
                max_retries=2,
                retry_delay_seconds=0.01
            )
        assert "Batch write failed permanently for batch #1" in str(exc_info.value)

    # Checkpoint must be FAILED, with 0 processed_rows
    chk = test_db[COLLECTION_META_STATE].find_one({"id_run": "run_perm_fail"})
    assert chk is not None
    assert chk["status"] == "FAILED"
    assert chk["processed_rows"] == 0
    assert chk["last_completed_batch"] == 0
    assert chk["failed_batch"] == 1
    assert chk["failed_row_range"] == [1, 2]
    assert "Permanent connection error" in chk["error"]


def test_4_no_false_completed_status(mock_csv_file, test_db):
    """
    Test 4 — Verify no false COMPLETED status:
    Batch 1 -> SUCCESS
    Batch 2 -> FAILURE
    Batch 3 -> must not be processed as if Batch 2 succeeded
    Expected final state: run status != COMPLETED (is FAILED), checkpoint at Batch 1.
    """
    real_insert = insert_raw_batch
    batch_call_count = {"count": 0}

    def fail_on_batch_2(db, batch):
        batch_call_count["count"] += 1
        if batch_call_count["count"] > 1:
            # Batch 2 and beyond fail
            raise RuntimeError("Simulated failure on Batch 2")
        return real_insert(db, batch)

    with patch("src.ingestion.batch_loader.insert_raw_batch", side_effect=fail_on_batch_2):
        with pytest.raises(RuntimeError) as exc_info:
            load_batch_to_raw(
                str(mock_csv_file),
                id_run="run_partial_fail",
                db=test_db,
                batch_size=2,
                max_retries=2,
                retry_delay_seconds=0.01
            )
        assert "Batch write failed permanently for batch #2" in str(exc_info.value)


    # Final checkpoint MUST be FAILED, NOT COMPLETED!
    chk = test_db[COLLECTION_META_STATE].find_one({"id_run": "run_partial_fail"})
    assert chk is not None
    assert chk["status"] == "FAILED"
    assert chk["status"] != "COMPLETED"
    # Checkpoint remains at batch 1 (2 rows), NOT advanced to batch 2 or 3
    assert chk["last_completed_batch"] == 1
    assert chk["processed_rows"] == 2
    assert chk["failed_batch"] == 2
    assert chk["failed_row_range"] == [3, 4]

    # Exactly 2 records in orders_raw from Batch 1, Batch 2 and 3 were NOT saved
    assert count_raw(test_db, {"id_run": "run_partial_fail"}) == 2
