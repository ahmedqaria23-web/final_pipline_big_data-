import pytest
from pathlib import Path
from unittest.mock import patch

from src.mongodb.mongo_setup import initialize_database
from src.mongodb.repositories import (
    save_ingestion_checkpoint,
    get_ingestion_checkpoint,
    get_active_checkpoint,
    compute_file_fingerprint,
    AmbiguousCheckpointError,
    count_raw,
    COLLECTION_RAW,
    COLLECTION_META_STATE
)
from src.routing.file_router import inspect_and_route
from src.ingestion.batch_loader import load_batch_to_raw


@pytest.fixture
def sample_csv(tmp_path):
    f = tmp_path / "test_recovery.csv"
    lines = ["order_id,status,payment_amount,total_amount"]
    for i in range(1, 9):  # 8 rows = 4 batches of 2
        lines.append(f"ORD-{i:03d},مؤكد,1000.0,1000.0")
    f.write_text("\n".join(lines), encoding="utf-8")
    return f


@pytest.fixture
def diff_csv(tmp_path):
    f = tmp_path / "different_file.csv"
    f.write_text("order_id,status\nDIFF-1,مؤكد\nDIFF-2,مؤكد\n", encoding="utf-8")
    return f


@pytest.fixture
def test_db():
    db = initialize_database(db_name="test_checkpoint_recovery_db")
    yield db
    db.client.drop_database("test_checkpoint_recovery_db")


def test_1_interrupted_run_resumes_same_id_run(sample_csv, test_db):
    """
    Test 1 — Interrupted Run Resumes Same id_run:
    Run A, fingerprint = FILE_X, status = IN_PROGRESS.
    Batch 1 SUCCESS, Batch 2 SUCCESS, process stops before Batch 3.
    New invocation should find RUN_A and reuse id_run.
    """
    fingerprint = compute_file_fingerprint(sample_csv)

    # Simulate crash after Batch 2 (rows 1-4)
    save_ingestion_checkpoint(test_db, {
        "file_fingerprint": fingerprint,
        "file_name": sample_csv.name,
        "file_path": str(sample_csv),
        "file_size_bytes": sample_csv.stat().st_size,
        "id_run": "RUN_A_ORIGINAL",
        "last_completed_batch": 2,
        "processed_rows": 4,
        "batch_size": 2,
        "status": "IN_PROGRESS"
    })

    # Simulate fresh application invocation via inspect_and_route
    routing = inspect_and_route(sample_csv, db=test_db)
    assert routing["is_resumed"] is True
    assert routing["id_run"] == "RUN_A_ORIGINAL"

    # Now run loader
    res = load_batch_to_raw(str(sample_csv), db=test_db, batch_size=2)
    assert res["id_run"] == "RUN_A_ORIGINAL"
    assert res["status"] == "COMPLETED"
    assert res["loaded_raw"] == 8


def test_2_resume_starts_at_correct_batch(sample_csv, test_db):
    """
    Test 2 — Resume Starts at Correct Batch:
    Batch 1 & 2 already successful (rows 1-4).
    After restart:
    Batch 1 & 2 NOT processed again.
    Batch 3 (rows 5-6) and Batch 4 (rows 7-8) are processed.
    """
    fingerprint = compute_file_fingerprint(sample_csv)

    save_ingestion_checkpoint(test_db, {
        "file_fingerprint": fingerprint,
        "file_name": sample_csv.name,
        "file_path": str(sample_csv),
        "file_size_bytes": sample_csv.stat().st_size,
        "id_run": "RUN_A_BATCH_TEST",
        "last_completed_batch": 2,
        "processed_rows": 4,
        "batch_size": 2,
        "status": "IN_PROGRESS"
    })

    # Track batches passed to insert_raw_batch
    recorded_batches = []
    from src.mongodb.repositories import insert_raw_batch as real_insert

    def spy_insert(db, batch):
        recorded_batches.append([doc["number_row_source"] for doc in batch])
        return real_insert(db, batch)

    with patch("src.ingestion.batch_loader.insert_raw_batch", side_effect=spy_insert):
        res = load_batch_to_raw(str(sample_csv), id_run="RUN_A_BATCH_TEST", db=test_db, batch_size=2)

    # Only batch 3 (rows 5, 6) and batch 4 (rows 7, 8) were written!
    assert recorded_batches == [[5, 6], [7, 8]]
    assert res["status"] == "COMPLETED"
    assert res["batch_count"] == 4


def test_3_failed_batch_recovery(sample_csv, test_db):
    """
    Test 3 — Failed Batch Recovery:
    Batch 1 & 2 SUCCESS, Batch 3 permanent failure.
    Checkpoint: last_completed_batch = 2, status = FAILED.
    New execution must resume from Batch 3 and NOT skip it.
    """
    fingerprint = compute_file_fingerprint(sample_csv)

    save_ingestion_checkpoint(test_db, {
        "file_fingerprint": fingerprint,
        "file_name": sample_csv.name,
        "file_path": str(sample_csv),
        "file_size_bytes": sample_csv.stat().st_size,
        "id_run": "RUN_FAILED_RECOVERY",
        "last_completed_batch": 2,
        "processed_rows": 4,
        "batch_size": 2,
        "status": "FAILED",
        "failed_batch": 3,
        "failed_row_range": [5, 6]
    })

    # Active checkpoint lookup should find the FAILED run to allow recovery
    active = get_active_checkpoint(test_db, fingerprint)
    assert active is not None
    assert active["id_run"] == "RUN_FAILED_RECOVERY"
    assert active["status"] == "FAILED"

    # Execution resumes RUN_FAILED_RECOVERY starting at Batch 3
    recorded_batches = []
    from src.mongodb.repositories import insert_raw_batch as real_insert

    def spy_insert(db, batch):
        recorded_batches.append([doc["number_row_source"] for doc in batch])
        return real_insert(db, batch)

    with patch("src.ingestion.batch_loader.insert_raw_batch", side_effect=spy_insert):
        res = load_batch_to_raw(str(sample_csv), db=test_db, batch_size=2)

    assert res["id_run"] == "RUN_FAILED_RECOVERY"
    assert res["status"] == "COMPLETED"
    # Batch 3 (rows 5,6) was retried and written, followed by batch 4 (rows 7,8)
    assert recorded_batches == [[5, 6], [7, 8]]


def test_4_completed_run_gets_new_id_run(sample_csv, test_db):
    """
    Test 4 — Completed Run Gets New id_run:
    Run A, fingerprint = FILE_X, status = COMPLETED.
    Start the same file again.
    Expected: new id_run = RUN_B != RUN_A. Do NOT resume completed run.
    """
    fingerprint = compute_file_fingerprint(sample_csv)

    # First run completed successfully
    save_ingestion_checkpoint(test_db, {
        "file_fingerprint": fingerprint,
        "file_name": sample_csv.name,
        "file_path": str(sample_csv),
        "file_size_bytes": sample_csv.stat().st_size,
        "id_run": "RUN_COMPLETED_A",
        "last_completed_batch": 4,
        "processed_rows": 8,
        "batch_size": 2,
        "status": "COMPLETED"
    })

    # Start same file again
    routing = inspect_and_route(sample_csv, db=test_db)
    assert routing["is_resumed"] is False
    assert routing["id_run"] != "RUN_COMPLETED_A"
    assert routing["id_run"].startswith("run_")


def test_5_different_file_does_not_resume_old_run(sample_csv, diff_csv, test_db):
    """
    Test 5 — Different File Does Not Resume Old Run:
    Run A, fingerprint = FILE_X, status = IN_PROGRESS.
    Start FILE_Y.
    Expected: new id_run. Checkpoint for FILE_X must NOT be used for FILE_Y.
    """
    fingerprint_x = compute_file_fingerprint(sample_csv)

    save_ingestion_checkpoint(test_db, {
        "file_fingerprint": fingerprint_x,
        "file_name": sample_csv.name,
        "file_path": str(sample_csv),
        "file_size_bytes": sample_csv.stat().st_size,
        "id_run": "RUN_X_ACTIVE",
        "last_completed_batch": 2,
        "processed_rows": 4,
        "batch_size": 2,
        "status": "IN_PROGRESS"
    })

    # Route different file
    routing_y = inspect_and_route(diff_csv, db=test_db)
    assert routing_y["is_resumed"] is False
    assert routing_y["id_run"] != "RUN_X_ACTIVE"


def test_6_multiple_checkpoints_prefers_active(sample_csv, test_db):
    """
    Test 6 (Critical Edge Case) — Multiple Checkpoints:
    FILE_X has:
    RUN_OLD with status = COMPLETED
    RUN_ACTIVE with status = IN_PROGRESS
    Verify get_active_checkpoint selects RUN_ACTIVE, never RUN_OLD.
    """
    fingerprint = compute_file_fingerprint(sample_csv)

    # Old completed run
    save_ingestion_checkpoint(test_db, {
        "file_fingerprint": fingerprint,
        "file_name": sample_csv.name,
        "file_path": str(sample_csv),
        "file_size_bytes": sample_csv.stat().st_size,
        "id_run": "RUN_OLD_COMPLETED",
        "last_completed_batch": 4,
        "processed_rows": 8,
        "batch_size": 2,
        "status": "COMPLETED"
    })

    # Active interrupted run
    save_ingestion_checkpoint(test_db, {
        "file_fingerprint": fingerprint,
        "file_name": sample_csv.name,
        "file_path": str(sample_csv),
        "file_size_bytes": sample_csv.stat().st_size,
        "id_run": "RUN_ACTIVE_RESUME",
        "last_completed_batch": 2,
        "processed_rows": 4,
        "batch_size": 2,
        "status": "IN_PROGRESS"
    })

    active = get_active_checkpoint(test_db, fingerprint)
    assert active is not None
    assert active["id_run"] == "RUN_ACTIVE_RESUME"
    assert active["id_run"] != "RUN_OLD_COMPLETED"


def test_7_ambiguous_multiple_in_progress_raises(sample_csv, test_db):
    """
    Test 7 (Critical Edge Case) — Ambiguity Detection:
    If multiple IN_PROGRESS runs exist for the same fingerprint,
    must STOP and raise AmbiguousCheckpointError.
    """
    fingerprint = compute_file_fingerprint(sample_csv)

    # Insert two IN_PROGRESS checkpoints directly into meta_state
    test_db[COLLECTION_META_STATE].insert_many([
        {
            "pipeline": f"ingest_checkpoint_{fingerprint}_RUN_1",
            "file_fingerprint": fingerprint,
            "id_run": "RUN_1",
            "status": "IN_PROGRESS"
        },
        {
            "pipeline": f"ingest_checkpoint_{fingerprint}_RUN_2",
            "file_fingerprint": fingerprint,
            "id_run": "RUN_2",
            "status": "IN_PROGRESS"
        }
    ])

    with pytest.raises(AmbiguousCheckpointError) as exc_info:
        get_active_checkpoint(test_db, fingerprint)

    assert "Ambiguous state" in str(exc_info.value)
    assert "RUN_1" in str(exc_info.value)
    assert "RUN_2" in str(exc_info.value)
