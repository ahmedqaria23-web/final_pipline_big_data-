import pytest
from pymongo import ASCENDING

from src.mongodb.mongo_setup import initialize_database
from src.mongodb.repositories import (
    insert_quarantine_batch,
    count_quarantine,
    find_quarantine_sample,
    compute_quarantine_key,
    COLLECTION_QUARANTINE,
    COLLECTION_VALIDATED
)
from src.quality.classifier import classify_record


@pytest.fixture
def test_db():
    db = initialize_database(db_name="test_quarantine_key_db")
    yield db
    db.client.drop_database("test_quarantine_key_db")


def test_1_same_run_same_source_row_idempotent(test_db):
    """
    Test 1 — Same run + same source row:
    Insert/quarantine the same source occurrence twice.
    Expected: number of quarantine records = 1.
    Second operation updates/upserts rather than creates duplicate.
    """
    rec1 = {
        "id_run": "RUN_A",
        "source_row_number": 10,
        "id_order": "طلب-100001",
        "codes_error": ["DATE_IMPOSSIBLE_INVALID"],
        "details_error": ["First attempt error"]
    }
    rec2 = {
        "id_run": "RUN_A",
        "source_row_number": 10,
        "id_order": "طلب-100001",
        "codes_error": ["DATE_IMPOSSIBLE_INVALID"],
        "details_error": ["Retry attempt updated error"]
    }

    insert_quarantine_batch(test_db, [rec1])
    assert count_quarantine(test_db) == 1

    insert_quarantine_batch(test_db, [rec2])
    assert count_quarantine(test_db) == 1

    doc = test_db[COLLECTION_QUARANTINE].find_one({"_id": "RUN_A:10"})
    assert doc is not None
    assert doc["_id"] == "RUN_A:10"
    assert doc["details_error"] == ["Retry attempt updated error"]


def test_2_same_run_different_source_rows(test_db):
    """
    Test 2 — Same run + different source rows with identical id_order:
    Two records:
    RUN_A + طلب-100001 + source_row = 10
    RUN_A + طلب-100001 + source_row = 11
    Expected: number of quarantine records = 2 (no overwrite).
    """
    rec1 = {
        "id_run": "RUN_A",
        "source_row_number": 10,
        "id_order": "طلب-100001",
        "codes_error": ["PRICE_UNKNOWN"]
    }
    rec2 = {
        "id_run": "RUN_A",
        "source_row_number": 11,
        "id_order": "طلب-100001",
        "codes_error": ["ITEMS_EMPTY"]
    }

    insert_quarantine_batch(test_db, [rec1, rec2])
    assert count_quarantine(test_db) == 2

    doc1 = test_db[COLLECTION_QUARANTINE].find_one({"_id": "RUN_A:10"})
    doc2 = test_db[COLLECTION_QUARANTINE].find_one({"_id": "RUN_A:11"})

    assert doc1 is not None and doc1["_id"] == "RUN_A:10"
    assert doc2 is not None and doc2["_id"] == "RUN_A:11"
    assert doc1["id_order"] == "طلب-100001"
    assert doc2["id_order"] == "طلب-100001"


def test_3_different_runs_same_id_order(test_db):
    """
    Test 3 — Different runs + same id_order:
    RUN_A + طلب-100001 + row 10
    RUN_B + طلب-100001 + row 10
    Expected: number of quarantine records = 2 (no overwrite across runs).
    """
    rec1 = {
        "id_run": "RUN_A",
        "source_row_number": 10,
        "id_order": "طلب-100001",
        "codes_error": ["DATE_IMPOSSIBLE_INVALID"]
    }
    rec2 = {
        "id_run": "RUN_B",
        "source_row_number": 10,
        "id_order": "طلب-100001",
        "codes_error": ["DATE_IMPOSSIBLE_INVALID"]
    }

    insert_quarantine_batch(test_db, [rec1])
    insert_quarantine_batch(test_db, [rec2])

    assert count_quarantine(test_db) == 2
    doc_a = test_db[COLLECTION_QUARANTINE].find_one({"_id": "RUN_A:10"})
    doc_b = test_db[COLLECTION_QUARANTINE].find_one({"_id": "RUN_B:10"})

    assert doc_a is not None and doc_a["_id"] == "RUN_A:10"
    assert doc_b is not None and doc_b["_id"] == "RUN_B:10"


def test_4_missing_id_order(test_db):
    """
    Test 4 — Missing id_order:
    A quarantined record without id_order receives a deterministic quarantine identity
    using id_run + source_row_number. It must not fall back to constant/null/empty.
    """
    rec = {
        "id_run": "RUN_NO_ORDER_ID",
        "source_row_number": 57,
        "id_order": None,
        "codes_error": ["ID_ORDER_MISSING"]
    }

    insert_quarantine_batch(test_db, [rec])
    assert count_quarantine(test_db) == 1

    doc = test_db[COLLECTION_QUARANTINE].find_one({"_id": "RUN_NO_ORDER_ID:57"})
    assert doc is not None
    assert doc["_id"] == "RUN_NO_ORDER_ID:57"
    assert doc["id_order"] is None
    assert doc["source_row_number"] == 57


def test_5_classifier_quarantine_integration_and_preserved_codes(test_db):
    """
    Test 5 — Existing quarantine behavior through classifier:
    Verify reason codes, details, and stored record contents are preserved with correct _id.
    """
    raw_doc = {
        "id_run": "RUN_CLASSIFY_TEST",
        "number_row_source": 42,
        "file_source": "test_orders.csv",
        "record_raw": {
            "order_id": "ORD-BROKEN-99",
            "order_date": "2026-99-99",
            "items_json": "corrupted_items",
            "status": "مؤكد"
        }
    }

    outcome, payload = classify_record(raw_doc)
    assert outcome == "QUARANTINED"
    assert payload["_id"] == "RUN_CLASSIFY_TEST:42"
    assert payload["id_run"] == "RUN_CLASSIFY_TEST"
    assert payload["source_row_number"] == 42
    assert payload["file_source"] == "test_orders.csv"
    assert "DATE_IMPOSSIBLE_INVALID" in payload["codes_error"]
    assert "JSON_ITEMS_CORRUPTED" in payload["codes_error"]

    insert_quarantine_batch(test_db, [payload])
    doc = test_db[COLLECTION_QUARANTINE].find_one({"_id": "RUN_CLASSIFY_TEST:42"})
    assert doc is not None
    assert doc["_id"] == "RUN_CLASSIFY_TEST:42"
    assert set(doc["codes_error"]) == set(payload["codes_error"])


def test_6_orders_validated_unique_index_untouched(test_db):
    """
    Confirmation test:
    Verify that orders_validated unique index on id_order is intact and was not changed.
    """
    indexes = test_db[COLLECTION_VALIDATED].index_information()
    assert "ux_id_order" in indexes or any(
        idx.get("key") == [("id_order", 1)] and idx.get("unique") is True
        for idx in indexes.values()
    ), f"Unique index on orders_validated.id_order must exist, found: {indexes}"
