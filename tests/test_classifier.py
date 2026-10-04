import pytest
from src.quality.classifier import classify_record
from src.monitoring.metrics import verify_consistency_equation


def test_classify_valid_record():
    raw_doc = {
        "id_run": "test_run",
        "file_source": "test_data.jsonl",
        "number_row_source": 1,
        "record_raw": {
            "id_order": "ORD-VALID-100",
            "order_date": "2025-01-31T10:00:00Z",
            "status": "مؤكد",
            "customer": {
                "customer_id": "CUS-1",
                "name": "Ahmed",
                "phone": "967771234567",
                "email": "ahmed@example.com",
                "address": {"city": "صنعاء", "district": "حدة"}
            },
            "items": [{"sku": "SKU-1", "name": "Item 1", "qty": 1, "unit_price": 5000.0, "total": 5000.0}],
            "delivery": {"type": "سريع", "cost": 1000.0},
            "payment": {"method": "بطاقة", "status": "تم الدفع", "amount": 6000.0, "currency": "YER"},
            "total_amount": 6000.0
        }
    }
    outcome, payload = classify_record(raw_doc)
    assert outcome == "VALID"
    assert payload["quality_status"] == "valid"
    assert payload["id_order"] == "ORD-VALID-100"


def test_classify_corrected_record():
    raw_doc = {
        "id_run": "test_run",
        "file_source": "test_data.jsonl",
        "number_row_source": 2,
        "record_raw": {
            "id_order": "ORD-CORRECTED-101",
            "order_date": "2025/01/31",
            "status": " مؤكد ",
            "customer": {
                "customer_id": "CUS-2",
                "name": "Fatima",
                "phone": "967+ 77 123 4567",
                "email": "fatima@@example..com",
                "address": {"city": "عدن", "district": "المنصورة"}
            },
            "items": [{"sku": "SKU-2", "name": "Item 2", "qty": "٥", "unit_price": "2,000", "total": "10,000"}],
            "payment": {"method": "بطاقة", "status": "تم الدفع", "amount": 10000.0, "currency": "5000 لاير"},
            "total_amount": 10000.0
        }
    }
    outcome, payload = classify_record(raw_doc)
    assert outcome == "CORRECTED"
    assert payload["quality_status"] == "corrected"
    assert len(payload["corrections"]) > 0


def test_classify_quarantine_record_and_preserved_fields():
    raw_doc = {
        "id_run": "test_run_quarantine",
        "file_source": "orders_source.csv",
        "number_row_source": 42,
        "record_raw": {
            "id_order": "",
            "status": "حالة غير معروفة",
            "customer": {"name": "No ID"},
            "items": []
        }
    }
    outcome, payload = classify_record(raw_doc)
    assert outcome == "QUARANTINED"
    # Verify complete quarantine structure preservation
    assert payload["record_raw"] == raw_doc["record_raw"]
    assert "cleaned_attempt" in payload
    assert isinstance(payload["codes_error"], list)
    assert isinstance(payload["details_error"], list)
    assert payload["id_run"] == "test_run_quarantine"
    assert payload["file_source"] == "orders_source.csv"
    assert payload["source_row_number"] == 42
    assert payload["number_row_source"] == 42
    assert "quarantined_at" in payload


# ─────────────────────────────────────────────────────────────
# Required Error Categories Unit Verification
# ─────────────────────────────────────────────────────────────

def _make_base_valid_raw():
    return {
        "id_run": "run_error_test",
        "number_row_source": 10,
        "file_source": "batch.jsonl",
        "record_raw": {
            "id_order": "ORD-ERR-TEST",
            "order_date": "2025-01-31T10:00:00Z",
            "status": "مؤكد",
            "customer": {
                "customer_id": "CUS-10",
                "name": "Sami",
                "phone": "967771234567",
                "email": "sami@example.com",
                "address": {"city": "صنعاء", "district": "السبعين"}
            },
            "items": [{"sku": "SKU-A", "name": "Item A", "qty": 1, "unit_price": 5000.0, "total": 5000.0}],
            "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "YER", "amount": 5000.0},
            "total_amount": 5000.0
        }
    }


def test_error_category_id_order_missing():
    raw_doc = _make_base_valid_raw()
    raw_doc["record_raw"]["id_order"] = "   "
    outcome, payload = classify_record(raw_doc)
    assert outcome == "QUARANTINED"
    assert "ID_ORDER_MISSING" in payload["codes_error"]


def test_error_category_id_customer_missing():
    raw_doc = _make_base_valid_raw()
    raw_doc["record_raw"]["customer"]["customer_id"] = ""
    outcome, payload = classify_record(raw_doc)
    assert outcome == "QUARANTINED"
    assert "ID_CUSTOMER_MISSING" in payload["codes_error"]


def test_error_category_date_impossible_invalid():
    raw_doc = _make_base_valid_raw()
    raw_doc["record_raw"]["order_date"] = "invalid-date-9999-99-99"
    outcome, payload = classify_record(raw_doc)
    assert outcome == "QUARANTINED"
    assert "DATE_IMPOSSIBLE_INVALID" in payload["codes_error"]


def test_error_category_json_items_corrupted():
    raw_doc = _make_base_valid_raw()
    raw_doc["record_raw"]["items"] = [{"name": "Missing SKU", "qty": 1, "unit_price": 5000.0, "total": 5000.0}]
    outcome, payload = classify_record(raw_doc)
    assert outcome == "QUARANTINED"
    assert "JSON_ITEMS_CORRUPTED" in payload["codes_error"]


def test_error_category_items_empty():
    raw_doc = _make_base_valid_raw()
    raw_doc["record_raw"]["items"] = []
    outcome, payload = classify_record(raw_doc)
    assert outcome == "QUARANTINED"
    assert "ITEMS_EMPTY" in payload["codes_error"]


def test_error_category_price_unknown():
    raw_doc = _make_base_valid_raw()
    raw_doc["record_raw"]["items"] = [{"sku": "SKU-B", "name": "Item B", "qty": 2, "unit_price": None, "total": 1000.0}]
    outcome, payload = classify_record(raw_doc)
    assert outcome == "QUARANTINED"
    assert "PRICE_UNKNOWN" in payload["codes_error"]


def test_error_category_value_negative_ambiguous():
    raw_doc = _make_base_valid_raw()
    raw_doc["record_raw"]["items"] = [{"sku": "SKU-C", "name": "Item C", "qty": -5, "unit_price": 1000.0, "total": 5000.0}]
    outcome, payload = classify_record(raw_doc)
    assert outcome == "QUARANTINED"
    assert "VALUE_NEGATIVE_AMBIGUOUS" in payload["codes_error"]


def test_error_category_id_order_duplicate():
    raw_doc = _make_base_valid_raw()
    raw_doc["record_raw"]["is_duplicate"] = True
    outcome, payload = classify_record(raw_doc)
    assert outcome == "QUARANTINED"
    assert "ID_ORDER_DUPLICATE" in payload["codes_error"]


def test_error_category_conflicting_multiple():
    raw_doc = _make_base_valid_raw()
    raw_doc["record_raw"]["id_order"] = ""
    raw_doc["record_raw"]["order_date"] = "invalid"
    raw_doc["record_raw"]["items"] = []
    outcome, payload = classify_record(raw_doc)
    assert outcome == "QUARANTINED"
    assert "ERRORS_CONFLICTING_MULTIPLE" in payload["codes_error"]


def test_counter_consistency_equation():
    # run_raw_count = run_valid_count + run_corrected_count + run_quarantine_count
    assert verify_consistency_equation(
        run_raw_count=1000,
        run_valid_count=800,
        run_corrected_count=150,
        run_quarantine_count=50
    ) is True

    # Unbalanced counters must return False
    assert verify_consistency_equation(
        run_raw_count=1000,
        run_valid_count=800,
        run_corrected_count=150,
        run_quarantine_count=40  # Sum is 990 != 1000
    ) is False

