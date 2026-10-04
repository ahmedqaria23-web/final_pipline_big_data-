import pytest
from src.quality.quality_rules import (
    apply_quality_rules,
    normalize_arabic_digits,
    clean_thousands_separators,
    clean_currency,
    clean_number_words,
    clean_phone,
    clean_email,
    clean_date,
    clean_numeric_string
)
from src.quality.classifier import classify_record


def test_arabic_digits_normalization():
    record = {
        "id_order": "ORD-1",
        "items": [{"sku": "SKU-1", "name": "Item 1", "qty": "٥", "unit_price": "١٠٠0", "total": "5000"}]
    }
    cleaned = apply_quality_rules(record)
    assert cleaned["items"][0]["qty"] == 5
    assert cleaned["items"][0]["unit_price"] == 1000.0
    corr_rules = [c["rule_code"] for c in cleaned["corrections"]]
    assert "NUMERIC_ARABIC_WORDS_QTY" in corr_rules

    # Arabic decimal separator '٫' and thousands separator '٬'
    assert clean_numeric_string("٧٠٦٠٠٠٫٠") == 706000.0
    assert clean_numeric_string("١٢٥٬٠٠٠") == 125000
    assert normalize_arabic_digits("۱۲۳") == "123"


def test_currency_symbols_and_safe_handling():
    # 1. Arabic currency phrase correctly normalized to YER
    rec_yer = {
        "id_order": "ORD-2A",
        "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "ريال يمني", "amount": 5000},
        "items": [{"sku": "SKU-2", "name": "Item 2", "qty": 1, "unit_price": 5000.0, "total": 5000.0}]
    }
    cleaned_yer = apply_quality_rules(rec_yer)
    assert cleaned_yer["payment"]["currency"] == "YER"
    assert any(c["rule_code"] == "CURRENCY_NORM" for c in cleaned_yer["corrections"])

    # 2. Foreign currency (USD) is NOT guessed or overwritten to YER
    rec_usd = {
        "id_order": "ORD-2B",
        "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "USD", "amount": 20},
        "items": [{"sku": "SKU-2", "name": "Item 2", "qty": 1, "unit_price": 20.0, "total": 20.0}]
    }
    cleaned_usd = apply_quality_rules(rec_usd)
    assert cleaned_usd["payment"]["currency"] == "USD"
    # Verify classifier quarantines USD because YER is the only allowed currency
    outcome, payload = classify_record(rec_usd)
    assert outcome == "QUARANTINED"
    assert any("currency" in err.lower() for err in payload["details_error"])

    # 3. Unknown currency (XYZ) is NOT assumed to be YER
    rec_unknown = {
        "id_order": "ORD-2C",
        "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "XYZ", "amount": 100},
        "items": [{"sku": "SKU-2C", "name": "Item 2C", "qty": 1, "unit_price": 100.0, "total": 100.0}]
    }
    cleaned_unknown = apply_quality_rules(rec_unknown)
    assert cleaned_unknown["payment"]["currency"] == "XYZ"
    outcome_u, payload_u = classify_record(rec_unknown)
    assert outcome_u == "QUARANTINED"


def test_thousands_separators():
    record = {
        "id_order": "ORD-3",
        "items": [{"sku": "SKU-3", "name": "Item 3", "qty": 1, "unit_price": "125,000.00", "total": "125,000.00"}]
    }
    cleaned = apply_quality_rules(record)
    assert cleaned["items"][0]["unit_price"] == 125000.0
    assert cleaned["items"][0]["total"] == 125000.0

    # Arabic thousands separator
    assert clean_thousands_separators("125٬000.00") == "125000.00"
    assert clean_thousands_separators("125 000.00") == "125000.00"


def test_known_number_words():
    record = {
        "id_order": "ORD-4",
        "items": [{"sku": "SKU-4", "name": "Item 4", "qty": "ألفان", "unit_price": 10, "total": 20000}]
    }
    cleaned = apply_quality_rules(record)
    assert cleaned["items"][0]["qty"] == 2000
    assert any(c["rule_code"] == "NUMERIC_ARABIC_WORDS_QTY" for c in cleaned["corrections"])

    # Variations without hamza and colloquial words
    assert clean_number_words("الف") == 1000
    assert clean_number_words("الفين") == 2000
    assert clean_number_words("مليون") == 1000000


def test_phone_normalization():
    # Various dirty phone formats
    assert clean_phone("+967 77-123-4567") == "967771234567"
    assert clean_phone("00967731234567") == "967731234567"
    assert clean_phone("0771234567") == "967771234567"  # Local trunk prefix 0
    assert clean_phone("711234567") == "967711234567"
    assert clean_phone("967701234567") == "967701234567"
    assert clean_phone("٧٧١٢٣٤٥٦٧") == "967771234567"  # Arabic digits in phone

    record = {
        "id_order": "ORD-5",
        "customer": {"customer_id": "C-1", "name": "Ali", "phone": "+967 77-123-4567", "email": "ali@mail.com"}
    }
    cleaned = apply_quality_rules(record)
    assert cleaned["customer"]["phone"] == "967771234567"
    assert any(c["rule_code"] == "PHONE_NORM" for c in cleaned["corrections"])


def test_email_repair():
    assert clean_email("user@@mail..com") == "user@mail.com"
    record = {
        "id_order": "ORD-6",
        "customer": {"customer_id": "C-2", "name": "Omar", "phone": "967771234567", "email": "omar@@company..org"}
    }
    cleaned = apply_quality_rules(record)
    assert cleaned["customer"]["email"] == "omar@company.org"
    assert any(c["rule_code"] == "EMAIL_REPAIR" for c in cleaned["corrections"])


def test_date_normalization():
    assert clean_date("2025 /01 /31") == "2025-01-31"
    assert clean_date("٢٠٢٥/٠١/٣١") == "2025-01-31"
    assert clean_date("2025 /01 /31T10:00:00Z") == "2025-01-31T10:00:00Z"
    record = {"id_order": "ORD-7", "order_date": "2025 /01 /31"}
    cleaned = apply_quality_rules(record)
    assert cleaned["order_date"] == "2025-01-31"
    assert any(c["rule_code"] == "DATE_ISO_NORM" for c in cleaned["corrections"])


def test_status_normalization():
    # Localized Arabic synonyms and English statuses
    for dirty, expected in [("مدفوع", "مؤكد"), ("pending", "قيد الانتظار"), ("delivered", "تم التسليم"), ("cancelled", "ملغي")]:
        record = {"id_order": "ORD-8", "status": dirty}
        cleaned = apply_quality_rules(record)
        assert cleaned["status"] == expected
        assert any(c["rule_code"] == "STATUS_SYNONYM_NORM" for c in cleaned["corrections"])


def test_item_total_recalculation():
    # item total corrupted (0 instead of qty * price)
    record = {
        "id_order": "ORD-9",
        "items": [{"sku": "SKU-9", "name": "Item 9", "qty": 3, "unit_price": 2500.0, "total": 0.0}]
    }
    cleaned = apply_quality_rules(record)
    assert cleaned["items"][0]["total"] == 7500.0
    assert any(c["rule_code"] == "ITEM_TOTAL_RECALCULATE" for c in cleaned["corrections"])


def test_order_total_recalculation():
    # Order total corrupted (0 instead of items sum + delivery)
    record = {
        "id_order": "ORD-10",
        "total_amount": 0.0,
        "delivery_cost": 2000.0,
        "items": [{"sku": "SKU-10", "name": "Item 10", "qty": 2, "unit_price": 5000.0, "total": 10000.0}]
    }
    cleaned = apply_quality_rules(record)
    assert cleaned["total_amount"] == 12000.0
    assert any(c["rule_code"] == "ORDER_TOTAL_RECALCULATE" for c in cleaned["corrections"])


def test_payment_amount_match():
    record = {
        "id_order": "ORD-11",
        "total_amount": 12000.0,
        "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "YER", "amount": 0.0},
        "items": [{"sku": "SKU-11", "name": "Item 11", "qty": 1, "unit_price": 12000.0, "total": 12000.0}]
    }
    cleaned = apply_quality_rules(record)
    assert cleaned["payment"]["amount"] == 12000.0
    assert any(c["rule_code"] == "PAYMENT_AMOUNT_MATCH" for c in cleaned["corrections"])


def test_json_items_parsing():
    record = {
        "id_order": "ORD-12",
        "items_json": '[{"sku": "SKU-12", "name": "Phone", "qty": 1, "unit_price": 50000.0, "total": 50000.0}]'
    }
    cleaned = apply_quality_rules(record)
    assert isinstance(cleaned["items"], list)
    assert cleaned["items"][0]["sku"] == "SKU-12"
    assert any(c["rule_code"] == "PARSE_JSON_ITEMS" for c in cleaned["corrections"])


def test_audit_trail_structure():
    record = {
        "id_order": "ORD-13",
        "status": "pending",
        "order_date": "2025 /02 /15",
        "customer": {"name": "Test", "phone": "771234567", "email": "test@@mail..com"}
    }
    cleaned = apply_quality_rules(record)
    assert cleaned["quality_status"] == "corrected"
    assert len(cleaned["corrections"]) >= 4

    for entry in cleaned["corrections"]:
        assert "field" in entry
        assert "original_value" in entry
        assert "corrected_value" in entry
        assert "rule_code" in entry
        assert "timestamp" in entry


def test_no_guessing_missing_sku_quarantined():
    # If item has no SKU, it should NOT be invented; validator must quarantine it as JSON_ITEMS_CORRUPTED
    record = {
        "id_order": "ORD-14",
        "order_date": "2025-02-15T10:00:00Z",
        "status": "مؤكد",
        "customer": {"customer_id": "C-14", "name": "Test", "phone": "967771234567", "email": "test@mail.com", "address": {"city": "صنعاء", "district": "حدة"}},
        "items": [{"name": "Item without SKU", "qty": 1, "unit_price": 5000.0, "total": 5000.0}],
        "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "YER", "amount": 5000.0},
        "total_amount": 5000.0
    }
    cleaned = apply_quality_rules(record)
    assert "sku" not in cleaned["items"][0] or not cleaned["items"][0]["sku"]

    outcome, payload = classify_record(record)
    assert outcome == "QUARANTINED"
    assert "JSON_ITEMS_CORRUPTED" in payload["codes_error"]


def test_no_guessing_missing_district_quarantined():
    # If customer address is missing district, do NOT invent "المركز"; quarantine it
    record = {
        "id_order": "ORD-15",
        "order_date": "2025-02-15T10:00:00Z",
        "status": "مؤكد",
        "customer_id": "C-15",
        "customer_name": "Test Customer",
        "phone": "967771234567",
        "email": "test15@mail.com",
        "shipping_city": "صنعاء",
        # district is missing
        "items": [{"sku": "SKU-15", "name": "Item 15", "qty": 1, "unit_price": 5000.0, "total": 5000.0}],
        "payment": {"method": "بطاقة", "status": "تم الدفع", "currency": "YER", "amount": 5000.0},
        "total_amount": 5000.0
    }
    cleaned = apply_quality_rules(record)
    assert cleaned["customer"]["address"]["district"] == ""

    outcome, payload = classify_record(record)
    assert outcome == "QUARANTINED"
    assert any("district" in d.lower() for d in payload["details_error"])

