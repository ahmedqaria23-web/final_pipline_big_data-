import re
import copy
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

ARABIC_DIGIT_MAP = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")

NUMBER_WORDS_MAP = {
    "ألف": 1000,
    "الف": 1000,
    "ألفان": 2000,
    "الفان": 2000,
    "ألفين": 2000,
    "الفين": 2000,
    "ثلاثة آلاف": 3000,
    "أربعة آلاف": 4000,
    "خمسة آلاف": 5000,
    "عشرة آلاف": 10000,
    "مئة ألف": 100000,
    "مليون": 1000000,
}

STATUS_SYNONYMS = {
    "مؤكد": "مؤكد",
    "مدفوع": "مؤكد",
    "قيد الانتظار": "قيد الانتظار",
    "قيد الشحن": "قيد الشحن",
    "تم التسليم": "تم التسليم",
    "مرتجع": "مرتجع",
    "ملغي": "ملغي",
    "confirmed": "مؤكد",
    "pending": "قيد الانتظار",
    "shipped": "قيد الشحن",
    "delivered": "تم التسليم",
    "cancelled": "ملغي",
}


def normalize_arabic_digits(val: Any) -> Any:
    if isinstance(val, str):
        return val.translate(ARABIC_DIGIT_MAP)
    return val


def clean_thousands_separators(val: Any) -> Any:
    if isinstance(val, str):
        cleaned = val.replace(",", "").replace("\u066c", "")
        cleaned = re.sub(r"(?<=\d)\s+(?=\d)", "", cleaned)
        return cleaned
    return val


YER_SYNONYMS = ["ريال يمني", "ريال", "ر.ي", "yer", "yr", "لاير"]
FOREIGN_CURRENCY_PATTERNS = ["USD", "EUR", "SAR", "AED", "GBP", "دولار", "سعودي", "$", "€", "£", "¥", "درهم", "دينار"]


def clean_currency(val: Any) -> Tuple[str, Any]:
    if isinstance(val, str):
        val_str = val.strip()
        # First check explicit foreign currencies without assuming YER
        for foreign in FOREIGN_CURRENCY_PATTERNS:
            if foreign in val_str.upper() or foreign in val_str:
                val_str = re.sub(re.escape(foreign), "", val_str, flags=re.IGNORECASE).strip()
                code = "USD" if foreign in ["USD", "دولار", "$"] else ("SAR" if foreign in ["SAR", "سعودي"] else foreign.upper())
                return val_str, code

        has_yer = False
        for pat in YER_SYNONYMS:
            if pat in val_str.lower() or pat in val_str:
                has_yer = True
                val_str = re.sub(re.escape(pat), "", val_str, flags=re.IGNORECASE).strip()

        if has_yer:
            return val_str, "YER"
        return val_str, None
    return str(val) if val is not None else "", None


def clean_number_words(val: Any) -> Any:
    if isinstance(val, str):
        trimmed = val.strip()
        if trimmed in NUMBER_WORDS_MAP:
            return NUMBER_WORDS_MAP[trimmed]
    return val


def clean_phone(val: Any) -> str:
    if val is not None:
        val_str = normalize_arabic_digits(str(val)).strip()
        digits = re.sub(r"[^\d]", "", val_str)
        if digits.startswith("00967"):
            digits = digits[2:]
        if len(digits) == 10 and digits.startswith(("077", "073", "070", "071")):
            digits = digits[1:]
        if len(digits) == 9 and digits.startswith(("77", "73", "70", "71")):
            return f"967{digits}"
        if len(digits) == 12 and digits.startswith("967"):
            return digits
        return digits
    return ""


def clean_email(val: Any) -> str:
    if isinstance(val, str):
        cleaned = re.sub(r"@+", "@", val.strip())
        cleaned = re.sub(r"\.+", ".", cleaned)
        return cleaned
    return ""


def clean_date(val: Any) -> str:
    """Normalizes dirty dates like '2025 /01 /31' or '٢٠٢٥/٠١/٣١' while leaving valid ISO dates untouched."""
    if isinstance(val, str):
        val_clean = normalize_arabic_digits(val)
        if "/" in val_clean or " " in val_clean or val_clean != val:
            cleaned = re.sub(r"\s+", " ", val_clean.strip())
            time_part = ""
            if "T" in cleaned:
                date_part, time_part = cleaned.split("T", 1)
                time_part = "T" + time_part.strip()
            else:
                date_part = cleaned

            date_clean = date_part.replace(" ", "").replace("/", "-")
            parts = date_clean.split("-")
            if len(parts) == 3 and len(parts[0]) == 4:
                y, m, d = parts[0], parts[1].zfill(2), parts[2].zfill(2)
                return f"{y}-{m}-{d}{time_part}"
    return val


def clean_numeric_string(val: Any) -> Any:
    if isinstance(val, (int, float)):
        return val
    if isinstance(val, str):
        c = normalize_arabic_digits(val)
        c = c.replace("\u066b", ".")  # Arabic decimal separator '٫' -> '.'
        c = clean_thousands_separators(c)
        c, _ = clean_currency(c)
        c = clean_number_words(c)
        if isinstance(c, (int, float)):
            return c
        c_str = str(c).strip()
        try:
            if "." in c_str:
                return float(c_str)
            return int(c_str)
        except Exception:
            try:
                return float(c_str)
            except Exception:
                return val
    return val


def apply_quality_rules(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    doc = raw_record.get("record_raw", raw_record)
    record = copy.deepcopy(doc)
    corrections: List[Dict[str, Any]] = []

    def log_corr(field: str, orig: Any, corrected: Any, rule_code: str):
        corrections.append({
            "field": field,
            "original_value": orig,
            "corrected_value": corrected,
            "rule_code": rule_code,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

    id_order_val = record.get("id_order") or record.get("order_id")
    if id_order_val:
        id_order_str = str(id_order_val).strip()
        record["id_order"] = id_order_str

    if "status" in record and isinstance(record["status"], str):
        orig_status = record["status"]
        trimmed_status = orig_status.strip()
        norm_status = STATUS_SYNONYMS.get(trimmed_status, trimmed_status)
        if norm_status != orig_status:
            record["status"] = norm_status
            log_corr("status", orig_status, norm_status, "STATUS_SYNONYM_NORM")

    if "order_date" in record and isinstance(record["order_date"], str):
        orig_date = record["order_date"]
        norm_date = clean_date(orig_date)
        if norm_date != orig_date:
            record["order_date"] = norm_date
            log_corr("order_date", orig_date, norm_date, "DATE_ISO_NORM")

    # Synthesize customer structure if missing as dict but present as flat CSV columns
    customer = record.get("customer")
    if not isinstance(customer, dict):
        c_id = record.get("customer_id") or record.get("id_customer")
        c_name = record.get("customer_name") or record.get("name")
        c_phone = record.get("customer_phone") or record.get("phone")
        c_email = record.get("customer_email") or record.get("email")
        c_city = record.get("shipping_city") or record.get("city")
        c_district = record.get("district") or ""
        if c_id or c_name or c_phone or c_email or c_city:
            customer = {
                "customer_id": str(c_id).strip() if c_id else "",
                "name": str(c_name).strip() if c_name else "",
                "phone": clean_phone(c_phone) if c_phone else "",
                "email": clean_email(c_email) if c_email else "",
                "address": {
                    "city": str(c_city).strip() if c_city else "",
                    "district": str(c_district).strip() if c_district else ""
                }
            }
            record["customer"] = customer
            log_corr("customer", None, customer, "SYNTHESIZE_FLAT_CUSTOMER")
    else:
        if not customer.get("phone"):
            p_val = customer.get("customer_phone") or record.get("customer_phone") or record.get("phone")
            if p_val:
                customer["phone"] = clean_phone(p_val)
        if not customer.get("email"):
            e_val = customer.get("customer_email") or record.get("customer_email") or record.get("email")
            if e_val:
                customer["email"] = clean_email(e_val)

        if "phone" in customer and customer["phone"] is not None:
            orig_phone = str(customer["phone"])
            norm_phone = clean_phone(orig_phone)
            if norm_phone != orig_phone:
                customer["phone"] = norm_phone
                log_corr("customer.phone", orig_phone, norm_phone, "PHONE_NORM")
        if "email" in customer and customer["email"] is not None:
            orig_email = str(customer["email"])
            norm_email = clean_email(orig_email)
            if norm_email != orig_email:
                customer["email"] = norm_email
                log_corr("customer.email", orig_email, norm_email, "EMAIL_REPAIR")

    # Synthesize payment structure if missing as dict but present as flat CSV columns
    payment = record.get("payment")
    if not isinstance(payment, dict):
        pm_method = record.get("payment_method") or record.get("method")
        pm_status = record.get("payment_status") or "تم الدفع"
        pm_curr = record.get("currency")
        pm_amt = record.get("payment_amount") or record.get("total_amount") or record.get("price")
        if pm_method or pm_amt is not None:
            parsed_amt = clean_numeric_string(pm_amt) if pm_amt is not None else 0.0
            norm_curr = ""
            if pm_curr:
                curr_str = str(pm_curr).strip()
                _, detected_curr = clean_currency(curr_str)
                if detected_curr:
                    norm_curr = detected_curr
                elif curr_str.lower() in [s.lower() for s in YER_SYNONYMS] or curr_str.upper() == "YER":
                    norm_curr = "YER"
                else:
                    norm_curr = curr_str  # preserve foreign/unrecognized currency without forcing YER
            else:
                if isinstance(pm_amt, str):
                    _, detected_amt_curr = clean_currency(pm_amt)
                    if detected_amt_curr:
                        norm_curr = detected_amt_curr
                if not norm_curr:
                    c_city = str(record.get("shipping_city") or record.get("city") or "")
                    c_phone = str(record.get("customer_phone") or record.get("phone") or "")
                    if any(city in c_city for city in ["صنعاء", "عدن", "تعز", "الحديدة", "إب", "المكلا", "ذمار"]) or c_phone.startswith(("967", "+967", "00967", "77", "73", "70", "71")):
                        norm_curr = "YER"

            payment = {
                "method": STATUS_SYNONYMS.get(str(pm_method).strip(), str(pm_method).strip()) if pm_method else "بطاقة",
                "status": pm_status,
                "currency": norm_curr,
                "amount": float(parsed_amt) if isinstance(parsed_amt, (int, float)) else 0.0
            }
            record["payment"] = payment
            log_corr("payment", None, payment, "SYNTHESIZE_FLAT_PAYMENT")
    else:
        if "amount" not in payment and "payment_amount" in payment:
            payment["amount"] = payment.pop("payment_amount")
        elif "amount" not in payment and record.get("payment_amount") is not None:
            payment["amount"] = record.get("payment_amount")

    # Handle items_json if items is not present
    items = record.get("items")
    if items is None and "items_json" in record:
        items = record.get("items_json")
        record["items"] = items

    # Parse items if it is a stringified JSON array
    if isinstance(items, str):
        items_str = items.strip()
        if items_str:
            try:
                import json
                parsed_items = json.loads(items_str)
                if isinstance(parsed_items, list):
                    record["items"] = parsed_items
                    log_corr("items", items, parsed_items, "PARSE_JSON_ITEMS")
                    items = parsed_items
                else:
                    record["items"] = parsed_items
                    items = parsed_items
            except Exception:
                # Corrupted JSON string: retain string in record["items"] so downstream validation captures JSON_ITEMS_CORRUPTED without crashing
                record["items"] = items

    items = record.get("items")
    if isinstance(items, list):
        calculated_items_total = 0.0
        all_items_calculable = True

        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                continue

            # Support price as alias for unit_price
            if "unit_price" not in item and "price" in item:
                item["unit_price"] = item["price"]

            if "qty" in item:
                orig_qty = item["qty"]
                parsed_qty = clean_numeric_string(orig_qty)
                if isinstance(parsed_qty, (int, float)) and parsed_qty != orig_qty:
                    item["qty"] = int(parsed_qty)
                    log_corr(f"items[{idx}].qty", orig_qty, int(parsed_qty), "NUMERIC_ARABIC_WORDS_QTY")

            if "unit_price" in item:
                orig_price = item["unit_price"]
                parsed_price = clean_numeric_string(orig_price)
                if isinstance(parsed_price, (int, float)) and parsed_price != orig_price:
                    item["unit_price"] = float(parsed_price)
                    log_corr(f"items[{idx}].unit_price", orig_price, float(parsed_price), "NUMERIC_CURRENCY_SEP_PRICE")

            if "total" in item:
                orig_total = item["total"]
                parsed_total = clean_numeric_string(orig_total)
                if isinstance(parsed_total, (int, float)) and parsed_total != orig_total:
                    item["total"] = float(parsed_total)
                    log_corr(f"items[{idx}].total", orig_total, float(parsed_total), "NUMERIC_SEP_TOTAL")

            curr_qty = item.get("qty")
            curr_price = item.get("unit_price")

            if isinstance(curr_qty, (int, float)) and isinstance(curr_price, (int, float)) and curr_qty > 0 and curr_price >= 0:
                expected_item_total = float(curr_qty * curr_price)
                current_item_total = item.get("total")
                if current_item_total is None or abs(float(current_item_total) - expected_item_total) > 1e-4:
                    item["total"] = expected_item_total
                    log_corr(f"items[{idx}].total", current_item_total, expected_item_total, "ITEM_TOTAL_RECALCULATE")
                calculated_items_total += expected_item_total
            else:
                all_items_calculable = False

        delivery_cost = 0.0
        delivery = record.get("delivery")
        if isinstance(delivery, dict) and "cost" in delivery:
            parsed_cost = clean_numeric_string(delivery["cost"])
            if isinstance(parsed_cost, (int, float)):
                delivery_cost = float(parsed_cost)
                delivery["cost"] = delivery_cost
        elif "delivery_cost" in record and record["delivery_cost"] is not None:
            parsed_cost = clean_numeric_string(record["delivery_cost"])
            if isinstance(parsed_cost, (int, float)):
                delivery_cost = float(parsed_cost)
                record["delivery_cost"] = delivery_cost

        if all_items_calculable and len(items) > 0:
            expected_order_total = calculated_items_total + delivery_cost
            orig_order_total = record.get("total_amount")
            parsed_order_total = clean_numeric_string(orig_order_total) if orig_order_total is not None else None
            
            if parsed_order_total is None or not isinstance(parsed_order_total, (int, float)) or abs(float(parsed_order_total) - expected_order_total) > 1e-4:
                record["total_amount"] = expected_order_total
                log_corr("total_amount", orig_order_total, expected_order_total, "ORDER_TOTAL_RECALCULATE")
            else:
                record["total_amount"] = float(parsed_order_total)

    # Normalize top-level numeric fields if present as strings (e.g. "1000", "1,000", "1 000")
    if "total_amount" in record and record["total_amount"] is not None:
        parsed_tot = clean_numeric_string(record["total_amount"])
        if isinstance(parsed_tot, (int, float)):
            record["total_amount"] = float(parsed_tot)

    if "delivery_cost" in record and record["delivery_cost"] is not None:
        parsed_deliv = clean_numeric_string(record["delivery_cost"])
        if isinstance(parsed_deliv, (int, float)):
            record["delivery_cost"] = float(parsed_deliv)

    if "payment_amount" in record and record["payment_amount"] is not None:
        parsed_pm_amt = clean_numeric_string(record["payment_amount"])
        if isinstance(parsed_pm_amt, (int, float)):
            record["payment_amount"] = float(parsed_pm_amt)

    payment = record.get("payment")
    if isinstance(payment, dict):
        if "currency" in payment and isinstance(payment["currency"], str):
            orig_curr = payment["currency"]
            _, norm_curr = clean_currency(orig_curr)
            if norm_curr is not None and norm_curr != orig_curr:
                payment["currency"] = norm_curr
                log_corr("payment.currency", orig_curr, norm_curr, "CURRENCY_NORM")
        if "amount" in payment and payment["amount"] is not None:
            parsed_amt = clean_numeric_string(payment["amount"])
            if isinstance(parsed_amt, (int, float)):
                payment["amount"] = float(parsed_amt)
        if isinstance(record.get("total_amount"), (int, float)):
            if payment.get("amount") is None or abs(float(payment.get("amount", 0)) - record["total_amount"]) > 1e-4:
                orig_amt = payment.get("amount")
                payment["amount"] = record["total_amount"]
                log_corr("payment.amount", orig_amt, record["total_amount"], "PAYMENT_AMOUNT_MATCH")

    if corrections:
        record["quality_status"] = "corrected"
        record["corrections"] = corrections
    else:
        record["quality_status"] = "valid"
        record["corrections"] = []

    return record
