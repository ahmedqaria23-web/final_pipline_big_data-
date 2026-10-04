import re
from datetime import datetime
from typing import Dict, Any, List, Tuple

EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PHONE_PATTERN = re.compile(r"^(967)?(77|73|70|71)\d{7}$")

ALLOWED_STATUSES = {
    "قيد الانتظار", "مؤكد", "قيد الشحن",
    "تم التسليم", "مرتجع", "ملغي"
}

ALLOWED_PAYMENT_METHODS = {
    "نقدًا عند التسليم", "بطاقة", "محفظة إلكترونية"
}

ALLOWED_PAYMENT_STATUSES = {
    "بانتظار الدفع", "تم الدفع", "مرفوض"
}

ALLOWED_CURRENCY = {"YER"}


def is_iso_datetime(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    val_clean = value.strip()
    if val_clean.endswith("Z") or val_clean.endswith("z"):
        val_clean = val_clean[:-1] + "+00:00"
    try:  
        datetime.fromisoformat(val_clean)
        return True
    except (ValueError, TypeError):
        try:
            from datetime import date
            date.fromisoformat(val_clean)
            return True
        except (ValueError, TypeError):
            return False


def validate_order(record: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Validates an order document against programmatic business rules.
    Returns a list of tuples: (error_code, error_detail)
    """
    errors: List[Tuple[str, str]] = []

    if not isinstance(record, dict):
        return [("JSON_ITEMS_CORRUPTED", "Record is not a valid JSON dictionary")]

    # 1. Top-Level Business Key check
    id_order = record.get("id_order") or record.get("order_id")
    if not id_order or not isinstance(id_order, str) or not id_order.strip():
        errors.append(("ID_ORDER_MISSING", "id_order is missing or empty"))

    # 2. Date check
    order_date = record.get("order_date")
    if not order_date or not is_iso_datetime(order_date):
        errors.append(("DATE_IMPOSSIBLE_INVALID", f"order_date is missing or invalid: {order_date}"))

    # 3. Status check
    status = record.get("status")
    if status not in ALLOWED_STATUSES:
        errors.append(("ERRORS_CONFLICTING_MULTIPLE", f"status value not allowed: {status}"))

    # 4. Customer checks
    customer = record.get("customer")
    if not isinstance(customer, dict):
        errors.append(("ID_CUSTOMER_MISSING", "customer structure is missing or not an object"))
    else:
        if not customer.get("customer_id") or not str(customer.get("customer_id")).strip():
            errors.append(("ID_CUSTOMER_MISSING", "customer.customer_id is missing"))
        
        if not customer.get("name") or not str(customer.get("name")).strip():
            errors.append(("ID_CUSTOMER_MISSING", "customer.name is missing or empty"))

        email = customer.get("email")
        if not email or not EMAIL_PATTERN.match(str(email)):
            errors.append(("ERRORS_CONFLICTING_MULTIPLE", f"customer.email pattern invalid: {email}"))
            
        phone = customer.get("phone")
        if not phone or not PHONE_PATTERN.match(str(phone)):
            errors.append(("ERRORS_CONFLICTING_MULTIPLE", f"customer.phone pattern invalid: {phone}"))

        address = customer.get("address")
        if not isinstance(address, dict) or not address.get("city") or not address.get("district"):
            errors.append(("ERRORS_CONFLICTING_MULTIPLE", "customer.address missing required city or district"))

    # 5. Items checks
    items = record.get("items")
    if items is None and "items_json" in record:
        items = record.get("items_json")
    if not isinstance(items, list):
        errors.append(("JSON_ITEMS_CORRUPTED", "items is not a valid array"))
    elif len(items) == 0:
        errors.append(("ITEMS_EMPTY", "items array is empty"))
    else:
        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                errors.append(("JSON_ITEMS_CORRUPTED", f"items[{idx}] is not an object"))
                continue

            if not item.get("sku") or not str(item.get("sku")).strip():
                errors.append(("JSON_ITEMS_CORRUPTED", f"items[{idx}].sku is missing"))
            if not item.get("name") or not str(item.get("name")).strip():
                errors.append(("JSON_ITEMS_CORRUPTED", f"items[{idx}].name is missing"))

            qty = item.get("qty")
            if qty is None or not isinstance(qty, int) or qty <= 0:
                errors.append(("VALUE_NEGATIVE_AMBIGUOUS", f"items[{idx}].qty must be a positive integer, got: {qty}"))

            price = item.get("unit_price")
            if price is None:
                errors.append(("PRICE_UNKNOWN", f"items[{idx}].unit_price is missing"))
            elif not isinstance(price, (int, float)) or price < 0:
                errors.append(("VALUE_NEGATIVE_AMBIGUOUS", f"items[{idx}].unit_price must be non-negative, got: {price}"))

            total = item.get("total")
            if total is None or not isinstance(total, (int, float)) or total < 0:
                errors.append(("VALUE_NEGATIVE_AMBIGUOUS", f"items[{idx}].total must be non-negative, got: {total}"))

    # 6. Delivery checks (optional object, but type must be valid if specified)
    delivery = record.get("delivery")
    if delivery is not None and isinstance(delivery, dict):
        if "type" in delivery and delivery["type"] not in {"عادي", "سريع"}:
            errors.append(("ERRORS_CONFLICTING_MULTIPLE", f"delivery.type invalid: {delivery.get('type')}"))

    # 7. Payment checks
    payment = record.get("payment")
    if not isinstance(payment, dict):
        errors.append(("ERRORS_CONFLICTING_MULTIPLE", "payment structure missing or invalid"))
    else:
        if payment.get("method") not in ALLOWED_PAYMENT_METHODS:
            errors.append(("ERRORS_CONFLICTING_MULTIPLE", f"payment.method invalid: {payment.get('method')}"))
        if payment.get("status") not in ALLOWED_PAYMENT_STATUSES:
            errors.append(("ERRORS_CONFLICTING_MULTIPLE", f"payment.status invalid: {payment.get('status')}"))
        if payment.get("currency") not in ALLOWED_CURRENCY:
            errors.append(("ERRORS_CONFLICTING_MULTIPLE", f"payment.currency invalid: {payment.get('currency')}"))
        amount = payment.get("amount")
        if amount is None or not isinstance(amount, (int, float)) or amount < 0:
            errors.append(("VALUE_NEGATIVE_AMBIGUOUS", f"payment.amount invalid: {amount}"))

    # 8. Total Amount check
    total_amount = record.get("total_amount")
    if total_amount is None or not isinstance(total_amount, (int, float)) or total_amount < 0:
        errors.append(("VALUE_NEGATIVE_AMBIGUOUS", f"total_amount invalid: {total_amount}"))

    # 9. Duplicate order check
    if record.get("is_duplicate"):
        errors.append(("ID_ORDER_DUPLICATE", f"Duplicate order ID detected: {id_order}"))

    # 10. Flag multiple conflicting errors
    if len(errors) > 1 and not any(code == "ERRORS_CONFLICTING_MULTIPLE" for code, _ in errors):
        errors.append(("ERRORS_CONFLICTING_MULTIPLE", f"Multiple conflicting errors encountered ({len(errors)} errors)"))

    return errors
