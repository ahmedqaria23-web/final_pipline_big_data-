from datetime import datetime, timezone
from typing import Dict, Any, Tuple

from src.quality.quality_rules import apply_quality_rules
from src.quality.validator import validate_order


def classify_record(raw_doc: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """
    Processes a raw record document from orders_raw.
    Returns:
        outcome: 'VALID' | 'CORRECTED' | 'QUARANTINED'
        payload: Processed document ready for orders_validated or quarantine_orders.
    """
    id_run = raw_doc.get("id_run", "unknown_run")
    source_row = raw_doc.get("number_row_source", 0)
    raw_payload = raw_doc.get("record_raw", raw_doc)

    file_source = raw_doc.get("file_source", "")

    try:
        # 1. Apply Automatic Quality Cleaning Rules
        cleaned_record = apply_quality_rules(raw_doc)

        # 2. Run Programmatic Business Validation
        validation_errors = validate_order(cleaned_record)
    except Exception as exc:
        id_order_fallback = (
            raw_doc.get("id_order")
            or raw_payload.get("id_order")
            or raw_payload.get("order_id")
        )
        quarantine_payload = {
            "_id": f"{id_run}:{source_row}",
            "id_run": id_run,
            "file_source": file_source,
            "id_order": id_order_fallback,
            "source_row_number": source_row,
            "number_row_source": source_row,
            "quarantined_at": datetime.now(timezone.utc).isoformat(),
            "codes_error": ["JSON_ITEMS_CORRUPTED", "ERRORS_CONFLICTING_MULTIPLE"],
            "details_error": [f"Unrecoverable classification exception: {str(exc)}"],
            "record_raw": raw_payload,
            "cleaned_attempt": {}
        }
        return "QUARANTINED", quarantine_payload

    if validation_errors:
        error_codes = list(set([err[0] for err in validation_errors]))
        error_details = [err[1] for err in validation_errors]

        id_order_val = (
            cleaned_record.get("id_order")
            or cleaned_record.get("order_id")
            or raw_payload.get("id_order")
            or raw_payload.get("order_id")
        )

        quarantine_payload = {
            "_id": f"{id_run}:{source_row}",
            "id_run": id_run,
            "file_source": file_source,
            "id_order": id_order_val,
            "source_row_number": source_row,
            "number_row_source": source_row,
            "quarantined_at": datetime.now(timezone.utc).isoformat(),
            "codes_error": error_codes,
            "details_error": error_details,
            "record_raw": raw_payload,
            "cleaned_attempt": cleaned_record
        }
        return "QUARANTINED", quarantine_payload

    # If clean, check if modifications were made
    corrections = cleaned_record.get("corrections", [])
    if corrections:
        cleaned_record["quality_status"] = "corrected"
        cleaned_record["processed_at"] = datetime.now(timezone.utc).isoformat()
        return "CORRECTED", cleaned_record
    else:
        cleaned_record["quality_status"] = "valid"
        cleaned_record["processed_at"] = datetime.now(timezone.utc).isoformat()
        return "VALID", cleaned_record
