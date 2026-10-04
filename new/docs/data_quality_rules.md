# Data Quality Rules & Quarantine Documentation

## Automatic Cleaning & Correction Rules

Data transformations are applied **only** when deterministic and safe. The pipeline implements **14 automatic cleaning and normalization rules**:

| # | Rule Code (`rule_code`) | Target Field (`field`) | Description | Example Input | Normalized Output |
| :---: | :--- | :--- | :--- | :--- | :--- |
| **1** | `STATUS_SYNONYM_NORM` | `status` | Standardizes status synonyms and localized terms | `"مدفوع"`, `"pending"`, `"delivered"` | `"مؤكد"`, `"قيد الانتظار"`, `"تم التسليم"` |
| **2** | `DATE_ISO_NORM` | `order_date` | Normalizes dirty date formats to ISO-8601 | `"2025 /01 /31"` | `"2025-01-31"` |
| **3** | `PHONE_NORM` | `customer.phone` | Normalizes phone number format and prepends country code | `"+967 77-123-4567"` | `"967771234567"` |
| **4** | `EMAIL_REPAIR` | `customer.email` | Fixes duplicated `@` or consecutive `.` symbols | `"user@@company..org"` | `"user@company.org"` |
| **5** | `NUMERIC_ARABIC_WORDS_QTY` | `items[].qty` | Converts Arabic numerals and textual words to integers | `"٥"`, `"ألف"` | `5`, `1000` |
| **6** | `NUMERIC_CURRENCY_SEP_PRICE` | `items[].unit_price` | Cleans currency text and comma separators from unit prices | `"2,500 لاير"`, `"ألفان"` | `2500.0`, `2000.0` |
| **7** | `NUMERIC_SEP_TOTAL` | `items[].total` | Cleans thousands separators from item totals | `"12,500.00"` | `12500.0` |
| **8** | `ITEM_TOTAL_RECALCULATE` | `items[].total` | Mathematically validates and recalculates item total ($qty \times unit\_price$) | `total = 0`, expected `12500.0` | `total = 12500.0` |
| **9** | `ORDER_TOTAL_RECALCULATE` | `total_amount` | Recalculates total order amount ($\sum items + delivery\_cost$) | `"٠"` | `17500.0` |
| **10** | `CURRENCY_NORM` | `payment.currency` | Standardizes currency code | `"ريال يمني"` | `"YER"` |
| **11** | `PAYMENT_AMOUNT_MATCH` | `payment.amount` | Synchronizes payment amount with final cleaned order total | `0.0` | `17500.0` |
| **12** | `PARSE_JSON_ITEMS` | `items` | Parses stringified JSON arrays into structured lists of item objects | `"[{\"sku\":\"A\"}]"` | `[{"sku": "A"}]` |
| **13** | `SYNTHESIZE_FLAT_CUSTOMER` | `customer` | Reconstructs nested customer object from flat CSV headers | `customer_name="Ali"` | `{"name": "Ali", ...}` |
| **14** | `SYNTHESIZE_FLAT_PAYMENT` | `payment` | Reconstructs nested payment object from flat CSV headers | `payment_method="بطاقة"` | `{"method": "بطاقة", ...}` |

---

## Audit Trail Format

Every corrected record preserves a full, immutable audit log inside `corrections`:

```json
{
  "quality_status": "corrected",
  "corrections": [
    {
      "field": "customer.phone",
      "original_value": "+967 77-123-4567",
      "corrected_value": "967771234567",
      "rule_code": "PHONE_NORM",
      "timestamp": "2026-09-02T22:45:06.123Z"
    },
    {
      "field": "status",
      "original_value": "مدفوع",
      "corrected_value": "مؤكد",
      "rule_code": "STATUS_SYNONYM_NORM",
      "timestamp": "2026-09-02T22:45:06.124Z"
    }
  ]
}
```

- When data is clean originally: `quality_status = "valid"` and `corrections = []`.
- When any correction occurs: `quality_status = "corrected"` and `corrections` contains the audit entries.

---

## Quarantine Error Codes

Records that fail business validation or contain fatal corruptions are routed to `quarantine_orders` with the following standardized error codes:

| Error Code (`codes_error`) | Description |
| :--- | :--- |
| **`ID_ORDER_MISSING`** | Missing or empty business order key (`id_order`). |
| **`ID_CUSTOMER_MISSING`** | Missing customer structure or `customer_id` / `name`. |
| **`DATE_IMPOSSIBLE_INVALID`** | Unparseable, corrupted, or impossible date. |
| **`JSON_ITEMS_CORRUPTED`** | Malformed or corrupted JSON items payload. |
| **`ITEMS_EMPTY`** | Order contains zero items (`items: []`). |
| **`PRICE_UNKNOWN`** | Missing unit price that cannot be inferred. |
| **`VALUE_NEGATIVE_AMBIGUOUS`** | Negative quantities or negative financial values. |
| **`ID_ORDER_DUPLICATE`** | Duplicate business key within the same ingestion run. |
| **`ERRORS_CONFLICTING_MULTIPLE`** | Multiple conflicting validation errors preventing safe resolution. |

Every quarantined document preserves the original `record_raw`, `codes_error`, `details_error`, `id_run`, and `source_row_number`.
