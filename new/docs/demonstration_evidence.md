# Professor Evaluation & Live Demonstration Evidence Guide

This document provides a step-by-step checklist and guide for presenting the **University Hybrid Data Engineering Pipeline** to the evaluating professor.

---

## 📋 Evaluation Checklist & Evidence Steps

### 1. PySpark Distributed Processing Demonstration
- **Location:** Streamlit UI → `⚡ Spark Monitor` page / `📂 Upload & Run` (select large dataset or PySpark route).
- **Key Evidence to Show:**
  - `SparkSession` initialized with explicit `StructType` fixed schema (`StringType` fields).
  - Processing distributed across Spark RDD/DataFrame partitions (`partitions > 1`).
  - Worker-level partition writing directly to MongoDB `orders_raw` without calling `toLocalIterator()` or pulling records to Python Driver.
  - Zero silent fallback to Python batch on PySpark errors.

### 2. ELT Architecture — Raw First Ingestion
- **Location:** Streamlit UI → `📦 Raw Data` page / MongoDB Compass (`university_pipeline.orders_raw`).
- **Key Evidence to Show:**
  - Raw collection contains unmodified raw records (`record_raw`) alongside tracking metadata:
    - `id_run` (Unique pipeline execution ID)
    - `file_source` (Input dataset filename)
    - `number_row_source` (1-indexed source row number)
    - `at_ingested` (UTC timestamp)
    - `engine_used` (`python_batch` or `pyspark`)

### 3. Data Quality & Audit Trail
- **Location:** Streamlit UI → `✨ Data Quality` page / MongoDB Compass (`university_pipeline.orders_validated`).
- **Key Evidence to Show:**
  - 8+ automated data quality cleaning rules applied (Arabic numerals, email syntax, phone, currency, date formatting, thousand separators).
  - Every transformed record contains a full `corrections` audit trail array:
    ```json
    {
      "field": "phone",
      "original_value": "0771234567",
      "corrected_value": "967771234567",
      "rule_code": "PHONE_NORMALIZED"
    }
    ```

### 4. Quarantine Store Isolation
- **Location:** Streamlit UI → `🛡️ Quarantine` page / MongoDB Compass (`university_pipeline.quarantine_orders`).
- **Key Evidence to Show:**
  - Uncorrectable corrupt records (missing order ID, negative amount, corrupt JSON) isolated in `quarantine_orders`.
  - Record contains explicit `codes_error` (e.g., `ID_ORDER_MISSING`, `PRICE_UNKNOWN`) and original `record_raw`. Zero data loss.

### 5. True Idempotency Verification
- **Location:** Streamlit UI → `🛡️ Idempotency` page.
- **Key Evidence to Show:**
  - **Run 1:** `Inserted = N, Updated = 0, Unchanged = 0`
  - **Run 2 (Identical Dataset Rerun):** `Inserted = 0, Updated = 0, Unchanged = N` (Zero duplicates, zero false updates).
  - **Run 3 (Update Order Status):** `Inserted = 0, Updated = 1, Unchanged = N-1` (In-place update on `id_order` unique index).

### 6. Run Consistency Equation & Metrics
- **Location:** Streamlit UI → `📈 Dashboard` / `reports/results.json`.
- **Key Evidence to Show:**
  - Mathematical verification: `loaded_raw == count_valid + count_corrected + count_quarantine`.
  - Recorded throughput (records/sec), execution seconds, and partition details persisted in `reports/results.json`.

### 7. Requirements Compliance Dashboard
- **Location:** Streamlit UI → `📋 Requirements` page.
- **Key Evidence to Show:**
  - Live compliance statistics and matrix detailing status (`PASS`, `PARTIAL`, `FAIL`, `NOT REQUIRED — Individual Student`) for all professor requirements.
