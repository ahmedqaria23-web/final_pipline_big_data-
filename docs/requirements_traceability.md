# Requirements Traceability Matrix

This matrix maps every requirement from the official PDF specification (`midterm data pipeline project.pdf`) to its verified technical implementation, Python module, primary functions, and test files.

| Requirement ID | Requirement Description | Implementation Module | Primary Function / Symbol | Test File | Status |
| :--- | :--- | :--- | :--- | :--- | :---: |
| **REQ-01** | **Reproducible Small Sample Script** | `create_small_sample.py` | `generate_sample()` | Direct CLI / Test runs | ✅ PASS |
| **REQ-02** | **File Discovery & `id_run` Generation** | `src/routing/file_router.py` | `route_file()` | `tests/test_router.py` | ✅ PASS |
| **REQ-03** | **Engine Selection Router** (Threshold = 50MB) | `src/routing/file_router.py` | `route_file()`, `execute_ingestion()` | `tests/test_router.py` | ✅ PASS |
| **REQ-04** | **Python Streaming Batch Loader** (No full RAM load) | `src/ingestion/batch_loader.py` | `load_batch_to_raw()`, `stream_records()` | `tests/test_spark_loader_idempotency.py` | ✅ PASS |
| **REQ-05** | **PySpark Ingestion Path** (Explicit schema, parallel write) | `src/ingestion/spark_loader.py` | `load_spark_to_raw()`, `get_csv_fixed_schema()` | `tests/test_spark_loader_idempotency.py` | ✅ PASS |
| **REQ-06** | **ELT Pattern Enforcement** (`orders_raw` BEFORE cleaning) | `src/pipeline/elt_pipeline.py` | `run_elt_pipeline()` | `tests/test_performance_and_direct_upsert.py` | ✅ PASS |
| **REQ-07** | **14 Automatic Cleaning Rules** | `src/quality/quality_rules.py` | `apply_quality_rules()` | `tests/test_cleaning_rules.py` | ✅ PASS |
| **REQ-08** | **Audit Trail Preservation** (`quality_status`, `corrections`) | `src/quality/quality_rules.py` | `apply_quality_rules()` | `tests/test_cleaning_rules.py` | ✅ PASS |
| **REQ-09** | **Business & Schema Validation** | `src/quality/validator.py` | `validate_order()` | `tests/test_validator.py` | ✅ PASS |
| **REQ-10** | **Record Classification** (`VALID`, `CORRECTED`, `QUARANTINED`) | `src/quality/classifier.py` | `classify_record()` | `tests/test_classifier.py` | ✅ PASS |
| **REQ-11** | **Quarantine Management & 9 Error Codes** | `src/quality/validator.py` | `validate_order()`, `insert_quarantine_batch()` | `tests/test_classifier.py` | ✅ PASS |
| **REQ-12** | **Run Consistency Equation** (`raw = valid + corrected + quarantine`) | `src/monitoring/metrics.py` | `calculate_and_verify_metrics()` | `tests/test_performance_and_direct_upsert.py` | ✅ PASS |
| **REQ-13** | **Stable Business Key** (`id_order`) | `src/mongodb/mongo_setup.py` | `initialize_database()` | `tests/test_idempotency.py` | ✅ PASS |
| **REQ-14** | **Unique Index & Idempotent Upsert** (`orders_validated`) | `src/mongodb/repositories.py` | `upsert_validated_batch()`, `is_business_state_equal()` | `tests/test_idempotency.py` | ✅ PASS |
| **REQ-15** | **Path B Incremental Path** (Watermark, Versioning, Delta) | `src/incremental/incremental_loader.py` | `initial_load()`, `process_delta_batch()` | `test_task8_incremental_path_b.py` | ✅ PASS |
| **REQ-16** | **Metrics & Results Persistence** (`reports/results.json`) | `src/monitoring/metrics.py` | `save_run_metrics()` | `reports/results.json` | ✅ PASS |
| **REQ-17** | **Streamlit GUI Integration** (Interactive Dashboard) | `app.py`, `app/pages/` | Multi-page Streamlit Dashboard | Streamlit live dashboard | ✅ PASS |
| **REQ-18** | **Python vs PySpark Performance Comparison** | `reports/performance_comparison.md` | Benchmark Analysis Engine | `reports/performance_comparison.md` | ✅ PASS |
