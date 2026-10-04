# Notebook to Project Requirements Mapping (Phase 0)

This document maps all existing Jupyter Notebooks to official project requirements, identifying reusable logic, required refactoring, and target production modules.

| Notebook | Implemented Functionality | Official Requirement Satisfied | Reusable Code / Logic | Required Changes / Refactoring | Target Production Module |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `01_document_modeling (1).ipynb` | MongoDB Connection, Document Modeling, Embed vs Reference, Indexes, CRUD | MongoDB Database Setup, Schema validation, Unique Index on `id_order` | `MongoClient` initialization, document structures, index creation code | Refactor schema collection names to official names (`orders_raw`, `orders_validated`), standardize business key to `id_order` | `src/mongodb/mongo_setup.py`, `src/mongodb/repositories.py` |
| `03_data_quality_validation.ipynb` | Raw load, MongoDB JSON Schema validation, WriteError capture, Business validation, Quarantine | ELT pattern, Raw ingestion before cleaning, Schema validation, Business validation, Quarantine, Consistency equation | `validate_order()` business rules, Schema loading, WriteError handling for Quarantine, consistency assertion | Update schema required fields, support audit trail format, normalize `order_id` to `id_order`, structure error codes | `src/quality/validator.py`, `src/quality/classifier.py`, `src/mongodb/repositories.py` |
| `04_streaming_batch_loading.ipynb` | Streaming file reading line-by-line, batching with `insert_many`, memory monitoring with `psutil`, batch metrics | Python Streaming Batch Loading, Memory Efficiency, Throughput metrics | `stream_jsonl_to_mongodb()`, `memory_mb()`, batch execution & throughput calculations | Support CSV format in addition to JSONL, inject `id_run` & metadata into Raw, remove hardcoded file paths | `src/ingestion/batch_loader.py`, `src/monitoring/metrics.py` |
| `06_reliable_incremental_pipeline.ipynb` | Upsert, Idempotency, `$setOnInsert` vs `$set`, Versioning (Latest Wins), Checkpointing & Watermarks, Incremental Delta Loading | Idempotency & Upsert, Path B Advanced Incremental Processing, Delta processing | `upsert_order()`, `latest_wins()`, `get_watermark()`, `save_watermark()`, checkpoint resume logic | Adapt for full ELT pipeline flow, connect with `orders_validated` unique index on `id_order`, integrate with Streamlit UI | `src/incremental/incremental_loader.py`, `src/mongodb/repositories.py` |
| `clean_quarantine.ipynb` | Quarantine analysis, re-validation of quarantine items, statistics before/after cleaning | Quarantine management & audit visibility | Validation rules, Pandas status & error visualization summaries | Modularize re-validation logic for quarantine items so it can be called from Streamlit Quarantine page | `src/quality/quarantine_manager.py` |

---

## Code Reuse Summary

### REUSED
- **`01_document_modeling (1).ipynb`**: MongoDB connection setup and unique index creation logic -> `src/mongodb/mongo_setup.py`.
- **`03_data_quality_validation.ipynb`**: Schema validation loading, WriteError routing to quarantine, business validation checks -> `src/quality/validator.py`.
- **`04_streaming_batch_loading.ipynb`**: Line-by-line file streaming and batch insertion logic -> `src/ingestion/batch_loader.py`.
- **`06_reliable_incremental_pipeline.ipynb`**: Watermarking (`last_watermark`), version conflict handling (`version`), upsert logic -> `src/incremental/incremental_loader.py` & `src/mongodb/repositories.py`.

### REFACTORED / REWRITTEN
- **Business Key Normalization**: All references to `order_id` mapped strictly to `id_order` to comply with the official specification document.
- **Raw Ingestion Scope**: Ensured Raw records receive full metadata (`id_run`, `file_source`, `source_row_number`, `ingested_at`, `engine_used`, `raw_record`) BEFORE any cleaning or validation is attempted.
- **Quarantine Error Structure**: Refactored simple exception messages into structured quarantine documents containing explicit error codes (`ID_ORDER_MISSING`, `DATE_IMPOSSIBLE_INVALID`, etc.).

### NEW IMPLEMENTATIONS
- `src/routing/file_router.py`: Automatic file size threshold checking and engine selection.
- `src/ingestion/spark_loader.py`: Distributed PySpark loader with explicit schema definition.
- `src/quality/quality_rules.py`: 8+ deterministic data cleaning rules with full Audit Trail output.
- `src/pipeline/elt_pipeline.py` & `src/pipeline/pipeline_controller.py`: Core pipeline execution engine and Streamlit state controller.
- `app/app.py` & `app/pages/*`: 12-page interactive Streamlit GUI dashboard.
- `create_small_sample.py`: Reproducible dataset sampler CLI script.
