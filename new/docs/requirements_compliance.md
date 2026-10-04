# Requirements Compliance Matrix

**Project**: Big Data Midterm Pipeline  
**PDF Reference**: `docs/midterm data pipeline project.pdf`  
**Last Updated**: 2026-09-02  
**Verified By**: Automated code inspection + live pipeline execution + pytest test suite

---

## Summary

| Status | Count |
| :--- | :---: |
| ✅ **PASS** | **45** |
| ⚠️ **PARTIAL** | **1** |
| ❌ **FAIL** | **0** |

---

## Full Requirements Matrix

| ID | Requirement | PDF Section | Implementation | File(s) | Status | Evidence |
| :--- | :--- | :--- | :--- | :--- | :---: | :--- |
| **REQ-01** | Create small reproducible sample | §6.1 | `generate_sample()` streaming via `csv.reader`/line iterator, no Pandas | `create_small_sample.py` | ✅ PASS | `python create_small_sample.py --input X --rows 5000` works; no full-file RAM load |
| **REQ-02** | Sample is streaming (no Pandas, no full RAM load) | §6.1 | Line-by-line read with early-exit break after N rows | `create_small_sample.py` | ✅ PASS | No `import pandas` in file; uses `csv.reader` + `open()` |
| **REQ-03** | Sample row count is configurable | §6.1 | `--rows` argument via `argparse` | `create_small_sample.py` | ✅ PASS | Configurable row count supported |
| **REQ-04** | Original source file not modified | §6.1 | Reads with `open(..., 'r')`, writes to separate output path | `create_small_sample.py` | ✅ PASS | Source file opened strictly read-only |
| **REQ-05** | File Router exists as single entry | §6.2 | `route_file()` with file path, size, threshold, engine | `src/routing/file_router.py` | ✅ PASS | Returns routing decision |
| **REQ-06** | Router threshold is configurable | §6.2 | `FILE_SIZE_THRESHOLD_MB` in `settings.py` + `.env` | `config/settings.py` | ✅ PASS | Default 50.0 MB from settings |
| **REQ-07** | Router prints routing decision | §6.2 | `logger.info(...)` with File, Size, Threshold, Engine | `src/routing/file_router.py` | ✅ PASS | Verified in pipeline logs |
| **REQ-08** | Small file → Python Batch | §6.2 | Router selects `python_batch` when `size <= threshold` | `src/routing/file_router.py` | ✅ PASS | `test_file_router_small_file` PASSED |
| **REQ-09** | Large file → PySpark | §6.2 | Router selects `pyspark` when `size > threshold` | `src/routing/file_router.py` | ✅ PASS | `test_file_router_large_file_threshold` PASSED |
| **REQ-10** | Python Batch uses `csv.DictReader` | §6.3 | `stream_records()` uses `csv.DictReader(f)` for CSV | `src/ingestion/batch_loader.py` | ✅ PASS | No `pandas.read_csv()` |
| **REQ-11** | Python Batch is streaming (no full RAM) | §6.3 | Generator `stream_records()` with `yield`; batch collected up to `batch_size` | `src/ingestion/batch_loader.py` | ✅ PASS | Line-by-line streaming |
| **REQ-12** | Batch Size is configurable | §6.3 | `BATCH_SIZE_DEFAULT` from `settings.py` + CLI argument | `config/settings.py` | ✅ PASS | Default 5000; CLI configurable |
| **REQ-13** | Batch logging (number, records, time, throughput, errors) | §6.3 | `logger.info` per batch with checkpoint info | `src/ingestion/batch_loader.py` | ✅ PASS | Printed and logged per batch |
| **REQ-14** | Bulk write for Python Batch | §6.3 | `insert_raw_batch()` uses `ReplaceOne` operations via `bulk_write` | `src/mongodb/repositories.py` | ✅ PASS | `pymongo.ReplaceOne` with `bulk_write` |
| **REQ-15** | PySpark uses `SparkSession` + DataFrame API | §6.4 | `SparkSession.builder...getOrCreate()` + DataFrame operations | `src/ingestion/spark_loader.py` | ✅ PASS | No `collect()` for data |
| **REQ-16** | PySpark uses Fixed `StructType` Schema | §6.4 | `get_csv_fixed_schema()` returns explicit `StructType([StructField(...)])` | `src/ingestion/spark_loader.py` | ✅ PASS | Predefined `StructType`, no `inferSchema` |
| **REQ-17** | Sensitive fields read as StringType | §6.4 | `price`, `qty`, `phone`, `email`, `date`, `items`, `status`, `currency`, `total_amount` read as `StringType()` | `src/ingestion/spark_loader.py` | ✅ PASS | Preserves raw values |
| **REQ-18** | Spark parallelism: no `collect()` for data | §6.5 | Data written via Mongo Spark Connector `.write.format("mongodb")...save()` | `src/ingestion/spark_loader.py` | ✅ PASS | Distributed write to MongoDB |
| **REQ-19** | No `toLocalIterator()` for processing | §6.5 | Code search confirms no `toLocalIterator()` usage | `src/ingestion/spark_loader.py` | ✅ PASS | No iterator collect |
| **REQ-20** | ELT: Load to Raw FIRST, transform LATER | §7.1 | `batch_loader.py` and `spark_loader.py` write unchanged raw data; cleaning happens in `elt_pipeline.py` | `src/pipeline/elt_pipeline.py` | ✅ PASS | Raw documents contain `record_raw` |
| **REQ-21** | `orders_raw` schema: id_run, file_source, number_row_source, at_ingested, engine_used, record_raw | §7.2 | All required fields written in `raw_doc` dict | `src/ingestion/batch_loader.py` | ✅ PASS | Verified in MongoDB `orders_raw` |
| **REQ-22** | Raw `_id` = id_run + number_row_source (historical trace) | §7.3 | `_id = f"{id_run}:{number_row_source}"` | `src/mongodb/repositories.py` | ✅ PASS | Multiple runs preserve historical raw traces |
| **REQ-23** | 14 Cleaning Rules implemented | §8.1 | 14 rules: Arabic digits, currency, thousands sep., number words, phone, email, date, status, order total recalc, item total recalc, payment match, json items parse, customer/payment synthesis | `src/quality/quality_rules.py` | ✅ PASS | Verified in `test_cleaning_rules.py` |
| **REQ-24** | No guessing in cleaning (unsafe → Quarantine) | §8.2 | Validation logic quarantines ambiguous/conflicting values | `src/quality/validator.py` | ✅ PASS | `VALUE_NEGATIVE_AMBIGUOUS`, `ERRORS_CONFLICTING_MULTIPLE` |
| **REQ-25** | Audit Trail on corrected records | §9.1 | `corrections` list with `field`, `original_value`, `corrected_value`, `rule_code` | `src/quality/quality_rules.py` | ✅ PASS | Verified in `test_classifier.py` |
| **REQ-26** | `quality_status` field on all records | §9.1 | Classifier sets `quality_status = "valid" / "corrected" / "quarantined"` | `src/quality/classifier.py` | ✅ PASS | Tested in `test_classifier.py` |
| **REQ-27** | Classification: VALID / CORRECTED / QUARANTINED | §10.1 | `classify_record()` returns one of 3 outcomes | `src/quality/classifier.py` | ✅ PASS | 3 test cases pass |
| **REQ-28** | No records disappear without classification | §10.2 | Every raw record is classified into valid, corrected, or quarantine | `src/pipeline/elt_pipeline.py` | ✅ PASS | Consistency equation enforces this |
| **REQ-29** | Quarantine: record_raw, codes_error, details_error, id_run | §11.1 | Quarantine store holds all error fields and original raw record | `src/mongodb/repositories.py` | ✅ PASS | Verified in `quarantine_orders` |
| **REQ-30** | Quarantine error codes match spec | §11.2 | `ID_ORDER_MISSING`, `ID_CUSTOMER_MISSING`, `DATE_IMPOSSIBLE_INVALID`, `JSON_ITEMS_CORRUPTED`, `ITEMS_EMPTY`, `PRICE_UNKNOWN`, `VALUE_NEGATIVE_AMBIGUOUS`, `ID_ORDER_DUPLICATE`, `ERRORS_CONFLICTING_MULTIPLE` | `src/quality/validator.py` | ✅ PASS | All 9 error codes verified |
| **REQ-31** | MongoDB: 4 collections exist | §12.1 | `initialize_database()` creates: `orders_raw`, `orders_validated`, `quarantine_orders`, `meta_state` | `src/mongodb/mongo_setup.py` | ✅ PASS | Initialized with schemas & indexes |
| **REQ-32** | `orders_validated`: $jsonSchema + Unique Index on id_order | §12.2 | `create_collection(VALIDATED, validator={$jsonSchema})` + `create_index(id_order, unique=True)` | `src/mongodb/mongo_setup.py` | ✅ PASS | Strict JSON schema + `ux_id_order` |
| **REQ-33** | Upsert using `ReplaceOne` with id_order | §13.1 | `upsert_validated_batch()` uses `ReplaceOne({"id_order": ...}, record, upsert=True)` | `src/mongodb/repositories.py` | ✅ PASS | Idempotent bulk upsert |
| **REQ-34** | Idempotency: Re-run produces Inserted=0, Updated=0, Unchanged=N | §14.1 | Business state comparison prevents duplicate updates | `src/mongodb/repositories.py` | ✅ PASS | `test_business_state_equal_identical_records` PASSED |
| **REQ-35** | Consistency Equation: raw = valid + corrected + quarantine | §15.1 | `loaded_raw == (count_valid + count_corrected + count_quarantine)` | `src/monitoring/metrics.py` | ✅ PASS | All runs verified |
| **REQ-36** | Metrics: all required fields stored | §16.1 | `calculate_and_verify_metrics()` tracks: id_run, file_name, file_size_mb, used_engine, read_rows, loaded_raw, count_valid, count_corrected, count_quarantine, seconds_elapsed, throughput, partitions, size_batch, counts_case_error, count_inserted, count_updated, count_unchanged | `src/monitoring/metrics.py` | ✅ PASS | Verified in `reports/results.json` |
| **REQ-37** | Results stored in `reports/results.json` | §16.2 | `save_run_metrics()` records telemetry | `src/monitoring/metrics.py` | ✅ PASS | Real run metrics persisted |
| **REQ-38** | Real performance data only (no fake metrics) | §17.1 | All execution metrics calculated from live timer and DB counts | `src/pipeline/elt_pipeline.py` | ✅ PASS | Verified |
| **REQ-39** | Test suite passes: `python -m pytest -q` | §19.1 | 28 tests across 7 test files | `tests/` | ✅ PASS | `28 passed in 29.39s` (100% success) |
| **REQ-40** | Configuration centralized (no hardcoded paths) | §20.1 | All settings in `config/settings.py` reading from `.env` | `config/settings.py` | ✅ PASS | Centralized configuration |
| **REQ-41** | MongoDB URI not in Git | §20.2 | `MONGO_URI` in `.env` excluded by `.gitignore` | `.gitignore`, `.env.example` | ✅ PASS | Excluded from version control |
| **REQ-42** | README allows standalone setup by reviewer | §21.1 | Complete setup and execution guide | `README.md` | ✅ PASS | Fully synchronized |
| **REQ-43** | GitHub readiness: .gitignore, .env.example, LICENSE | §22.1 | All files present and configured | Root directory | ✅ PASS | Verified |
| **REQ-44** | Incremental loading (Path B) | §23.1 | Watermark CDC engine with `initial_load()` and `process_delta_batch()` | `src/incremental/incremental_loader.py` | ✅ PASS | All 4 experiments tested and verified |
| **REQ-45** | Performance comparison: Python vs PySpark | §17.2 | `reports/performance_comparison.md` with benchmark data | `reports/performance_comparison.md` | ⚠️ PARTIAL | Python Batch benchmarked; Spark tested on multi-partition sample |
| **REQ-46** | Streamlit UI does not show fake data | §24.1 | UI reads from live MongoDB + `results.json` | `app.py`, `app/pages/` | ✅ PASS | Live UI data binding |

---

## Verification Commands

```bash
# Run full pytest suite (28 tests)
python -m pytest -q

# Run Path B incremental verification
python -c "from src.incremental.incremental_loader import get_watermark; from src.mongodb.mongo_setup import initialize_database; print('Watermark:', get_watermark(initialize_database()))"
```
