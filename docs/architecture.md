# System Architecture Documentation

## Overview

The application is a production-grade hybrid Data Engineering pipeline for e-commerce order processing built with Python, PySpark, MongoDB, and Streamlit. It adheres strictly to the **ELT (Extract, Load, Transform)** pattern.

```
                                  Dirty Input File (CSV / JSONL)
                                                │
                                                ▼
                                          File Discovery
                                                │
                                                ▼
                                         Generate id_run
                                                │
                                                ▼
                                           File Router
                                          /           \
               size <= 50MB              /             \           size > 50MB
                                        ▼               ▼
                                 Python Batch        PySpark
                                        \               /
                                         \             /
                                          ▼           ▼
                                     MongoDB orders_raw
                       (_id = id_run:number_row_source — Historical Trace)
                                                │
                                                ▼
                                      Cleaning & Validation
                                 (14 Rules + Audit Trail Logging)
                                                │
                                                ▼
                                         Classification
                                      /        │         \
                                     /         │          \
                                    ▼          ▼           ▼
                                  VALID    CORRECTED   QUARANTINED
                                    \          /           │
                                     \        /            ▼
                                      ▼      ▼     quarantine_orders
                                   orders_validated
                                (Schema Validation +
                              Unique Index on id_order +
                                  Idempotent Upsert)
                                                │
                                                ▼
                                        Metrics & Reports
                                      reports/results.json
                                                │
                                                ▼
                                       Streamlit Dashboard
```

## Architectural Principles

1. **Strict ELT Pattern**: Input data is ingested into `orders_raw` BEFORE any cleaning, filtering, or validation. The Raw layer preserves original values, all source duplicates, and execution metadata (`id_run`, `file_source`, `number_row_source`, `at_ingested`, `engine_used`, `record_raw`).
2. **Zero Data Loss**: Every raw record maps to exactly one of three states (`VALID`, `CORRECTED`, or `QUARANTINED`). The pipeline enforces:
   $$\text{run\_raw\_count} = \text{run\_valid\_count} + \text{run\_corrected\_count} + \text{run\_quarantine\_count}$$
3. **Idempotent Ingestion**: Repeated processing of identical datasets produces zero duplicate growth in `orders_validated` via Unique Index on `id_order` and atomic MongoDB `ReplaceOne(..., upsert=True)` operations.
4. **Path B Incremental Processing**: Supports Delta loading using `updated_at` watermark tracking and `version` conflict resolution ("Latest Wins").
5. **Decoupled Architecture**: The core pipeline engine (`src/pipeline/elt_pipeline.py`) runs independently via CLI (`run_pipeline.py`). Streamlit GUI (`app.py`) provides an interactive interface for monitoring and inspection.

## MongoDB Collections

- **`orders_raw`**: Historical append-only ingestion layer preserving pristine raw input documents. Primary key `_id = f"{id_run}:{number_row_source}"`.
- **`orders_validated`**: Business-ready collection protected by MongoDB JSON Schema validation and a Unique Index on `id_order` (`ux_id_order`). Contains valid records and safely corrected records.
- **`quarantine_orders`**: Isolated store for uncorrectable documents containing error codes (`codes_error`), error details (`details_error`), and original raw payloads (`record_raw`).
- **`meta_state`**: State storage tracking watermarks, run history, and pipeline checkpoints.
