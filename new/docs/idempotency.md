# Idempotency Design & Evidence

**Project**: Big Data Midterm Pipeline  
**Date**: 2026-09-02  

---

## Overview

Idempotency guarantees that executing the pipeline repeatedly with identical data produces the exact same final business state without duplicate entity growth, corrupted counts, or false metrics.

This system enforces idempotency across two distinct architectural layers:

1. **Raw Ingestion Layer (`orders_raw`)** — Historical Traceability & Source Fidelity.
2. **Validated Business Layer (`orders_validated`)** — Strict Business Key Uniqueness & Atomic Upsert.

---

## Layer 1: orders_raw (Historical Traceability)

### Mechanism: Run-Scoped `_id` and Checkpointing

In accordance with strict ELT standards:
- Every raw record receives a unique primary key:
  $$\_id = \text{f"}\{id\_run\}:\{number\_row\_source\}\text{"}$$
- All source records (including intra-file duplicate business keys) are preserved in `orders_raw` without arbitrary drop filtering.
- Re-running the same input file with a new `id_run` appends a new run trace to `orders_raw`, preserving complete historical lineage.
- Resuming a failed run within the same `id_run` uses `(file_fingerprint, id_run)` stored in `meta_state` to skip already-ingested batches without duplicating raw documents.

---

## Layer 2: orders_validated (Business Idempotency)

### Explicit MongoDB Upsert Policy

1. **Unique Business Index**:
   ```python
   db["orders_validated"].create_index([("id_order", ASCENDING)], unique=True, name="ux_id_order")
   ```
   A strict unique index is enforced on `id_order`. Any attempt to insert duplicate orders into `orders_validated` is blocked at the storage engine level.

2. **No "Insert-Then-Check" Anti-Pattern**:
   The pipeline never performs naive `insert()` followed by a post-check or catch-and-ignore. It utilizes atomic MongoDB bulk write operations:
   ```python
   ReplaceOne({"id_order": id_order}, new_record, upsert=True)
   ```

3. **Intra-Batch Deduplication**:
   When multiple records with the same `id_order` exist within a single processing batch, the pipeline consolidates to the latest valid document state before dispatching bulk operations. This eliminates intra-batch unique index collisions (`E11000 duplicate key error`).

4. **Business State Comparison (`is_business_state_equal`)**:
   - Prior to writing, candidate records are compared against existing MongoDB documents by business fields.
   - Dynamic, volatile execution metadata is stripped prior to comparison:
     `{"_id", "id_run", "at_ingested", "processed_at", "quarantined_at", "file_source", "number_row_source", "engine_used"}`
   - **Policy Outcomes**:
     * **Non-existent `id_order`**: Issued as `ReplaceOne(..., upsert=True)` $\to$ `inserted = 1`, `updated = 0`, `unchanged = 0`.
     * **Existing `id_order` with Identical Business State**: No write operation issued $\to$ `inserted = 0`, `updated = 0`, `unchanged = 1`.
     * **Existing `id_order` with Modified Business State**: Issued as `ReplaceOne(..., upsert=True)` $\to$ `inserted = 0`, `updated = 1`, `unchanged = 0`.

---

## Verified Experiment Evidence (Actual MongoDB Execution)

Recorded in `reports/results.json` (`id_run: audit_task_7_upsert_idempotency`):

### TEST A — FIRST RUN (10 fresh orders)
- **Input**: 10 new orders
- **Inserted**: `10`
- **Updated**: `0`
- **Unchanged**: `0`
- **Validated Total (`db.orders_validated.count_documents({})`)**: `10`

### TEST B — REPLAY (Same 10 orders re-ingested with new `id_run` and timestamps)
- **Input**: 10 identical orders
- **Inserted**: `0`
- **Updated**: `0`
- **Unchanged**: `10`
- **Validated Total (`db.orders_validated.count_documents({})`)**: `10` (Did NOT increase)
- **Individual Check**: `db.orders_validated.count_documents({"id_order": id}) == 1` for all 10 records.

### TEST C — UPDATE (1 existing order modified: status changed to "تم التسليم")
- **Input**: 1 modified order (`ORD-IDEM-001`)
- **Inserted**: `0`
- **Updated**: `1`
- **Unchanged**: `0`
- **Validated Total (`db.orders_validated.count_documents({})`)**: `10`
- **Duplicate Check**: `db.orders_validated.count_documents({"id_order": "ORD-IDEM-001"}) == 1` (Zero duplicate creation).
- **Verified State**: `status = "تم التسليم"`.

---

## Automated Test Coverage

```
tests/test_idempotency.py::test_business_state_equal_identical_records PASSED
tests/test_idempotency.py::test_business_state_equal_modified_record PASSED
tests/test_idempotency.py::test_compute_idempotency_key_with_id_order_and_whitespace PASSED
tests/test_idempotency.py::test_compute_idempotency_key_without_id_order_deterministic_hash PASSED
tests/test_spark_loader_idempotency.py::test_spark_loader_idempotency_reingestion PASSED
tests/test_spark_loader_idempotency.py::test_spark_loader_historical_traceability PASSED
tests/test_spark_loader_idempotency.py::test_crash_recovery_and_checkpoint_resume PASSED
tests/test_spark_loader_idempotency.py::test_mongodb_unique_protection PASSED
```
