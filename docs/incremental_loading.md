# Path B — Incremental Loading & Watermark Management

## Overview

Path B implements an advanced Change Data Capture (CDC) / Incremental Data Pipeline inside [`src/incremental/incremental_loader.py`](file:///d:/level_4/big%20data/practical/half_project/src/incremental/incremental_loader.py). Rather than reprocessing the full source dataset on every run, the pipeline executes an Initial Baseline Load once, and subsequently processes Delta updates (new and updated records) incrementally.

---

## Technical Specifications & Architecture

1. **High-Watermark Tracking (`updated_at`)**:
   - Primary metric: `updated_at` (ISO-8601 UTC Timestamp).
   - Fallback metric: `order_date` for initial baseline records without explicit `updated_at`.
   - State persistence: Stored in MongoDB `meta_state` collection under `pipeline: "path_b_incremental"`.

2. **Delta Filtering vs. Replay Semantics**:
   - **Streaming CDC Mode (`replay_mode=False`)**: Records where $\text{record.updated\_at} \le \text{watermark}$ are categorized as `filtered_by_watermark` and skipped before reaching the Upsert layer. In this mode, `count_unchanged` is **0** (we do not claim unchanged when records were filtered prior to Upsert).
   - **Replay / Verification Mode (`replay_mode=True` or `force_process=True`)**: Records are passed through to `upsert_validated_batch`. The Upsert layer evaluates existing documents via `is_business_state_equal()`. Replayed identical records are counted as `count_unchanged = N` and `count_inserted = 0`, `count_updated = 0`.

3. **Version Conflict Resolution (Latest-Wins & Stale Rejection)**:
   - Evaluates incoming `version` against stored `version` in `orders_validated`:
     - If $\text{incoming\_version} > \text{stored\_version}$ or ($\text{incoming\_version} == \text{stored\_version}$ and $\text{incoming.updated\_at} \ge \text{stored.updated\_at}$): Record is accepted for upsert.
     - If $\text{incoming\_version} < \text{stored\_version}$: Stale update is rejected (`conflicts_rejected`).

4. **Cumulative Operation Idempotency (`processed_events`)**:
   - For events with explicit identifiers (`event_id`), the pipeline records each processed event in MongoDB `processed_events` (`event_id` unique index).
   - Replaying the same cumulative event does not apply its effect twice (`events_skipped += 1`).

---

## Verified Demonstration Sequence (Task 8 Test Suite)

Tested in [`tests/test_task8_incremental_path_b.py`](file:///d:/level_4/big%20data/practical/half_project/tests/test_task8_incremental_path_b.py):

| Step | Operation | Input Records | `count_inserted` | `count_updated` | `count_unchanged` | `filtered_by_watermark` | Watermark | Total Validated |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1. Initial** | **Baseline Load** | 3 | **3** | **0** | **0** | 0 | `2025-01-31T11:00:00Z` | **3** |
| **2. Delta 1** | **1 Update + 1 Insert** | 2 | **1** | **1** | **0** | 0 | `2025-02-01T12:00:00Z` | **4** |
| **3A. Replay** | **Stream Mode (CDC)** | 2 | **0** | **0** | **0** | **2** | `2025-02-01T12:00:00Z` | **4** |
| **3B. Replay** | **Upsert Mode** | 2 | **0** | **0** | **2** | 0 | `2025-02-01T12:00:00Z` | **4** |
| **4. Delta 2** | **1 Update + 1 Insert** | 2 | **1** | **1** | **0** | 0 | `2025-02-02T15:00:00Z` | **5** |

---

## Core Functions in `src/incremental/incremental_loader.py`

- `get_watermark(db: Database, pipeline_name: str = "path_b_incremental") -> str`
- `save_watermark(db: Database, new_watermark: str, pipeline_name: str = "path_b_incremental", additional_meta: Optional[Dict] = None)`
- `initial_load(db: Database, records: List[Dict], pipeline_name: str = "path_b_incremental") -> Dict[str, Any]`
- `process_delta_batch(db: Database, records: List[Dict], pipeline_name: str = "path_b_incremental", force_process: bool = False, replay_mode: bool = False) -> Dict[str, Any]`
