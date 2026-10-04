import sys
import csv
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Generator, Tuple, List, Callable, Optional
from pymongo.database import Database

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import hashlib

from config.settings import BATCH_SIZE, COLLECTION_RAW
from src.mongodb.repositories import (
    insert_raw_batch,
    get_ingestion_checkpoint,
    save_ingestion_checkpoint,
    get_active_checkpoint,
    compute_file_fingerprint
)

logger = logging.getLogger(__name__)


def stream_records(file_path: Path) -> Generator[Tuple[int, Dict[str, Any]], None, None]:
    suffix = file_path.suffix.lower()
    
    with open(file_path, "r", encoding="utf-8") as f:
        if suffix == ".jsonl":
            for row_num, line in enumerate(f, start=1):
                line_str = line.strip()
                if not line_str:
                    continue
                try:
                    parsed = json.loads(line_str)
                except json.JSONDecodeError:
                    parsed = {"_unparseable_line": line_str}
                yield row_num, parsed
        elif suffix == ".csv":
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, start=1):
                yield row_num, dict(row)
        else:
            for row_num, line in enumerate(f, start=1):
                line_str = line.strip()
                if not line_str:
                    continue
                try:
                    parsed = json.loads(line_str)
                except Exception:
                    parsed = {"raw_line": line_str}
                yield row_num, parsed


def load_batch_to_raw(
    file_path: str,
    id_run: Optional[str] = None,
    db: Optional[Database] = None,
    batch_size: int = BATCH_SIZE,
    progress_callback: Optional[Callable[[str, float], None]] = None,
    max_retries: int = 3,
    retry_delay_seconds: float = 0.1
) -> Dict[str, Any]:
    """
    Memory-efficient streaming Python batch loader function.
    Reads file line-by-line and writes original raw records into MongoDB orders_raw.
    Supports persistent batch checkpoints for crash resumeability.
    """
    path = Path(file_path).resolve()
    stat = path.stat()
    file_fingerprint = compute_file_fingerprint(path)

    checkpoint = None
    if id_run:
        checkpoint = get_ingestion_checkpoint(db, file_fingerprint, id_run=id_run)

    # If id_run was not passed or checkpoint not found for id_run, look up active incomplete run
    if not checkpoint:
        active_chk = get_active_checkpoint(db, file_fingerprint)
        if active_chk and active_chk.get("id_run"):
            checkpoint = active_chk
            id_run = active_chk["id_run"]
            logger.info(f"[RESUME] Recovered active checkpoint for '{path.name}', resuming id_run='{id_run}'.")

    if not id_run:
        import uuid
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        unique_suffix = uuid.uuid4().hex[:6]
        id_run = f"run_{timestamp}_{unique_suffix}"

    logger.info(f"Starting Python Streaming Batch Loader for file '{path.name}', fingerprint='{file_fingerprint}', id_run='{id_run}'")
    start_time = time.perf_counter()


    last_completed_batch = 0
    last_processed_row = 0

    if checkpoint:
        if checkpoint.get("status") == "COMPLETED":
            raw_count = db[COLLECTION_RAW].count_documents({"id_run": id_run})
            if raw_count > 0:
                logger.info(f"[CHECKPOINT] File '{path.name}' for id_run '{id_run}' is marked COMPLETED in meta_state and orders_raw contains {raw_count:,} records. Fast-returning.")
                return {
                    "id_run": id_run,
                    "engine_used": "python_batch",
                    "file_source": path.name,
                    "file_size_mb": round(stat.st_size / (1024 * 1024), 2),
                    "read_rows": raw_count,
                    "loaded_raw": raw_count,
                    "batch_count": checkpoint.get("last_completed_batch", 0),
                    "size_batch": batch_size,
                    "elapsed_seconds": 0.0,
                    "throughput": 0.0,
                    "errors": 0,
                    "status": "COMPLETED"
                }
            else:
                logger.warning(f"[CHECKPOINT] File '{path.name}' (id_run='{id_run}') was marked COMPLETED, but orders_raw has 0 records. Re-ingesting.")
        else:
            last_completed_batch = checkpoint.get("last_completed_batch", 0)
            last_processed_row = checkpoint.get("processed_rows", 0)
            logger.info(f"[RESUME] Found incomplete checkpoint for '{path.name}' (id_run='{id_run}'). Resuming from batch {last_completed_batch + 1} (skipping first {last_processed_row:,} rows).")

    read_rows = last_processed_row
    loaded_raw = last_processed_row
    batch_count = last_completed_batch
    errors = 0
    
    batch: List[Dict[str, Any]] = []
    current_batch_start_row = last_processed_row + 1

    for row_num, raw_record in stream_records(path):
        if row_num <= last_processed_row:
            continue

        read_rows += 1
        id_order_val = raw_record.get("id_order") or raw_record.get("order_id")

        if id_order_val is not None:
            id_order_val = str(id_order_val).strip()

        _id = f"{id_run}:{row_num}"

        raw_doc = {
            "_id": _id,
            "id_run": id_run,
            "file_source": path.name,
            "number_row_source": row_num,
            "at_ingested": datetime.now(timezone.utc).isoformat(),
            "engine_used": "python_batch",
            "id_order": id_order_val if id_order_val else None,
            "record_raw": raw_record
        }
        batch.append(raw_doc)

        if len(batch) >= batch_size:
            batch_num = batch_count + 1
            batch_end_row = row_num
            write_success = False
            last_err = None

            for attempt in range(1, max_retries + 1):
                try:
                    inserted = insert_raw_batch(db, batch)
                    loaded_raw += inserted
                    batch_count = batch_num
                    last_processed_row = batch_end_row
                    write_success = True
                    break
                except Exception as err:
                    last_err = err
                    logger.warning(
                        f"[RETRY] Batch write failed for batch #{batch_num} (attempt {attempt}/{max_retries}, "
                        f"rows {current_batch_start_row}-{batch_end_row}, run_id='{id_run}', file='{path.name}'): {err}"
                    )
                    if attempt < max_retries and retry_delay_seconds > 0:
                        time.sleep(retry_delay_seconds)

            if not write_success:
                errors += 1
                save_ingestion_checkpoint(db, {
                    "file_fingerprint": file_fingerprint,
                    "file_name": path.name,
                    "file_path": str(path),
                    "file_size_bytes": stat.st_size,
                    "id_run": id_run,
                    "last_completed_batch": batch_count,
                    "processed_rows": last_processed_row,
                    "batch_size": batch_size,
                    "status": "FAILED",
                    "failed_batch": batch_num,
                    "failed_row_range": [current_batch_start_row, batch_end_row],
                    "error": str(last_err),
                    "retry_attempts": max_retries
                })
                logger.error(
                    f"[BATCH_FAILURE] Permanent Mongo failure on batch #{batch_num} (rows {current_batch_start_row}-{batch_end_row}) "
                    f"after {max_retries} retries for file '{path.name}', run_id='{id_run}': {last_err}"
                )
                raise RuntimeError(
                    f"Batch write failed permanently for batch #{batch_num} (rows {current_batch_start_row}-{batch_end_row}) "
                    f"after {max_retries} retries for file '{path.name}', run_id='{id_run}': {last_err}"
                ) from last_err

            # Update persistent checkpoint AFTER write succeeds
            save_ingestion_checkpoint(db, {
                "file_fingerprint": file_fingerprint,
                "file_name": path.name,
                "file_path": str(path),
                "file_size_bytes": stat.st_size,
                "id_run": id_run,
                "last_completed_batch": batch_count,
                "processed_rows": last_processed_row,
                "batch_size": batch_size,
                "status": "IN_PROGRESS"
            })
            logger.info(f"[CHECKPOINT] Batch #{batch_count} completed ({last_processed_row:,} total rows processed). Saved checkpoint.")

            if progress_callback:
                pct = 0.3 + min(0.25, (last_processed_row / 500000) * 0.25)
                progress_callback(f"Step 2/6: Ingested batch #{batch_count} ({last_processed_row:,} rows) into orders_raw...", pct)

            batch.clear()
            current_batch_start_row = last_processed_row + 1

    if batch:
        batch_num = batch_count + 1
        batch_end_row = read_rows
        write_success = False
        last_err = None

        for attempt in range(1, max_retries + 1):
            try:
                inserted = insert_raw_batch(db, batch)
                loaded_raw += inserted
                batch_count = batch_num
                last_processed_row = batch_end_row
                write_success = True
                break
            except Exception as err:
                last_err = err
                logger.warning(
                    f"[RETRY] Batch write failed for final batch #{batch_num} (attempt {attempt}/{max_retries}, "
                    f"rows {current_batch_start_row}-{batch_end_row}, run_id='{id_run}', file='{path.name}'): {err}"
                )
                if attempt < max_retries and retry_delay_seconds > 0:
                    time.sleep(retry_delay_seconds)

        if not write_success:
            errors += 1
            save_ingestion_checkpoint(db, {
                "file_fingerprint": file_fingerprint,
                "file_name": path.name,
                "file_path": str(path),
                "file_size_bytes": stat.st_size,
                "id_run": id_run,
                "last_completed_batch": batch_count,
                "processed_rows": last_processed_row,
                "batch_size": batch_size,
                "status": "FAILED",
                "failed_batch": batch_num,
                "failed_row_range": [current_batch_start_row, batch_end_row],
                "error": str(last_err),
                "retry_attempts": max_retries
            })
            logger.error(
                f"[BATCH_FAILURE] Permanent Mongo failure on final batch #{batch_num} (rows {current_batch_start_row}-{batch_end_row}) "
                f"after {max_retries} retries for file '{path.name}', run_id='{id_run}': {last_err}"
            )
            raise RuntimeError(
                f"Batch write failed permanently for final batch #{batch_num} (rows {current_batch_start_row}-{batch_end_row}) "
                f"after {max_retries} retries for file '{path.name}', run_id='{id_run}': {last_err}"
            ) from last_err

        save_ingestion_checkpoint(db, {
            "file_fingerprint": file_fingerprint,
            "file_name": path.name,
            "file_path": str(path),
            "file_size_bytes": stat.st_size,
            "id_run": id_run,
            "last_completed_batch": batch_count,
            "processed_rows": last_processed_row,
            "batch_size": batch_size,
            "status": "IN_PROGRESS"
        })
        if progress_callback:
            progress_callback(f"Step 2/6: Completed Raw Ingestion ({last_processed_row:,} records)", 0.55)

        batch.clear()

    # Mark checkpoint COMPLETED only after ALL batches have been successfully persisted
    save_ingestion_checkpoint(db, {
        "file_fingerprint": file_fingerprint,
        "file_name": path.name,
        "file_path": str(path),
        "file_size_bytes": stat.st_size,
        "id_run": id_run,
        "last_completed_batch": batch_count,
        "processed_rows": last_processed_row,
        "batch_size": batch_size,
        "status": "COMPLETED"
    })
    logger.info(f"[CHECKPOINT] Ingestion COMPLETED for file '{path.name}'. Marked checkpoint COMPLETED.")

    elapsed_seconds = round(time.perf_counter() - start_time, 4)
    throughput = round(loaded_raw / elapsed_seconds, 2) if elapsed_seconds > 0 else 0.0

    return {
        "id_run": id_run,
        "engine_used": "python_batch",
        "file_source": path.name,
        "file_size_mb": round(stat.st_size / (1024 * 1024), 2),
        "read_rows": read_rows,
        "loaded_raw": loaded_raw,
        "batch_count": batch_count,
        "size_batch": batch_size,
        "elapsed_seconds": elapsed_seconds,
        "throughput": throughput,
        "errors": 0,
        "status": "COMPLETED"
    }

