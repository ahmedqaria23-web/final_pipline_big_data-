import sys
import time
import logging
from pathlib import Path
from typing import Dict, Any, List, Callable, Optional, Tuple
from pymongo.database import Database

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from concurrent.futures import ProcessPoolExecutor
from config.settings import (
    setup_logging,
    COLLECTION_RAW,
    CLASSIFICATION_WORKERS,
    CLASSIFICATION_CHUNK_SIZE,
    MONGO_WRITE_BATCH_SIZE
)
from src.routing.file_router import inspect_and_route
from src.mongodb.mongo_setup import initialize_database
from src.mongodb.repositories import find_raw_sample, upsert_validated_batch, insert_quarantine_batch
from src.ingestion.batch_loader import load_batch_to_raw
from src.ingestion.spark_loader import load_spark_to_raw
from src.quality.classifier import classify_record
from src.monitoring.metrics import calculate_and_verify_metrics

setup_logging()
logger = logging.getLogger(__name__)


def classify_worker(raw_doc: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """Top-level worker function for parallel multiprocessing classification."""
    return classify_record(raw_doc)


def _stream_cursor_chunks(cursor, chunk_size: int):
    chunk = []
    for item in cursor:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def run_elt_pipeline(
    file_path: str,
    db: Optional[Database] = None,
    progress_callback: Optional[Callable[[str, float], None]] = None,
    threshold_mb: Optional[float] = None
) -> Dict[str, Any]:
    """
    Executes the full ELT pipeline:
    1. File discovery & engine routing.
    2. Raw ingestion BEFORE cleaning into orders_raw.
    3. Fetching raw records for quality checks & classification.
    4. Idempotent upsert into orders_validated and insertion into quarantine_orders.
    5. Run-level consistency equation verification and metric reporting.
    """
    pipeline_start_time = time.perf_counter()
    logger.info(f"Initiating ELT pipeline for file: {file_path}")

    if db is None:
        db = initialize_database()

    def update_progress(msg: str, pct: float):
        logger.info(f"[{pct*100:.0f}%] {msg}")
        if progress_callback:
            progress_callback(msg, pct)
        print(f"[{pct*100:.0f}%] {msg}")

    # Step 1: File Discovery & Router Selection
    update_progress("Step 1/6: Discovering file & selecting engine...", 0.1)
    if threshold_mb is not None:
        routing = inspect_and_route(file_path, threshold_mb=threshold_mb, db=db)
    else:
        routing = inspect_and_route(file_path, db=db)
    id_run = routing["id_run"]
    engine = routing["selected_engine"]
    file_size_mb = routing["file_size_mb"]

    # Step 2: Raw Ingestion BEFORE cleaning (ELT)
    update_progress(f"Step 2/6: Ingesting Raw data via {engine}...", 0.3)
    if engine == "python_batch":
        ingest_res = load_batch_to_raw(file_path, id_run, db, progress_callback=progress_callback)
        batch_or_partitions = ingest_res.get("size_batch", 1000)
    else:
        ingest_res = load_spark_to_raw(file_path, id_run, db, progress_callback=progress_callback)
        batch_or_partitions = ingest_res.get("partitions", 1)

    read_rows = ingest_res.get("read_rows", 0)
    actual_id_run = ingest_res.get("id_run", id_run)
    loaded_raw = db[COLLECTION_RAW].count_documents({"id_run": actual_id_run})

    if read_rows == 0:
        read_rows = loaded_raw

    # Step 3 & 4 & 5: Streaming Fetch, Cleaning, Validation, Classification & Batch Load
    update_progress(f"Step 3-5/6: Processing Data Quality & Classification for {read_rows:,} records in streaming batches...", 0.6)

    count_valid = 0
    count_corrected = 0
    count_quarantine = 0
    inserted_count = 0
    updated_count = 0
    unchanged_count = 0
    error_code_counts: Dict[str, int] = {}

    batch_validated: List[Dict[str, Any]] = []
    batch_quarantine: List[Dict[str, Any]] = []

    file_name = routing["file_name"]
    chunk_size = CLASSIFICATION_CHUNK_SIZE
    mongo_write_batch_size = MONGO_WRITE_BATCH_SIZE
    max_workers = CLASSIFICATION_WORKERS

    raw_cursor = db[COLLECTION_RAW].find({"id_run": actual_id_run}, batch_size=chunk_size)

    total_processed = 0
    total_classification_time = 0.0
    total_mongo_write_time = 0.0

    # Stream chunks through ProcessPoolExecutor when max_workers > 1, else process sequentially
    if max_workers <= 1:
        for raw_doc in raw_cursor:
            total_processed += 1
            t_c_start = time.perf_counter()
            outcome, payload = classify_record(raw_doc)
            total_classification_time += time.perf_counter() - t_c_start

            if outcome == "VALID":
                count_valid += 1
                batch_validated.append(payload)
            elif outcome == "CORRECTED":
                count_corrected += 1
                batch_validated.append(payload)
            else:
                count_quarantine += 1
                batch_quarantine.append(payload)
                for code in payload.get("codes_error", []):
                    error_code_counts[code] = error_code_counts.get(code, 0) + 1

            if len(batch_validated) >= mongo_write_batch_size:
                t_w_start = time.perf_counter()
                ins, upd, unc = upsert_validated_batch(db, batch_validated)
                inserted_count += ins
                updated_count += upd
                unchanged_count += unc
                total_mongo_write_time += time.perf_counter() - t_w_start
                batch_validated.clear()

            if len(batch_quarantine) >= mongo_write_batch_size:
                t_w_start = time.perf_counter()
                insert_quarantine_batch(db, batch_quarantine)
                total_mongo_write_time += time.perf_counter() - t_w_start
                batch_quarantine.clear()

            if total_processed % 2000 == 0 or total_processed == 1:
                pct = 0.6 + min(0.35, (total_processed / max(1, read_rows)) * 0.35)
                update_progress(f"Step 3-5/6: Classified & upserted {total_processed:,} / {read_rows:,} records...", pct)
    else:
        logger.info(f"[Multiprocessing] Launching ProcessPoolExecutor with {max_workers} workers (chunk_size={chunk_size})...")
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            for chunk in _stream_cursor_chunks(raw_cursor, chunk_size):
                t_class_start = time.perf_counter()
                chunk_len = len(chunk)
                try:
                    map_chunksize = max(1, chunk_len // (max_workers * 4))
                    results = list(executor.map(classify_worker, chunk, chunksize=map_chunksize))
                except Exception as err:
                    logger.error(f"Error during parallel worker execution: {err}. Falling back to inline execution for chunk.")
                    results = [classify_record(doc) for doc in chunk]

                class_duration = time.perf_counter() - t_class_start
                total_classification_time += class_duration

                t_write_start = time.perf_counter()
                for outcome, payload in results:
                    total_processed += 1
                    if outcome == "VALID":
                        count_valid += 1
                        batch_validated.append(payload)
                    elif outcome == "CORRECTED":
                        count_corrected += 1
                        batch_validated.append(payload)
                    else:
                        count_quarantine += 1
                        batch_quarantine.append(payload)
                        for code in payload.get("codes_error", []):
                            error_code_counts[code] = error_code_counts.get(code, 0) + 1

                if batch_validated:
                    ins, upd, unc = upsert_validated_batch(db, batch_validated)
                    inserted_count += ins
                    updated_count += upd
                    unchanged_count += unc
                    batch_validated.clear()

                if batch_quarantine:
                    insert_quarantine_batch(db, batch_quarantine)
                    batch_quarantine.clear()

                write_duration = time.perf_counter() - t_write_start
                total_mongo_write_time += write_duration

                class_rate = chunk_len / class_duration if class_duration > 0 else 0.0
                write_rate = chunk_len / write_duration if write_duration > 0 else 0.0
                pct = 0.6 + min(0.35, (total_processed / max(1, read_rows)) * 0.35)

                logger.info(
                    f"[Classification Chunk] processed={total_processed:,}/{read_rows:,} | "
                    f"Classify: {chunk_len} docs in {class_duration:.2f}s ({class_rate:.0f} rec/s) | "
                    f"MongoWrite: {write_duration:.2f}s ({write_rate:.0f} rec/s)"
                )
                update_progress(f"Step 3-5/6: Classified & upserted {total_processed:,} / {read_rows:,} records...", pct)



    if batch_validated:
        t_w_start = time.perf_counter()
        ins, upd, unc = upsert_validated_batch(db, batch_validated)
        inserted_count += ins
        updated_count += upd
        unchanged_count += unc
        total_mongo_write_time += time.perf_counter() - t_w_start
        batch_validated.clear()

    if batch_quarantine:
        t_w_start = time.perf_counter()
        insert_quarantine_batch(db, batch_quarantine)
        total_mongo_write_time += time.perf_counter() - t_w_start
        batch_quarantine.clear()

    # Step 6: Calculate Metrics & Verify Consistency Equation
    update_progress("Step 6/6: Calculating metrics & verifying consistency equation...", 1.0)
    pipeline_elapsed = round(time.perf_counter() - pipeline_start_time, 4)
    throughput = round(read_rows / pipeline_elapsed, 2) if pipeline_elapsed > 0 else 0.0

    class_rate_avg = read_rows / total_classification_time if total_classification_time > 0 else 0.0
    write_rate_avg = read_rows / total_mongo_write_time if total_mongo_write_time > 0 else 0.0

    logger.info(
        f"\n==================================================\n"
        f"Pipeline Telemetry Summary:\n"
        f"Total Records Processed: {read_rows:,}\n"
        f"Classification Time:     {total_classification_time:.2f} s ({class_rate_avg:.0f} records/sec)\n"
        f"MongoDB Write Time:      {total_mongo_write_time:.2f} s ({write_rate_avg:.0f} records/sec)\n"
        f"Total Pipeline Time:     {pipeline_elapsed:.2f} s\n"
        f"Overall Rate:            {throughput:,.1f} records/sec\n"
        f"=================================================="
    )


    metrics = calculate_and_verify_metrics(
        id_run=id_run,
        file_name=routing["file_name"],
        file_size_mb=file_size_mb,
        used_engine=engine,
        read_rows=read_rows,
        loaded_raw=loaded_raw,
        count_valid=count_valid,
        count_corrected=count_corrected,
        count_quarantine=count_quarantine,
        count_inserted=inserted_count,
        count_updated=updated_count,
        count_unchanged=unchanged_count,
        seconds_elapsed=pipeline_elapsed,
        throughput=throughput,
        batch_or_partitions=batch_or_partitions,
        counts_case_error=error_code_counts,
        threshold_mb=routing["threshold_mb"],
        reason=routing["reason"]
    )

    return metrics
