import os
import uuid
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Union, Optional

from config.settings import SMALL_FILE_THRESHOLD_MB
from src.mongodb.repositories import compute_file_fingerprint, get_active_checkpoint

logger = logging.getLogger(__name__)


def inspect_and_route(
    file_path: Union[str, Path],
    threshold_mb: float = SMALL_FILE_THRESHOLD_MB,
    db: Optional[Any] = None
) -> Dict[str, Any]:
    """
    Single execution entry point router function.
    Inspects file size in MB and selects engine ('python_batch' vs 'pyspark').
    Performs pre-flight input file validation.
    Checks for active IN_PROGRESS/FAILED runs across restarts to reuse id_run.
    """
    path = Path(file_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Input file not found at path: {path}")

    if not path.is_file():
        raise ValueError(f"Target path is not a valid file: {path}")

    file_size_bytes = path.stat().st_size
    if file_size_bytes == 0:
        raise ValueError(f"Input file is empty (0 bytes): {path}")

    allowed_exts = {".csv", ".jsonl", ".json"}
    if path.suffix.lower() not in allowed_exts:
        raise ValueError(f"Unsupported file format '{path.suffix}'. Allowed formats: {allowed_exts}")

    file_size_mb = file_size_bytes / (1024 * 1024)
    file_size_mb_display = round(file_size_mb, 2)
    file_fingerprint = compute_file_fingerprint(path)

    active_chk = None
    if db is not None:
        active_chk = get_active_checkpoint(db, file_fingerprint)

    if active_chk and active_chk.get("id_run"):
        id_run = active_chk["id_run"]
        is_resumed = True
        logger.info(f"File Router resuming existing active run '{id_run}' for file '{path.name}' (status={active_chk.get('status')})")
    else:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        unique_suffix = uuid.uuid4().hex[:6]
        id_run = f"run_{timestamp}_{unique_suffix}"
        is_resumed = False

    if file_size_mb <= threshold_mb:
        selected_engine = "python_batch"
        reason = (
            f"File size ({file_size_mb_display} MB) is <= threshold ({threshold_mb} MB). "
            "Selected streaming Python Batch Loader."
        )
    else:
        selected_engine = "pyspark"
        reason = (
            f"File size ({file_size_mb_display} MB) exceeds threshold ({threshold_mb} MB). "
            "Selected distributed PySpark Loader."
        )

    logger.info(f"File Router decision for '{path.name}': Engine={selected_engine.upper()}, Size={file_size_mb_display}MB, ID_Run={id_run}, Resumed={is_resumed}")

    return {
        "id_run": id_run,
        "file_path": str(path),
        "file_name": path.name,
        "file_fingerprint": file_fingerprint,
        "file_size_bytes": file_size_bytes,
        "file_size_mb": file_size_mb_display,
        "threshold_mb": threshold_mb,
        "selected_engine": selected_engine,
        "reason": reason,
        "is_resumed": is_resumed,
        "created_at": datetime.now(timezone.utc).isoformat()
    }

