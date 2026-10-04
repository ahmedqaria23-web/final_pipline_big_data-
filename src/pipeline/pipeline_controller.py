import sys
from pathlib import Path
from typing import Dict, Any, Callable, Optional

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.pipeline.elt_pipeline import run_elt_pipeline
from src.mongodb.mongo_setup import get_mongo_db, get_mongo_client
from src.monitoring.metrics import load_latest_results
from config.settings import MONGODB_DATABASE


def check_system_status() -> Dict[str, Any]:
    """
    Checks MongoDB connection health and fast database collection statistics.
    Uses estimated_document_count() for instantaneous metadata lookup without disk scanning.
    """
    status = {
        "mongodb_connected": False,
        "database_name": MONGODB_DATABASE,
        "collections": {}
    }
    try:
        db = get_mongo_db()
        get_mongo_client().admin.command("ping")
        status["mongodb_connected"] = True

        for col_name in ["orders_raw", "orders_validated", "quarantine_orders", "meta_state"]:
            try:
                status["collections"][col_name] = db[col_name].estimated_document_count()
            except Exception:
                status["collections"][col_name] = db[col_name].count_documents({})
    except Exception as err:
        status["error"] = str(err)

    return status


def run_pipeline_for_file(
    file_path: str,
    progress_callback: Optional[Callable[[str, float], None]] = None,
    threshold_mb: Optional[float] = None
) -> Dict[str, Any]:
    """Executes the full ELT pipeline for a given input file path."""
    return run_elt_pipeline(file_path, progress_callback=progress_callback, threshold_mb=threshold_mb)


def get_latest_metrics() -> Dict[str, Any]:
    return load_latest_results()
