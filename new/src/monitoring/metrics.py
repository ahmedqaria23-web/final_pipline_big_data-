import sys
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.settings import REPORT_DIR


def save_results(metrics: Dict[str, Any], report_dir: Path = REPORT_DIR):
    report_path = Path(report_dir)
    report_path.mkdir(parents=True, exist_ok=True)
    output_file = report_path / "results.json"
    
    history = []
    if output_file.exists():
        try:
            with open(output_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    history = data
                elif isinstance(data, dict):
                    history = [data]
        except Exception:
            history = []

    history.append(metrics)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def verify_consistency_equation(
    run_raw_count: int,
    run_valid_count: int,
    run_corrected_count: int,
    run_quarantine_count: int
) -> bool:
    """
    Verifies the fundamental ELT consistency equation:
    run_raw_count = run_valid_count + run_corrected_count + run_quarantine_count
    Zero data loss guarantee: every raw record ends in exactly one processing result.
    """
    return run_raw_count == (run_valid_count + run_corrected_count + run_quarantine_count)


def calculate_and_verify_metrics(
    id_run: str,
    file_name: str,
    file_size_mb: float,
    used_engine: str,
    read_rows: int,
    loaded_raw: int,
    count_valid: int,
    count_corrected: int,
    count_quarantine: int,
    count_inserted: int,
    count_updated: int,
    count_unchanged: int,
    seconds_elapsed: float,
    throughput: float,
    batch_or_partitions: Any,
    counts_case_error: Dict[str, int],
    threshold_mb: float,
    reason :  str,
    report_dir: Path = REPORT_DIR
) -> Dict[str, Any]:
    
    sum_classified = count_valid + count_corrected + count_quarantine
    consistency_equation_verified = verify_consistency_equation(
        run_raw_count=loaded_raw,
        run_valid_count=count_valid,
        run_corrected_count=count_corrected,
        run_quarantine_count=count_quarantine
    )

    # Strictly verify throughput = read_rows / seconds_elapsed from actual observed execution
    observed_throughput = round(read_rows / seconds_elapsed, 2) if seconds_elapsed > 0 else 0.0

    metrics = {
        "id_run": id_run,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "file_name": file_name,
        "file_size_mb": file_size_mb,
        "threshold_mb": threshold_mb,
        "used_engine": used_engine,
        "engine_selection_reason": reason,
        "read_rows": read_rows,
        "loaded_raw": loaded_raw,
        "run_raw_count": loaded_raw,
        "count_valid": count_valid,
        "run_valid_count": count_valid,
        "count_corrected": count_corrected,
        "run_corrected_count": count_corrected,
        "count_quarantine": count_quarantine,
        "run_quarantine_count": count_quarantine,
        "sum_classified": sum_classified,
        "consistency_equation_verified": consistency_equation_verified,
        "count_inserted": count_inserted,
        "count_updated": count_updated,
        "count_unchanged": count_unchanged,
        "seconds_elapsed": seconds_elapsed,
        "throughput": observed_throughput,
        "throughput_records_per_sec": observed_throughput,
        "size_batch": batch_or_partitions if used_engine == "python_batch" else None,
        "partitions": batch_or_partitions if used_engine == "pyspark" else None,
        "input_partitions": batch_or_partitions if used_engine == "pyspark" else None,
        "batch_or_partitions": batch_or_partitions,
        "counts_case_error": counts_case_error
    }

    save_results(metrics, report_dir)
    return metrics


def load_latest_results(report_dir: Path = REPORT_DIR) -> Dict[str, Any]:
    output_file = Path(report_dir) / "results.json"
    if not output_file.exists():
        return {}
    try:
        with open(output_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list) and data:
                return data[-1]
            elif isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


class MetricsEngine:
    def __init__(self, report_dir: Path = REPORT_DIR):
        self.report_dir = Path(report_dir)

    def save_results(self, metrics: Dict[str, Any]) -> None:
        save_results(metrics, self.report_dir)

    def calculate_and_verify_metrics(self, **kwargs) -> Dict[str, Any]:
        if "report_dir" not in kwargs:
            kwargs["report_dir"] = self.report_dir
        return calculate_and_verify_metrics(**kwargs)

    def load_latest_results(self) -> Dict[str, Any]:
        return load_latest_results(self.report_dir)

