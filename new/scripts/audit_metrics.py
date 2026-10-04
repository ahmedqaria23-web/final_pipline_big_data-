import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

def audit_results_json():
    json_path = ROOT_DIR / "reports" / "results.json"
    if not json_path.exists():
        print(f"[ERROR] {json_path} does not exist.")
        return

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        print(f"[ERROR] results.json is not a list (got {type(data)}).")
        return

    print(f"Total entries in results.json: {len(data)}")

    required_fields = [
        "id_run",
        "file_name",
        "file_size_mb",
        "used_engine",
        "read_rows",
        "loaded_raw",
        "count_valid",
        "count_corrected",
        "count_quarantine",
        "seconds_elapsed",
        "throughput",
        "counts_case_error",
        "count_inserted",
        "count_updated",
        "count_unchanged"
    ]

    all_run_ids = []
    issues_found = []

    for idx, entry in enumerate(data):
        # Ignore standalone non-pipeline experiment entries like audit_task_7
        if "experiment" in entry and "file_name" not in entry:
            print(f"Entry #{idx}: Standalone experiment ({entry.get('experiment')})")
            continue

        run_id = entry.get("id_run")
        if not run_id:
            issues_found.append((idx, "MISSING_ID_RUN", f"Entry #{idx} has no id_run"))
        else:
            all_run_ids.append(run_id)

        # Check required fields
        missing = [f for f in required_fields if f not in entry]
        if missing:
            issues_found.append((idx, "MISSING_FIELDS", f"Run {run_id} missing {missing}"))

        # Check partitions OR size_batch
        has_batch = "size_batch" in entry and entry["size_batch"] is not None
        has_part = "partitions" in entry and entry["partitions"] is not None
        if not (has_batch or has_part):
            issues_found.append((idx, "MISSING_BATCH_OR_PARTITIONS", f"Run {run_id} has neither size_batch nor partitions populated"))

        # Check throughput = read_rows / seconds_elapsed
        read_rows = entry.get("read_rows", 0)
        seconds_elapsed = entry.get("seconds_elapsed", 0.0)
        throughput = entry.get("throughput")

        if seconds_elapsed > 0 and read_rows > 0:
            expected_throughput = round(read_rows / seconds_elapsed, 2)
            if throughput is not None:
                # Check within 5% tolerance due to floating point timing / rounding
                diff = abs(throughput - expected_throughput)
                if diff > max(1.0, 0.05 * expected_throughput):
                    issues_found.append((idx, "THROUGHPUT_MISMATCH", f"Run {run_id}: throughput={throughput}, expected={expected_throughput} (diff={diff})"))

    # Check for duplicate id_run
    seen = set()
    duplicates = set()
    for rid in all_run_ids:
        if rid in seen:
            duplicates.add(rid)
        seen.add(rid)

    print(f"\nUnique id_run count: {len(seen)} / Total run entries: {len(all_run_ids)}")
    if duplicates:
        print(f"[WARNING] Duplicate id_runs found: {duplicates}")
    else:
        print("[OK] All id_runs are unique!")

    print(f"\nTotal issues found across historical entries: {len(issues_found)}")
    for idx, code, msg in issues_found[:20]:
        print(f"  [{code}] #{idx}: {msg}")
    if len(issues_found) > 20:
        print(f"  ... and {len(issues_found) - 20} more issues.")

if __name__ == "__main__":
    audit_results_json()
