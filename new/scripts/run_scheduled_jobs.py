"""
CLI Script to list, inspect, and trigger scheduled jobs on demand.
"""

import sys
import json
import argparse
from pathlib import Path

# Add project root to sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# Ensure stdout handles UTF-8 strings gracefully on Windows
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
    except Exception:
        pass

from src.mongodb.mongo_setup import get_mongo_db
from src.scheduler.jobs import list_jobs, run_job, get_job_logs


def main():
    parser = argparse.ArgumentParser(description="Manage and execute Scheduled Jobs.")
    parser.add_argument("--list", action="store_true", help="List all registered jobs and their status.")
    parser.add_argument("--run", type=str, help="Name of the job to execute manually.")
    parser.add_argument("--logs", action="store_true", help="View recent execution audit logs.")
    args = parser.parse_args()

    db = get_mongo_db()
    print("=" * 65)
    print("Scheduled Jobs Runner (Phase 5)")
    print("=" * 65)

    if args.run:
        print(f"\n>>> Triggering manual execution of job: '{args.run}' ...")
        try:
            res = run_job(args.run, trigger_type="manual", db=db)
            print(f"SUCCESS: Job completed in {res['duration_seconds']}s with status {res['status']}.")
            print("Execution details:")
            print(json.dumps(res, indent=2, ensure_ascii=False))
        except Exception as e:
            print(f"FAILED: {e}")
            sys.exit(1)
        return

    if args.logs:
        print("\nRecent Job Execution Logs from MongoDB:")
        logs = get_job_logs(limit=5, db=db)
        for lg in logs:
            print(f" - [{lg.get('start_time')}] {lg.get('job_name')} | Status: {lg.get('status')} | Duration: {lg.get('duration_seconds')}s")
        return

    # Default action: list jobs
    jobs = list_jobs(db=db)
    print(f"\nDiscovered {len(jobs)} registered jobs:")
    for j in jobs:
        print(f"\n* Name        : {j['name']}")
        print(f"  Description : {j['description']}")
        print(f"  Interval    : Every {j['interval_minutes']} minutes")
        print(f"  Last Status : {j['last_status']} (at {j['last_run_time']})")


if __name__ == "__main__":
    main()
