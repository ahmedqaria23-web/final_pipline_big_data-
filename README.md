# ⚡ Enterprise Big Data ELT Pipeline & Unified Analytics Platform

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Apache PySpark](https://img.shields.io/badge/Apache%20PySpark-3.5%2B-E25A1C.svg?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-6.0%2B-green.svg?logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110%2B-009688.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Tests](https://img.shields.io/badge/Tests-129%20Passed%20(100%25)-brightgreen.svg?logo=pytest&logoColor=white)](https://docs.pytest.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

An enterprise-grade **Hybrid ELT (Extract-Load-Transform) Data Engineering Pipeline & Unified Analytics Platform** designed to ingest, normalize, and validate high-volume e-commerce datasets. Features dynamic file-size engine routing between a memory-efficient **Python Streaming Batch Loader** and a distributed **Apache PySpark Engine**, deterministic **14-rule automated cleaning**, strict **quarantine isolation**, **3-stage idempotency**, **analytical indexes with execution plan explain analysis**, **5 production aggregation pipelines**, **incremental materialized views ($merge)**, **audit-logged background job scheduler**, and a **Unified FastAPI Execution Interface**.

---

## 📑 Table of Contents

- [1. Project Overview](#1-project-overview)
- [2. Architecture](#2-architecture)
- [3. Prerequisites](#3-prerequisites)
- [4. Python Version](#4-python-version)
- [5. MongoDB Requirements](#5-mongodb-requirements)
- [6. Installation](#6-installation)
- [7. Environment Configuration](#7-environment-configuration)
- [8. .env.example](#8-envexample)
- [9. requirements.txt](#9-requirementstxt)
- [10. How to Start MongoDB](#10-how-to-start-mongodb)
- [11. How to Run the Existing Ingestion Pipeline](#11-how-to-run-the-existing-ingestion-pipeline)
- [12. How to Run Small Data](#12-how-to-run-small-data)
- [13. How to Run Large Data](#13-how-to-run-large-data)
- [14. How to Run Tests](#14-how-to-run-tests)
- [15. How to Create Indexes](#15-how-to-create-indexes)
- [16. How to Run Queries](#16-how-to-run-queries)
- [17. How to Run Explain](#17-how-to-run-explain)
- [18. How to Run Aggregation Reports](#18-how-to-run-aggregation-reports)
- [19. How to Refresh Materialized Views](#19-how-to-refresh-materialized-views)
- [20. How to Run Scheduled Jobs](#20-how-to-run-scheduled-jobs)
- [21. How to Manually Execute Scheduled Jobs](#21-how-to-manually-execute-scheduled-jobs)
- [22. How to Start FastAPI](#22-how-to-start-fastapi)
- [23. Swagger URL](#23-swagger-url)
- [24. API Endpoint Examples](#24-api-endpoint-examples)
- [25. Expected Response Structure](#25-expected-response-structure)
- [26. Troubleshooting](#26-troubleshooting)
- [27. Project Structure](#27-project-structure)

---

## 1. Project Overview

Real-world e-commerce datasets are messy: unstructured fields, inconsistent numerals, irregular date formats, mixed phone formats, and malformed nested records. 

This platform strictly follows the **ELT (Extract → Load → Transform)** paradigm:
1. **Raw-First Ingestion**: Every raw record is streamed directly into MongoDB `orders_raw` with an execution run identifier (`id_run`) and source row index (`number_row_source`) **prior** to any data transformation.
2. **Zero Data Loss Guarantee**: Records are deterministically validated into `VALID` (stored in `orders_validated`) or `QUARANTINED` (stored in `quarantine_orders`). No record is silently dropped.
3. **Analytical Indexing**: Specialized single-field and compound indexes optimize high-volume queries with verified execution stats (`COLLSCAN` -> `IXSCAN`).
4. **Aggregation Reports**: 5 independent aggregation pipelines analyze sales by city, top products, customer value, period trends, and status distributions.
5. **Incremental Materialized Views**: High-performance summaries (`daily_sales_summary`, `top_products_summary`) updated incrementally via `$merge` and persistent watermarks.
6. **Background Scheduling**: Automated background tasks with audit logging and manual on-demand execution.
7. **Unified FastAPI Layer**: A thin, clean API layer providing complete operational control over all services.

---

## 2. Architecture

```
                          ┌────────────────────────────┐
                          │    CSV / JSONL Raw File    │
                          └─────────────┬──────────────┘
                                        │
                                        ▼
                          ┌────────────────────────────┐
                          │     File Engine Router     │
                          │ (Threshold: 200 MB Default)│
                          └──────┬──────────────┬──────┘
                                 │              │
                   File <= 200MB │              │ File > 200MB
                                 ▼              ▼
                    ┌──────────────────┐  ┌───────────────────┐
                    │   Python Batch   │  │  Apache PySpark   │
                    │ Streaming Loader │  │ Distributed Load  │
                    └────────┬─────────┘  └────────┬──────────┘
                             │                     │
                             └──────────┬──────────┘
                                        ▼
                     ┌─────────────────────────────────────┐
                     │          MongoDB: orders_raw        │
                     │  _id = id_run:number_row_source     │
                     └──────────────────┬──────────────────┘
                                        ▼
                     ┌─────────────────────────────────────┐
                     │   14-Rule Data Cleaning Engine      │
                     │   & Business Validation Classifier  │
                     └──────────┬───────────────────┬──────┘
                                │                   │
                        Valid   │                   │ Invalid
                                ▼                   ▼
             ┌────────────────────────┐       ┌────────────────────────┐
             │MongoDB orders_validated│       │MongoDB quarantine_order│
             │Unique Key: id_order    │       │Error codes + raw record│
             └───────────┬────────────┘       └────────────────────────┘
                         │
        ┌────────────────┼────────────────────────┐
        ▼                ▼                        ▼
┌──────────────┐ ┌───────────────┐      ┌──────────────────┐
│  Analytics   │ │  Aggregation  │      │   Incremental    │
│   Indexes    │ │  5 Pipelines  │      │Materialized Views│
└───────┬──────┘ └───────┬───────┘      └────────┬─────────┘
        │                │                       │
        └────────────────┼───────────────────────┘
                         │
                         ▼
        ┌────────────────────────────────────────┐
        │  APScheduler Background Audit Jobs     │
        └────────────────┬───────────────────────┘
                         │
                         ▼
        ┌────────────────────────────────────────┐
        │     Unified FastAPI Execution API      │
        │        Swagger UI at /docs             │
        └────────────────────────────────────────┘
```

---

## 3. Prerequisites

- **Operating System**: Linux (Ubuntu 20.04+), macOS (12+), or Windows (10/11 64-bit).
- **RAM**: Minimum 8 GB (16 GB recommended for large dataset PySpark processing).
- **Disk Space**: At least 5 GB free disk space.
- **Java**: Java JDK 8 or 11 (required for Apache PySpark). Verify with `java -version`.
- **Network**: Local port `27017` (MongoDB) and port `8000` (FastAPI).

---

## 4. Python Version

- **Required**: Python **3.10** or **3.11** (Python 3.11 recommended).
- Verify installed version:
```bash
python --version
# Output: Python 3.11.x
```

---

## 5. MongoDB Requirements

- **Supported Versions**: MongoDB Community Server **6.0+** or **7.0+**.
- **Default Port**: `27017`.
- **Database Name**: `ecommerce_store` (configured in `.env`).
- **Required Storage Engine**: WiredTiger (standard default).

---

## 6. Installation

### Step 1: Clone the Repository
```bash
git clone <repository_url>
cd <repository_directory>
```

### Step 2: Create and Activate Virtual Environment
- **On Linux / macOS**:
```bash
python3 -m venv .venv
source .venv/bin/activate
```
- **On Windows (PowerShell)**:
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### Step 3: Install Required Dependencies
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

## 7. Environment Configuration

Copy `.env.example` to create your local `.env`:

- **On Linux / macOS**:
```bash
cp .env.example .env
```
- **On Windows**:
```powershell
Copy-Item .env.example .env
```

---

## 8. .env.example

```ini
# MongoDB Connection
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=ecommerce_store

# Pipeline Routing & Performance
SMALL_FILE_THRESHOLD_MB=200
BATCH_SIZE=1000

# Directories
DATA_DIRECTORY=data
REPORT_DIRECTORY=reports

# Classification Performance Tuning
CLASSIFICATION_WORKERS=1
CLASSIFICATION_CHUNK_SIZE=5000
MONGO_WRITE_BATCH_SIZE=5000

# Scheduler & Background Jobs
SCHEDULER_TIMEZONE=UTC
JOB_REFRESH_MV_INTERVAL_MINUTES=60
JOB_REPORT_INTERVAL_MINUTES=360

# FastAPI Server
API_HOST=0.0.0.0
API_PORT=8000
```

---

## 9. requirements.txt

```text
pymongo>=4.6.0
pyspark>=3.5.0
streamlit>=1.30.0
pandas>=2.0.0
plotly>=5.18.0
python-dotenv>=1.0.0
psutil>=5.9.0
pytest>=7.4.0
matplotlib>=3.8.0
apscheduler>=3.10.0
fastapi>=0.110.0
uvicorn>=0.28.0
httpx>=0.27.0
```

---

## 10. How to Start MongoDB

- **Using Docker**:
```bash
docker run -d --name mongodb -p 27017:27017 mongo:7.0
```

- **On Linux (Systemd Service)**:
```bash
sudo systemctl start mongod
sudo systemctl status mongod
```

- **On macOS (Homebrew)**:
```bash
brew services start mongodb-community
```

- **On Windows (Service)**:
```powershell
net start MongoDB
```

Verify connection:
```bash
python -c "from pymongo import MongoClient; print(MongoClient('mongodb://localhost:27017').server_info()['version'])"
```

---

## 11. How to Run the Existing Ingestion Pipeline

The pipeline automatically inspects file size and routes to the appropriate engine:
- Files **<= 200 MB**: Routed to **Python Streaming Batch Loader**.
- Files **> 200 MB**: Routed to **Apache PySpark Distributed Loader**.

Execute via Python CLI:
```bash
python -c "from src.pipeline.pipeline_controller import run_pipeline_for_file; print(run_pipeline_for_file('data/sample_orders.csv'))"
```

---

## 12. How to Run Small Data

For files below the 200 MB threshold:
```bash
python -c "from src.pipeline.elt_pipeline import run_elt_pipeline; print(run_elt_pipeline('data/sample_orders.csv', threshold_mb=200.0))"
```

---

## 13. How to Run Large Data

To force or test large dataset processing through Apache PySpark:
```bash
python -c "from src.pipeline.pipeline_controller import run_pipeline_for_file; print(run_pipeline_for_file('data/large_dataset.csv', threshold_mb=0.001))"
```

---

## 14. How to Run Tests

The repository includes a comprehensive 129-test automated suite covering all 7 phases:

Run all tests:
```bash
pytest -v
```

Run specific test modules:
```bash
# Midterm Ingestion & Idempotency tests
pytest tests/test_router.py tests/test_classifier.py tests/test_idempotency.py -v

# Phase 2: Indexes and Queries
pytest tests/test_final_indexes_and_queries.py -v

# Phase 3: Aggregations
pytest tests/test_final_aggregations.py -v

# Phase 4: Materialized Views
pytest tests/test_final_materialized_views.py -v

# Phase 5: Scheduled Jobs
pytest tests/test_final_scheduler.py -v

# Phase 6: FastAPI Layer
pytest tests/test_final_api.py -v

# Phase 7: Generalization for Unknown Datasets
pytest tests/test_final_generalization.py -v
```

---

## 15. How to Create Indexes

Run index creation programmatically:
```bash
python -c "from src.analytics.indexes import ensure_analytics_indexes; print(ensure_analytics_indexes())"
```

List active indexes in `orders_validated`:
```bash
python -c "from src.analytics.indexes import get_active_indexes; print(get_active_indexes())"
```

---

## 16. How to Run Analytical Queries

Execute queries programmatically through the Query Service:

```python
from src.analytics.queries import (
    get_orders_by_customer,
    get_orders_by_city,
    get_orders_by_status,
    get_orders_by_date_range,
    get_high_value_orders,
    get_orders_by_city_and_status
)

# 1. Orders by customer
orders = get_orders_by_customer(customer_id="CUS-001", limit=10)

# 2. Orders by city
orders_city = get_orders_by_city(city="Tokyo", limit=10)

# 3. Orders by status
orders_status = get_orders_by_status(status="delivered", limit=10)

# 4. Orders in date range
orders_range = get_orders_by_date_range(start_date="2025-01-01", end_date="2025-01-31")

# 5. High-value orders
high_val = get_high_value_orders(min_amount=1000.0, limit=10)
```

CLI Quick Run:
```bash
python -c "from src.analytics.queries import get_high_value_orders; print(len(get_high_value_orders(min_amount=500.0, limit=5)))"
```

---

## 17. How to Run Index Explain Experiments

Compare query execution plans (`COLLSCAN` vs `IXSCAN`) before and after index creation:

```bash
python -c "from src.analytics.indexes import run_explain_experiment; print(run_explain_experiment())"
```
This generates:
- `reports/explain_before_after_indexes.json`
- `reports/explain_before_after_indexes.md`

---

## 18. How to Run Aggregation Reports

Execute any of the 5 production aggregation pipelines:

```python
from src.analytics.reports import (
    report_sales_by_city,
    report_top_products,
    report_top_customers,
    report_sales_by_period,
    report_orders_by_status
)

# 1. Sales by City
print(report_sales_by_city(limit=5))

# 2. Top Products
print(report_top_products(limit=5))

# 3. Top Customers
print(report_top_customers(limit=5))

# 4. Sales by Period (daily or monthly)
print(report_sales_by_period(period="daily", limit=7))

# 5. Orders by Status
print(report_orders_by_status())
```

CLI Quick Run:
```bash
python -c "from src.analytics.reports import report_sales_by_city; print(report_sales_by_city(limit=3))"
```

---

## 19. How to Refresh Materialized Views

Refresh `daily_sales_summary` and `top_products_summary`:

- **Incremental Refresh (Default - processes delta records using watermark)**:
```bash
python -c "from src.views.materialized_views import refresh_materialized_views; print(refresh_materialized_views(full_refresh=False))"
```

- **Full Refresh (Recalculates entire collection from scratch)**:
```bash
python -c "from src.views.materialized_views import refresh_materialized_views; print(refresh_materialized_views(full_refresh=True))"
```

---

## 20. How to Run Scheduled Jobs

Start the background APScheduler process:

```python
from src.scheduler.jobs import start_scheduler, stop_scheduler
import time

start_scheduler()
print("Scheduler running in background. Press Ctrl+C to terminate.")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    stop_scheduler()
```

---

## 21. How to Manually Execute Scheduled Jobs

Execute scheduled jobs directly on demand:

- **Run Materialized View Refresh Job**:
```bash
python -c "from src.scheduler.jobs import run_job; print(run_job('refresh_materialized_views'))"
```

- **Run Periodic Executive Report Job**:
```bash
python -c "from src.scheduler.jobs import run_job; print(run_job('generate_periodic_report'))"
```

Audit execution logs are saved to MongoDB collection `scheduled_job_logs` and `reports/job_logs.json`.

---

## 22. How to Start FastAPI

Run the production ASGI server with Uvicorn:

```bash
python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
```
Or directly:
```bash
python -m src.api.main
```

---

## 23. Swagger URL

Once the server is running, access interactive OpenAPI documentation at:
- **Swagger UI**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **ReDoc**: [http://localhost:8000/redoc](http://localhost:8000/redoc)
- **OpenAPI Schema (JSON)**: [http://localhost:8000/openapi.json](http://localhost:8000/openapi.json)

---

## 24. API Endpoint Examples

### 1. Health Check
```bash
curl -X GET http://localhost:8000/health
```

### 2. Ingest Data File (Calls Midterm Pipeline)
```bash
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{"file_path": "data/sample_orders.csv", "threshold_mb": 200.0, "engine": "auto"}'
```

### 3. Create Analytics Indexes
```bash
curl -X POST http://localhost:8000/indexes
```

### 4. List Available Queries
```bash
curl -X GET http://localhost:8000/queries
```

### 5. Execute Query
```bash
curl -X GET "http://localhost:8000/queries/orders_by_city?city=Tokyo&limit=10"
```

### 6. List Available Aggregations
```bash
curl -X GET http://localhost:8000/aggregations
```

### 7. Execute Aggregation Report
```bash
curl -X GET "http://localhost:8000/aggregations/sales_by_city?limit=5"
```

### 8. Refresh Materialized Views
```bash
curl -X POST http://localhost:8000/refresh-mv \
  -H "Content-Type: application/json" \
  -d '{"full_refresh": false}'
```

### 9. List Scheduled Jobs
```bash
curl -X GET http://localhost:8000/jobs
```

### 10. Run Job Manually
```bash
curl -X POST http://localhost:8000/jobs/refresh_materialized_views/run
```

---

## 25. Expected Response Structure

### Health Check Response (`GET /health`)
```json
{
  "status": "healthy",
  "database": "connected",
  "scheduler": "running",
  "timestamp_utc": "2026-10-04T12:00:00.000000+00:00"
}
```

### Ingestion Response (`POST /ingest`)
```json
{
  "status": "SUCCESS",
  "message": "Successfully ingested 'sample_orders.csv'",
  "metrics": {
    "read_rows": 1000,
    "valid_rows": 920,
    "quarantined_rows": 80,
    "inserted": 920,
    "updated": 0,
    "unchanged": 0
  }
}
```

### Query Execution Response (`GET /queries/{name}`)
```json
{
  "query_name": "orders_by_city",
  "count": 10,
  "parameters": {
    "city": "Tokyo",
    "limit": 10,
    "skip": 0
  },
  "records": [
    {
      "id_order": "ORD-001",
      "status": "delivered",
      "total_amount": 1250.0,
      "customer": {
        "customer_id": "CUS-100",
        "name": "Kenji Sato",
        "address": {"city": "Tokyo"}
      }
    }
  ]
}
```

### Structured Error Response (`404 Not Found`)
```json
{
  "detail": "Query 'unknown_query' not found. Available queries: orders_by_customer, orders_by_city, orders_by_status, orders_by_date_range, high_value_orders, orders_by_city_and_status"
}
```

---

## 26. Troubleshooting

| Issue | Root Cause | Solution |
| :--- | :--- | :--- |
| `ServerSelectionTimeoutError: localhost:27017` | MongoDB server is not running | Start MongoDB service (`docker run ...` or `net start MongoDB`). Verify connection with `mongosh`. |
| `JAVA_HOME is not set` when running PySpark | Java JDK is missing or path not configured | Install OpenJDK 8 or 11. Set `JAVA_HOME` environment variable to JDK installation path. |
| `400 Bad Request: Target file not found` on `/ingest` | Relative path not resolvable from server cwd | Provide full relative path from repository root (e.g., `data/sample_orders.csv`) or absolute path. |
| `TypeError: Query parameters missing` | Required parameter not passed | Review endpoint parameters in `GET /queries` or Swagger UI (`/docs`). |
| `Port 8000 already in use` | Another process is using port 8000 | Specify alternate port: `python -m uvicorn src.api.main:app --port 8080`. |

---

## 27. Project Structure

```text
.
├── config/
│   ├── __init__.py
│   └── settings.py               # Externalized pipeline, database & scheduler settings
├── data/
│   └── sample_orders.csv         # Sample dataset for ingestion testing
├── reports/
│   ├── explain_before_after_indexes.json
│   ├── explain_before_after_indexes.md
│   ├── scheduled_periodic_report.json
│   └── job_logs.json
├── src/
│   ├── analytics/
│   │   ├── indexes.py            # Analytical indexes and before/after explain experiments
│   │   ├── queries.py            # 6 Practical dynamic queries & query registry
│   │   └── reports.py            # 5 Production MongoDB aggregation pipelines
│   ├── api/
│   │   ├── __init__.py
│   │   ├── main.py               # Thin FastAPI layer over existing services
│   │   └── schemas.py            # Pydantic v2 validation models
│   ├── cleaning/
│   │   ├── cleaner.py            # 14-rule deterministic data cleaning
│   │   └── rules.py
│   ├── classification/
│   │   └── classifier.py         # 3-way record classifier (Valid / Corrected / Quarantine)
│   ├── incremental/
│   │   └── incremental_loader.py # Watermark & delta detection service
│   ├── ingestion/
│   │   ├── batch_loader.py       # Python streaming batch loader (small datasets)
│   │   └── spark_loader.py       # Apache PySpark distributed loader (large datasets)
│   ├── monitoring/
│   │   └── metrics.py            # Pipeline performance & operational metrics
│   ├── mongodb/
│   │   ├── mongo_setup.py        # Centralized MongoDB connection factory
│   │   └── repositories.py       # Raw, Validated, and Quarantine collections access
│   ├── pipeline/
│   │   ├── elt_pipeline.py       # Core ELT pipeline execution controller
│   │   └── pipeline_controller.py# Dynamic file router and pipeline runner
│   ├── routing/
│   │   └── file_router.py        # 200 MB threshold engine router
│   ├── scheduler/
│   │   └── jobs.py               # APScheduler background tasks & manual execution runner
│   ├── validation/
│   │   └── validator.py          # Business rules & schema validation
│   └── views/
│       └── materialized_views.py # Incremental $merge Materialized Views engine
├── tests/
│   ├── test_batch_failure.py
│   ├── test_checkpoint_recovery.py
│   ├── test_classifier.py
│   ├── test_cleaning_rules.py
│   ├── test_elt_pipeline.py
│   ├── test_final_aggregations.py
│   ├── test_final_api.py
│   ├── test_final_generalization.py
│   ├── test_final_indexes_and_queries.py
│   ├── test_final_materialized_views.py
│   ├── test_final_scheduler.py
│   ├── test_idempotency.py
│   ├── test_quarantine_key.py
│   ├── test_router.py
│   ├── test_spark_loader_idempotency.py
│   └── test_validator.py
├── .env.example                  # Environment configuration template
├── pytest.ini                    # Pytest configuration
├── requirements.txt              # Production and testing dependencies
└── README.md                     # Comprehensive project documentation & run guide
```
