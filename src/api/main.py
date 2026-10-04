"""
FastAPI Unified Execution Interface.
Thin REST API layer delegating directly to existing project services.
Never duplicates business or ingestion logic.
"""

import sys
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from fastapi import FastAPI, HTTPException, Request, status, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from src.mongodb.mongo_setup import get_mongo_client, get_mongo_db
from src.pipeline.pipeline_controller import run_pipeline_for_file
from src.analytics.indexes import create_analytics_indexes, list_indexes
from src.analytics.queries import (
    QUERY_REGISTRY,
    list_registered_queries,
    execute_registered_query
)
from src.analytics.reports import (
    AGGREGATION_REGISTRY,
    list_registered_aggregations,
    execute_registered_aggregation
)
from src.views.materialized_views import refresh_materialized_views
from src.scheduler.jobs import (
    REGISTERED_JOBS,
    list_jobs,
    run_job,
    start_scheduler,
    stop_scheduler,
    get_scheduler
)
from src.api.schemas import (
    HealthResponse,
    IngestRequest,
    RefreshMvRequest,
    IndexesResponse,
    QueryDescriptorResponse,
    AggregationDescriptorResponse,
    JobRunResponse,
    ErrorResponse
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages background scheduler lifecycle cleanly with the FastAPI app."""
    try:
        start_scheduler()
        logger.info("FastAPI Lifespan: APScheduler initialized and running.")
    except Exception as e:
        logger.warning(f"Could not automatically start scheduler in lifespan: {e}")
    yield
    try:
        stop_scheduler()
        logger.info("FastAPI Lifespan: APScheduler cleanly terminated.")
    except Exception as e:
        logger.warning(f"Error stopping scheduler in lifespan: {e}")


app = FastAPI(
    title="Big Data Platform Unified Execution API",
    description="Unified REST API layer for End-to-End Ingestion, Analytics Queries, Aggregation Reports, Materialized Views, and Scheduled Jobs.",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Enable CORS for frontend and dashboard integrations
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────────────────────
# 1. GET /health
# ─────────────────────────────────────────────────────────────
@app.get(
    "/health",
    response_model=HealthResponse,
    tags=["System"],
    summary="System Health & Database Liveness Check"
)
def get_health():
    """Checks MongoDB liveness and background scheduler state."""
    db_status = "disconnected"
    try:
        client = get_mongo_client()
        client.admin.command("ping")
        db_status = "connected"
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database service unavailable: {str(e)}"
        )

    sched = get_scheduler()
    sched_status = "running" if (sched and sched.running) else "stopped"

    return {
        "status": "healthy",
        "database": db_status,
        "scheduler": sched_status,
        "timestamp_utc": datetime.now(timezone.utc).isoformat()
    }


# ─────────────────────────────────────────────────────────────
# 2. POST /ingest
# ─────────────────────────────────────────────────────────────
@app.post(
    "/ingest",
    status_code=status.HTTP_201_CREATED,
    tags=["Ingestion"],
    summary="Ingest Dataset using Existing Midterm Pipeline"
)
def post_ingest(request_body: IngestRequest):
    """
    Delegates strictly to existing run_pipeline_for_file controller.
    Preserves File Router, Raw, Cleaning, Validation, and MongoDB Persistence.
    """
    file_path = request_body.file_path
    path_obj = Path(file_path)
    if not path_obj.exists():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Target file not found at path: {file_path}"
        )

    engine = request_body.engine or "auto"
    threshold = request_body.threshold_mb or 200.0

    try:
        metrics = run_pipeline_for_file(
            file_path=str(path_obj),
            threshold_mb=threshold,
            engine=engine
        )
        return {
            "status": "SUCCESS",
            "message": f"Successfully ingested '{path_obj.name}'",
            "metrics": metrics
        }
    except Exception as e:
        logger.error(f"Ingestion failed via API: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Pipeline execution failure: {str(e)}"
        )


# ─────────────────────────────────────────────────────────────
# 3. POST /indexes
# ─────────────────────────────────────────────────────────────
@app.post(
    "/indexes",
    response_model=IndexesResponse,
    tags=["Indexes"],
    summary="Create & Verify Analytics Indexes"
)
def post_indexes():
    """Creates the 3 analytics indexes on orders_validated and returns active indexes."""
    try:
        created = create_analytics_indexes()
        all_indexes = list_indexes()
        return {
            "status": "SUCCESS",
            "created_indexes": created,
            "active_indexes_count": len(all_indexes)
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Index creation failed: {str(e)}"
        )


# ─────────────────────────────────────────────────────────────
# 4. GET /queries & GET /queries/{name}
# ─────────────────────────────────────────────────────────────
@app.get(
    "/queries",
    response_model=List[QueryDescriptorResponse],
    tags=["Queries"],
    summary="List Registered Dynamic Queries"
)
def get_queries():
    """Returns dynamic registry descriptors for all available queries."""
    return list_registered_queries()


@app.get(
    "/queries/{name}",
    tags=["Queries"],
    summary="Execute Specific Registered Query"
)
def execute_query(name: str, request: Request):
    """
    Executes a registered query by name using dynamic URL query parameters.
    """
    if name not in QUERY_REGISTRY:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Query '{name}' not found. Available queries: {list(QUERY_REGISTRY.keys())}"
        )

    # Convert query parameters from request
    raw_params = dict(request.query_params)
    converted_params = {}

    # Cast integer and float types based on descriptor
    q_meta = QUERY_REGISTRY[name]
    expected_param_types = {p["name"]: p.get("type", "str") for p in q_meta.get("parameters", [])}

    for k, v in raw_params.items():
        expected_type = expected_param_types.get(k, "str")
        try:
            if expected_type == "int":
                converted_params[k] = int(v)
            elif expected_type == "float":
                converted_params[k] = float(v)
            else:
                converted_params[k] = v
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid parameter type for '{k}': expected {expected_type}, got '{v}'"
            )

    try:
        results = execute_registered_query(name, converted_params)
        return {
            "query_name": name,
            "count": len(results),
            "data": results
        }
    except ValueError as ve:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(ve)
        )
    except Exception as e:
        logger.error(f"Error executing query '{name}': {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal database query error: {str(e)}"
        )


# ─────────────────────────────────────────────────────────────
# 5. GET /aggregations & GET /aggregations/{name}
# ─────────────────────────────────────────────────────────────
@app.get(
    "/aggregations",
    response_model=List[AggregationDescriptorResponse],
    tags=["Aggregations"],
    summary="List Registered Aggregation Reports"
)
def get_aggregations():
    """Returns dynamic registry descriptors for all available aggregation reports."""
    return list_registered_aggregations()


@app.get(
    "/aggregations/{name}",
    tags=["Aggregations"],
    summary="Execute Specific Aggregation Report"
)
def execute_aggregation(name: str, request: Request):
    """
    Executes a registered aggregation report by name using dynamic URL query parameters.
    """
    if name not in AGGREGATION_REGISTRY:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Aggregation '{name}' not found. Available reports: {list(AGGREGATION_REGISTRY.keys())}"
        )

    raw_params = dict(request.query_params)
    converted_params = {}

    agg_meta = AGGREGATION_REGISTRY[name]
    expected_param_types = {p["name"]: p.get("type", "str") for p in agg_meta.get("parameters", [])}

    for k, v in raw_params.items():
        expected_type = expected_param_types.get(k, "str")
        try:
            if expected_type == "int":
                converted_params[k] = int(v)
            elif expected_type == "float":
                converted_params[k] = float(v)
            else:
                converted_params[k] = v
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid parameter type for '{k}': expected {expected_type}, got '{v}'"
            )

    try:
        results = execute_registered_aggregation(name, converted_params)
        return {
            "aggregation_name": name,
            "count": len(results),
            "data": results
        }
    except ValueError as ve:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(ve)
        )
    except Exception as e:
        logger.error(f"Error executing aggregation '{name}': {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal database aggregation error: {str(e)}"
        )


# ─────────────────────────────────────────────────────────────
# 6. POST /refresh-mv
# ─────────────────────────────────────────────────────────────
@app.post(
    "/refresh-mv",
    tags=["Materialized Views"],
    summary="Trigger Materialized Views Refresh"
)
def post_refresh_mv(body: Optional[RefreshMvRequest] = None):
    """
    Refreshes daily_sales_summary and top_products_summary using $merge and CDC watermark.
    """
    full_refresh = body.full_refresh if body else False
    try:
        result = refresh_materialized_views(full_refresh=full_refresh)
        return result
    except Exception as e:
        logger.error(f"Error during Materialized View refresh: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Materialized view refresh error: {str(e)}"
        )


# ─────────────────────────────────────────────────────────────
# 7. GET /jobs & POST /jobs/{name}/run
# ─────────────────────────────────────────────────────────────
@app.get(
    "/jobs",
    tags=["Scheduler"],
    summary="List Scheduled Background Jobs"
)
def get_jobs():
    """Lists registered scheduled jobs and their current execution status."""
    try:
        return list_jobs()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to query scheduled jobs: {str(e)}"
        )


@app.post(
    "/jobs/{name}/run",
    response_model=JobRunResponse,
    tags=["Scheduler"],
    summary="Manually Trigger a Scheduled Job"
)
def post_run_job(name: str):
    """
    Manually triggers a registered job using the exact same function as scheduled triggers.
    Audits execution details into MongoDB.
    """
    if name not in REGISTERED_JOBS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{name}' not found. Available jobs: {list(REGISTERED_JOBS.keys())}"
        )

    try:
        result = run_job(name, trigger_type="manual")
        return result
    except Exception as e:
        logger.error(f"Job execution failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Job execution failed: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    print("Starting Big Data Platform API on http://0.0.0.0:8000 (Swagger: http://localhost:8000/docs)")
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=True)
