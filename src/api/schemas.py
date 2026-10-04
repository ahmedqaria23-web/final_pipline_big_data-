"""
Pydantic Request & Response Schemas for the FastAPI service layer.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = Field(..., json_schema_extra={"example": "healthy"})
    database: str = Field(..., json_schema_extra={"example": "connected"})
    scheduler: str = Field(..., json_schema_extra={"example": "running"})
    timestamp_utc: str


class IngestRequest(BaseModel):
    file_path: str = Field(..., description="Absolute or relative path to CSV data file", json_schema_extra={"example": "data/sample_orders.csv"})
    threshold_mb: Optional[float] = Field(200.0, description="Routing threshold between Python Batch and PySpark in MB")
    engine: Optional[str] = Field("auto", description="'auto', 'python_batch', or 'pyspark'")


class RefreshMvRequest(BaseModel):
    full_refresh: Optional[bool] = Field(False, description="Set True for complete recalculation, False for incremental delta")


class IndexesResponse(BaseModel):
    status: str
    created_indexes: List[str]
    active_indexes_count: int


class QueryDescriptorResponse(BaseModel):
    name: str
    description: str
    parameters: List[Dict[str, Any]]


class AggregationDescriptorResponse(BaseModel):
    name: str
    description: str
    parameters: List[Dict[str, Any]]


class JobRunResponse(BaseModel):
    job_name: str
    trigger_type: str
    status: str
    duration_seconds: float
    start_time: str
    end_time: str
    details: Optional[Any] = None
    error: Optional[str] = None


class ErrorResponse(BaseModel):
    error: str
    detail: str
