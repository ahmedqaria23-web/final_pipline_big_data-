# Performance & Scalability Design

**Project**: Big Data Midterm Pipeline  
**Date**: 2026-09-02

---

## Design Philosophy

The pipeline is designed to be **engine-agnostic** with respect to performance:

- **Small files (≤ 200 MB)**: Python Streaming Batch — minimal overhead, crash-safe checkpoints, high per-batch throughput
- **Large files (> 200 MB)**: PySpark Distributed Engine — parallel partitioned reads, Spark-native MongoDB writes, no Python driver bottleneck

The threshold is configurable via `SMALL_FILE_THRESHOLD_MB` in `config/.env`.

---

## Python Batch Performance Design

### Streaming Architecture
- Uses `csv.DictReader` + `yield` generator — never loads full file into RAM
- Records are collected in memory only up to `BATCH_SIZE` (default: 1000)
- Each batch is written via `pymongo.BulkWrite` and checkpointed

### Crash Resumability
- Persistent checkpoints saved to `meta_state` collection after every batch
- On restart, pipeline resumes from last completed batch (skips already-processed rows)
- `status = "COMPLETED"` checkpoint enables instant fast-return on identical re-runs

### Throughput Measurements (Real Data)

| Metric              | Value              |
|---------------------|--------------------|
| Average throughput  | ~3,224 records/sec |
| Min throughput      | ~2,900 records/sec |
| Max throughput      | ~3,861 records/sec |
| Records per run     | 3,000              |
| Batch size          | 1,000              |
| Consistency rate    | 100% (101/101 runs)|

---

## PySpark Performance Design

### Fixed Schema for Performance
Using an explicit `StructType` schema avoids the schema inference scan (which reads the entire file twice). This reduces startup overhead significantly:

```python
schema = StructType([
    StructField("id_order",      StringType(), True),
    StructField("order_date",    StringType(), True),
    # ... 14 more fields
])
df = spark.read.schema(schema).option("header", "true").csv(path)
```

### Partitioning Strategy
- Spark auto-partitions based on HDFS block size (default: 128 MB)
- `local[*]` mode uses all available CPU cores
- No manual `repartition()` — Spark determines optimal partition count

### MongoDB Spark Connector Settings
```python
.option("operationType", "replace")     # upsert semantics
.option("upsertDocument", "true")
.option("idFieldList", "_id")           # match on deterministic _id
.option("batchSize", "80000")           # high-throughput batch writes
.option("ordered", "false")             # parallel unordered writes
```

---

## Parallelism Evidence

When PySpark runs, the Spark Web UI (http://localhost:4040) shows:
- **Input Partitions**: typically 1-8 for local mode, based on file size
- **Jobs**: 1 per write action
- **Tasks**: 1 per partition (parallel execution)
- **No driver bottleneck**: data flows Spark → MongoDB Connector directly

---

## Classification Parallelism (Post-Raw)

The quality classification step supports parallel processing:

```python
# config/settings.py
CLASSIFICATION_WORKERS = int(os.getenv("CLASSIFICATION_WORKERS", "1"))
CLASSIFICATION_CHUNK_SIZE = int(os.getenv("CLASSIFICATION_CHUNK_SIZE", "5000"))
```

With `CLASSIFICATION_WORKERS > 1`, `ProcessPoolExecutor` distributes classification across CPU cores.

---

## Scaling Recommendations

| Dataset Size  | Recommended Engine    | Batch/Partition Config              |
|---------------|-----------------------|-------------------------------------|
| < 10 MB       | python_batch          | BATCH_SIZE=500                      |
| 10-200 MB     | python_batch          | BATCH_SIZE=1000-5000                |
| 200 MB - 2 GB | pyspark               | Default Spark partitioning          |
| > 2 GB        | pyspark + cluster     | Set `spark.executor.instances` > 1  |
