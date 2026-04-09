# Resource Constraints

**Document:** `resource_constraints.md`
**Version:** 1.0
**Status:** Final

Your pipeline runs inside a Docker container with hard resource limits enforced by the scoring system. These are not soft suggestions. Exceeding any limit produces a non-zero exit code, which results in zero correctness points for that stage.

---

## 1. Constraints Summary

| Resource | Limit | Enforcement |
|---|---|---|
| RAM | 2 GB hard ceiling | `--memory=2g --memory-swap=2g`; kernel OOM-kills the container at limit |
| CPU | 2 vCPU | `--cpus=2`; Docker cgroups enforcement |
| Wall-clock time | 30 minutes | External `timeout 1800` wrapper; container killed at T+1800s |
| Network | None | `--network=none`; blocked at the kernel level |
| Writable filesystem | `/data/output/` and `/tmp` only | `--read-only` with tmpfs for `/tmp` |
| Temporary disk | 512 MB (`/tmp`) | `--tmpfs /tmp:rw,size=512m` |

---

## 2. RAM: 2 GB

**What it means:** The container is allocated 2 GB of RAM plus 2 GB of swap (swap disabled — `--memory-swap=2g` equals `--memory` meaning no additional swap). If your pipeline's resident memory exceeds 2 GB, the kernel kills the container with exit code `137` (OOM kill).

**Why this limit exists:** Stage 2 data is approximately 2.4 GB on disk (3 million transaction records) — this **exceeds** the 2 GB RAM allocation. It is physically impossible to load all records into memory simultaneously. This constraint makes streaming, partitioned, or lazy-evaluation processing **mandatory**, not merely recommended. Pipelines must process data incrementally (e.g., chunked reads, Spark partitioned scans, or DuckDB out-of-core execution) to avoid immediate OOM termination.

**What works within 2 GB:**
- PySpark in `local[2]` mode with a 1 GB executor heap (`spark.executor.memory=1g`). The JVM overhead and driver memory together with the executor fit within 2 GB on well-partitioned workloads, but leave very little headroom — partition sizes must be kept small.
- Pandas operations on small subsets of data — reading one table at a time, processing in chunks, writing before loading the next. At Stage 2 scale, even a single table may not fit in memory; chunk-based reading is mandatory.
- DuckDB for SQL-based transformations — DuckDB is memory-efficient and handles data larger than RAM using spill-to-disk (note: `/tmp` is limited to 512 MB, so very large spill operations may fail).

**What does not work within 2 GB:**
- `pandas.read_json()` on the full 2.25 GB Stage 2 JSONL file. This will allocate several multiples of the file size in memory and OOM.
- `spark_df.toPandas()` on a large Spark DataFrame. This collects all data to the driver.
- Loading all three source tables into Pandas DataFrames simultaneously at Stage 2 scale.

**Configure your SparkSession correctly:**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("nedbank-de-pipeline") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "512m") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```

**Monitor peak memory during local testing:**

```bash
docker stats --no-stream <container_id>
```

Run this in a second terminal while your pipeline executes. Aim to stay below 1.6 GB (80% of allocation) — this earns additional efficiency points.

---

## 3. CPU: 2 vCPU

**What it means:** Your container is allocated 2 virtual CPU cores. Docker enforces this via cgroups. Spark should be configured to use `local[2]` to match the allocation.

**What it means in practice:** Spark parallelism is limited to 2 concurrent tasks. Partitioning your DataFrames to 2–4 partitions is typically optimal. Over-partitioning (e.g., 200 default Spark shuffle partitions) adds scheduling overhead with no parallelism benefit.

**Recommended Spark configuration:**

```python
.config("spark.default.parallelism", "2") \
.config("spark.sql.shuffle.partitions", "4")
```

---

## 4. Wall-Clock Time: 30 Minutes

**What it means:** Your pipeline must start, process all data, write all outputs, and exit with code `0` within 1800 seconds of the container starting. The scoring system wraps the `docker run` invocation with a timeout; when the timer expires, the container is killed and receives exit code `124`.

**Why this limit exists:** The 30-minute limit is calibrated so that a well-optimised PySpark pipeline can process Stage 2 data (3 million transactions, 300,000 accounts, 240,000 customers) comfortably. The constraint distinguishes efficient pipelines from inefficient ones. Execution time is a scored dimension — pipelines in the lowest quartile of the cohort receive additional scalability points.

**What causes timeouts:**
- Redundant full-dataset scans (reading the same source file multiple times)
- Unnecessary `.collect()` on large DataFrames followed by Python-level processing
- Unoptimised joins that trigger expensive shuffle operations
- Inefficient Delta write configurations (too many small files, excessive compaction)

**Test locally:**

```bash
time docker run --rm --network=none --memory=2g --cpus=2 \
  -v /path/to/your/data:/data \
  my-submission:test
```

If this completes well under 30 minutes on the provided sample data (10,000 rows), profile your pipeline on the full Stage 1 data (1 million transactions) before submitting. Stage 2 volume is 3× Stage 1.

---

## 5. Network: None

**What it means:** The container has no network access during execution. DNS resolution fails. HTTP requests fail. Connections to external services (including PyPI, Hugging Face, any API) fail.

**What this means in practice:** All Python packages must be installed during the Docker `build` phase, not at runtime. Your `requirements.txt` and `Dockerfile` must contain everything your pipeline needs.

**Common failure pattern:**

```python
# This fails at runtime — PyPI is unreachable
import subprocess
subprocess.run(["pip", "install", "some-package"])
```

**Checking for runtime installs:**

```bash
grep -r "pip install\|subprocess.*pip\|os.system.*pip" pipeline/
```

Any result here is a runtime dependency that will fail in the scoring container.

---

## 6. Writable Filesystem

**What it means:** The container filesystem is read-only (`--read-only`). Your pipeline can only write to two locations:

- `/data/output/` — mounted from the host; this is where all pipeline outputs go
- `/tmp` — 512 MB in-memory tmpfs; available for intermediate files, Spark shuffle spill, or working data

**What this means in practice:**
- Spark's default temp directory (`/tmp/spark-*`) works within the 512 MB `/tmp` allocation. For large Spark jobs, spill-to-disk may exhaust this. Configure Spark's temp directory explicitly and monitor usage.
- Writing intermediate results to `/data/output/` and then reading them back is permitted — but counts against your execution time.
- Attempting to write to `/app/`, `/root/`, or any other path outside `/data/output/` and `/tmp` will raise a `PermissionError` and likely crash your pipeline.

**Safe temporary file pattern:**

```python
import tempfile
import os

# Always use /tmp explicitly for temporary files
with tempfile.NamedTemporaryFile(dir="/tmp", suffix=".parquet", delete=True) as f:
    ...
```

---

## 7. Tips for Staying Within Limits

1. **Test with production-scale flags from the beginning.** Running `docker run -m 2g --cpus=2` locally on Stage 1 data will surface memory and timing problems before you encounter them in the scorer.

2. **Use Spark lazy evaluation.** Avoid materialising DataFrames to Pandas until absolutely necessary, and only on small result sets.

3. **Read each source file once.** Cache a DataFrame if you need to reference it multiple times. Reading the same JSONL file twice at Stage 2 scale costs roughly 2 minutes of wall-clock time.

4. **Partition appropriately.** For 2-core execution, 2–4 output partitions is generally optimal. `spark.sql.shuffle.partitions=200` (the Spark default) is wasteful at this scale.

5. **Monitor during development.** Use `docker stats` to track memory and CPU utilisation during local test runs. The scorer captures peak memory — pipelines that stay below 1.6 GB (80% of allocation) receive efficiency credit.

6. **Install everything at build time.** Run `docker build` after any `requirements.txt` change and verify the image builds cleanly before pushing your tag.
