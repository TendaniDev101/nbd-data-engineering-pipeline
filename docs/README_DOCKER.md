# Docker Base Image — Nedbank DE Challenge

This document explains how to use the `nedbank-de-challenge/base:1.0` image as the foundation for your submission.

---

## What is in the base image

| Component | Version |
|---|---|
| Python | 3.11 (slim-bookworm) |
| Java | OpenJDK 17 (default-jdk-headless) |
| PySpark | 3.5.0 |
| Delta Lake (delta-spark) | 3.1.0 |
| pandas | 2.1.0 |
| PyArrow | 14.0.0 |
| PyYAML | 6.0.1 |
| DuckDB | 0.10.0 |

Do not upgrade `delta-spark` or `pyarrow` independently — they have known breaking-change interactions with each other at these pinned versions.

---

## How to extend this image

Your submission must include a `Dockerfile` at the root of your repository. The minimum required content is:

```dockerfile
FROM nedbank-de-challenge/base:1.0

# Copy your pipeline code
COPY pipeline/ /app/pipeline/
COPY config/   /data/config/

# Install any additional dependencies (keep this lean — you have 15 min to execute)
# RUN pip install --no-cache-dir <your-extra-packages>
```

Your `Dockerfile` must **not** redefine `WORKDIR`, `JAVA_HOME`, `SPARK_HOME`, or `PYSPARK_PYTHON` unless you have a specific reason and have tested thoroughly.

The scoring system invokes your container as:

```bash
docker run \
  --network=none \
  --cap-drop=ALL \
  --pids-limit=512 \
  --read-only \
  --tmpfs /tmp \
  --memory=2g \
  --cpus="2" \
  --volume /challenge/input:/data/input:ro \
  --volume /challenge/config:/data/config:ro \
  --volume /challenge/stream:/data/stream:ro \
  --volume /challenge/output:/data/output:rw \
  <your-image-tag> \
  python pipeline/run_all.py
```

Your pipeline entry point must be `pipeline/run_all.py`.

---

## How to build and run locally

### Build the base image

```bash
docker build -t nedbank-de-challenge/base:1.0 -f Dockerfile.base .
```

### Verify the base image

```bash
docker run --rm nedbank-de-challenge/base:1.0 python -c \
  "import pyspark; import delta; print('PySpark:', pyspark.__version__); print('OK')"
# Expected output: PySpark: 3.5.0\nOK
```

### Build your submission image

```bash
docker build -t my-submission:latest .
```

### Run locally with simulated mount points

```bash
docker run --rm \
  --memory=2g \
  --cpus="2" \
  -v $(pwd)/data/input:/data/input:ro \
  -v $(pwd)/data/config:/data/config:ro \
  -v $(pwd)/data/stream:/data/stream:ro \
  -v $(pwd)/data/output:/data/output:rw \
  my-submission:latest \
  python pipeline/run_all.py
```

The `--memory=2g --cpus="2"` flags mirror the scoring system constraints exactly. Test under these constraints before submitting.

---

## Resource constraints

Your container runs inside the following hard limits. These are enforced by the scoring system and cannot be negotiated.

| Resource | Limit | Notes |
|---|---|---|
| RAM | 2 GB | Applies to the entire container, including the JVM heap for Spark |
| CPU | 2 vCPU | Spark local mode only — no YARN, no cluster |
| Execution timeout | 15 minutes | Container is killed with SIGKILL at 15 min (exit code 137) |
| Network | None | `--network=none` — no outbound or inbound connections |
| Filesystem | Read-only (root) | Only `/data/output` and `/tmp` are writable |

**Practical implications:**

- PySpark's JVM will consume a significant share of the 2 GB limit. Tune `spark.driver.memory` in your SparkSession to leave headroom for Python workers (recommend no more than 1 GB for the driver).
- pandas operations on the full Stage 2 dataset volume will likely fail under the 2 GB constraint. Use PySpark or DuckDB for large transformations, and only collect to pandas for final aggregations on small result sets.
- All temporary Spark shuffle and checkpoint data must go to `/tmp` (the only tmpfs mount). If your pipeline generates large shuffle, reduce partition sizes or repartition aggressively.
- The 15-minute clock starts when the container process starts, not when your pipeline logic begins. Minimise import overhead.

---

## Mount points and their purposes

| Mount path | Access | Purpose |
|---|---|---|
| `/data/input` | Read-only | Challenge datasets: `customers.csv`, `accounts.csv`, `transactions.jsonl` |
| `/data/config` | Read-only | Your pipeline config: `pipeline_config.yaml`; Stage 2 also: `dq_rules.yaml` |
| `/data/stream` | Read-only | Stage 3 streaming batch files (JSONL micro-batches) |
| `/data/output` | Read-write | All pipeline outputs — bronze, silver, gold Parquet; DQ report; stream gold |
| `/tmp` | Read-write (tmpfs) | Spark scratch space only — do not write final outputs here |

Your pipeline must write outputs to the following paths inside `/data/output`:

```
/data/output/bronze/          # Raw ingested data as Parquet
/data/output/silver/          # Cleaned / validated data as Parquet
/data/output/gold/            # Final aggregated business views as Parquet
/data/output/stream_gold/     # Stage 3 streaming aggregations as Parquet
```

The scoring system reads from these exact paths. Any output written elsewhere will not be scored.

---

## Exit code requirements

Your container must exit with one of the following codes:

| Exit code | Meaning | Scoring outcome |
|---|---|---|
| `0` | Pipeline completed successfully | Outputs are evaluated for correctness |
| `1` | Pipeline error (unhandled exception or explicit failure) | Zero score for that stage |
| `124` | Timeout (sent by scoring system wrapper) | Zero score; pipeline was too slow |
| `137` | Out-of-memory kill (OOM) | Zero score; pipeline used too much memory |

Any non-zero exit code results in a zero score for the run. Ensure your `pipeline/run_all.py` script explicitly calls `sys.exit(0)` on successful completion (Python exits with 0 by default, but be explicit).

Do not use `try/except` to suppress genuine errors — a silent failure that writes corrupt outputs scores worse than a clean exit code `1`.

---

## Submission checklist

Before tagging your submission commit, confirm:

- [ ] `docker build` exits with code `0`
- [ ] `docker run` (with all four volume mounts) exits with code `0`
- [ ] All four output directories contain Parquet files
- [ ] Pipeline completes within 15 minutes under `--memory=2g --cpus="2"` constraints
- [ ] `pipeline_config.yaml` is present in your `config/` directory
- [ ] `requirements.txt` lists any packages you added beyond the base image
- [ ] Your `Dockerfile` starts with `FROM nedbank-de-challenge/base:1.0`
