# Docker Interface Contract

**Document:** `docker_interface_contract.md`
**Version:** 1.0
**Status:** Final

This document is the authoritative specification of the interface between your Docker image and the automated scoring system. The scoring system is built to this contract. If the contract and your submission conflict, your submission will be scored as a failure.

---

## 1. Base Image

Your `Dockerfile` must extend the challenge base image:

```dockerfile
FROM nedbank-de-challenge/base:1.0
```

The base image is based on `python:3.11-slim-bookworm` and includes:
- OpenJDK (headless) with `JAVA_HOME` configured
- PySpark 3.5.0
- delta-spark 3.1.0
- pandas 2.1.0
- pyarrow 14.0.0
- pyyaml 6.0.1

Pull it before building your image:

```bash
docker pull nedbank-de-challenge/base:1.0
```

---

## 2. Extending the Base Image

A minimal working `Dockerfile` looks like this:

```dockerfile
FROM nedbank-de-challenge/base:1.0

# Copy your pipeline code and config
COPY pipeline/ /app/pipeline/
COPY config/ /app/config/
COPY requirements.txt /app/

# Install any additional dependencies beyond the base image
# (leave requirements.txt empty if you have none)
RUN pip install --no-cache-dir -r /app/requirements.txt

# Entry point — must run the complete pipeline end-to-end without interactive input
CMD ["python", "pipeline/run_all.py"]
```

**Requirements for your Dockerfile:**
- `CMD` or `ENTRYPOINT` must run the complete pipeline without any interactive input.
- All Python dependencies must be installable from PyPI. Private indices and local path installs are not supported.
- Do not pre-populate `/data/output/` in the image. Output directories are created by your pipeline at runtime.
- Do not bake credentials or secrets into the image.

---

## 3. Mount Points

The scoring system mounts a single host directory to `/data` inside the container. All input and output paths live under `/data/`.

### Input (read-only)

| Mount path | Contents | Stage |
|---|---|---|
| `/data/input/accounts.csv` | Fintech account data (CSV) | All stages |
| `/data/input/transactions.jsonl` | Fintech transaction data (JSONL) | All stages |
| `/data/input/customers.csv` | Bank customer data (CSV) | All stages |
| `/data/config/pipeline_config.yaml` | Runtime configuration; the scoring system may inject overrides | All stages |
| `/data/stream/` | Directory of micro-batch JSONL files, arriving during execution | Stage 3 only |

Your pipeline must not write to any input path. These mounts are enforced read-only by the scoring system (`--read-only` with explicit output exceptions).

### Output (read-write)

| Mount path | Contents | Stage |
|---|---|---|
| `/data/output/bronze/` | Bronze layer Delta tables | All stages |
| `/data/output/silver/` | Silver layer Delta tables | All stages |
| `/data/output/gold/` | Gold layer Delta tables (scored) | All stages |
| `/data/output/dq_report.json` | Data quality summary report | Stage 2+ |
| `/data/output/stream_gold/` | Streaming gold tables | Stage 3 only |

Your pipeline is responsible for creating these directories. They do not exist at container start.

The only writable paths are `/data/output/` and `/tmp` (see Section 6). All other filesystem locations are read-only.

---

## 4. Invocation Command

The scoring system invokes your container as follows:

```bash
docker run \
  --rm \
  --name "scoring_${PARTICIPANT_ID}_s${STAGE}" \
  --network=none \
  --memory=2g --memory-swap=2g \
  --cpus=2 \
  --pids-limit=512 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  --cap-drop=ALL \
  --security-opt no-new-privileges \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -v "${DATA_DIR}:/data" \
  candidate-submission:latest
```

Key flags:
- `--network=none` — No internet access during execution.
- `--read-only` — The container filesystem is read-only except for `/data/output/` and `/tmp`.
- `--tmpfs /tmp:rw,size=512m` — `/tmp` is available as a writable in-memory tmpfs, limited to 512 MB.
- `--cap-drop=ALL` — All Linux capabilities dropped.
- `--memory=2g --memory-swap=2g` — Hard memory ceiling (see Section 5).
- `--cpus=2` — Hard CPU ceiling (see Section 5).

The scoring system wraps the `docker run` invocation in a 30-minute (`1800s`) timeout. If your container has not exited by T+30 minutes, it will be killed.

---

## 5. Exit Codes

The scoring system reads the exit code before attempting to process any outputs.

| Code | Meaning | Scoring consequence |
|---|---|---|
| `0` | Pipeline completed successfully | Outputs are read and scored |
| `1` | Pipeline failed (exception or explicit non-zero exit) | Zero points for correctness; partial outputs ignored |
| `124` | Killed by the 30-minute timeout | Zero points for correctness and scalability |
| `137` | Killed by the kernel due to OOM (memory limit exceeded) | Zero points for correctness; OOM recorded |

Any non-zero exit code results in zero correctness points for that stage, regardless of what is present in `/data/output/`.

---

## 6. Resource Constraints Summary

| Resource | Limit | Enforcement mechanism |
|---|---|---|
| RAM | 2 GB hard limit | Docker `--memory=2g --memory-swap=2g`; OOM kill at limit |
| CPU | 2 vCPU | Docker `--cpus=2`; Spark should run as `local[2]` |
| Wall-clock time | 30 minutes | External `timeout 1800` wrapper; container killed at T+1800s |
| Network | None | `--network=none`; all dependencies must be baked into the image |
| Writable filesystem | `/data/output/` and `/tmp` only | `--read-only` with explicit tmpfs for `/tmp` |
| Temporary disk (`/tmp`) | 512 MB | `--tmpfs /tmp:rw,size=512m` |

See `resource_constraints.md` for practical guidance on working within these limits.

---

## 7. What the Scoring System Does Not Do

- It does not run `docker-compose`. Only `docker run` is used.
- It does not debug your Dockerfile. A build failure results in zero points.
- It does not create output directories for you. Your pipeline must create them.
- It does not provide internet access during execution, even momentarily.
- It does not retry failed runs.
- It does not read outputs if the exit code is non-zero.
