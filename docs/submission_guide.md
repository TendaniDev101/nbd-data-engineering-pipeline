# Submission Guide

**Document:** `submission_guide.md`
**Version:** 1.0
**Status:** Final

---

## 1. Required Repository Structure

Your submission repository must contain the following layout at the time you create your stage tag. Files outside this structure are ignored by the scoring system.

```
your-submission/
├── Dockerfile                      # Required. Must extend nedbank-de-challenge/base:1.0
├── docker-compose.yml              # Optional. For local development only — not used by scorer
├── pipeline/
│   ├── ingest.py                   # Bronze layer ingestion
│   ├── transform.py                # Bronze → Silver transformation
│   ├── provision.py                # Silver → Gold provisioning
│   └── run_all.py                  # Entry point; calls ingest → transform → provision in sequence
├── config/
│   ├── pipeline_config.yaml        # Required. Pipeline configuration (paths, settings)
│   └── dq_rules.yaml               # Required from Stage 2 onward
├── adr/
│   └── stage3_adr.md               # Required at Stage 3 only
├── requirements.txt                # Python dependencies beyond the base image (may be empty)
└── README.md                       # Required. Must explain how to run the pipeline locally
```

**Notes on specific paths:**
- `output/` must **not** be committed. Add it to `.gitignore`. The scoring system provides the output directory via volume mount — pre-existing output in the image will not be used.
- `adr/` is not required until Stage 3. Do not create it at Stages 1 or 2.
- `config/dq_rules.yaml` is not required at Stage 1. It becomes required at Stage 2.
- Additional modules under `pipeline/` are permitted. The naming above is a minimum; you may add `utils.py`, `models.py`, etc.
- `docker-compose.yml` is for your local development workflow only. The scoring system does not use it.

---

## 2. Git Tagging Protocol

Each stage submission is identified by a specific Git tag on your repository. The scoring system checks out the exact tagged commit — nothing else is visible.

| Stage | Tag name | Deadline |
|---|---|---|
| Stage 1 | `stage1-submission` | End of Day 7 |
| Stage 2 | `stage2-submission` | End of Day 14 |
| Stage 3 | `stage3-submission` | End of Day 21 |

**How to create and push a tag:**

```bash
# Ensure all your changes are committed
git add -A
git commit -m "Stage 1 final submission"

# Create an annotated tag
git tag -a stage1-submission -m "Stage 1 submission"

# Push both the commit and the tag
git push origin main
git push origin stage1-submission
```

**Important:**
- The tag must be present in the remote repository before the deadline. A local-only tag is not visible to the scorer.
- Commits pushed after the tag deadline are not visible to the scoring system. The scorer checks out the tagged commit, not `HEAD`.
- You can overwrite an existing tag before the deadline if you need to resubmit: `git tag -fa stage1-submission -m "Stage 1 resubmission"` followed by `git push origin stage1-submission --force`. Do not do this after the deadline.
- Stage tags are cumulative. Your `stage2-submission` tag must contain all of your Stage 1 code plus your Stage 2 additions. Do not create a separate repository per stage.

---

## 3. How to Submit

1. Push your tagged commit to your remote repository (the platform will provide a repository URL when you register, or you may use your own public repository with access granted to the scoring system).
2. On the challenge platform, navigate to the submission form for the relevant stage.
3. Paste your repository URL into the submission field and confirm.
4. The scoring system will clone your repository, check out the tagged commit, build your Docker image, and execute the pipeline.
5. You will receive a confirmation email when scoring begins. Score results are published to the leaderboard within a few hours of the stage deadline closing.

Only the most recent submitted URL per stage is used. If you update your repository URL after submission, resubmit on the platform.

---

## 4. What Happens After You Submit

The scoring system performs the following steps in order:

1. **Clone** — `git clone --depth=50 <your-repo-url>`, then `git checkout <stage-tag>`. If the tag is missing, scoring fails immediately.
2. **Security scan** — Static grep for prohibited patterns (network calls, filesystem exfiltration, crypto operations). Triggered submissions are quarantined and reviewed manually.
3. **Docker build** — `docker build -t candidate-submission:latest .` from the repository root. A build failure results in zero points for the stage.
4. **Pipeline execution** — `docker run` with the full resource constraints and security flags documented in `docker_interface_contract.md`. The run is wrapped in a 30-minute timeout.
5. **Exit code check** — If the exit code is non-zero, scoring stops and zero correctness points are recorded.
6. **Output validation** — Validation queries are run against your Gold layer Delta tables. DQ report is parsed (Stage 2+). Streaming table lag is measured (Stage 3).
7. **Static analysis** — Code structure, configuration externalisation, and anti-pattern detection.
8. **Score computation** — Scores computed across all four dimensions (Correctness, Scalability, Maintainability, Efficiency).
9. **Leaderboard update** — Score breakdown published to the participant leaderboard.

---

## 5. Verifying Your Submission Locally Before Submitting

Run the following before pushing your stage tag. These steps mirror what the scoring system does.

**Step 1: Verify the Docker build is clean**

```bash
# Build from a clean context — ensure no stale layers
docker build --no-cache -t my-submission:test .
```

**Step 2: Run with the same constraints as the scoring system**

```bash
# Create a local data directory with your test data
mkdir -p /tmp/test-data/input /tmp/test-data/output /tmp/test-data/config

# Copy your input files
cp accounts.csv /tmp/test-data/input/
cp transactions.jsonl /tmp/test-data/input/
cp customers.csv /tmp/test-data/input/
cp config/pipeline_config.yaml /tmp/test-data/config/

# Run with scoring-equivalent constraints
docker run --rm \
  --network=none \
  --memory=2g --memory-swap=2g \
  --cpus=2 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  -v /tmp/test-data:/data \
  my-submission:test

echo "Exit code: $?"
```

**Step 3: Check outputs exist**

```bash
ls /tmp/test-data/output/bronze/
ls /tmp/test-data/output/silver/
ls /tmp/test-data/output/gold/
# Stage 2+:
cat /tmp/test-data/output/dq_report.json
```

**Step 4: Run the provided local test harness**

```bash
bash run_tests.sh --stage 1 --data-dir /tmp/test-data --image my-submission:test
```

The harness checks: Docker build success, container exits 0, output directories created, Gold layer readable by DuckDB, validation queries execute without error, DQ report present and parseable (Stage 2+), streaming tables present (Stage 3).

**Step 5: Verify your tag is pushed**

```bash
git ls-remote origin refs/tags/stage1-submission
```

If this returns a hash, the tag is visible to the scoring system. If it returns nothing, your tag has not been pushed.

---

## 6. Common Mistakes to Avoid

**Tag name errors** — The scoring system requires exact tag names: `stage1-submission`, `stage2-submission`, `stage3-submission`. Tags named `stage-1`, `stage_1`, `v1`, or `submission-stage1` will not be found.

**Committing the output directory** — Output files are large and will slow your repository. More importantly, the scoring system mounts its own output directory — any output baked into your image is unreachable. Add `output/` to `.gitignore` on day one.

**Dependencies not in the image** — The container has no network access during execution. Any package not installed in your `Dockerfile` will cause an import error at runtime. If you use `pip install` anywhere in your pipeline code, it will fail. Install everything at build time.

**Hardcoded paths** — The scoring system mounts data at `/data/input/`. Do not hardcode paths relative to your local development environment. Use `pipeline_config.yaml` for all paths.

**Interactive input** — The container is not attached to a terminal. Any `input()` call or interactive prompt will hang until the timeout kills the container.

**Missing `dq_rules.yaml` at Stage 2** — The scoring system checks for this file and verifies that your pipeline reads DQ rules from it rather than hardcoding handling logic. Its absence is scored as a maintainability failure.

**Missing `stage3_adr.md` at Stage 3** — The file must exist at `adr/stage3_adr.md` in the repository at the `stage3-submission` tag. An absent ADR receives zero points for that scoring criterion.

**Rewriting between stages** — The Stage 1→2 diff is analysed. A submission that differs from Stage 1 by a large fraction of total line count (effectively a rewrite) is penalised on maintainability. Build for extension from the start.
