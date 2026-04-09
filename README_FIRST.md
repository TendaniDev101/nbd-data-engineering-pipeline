# Nedbank Data Engineering Challenge — Start Here

## Quick Start

1. Read `challenge_brief.md` — understand what you're building
2. Read `data_dictionary.md` — understand the data
3. Read `output_schema_spec.md` — understand what to produce
4. Copy `starter_kit/` as your project base
5. Build and test: `./run_tests.sh --stage 1`
6. Submit your repo URL on the Otinga platform

---

## Pack Contents

| File | Description |
|---|---|
| `README_FIRST.md` | This file — start here |
| `challenge_brief.md` | Full challenge description, objectives, and evaluation criteria |
| `data_dictionary.md` | Field-by-field reference for all source data files |
| `output_schema_spec.md` | Exact schema and format requirements for all pipeline outputs |
| `docker_interface_contract.md` | How your container will be invoked during automated evaluation |
| `README_DOCKER.md` | Step-by-step guide to building and running your Docker container |
| `submission_guide.md` | How to tag and submit your solution at each stage |
| `validation_queries.sql` | SQL queries used to validate your output tables |
| `challenge_rules.md` | Competition rules, code-of-conduct, and disqualification criteria |
| `resource_constraints.md` | CPU, memory, and time limits applied during evaluation |
| `Dockerfile.base` | Base image definition — extend or use as-is in your submission |
| `dq_report_template.json` | Template for the data quality report your pipeline must produce |
| `adr_template.md` | Architecture Decision Record template (used in Stage 3) |
| `stage2_spec_addendum.md` | Stage 2 requirements: volume increase, DQ issues, schema change |
| `stage3_spec_addendum.md` | Stage 3 requirements: streaming ingestion path |
| `stream_interface_spec.md` | Technical specification of the Stage 3 streaming interface |
| `starter_kit/` | Project scaffold — copy this as your starting point |
| `starter_kit/README.md` | Starter kit overview and usage instructions |
| `starter_kit/Dockerfile` | Submission Dockerfile (extend `Dockerfile.base`) |
| `starter_kit/requirements.txt` | Python dependencies for the starter kit |
| `starter_kit/.gitignore` | Recommended git ignore rules |
| `starter_kit/pipeline/__init__.py` | Pipeline package init |
| `starter_kit/pipeline/ingest.py` | Batch ingestion stub |
| `starter_kit/pipeline/provision.py` | Database provisioning stub |
| `starter_kit/pipeline/run_all.py` | Top-level pipeline runner |
| `starter_kit/pipeline/stream_ingest.py` | Streaming ingestion stub (Stage 3) |
| `starter_kit/pipeline/transform.py` | Transformation logic stub |
| `starter_kit/config/pipeline_config.yaml` | Pipeline configuration file |
| `starter_kit/config/dq_rules.yaml` | Data quality rules configuration |
| `starter_kit/adr/stage3_adr.md` | Pre-populated ADR for the Stage 3 streaming extension |
| `run_tests.sh` | Test harness — run locally before submitting |

---

## Key Documents

| Purpose | File |
|---|---|
| Challenge Brief | `challenge_brief.md` |
| Data Dictionary | `data_dictionary.md` |
| Output Schema | `output_schema_spec.md` |
| Docker Setup | `docker_interface_contract.md` + `README_DOCKER.md` |
| Submission Guide | `submission_guide.md` |
| Validation Queries | `validation_queries.sql` |
| Testing Harness | `run_tests.sh` |
| Challenge Rules | `challenge_rules.md` |

---

## Data Files

Data files are distributed separately due to file size. They are not included in this pack archive.

| Stage | Directory | Contents | Approx. Size |
|---|---|---|---|
| Stage 1 | `data/stage1/` | `customers.csv`, `accounts.csv`, `transactions.jsonl` | ~473 MB |
| Stage 2 | `data/stage2/` | Same files, 3× volume, with DQ issues | ~1.6 GB |
| Stage 3 | `data/stage3_stream/` | 12 micro-batch JSONL stream files | ~1.7 MB |
| Sample | `data/sample/` | Small subset of Stage 1 data for local testing | ~5 MB |

The `sample/` dataset is sized to run comfortably on a laptop. Use it while developing and only switch to the full dataset when you are ready to validate.

When the test harness runs (`./run_tests.sh --stage 1`), it expects data to be mounted at `/data/stage1/` inside the container. See `docker_interface_contract.md` for the full mount specification.

---

## Stages at a Glance

| Stage | Days | What Changes |
|---|---|---|
| 1 | 1–7 | Build the core batch pipeline |
| 2 | 8–14 | Handle 3× volume, DQ issues, and a new schema field |
| 3 | 15–21 | Add a streaming ingestion path alongside batch |

Stage 2 and Stage 3 specification addenda (`stage2_spec_addendum.md`, `stage3_spec_addendum.md`) are included in this pack so you can read ahead, but they are not evaluated until those stages open.

---

## Support

For technical questions, clarifications, and issue reports, contact the challenge support channel provided in your participant onboarding email. Do not share your solution code in the support channel.
