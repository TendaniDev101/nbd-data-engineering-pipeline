# Nedbank Data & Analytics Challenge
## Data Engineering Track - Challenge Brief

**Track:** Data Engineering
**Format:** Individual submission, remote execution
**Eligibility:** South African citizens and permanent residents only
**Qualification threshold:** 50 points to advance; top 25 proceed to on-site finale

## Welcome

Thank you for taking part in the Nedbank Data & Analytics Challenge. We're excited to have you here.

This challenge is built for experienced data engineers - people who have designed, built, and maintained production pipelines. It's not a quiz or a timed coding exercise. It's a real engineering problem, grounded in a realistic business scenario, and your solution will be evaluated the way production code is evaluated: does it work, does it scale, and can it survive change?

We've designed this to be challenging, fair, and genuinely interesting. If you enjoy building things that work under pressure, you're in the right place. Good luck - we look forward to seeing what you build.

## The Challenge

A major South African retail bank is executing a strategic initiative to reach the unbanked and underbanked market. Rather than build new infrastructure from scratch, the bank has partnered with a fintech that already operates a fully digital, mobile-first virtual account product. The arrangement is straightforward: the fintech holds and operates the virtual accounts and processes transactions; the bank provides underlying customer identity and risk data and receives transactional data back for analytics, regulatory reporting, and AI-powered decisioning.

The data flows are not straightforward. The fintech delivers account and transaction data in a combination of formats through daily batch extracts. The bank's own customer records live in an on-premise system currently mid-migration to Azure. Your job is to build the ingestion and provisioning infrastructure that makes this data usable: clean, queryable, auditable, and ready for analytics and AI workloads.

Banking domain knowledge is not required to complete this challenge. The scenario provides realistic context for the engineering problem - it does not require you to understand financial products, credit risk, or regulatory frameworks.

**A note on production reality:** Like any real-world data engineering role, requirements may evolve. Curveballs - changing data volumes, schema drift, new data sources, quality issues - are part of the job. Design accordingly.

## What You're Building

A **medallion data pipeline** that processes three data sources through Bronze, Silver, and Gold layers and produces analytical outputs in **Delta Parquet format**.

```
Bronze          Silver              Gold
Raw ingest  →   Standardised    →   Dimensional model
(as-arrived)    (typed, linked,     (fact_transactions,
                deduplicated)       dim_accounts,
                                    dim_customers)
```

The three data sources are:

| Source | Format | Delivery | Content |
|---|---|---|---|
| Fintech account data | CSV | Daily batch | Virtual account records (~100K records) |
| Fintech transaction data | JSONL | Daily batch | Transaction events (~1M records) |
| Bank customer data | CSV | Daily batch from on-prem | Customer master records (~80K records) |

The Gold layer is a dimensional model: a `fact_transactions` table joined to `dim_accounts` and `dim_customers`. The output schema is fully specified in the data pack. You will receive three validation queries upfront - your pipeline must produce output that satisfies all three.

## The Requirements

Build a pipeline that:

- Ingests all three source datasets into a **Bronze layer** - raw, unmodified, with ingestion timestamp added, partitioned by source
- Transforms Bronze → **Silver layer** - standardised types, consistent date formats, deduplication on primary keys, account-to-customer linkage resolved, schema enforced
- Provisions Silver → **Gold layer** - a dimensional model that passes the three supplied validation queries

**Acceptance criteria:**
- Pipeline executes end-to-end inside the Docker container within the time limit
- Gold layer passes all three validation queries
- Bronze, Silver, and Gold layers are distinct and separable
- Output is in Delta Parquet format

**A note on design:** Requirements in production change. Designs that hardcode file paths, schemas, and thresholds will require more rework than designs built for extension. Build for change.

## Technical Environment

### Runtime

Your pipeline runs inside a Docker container built on the provided base image:

| Component | Version |
|---|---|
| Python | 3.11 |
| PySpark | 3.5 |
| delta-spark | Pre-installed |

**Resource constraints (enforced by Docker):**
- 2 GB RAM
- 2 vCPU
- Strict execution time limit

Your pipeline runs with no internet access. It must not depend on external services at runtime.

### Tool Freedom

You may use any language, library, or framework that runs inside the Docker container. There are no restrictions on:
- Programming language (Python, Scala, SQL, R, or any combination)
- Orchestration approach (single script, DAGs, notebooks)
- Data processing framework (PySpark, DuckDB, pandas, Polars, dbt)

**Azure** (ADLS Gen 2) is available as an optional storage target - participants will be provided with Azure credentials. Use of cloud resources is not required and provides **no scoring advantage**. A fully local Docker implementation scores equally on all automated dimensions.

### Out-of-Scope Technologies

The following are explicitly excluded from the competition environment:

| Technology | Reason |
|---|---|
| Ab Initio | Licensed; not accessible to external participants |
| Azure Data Factory | GUI-based; not assessable in a code submission |
| Kubernetes | Infrastructure concern, not a candidate deliverable |

These exclusions exist to ensure a level playing field and to keep the assessment focused on engineering fundamentals rather than platform licences or infrastructure provisioning.

## Submission Format

### How to Submit

Submit your Git repository URL on the Otinga platform before the deadline.

The scoring system clones your repository, builds your Docker image, and runs your pipeline against the evaluation dataset. You do not need to be available for the scoring run.

### Required Repository Structure

```
Dockerfile                  # Must extend the provided base image
pipeline/
  ingest.py                 # Bronze layer ingestion
  transform.py              # Bronze → Silver
  provision.py              # Silver → Gold
config/
  pipeline_config.yaml      # Pipeline configuration (paths, thresholds)
requirements.txt
README.md
```

Additional modules under `pipeline/` are permitted. The three named entry points are required.

### Docker Interface Contract

**Inputs (mounted at runtime by the scoring system):**

```
/data/input/accounts.csv
/data/input/transactions.jsonl
/data/input/customers.csv
/data/config/pipeline_config.yaml
```

**Outputs (written by your pipeline):**

```
/data/output/bronze/                # Bronze layer (Delta Parquet)
/data/output/silver/                # Silver layer (Delta Parquet)
/data/output/gold/                  # Gold layer (Delta Parquet)
```

**Invocation:**
```bash
docker run \
  -v /path/to/data:/data \
  -m 2g --cpus="2" \
  candidate-submission:latest
```

Your Dockerfile must extend the provided base image. Your container must exit with code `0` on success and a non-zero code on failure. It must not require interactive input.

## How You're Scored

Scoring is fully automated across four dimensions. The scoring system runs your pipeline, inspects the output, and analyses your code.

| Dimension | Weight | What Is Measured |
|---|---|---|
| **Correctness** | 40% | Do your outputs match expected results? Do the validation queries pass? |
| **Scalability** | 25% | Does your pipeline complete within the time limit? Does it stay within memory limits? How does your execution time compare to the cohort? |
| **Maintainability** | 20% | Is configuration externalised? Are your modules separable? Is your code structured for change? |
| **Efficiency** | 15% | Are you using Spark idiomatically? Peak memory below 80% of allocation? Any redundant full-dataset scans? |

**Inefficient Spark patterns are penalised.** Calling `.collect()` on large distributed datasets, using pandas DataFrames at scale where native Spark operations apply, looping over DataFrames - these patterns reduce your efficiency score and may cause you to breach the time limit.

**Minimum threshold:** 50 points to qualify for the on-site finale. The top 25 scorers above this threshold advance.

## What Happens Next

### Advancing to the On-Site Finale

The top 25 performers advance to an invite-only technical session at Nedbank. This is not a presentation - it is a technical conversation.

**Format:**
- 20-minute technical interrogation per finalist
- Panel: Nedbank senior data engineers (the people who designed this challenge) + Otinga lead
- The panel will have read your code before you arrive

The panel will ask you to walk through specific implementation decisions, propose hypothetical changes to your architecture, and challenge the trade-offs you made. A candidate who built their pipeline will answer these questions fluently. Bring your laptop. Your code must be accessible.

### Prizes

Over R250,000 in prizes across the Data Engineering and Machine Learning tracks, awarded to 1st, 2nd, and 3rd place finishers.

### Employment Opportunity

Top-performing participants will be eligible for potential employment opportunities at Nedbank. Outstanding candidates from both tracks will be fast-tracked into Nedbank's hiring process, with outreach within 3 business days of the on-site event.

## AI Usage Policy

You may use any tools, including AI assistants. However, if your submission includes AI-generated code, you are required to disclose this.

Explainability of your code is critically important. Regardless of whether you used AI tools or not, you will be asked detailed questions about your implementation - how it works, why you made specific design choices, and what trade-offs you considered. You must be able to explain and defend every line.

AI-generated code that you cannot explain is a liability, not an asset. The on-site interrogation is designed to distinguish candidates who built their pipeline from candidates who submitted one.

## Getting Started

- **Download** the data pack and base Docker image from the Otinga platform. The pack includes:
  - Three source datasets (accounts, transactions, customers)
  - Full data dictionary for all three sources
  - Output schema specification (Gold layer)
  - Three validation queries your pipeline must satisfy
  - A starter repository with the base Docker image and interface contract

- **Build locally.** Test using the provided testing harness before submitting. The evaluation environment is identical to the Docker setup in the starter repository.

- **Submit** your repository URL on the Otinga platform before the deadline.

*Questions about the technical environment and Docker setup can be directed to the challenge support channel. Problem-solving hints will not be provided.*
