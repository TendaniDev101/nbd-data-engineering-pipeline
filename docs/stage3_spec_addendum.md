# Stage 3 Specification Addendum — Data Engineering Track

**Stage:** 3 — Architecture Pivot
**Window:** Days 15–21
**Released:** After Stage 2 submission closes
**Submission tag:** `stage3-submission`

---

## 1. What's Changed

A new data source has appeared. The mobile product team has a requirement: the virtual account app must display current balance and recent transactions in near-real-time. The daily batch pipeline is insufficient — customers expect balance updates within seconds of a transaction.

The fintech has agreed to provide a real-time transaction event stream alongside the daily batch extract. A new streaming ingestion path must be added to your pipeline. The batch pipeline must continue to run correctly.

---

## 2. The Streaming Feed

A directory of micro-batch JSONL files is available at:

```
/data/stream/
```

All 12 stream batch files are pre-staged in the `/data/stream/` mount at container start. Your pipeline must discover and process them in filename order (lexicographic sort gives correct chronological order). Files are not delivered incrementally during execution. Each file contains between 50 and 500 transaction events. Files are named `stream_{date}_{time}_{sequence}.jsonl` where date is `YYYYMMDD`, time is `HHMMSS`, and sequence is a zero-padded 4-digit batch number (e.g., `stream_20260320_143000_0001.jsonl`, `stream_20260320_143500_0002.jsonl`).

The transaction schema is identical to Stage 2 `transactions.jsonl`, including the `merchant_subcategory` field. The same data quality variance that applies to Stage 2 batch data may be present in stream events.

Your pipeline must poll the directory, discover all pre-staged files, and process them in sequence. See `stream_interface_spec.md` for the full technical specification of the streaming interface, file format, and polling contract.

---

## 3. New Output Tables

Two new Gold tables are required, written to `/data/output/stream_gold/`:

### `current_balances`

Written to `/data/output/stream_gold/current_balances/`.

One row per `account_id`. Updated by upsert/merge as each micro-batch is processed. Not an append table.

| Field | Type | Nullable | Description |
|---|---|---|---|
| `account_id` | STRING | No | Natural key. References a valid account from the batch pipeline. |
| `current_balance` | DECIMAL(18,2) | No | Most recent computed balance for this account. |
| `last_transaction_timestamp` | TIMESTAMP | No | Timestamp of the most recent transaction event processed. |
| `updated_at` | TIMESTAMP | No | When this row was last written by the streaming pipeline. Must be within 5 minutes of the source event timestamp (SLA). |

### `recent_transactions`

Written to `/data/output/stream_gold/recent_transactions/`.

Maintains the last 50 transactions per account by `transaction_timestamp`. Implemented as a Delta merge/upsert keyed on `(account_id, transaction_id)`. Rows beyond position 50 per account must be deleted on each merge cycle.

| Field | Type | Nullable | Description |
|---|---|---|---|
| `account_id` | STRING | No | Account reference. |
| `transaction_id` | STRING | No | Unique transaction identifier from the streaming event. |
| `transaction_timestamp` | TIMESTAMP | No | Timestamp of the transaction event. |
| `amount` | DECIMAL(18,2) | No | Transaction amount. |
| `transaction_type` | STRING | No | Values: `DEBIT`, `CREDIT`, `FEE`, `REVERSAL`. |
| `channel` | STRING | Yes | Channel used. Nullable (may be absent from stream events). |
| `updated_at` | TIMESTAMP | No | When this row was written by the streaming pipeline. |

Full schema reference: `output_schema_spec.md` §5 and §6.

---

## 4. SLA Requirement

The Gold layer must reflect stream events within **5 minutes of file arrival**.

Measured as: `updated_at` in output tables versus the source event timestamp in the stream file.

| Latency | Credit |
|---|---|
| ≤ 300 seconds (5 minutes) | Full SLA credit |
| 301–600 seconds | Partial credit |
| > 600 seconds | No SLA credit |

The scoring harness checks `current_balances.updated_at` and `recent_transactions.updated_at` against the event timestamps in the processed stream files.

---

## 5. What You Don't Need

No production-grade streaming infrastructure is required.

- No Kafka
- No Azure Event Hubs
- No real message queue
- No Kubernetes
- No CI/CD pipeline

**Directory polling is sufficient and expected.** A pipeline that checks `/data/stream/` every 60 seconds, identifies new files since the last check, processes each file with an incremental Delta merge, and writes updated output within the SLA window is a complete and valid solution.

A more sophisticated event-driven implementation is welcome, but the ADR (Section 6) is how you demonstrate you understand the trade-off — not the implementation itself.

---

## 6. Architecture Decision Record

You must submit an ADR at:

```
adr/stage3_adr.md
```

The ADR must address three questions:

1. **How did your Stage 1 architecture facilitate or hinder the streaming extension?** Reference specific design choices in your own pipeline — what helped, what created friction, and approximately what fraction of Stage 1/2 code survived intact.

2. **What design decisions in Stage 1 would you change in hindsight?** Be specific: name files, classes, or patterns in your own code and explain why they would have been different with Stage 3 visibility.

3. **How would you approach this differently if you had known Stage 3 was coming from the start?** Describe the Day 1 architecture you would have chosen: ingestion patterns, state management, output structure, entry point design.

Use the template at `adr_template.md`. The instructions section in the template must be removed before submission.

**Scoring:** 2 points (human-reviewed at finalist stage). Both points require all three questions addressed with specific, concrete reasoning about your own pipeline. General statements ("I would have made it more modular") do not score. See `adr_template.md` for the specificity standard.

---

## 7. Batch Pipeline Must Still Work

The Stage 2 batch pipeline must continue to run correctly.

- All Stage 1 and Stage 2 validation queries must still pass on Stage 2 data
- The `dq_report.json` must still be produced
- `config/dq_rules.yaml` must still govern DQ handling
- The 30-minute batch time limit still applies

The streaming path is an addition to the pipeline. It does not replace or supersede the batch path. Both must execute in the same container run.

---

## 8. Submission

Tag your submission commit as `stage3-submission`:

```bash
git tag stage3-submission
git push origin stage3-submission
```

All Docker constraints from Stage 1 and Stage 2 apply. The container must:
- Run the batch pipeline to completion (≤ 30 minutes)
- Start the streaming polling loop
- Process all stream files that arrive during the evaluation window
- Write updated `stream_gold/` tables within the 5-minute SLA

All 12 stream batch files are pre-staged in `/data/stream/` via a Docker mount at container start. The pipeline must process all files during the evaluation window.

---

*Stage 3 Specification Addendum — DE Track. Released after Stage 2 submission closes.*
