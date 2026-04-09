# Stream Interface Specification — DE Track Stage 3

**Document ID:** D-11
**Spec references:** `de_challenge_design_spec.md` §3.3; `output_schema_spec.md` §5–6; `stage3_spec_addendum.md`
**Status:** Final

---

## 1. Input: Stream Directory

| Property | Value |
|---|---|
| **Directory** | `/data/stream/` |
| **File naming** | `stream_YYYYMMDD_HHMMSS_NNNN.jsonl` — files are named `stream_{date}_{time}_{sequence}.jsonl` where date is `YYYYMMDD`, time is `HHMMSS`, and sequence is a zero-padded 4-digit batch number (e.g., `stream_20260320_143000_0001.jsonl`, `stream_20260320_143500_0042.jsonl`) |
| **Format** | JSONL — one complete JSON object per line, newline-delimited, UTF-8 |
| **Events per file** | 50–500 (irregular; do not assume uniform batch size) |
| **Delivery mechanism** | All 12 stream batch files are pre-staged in the `/data/stream/` mount at container start. Your pipeline must discover and process them in filename order (lexicographic sort gives correct chronological order). Files are not delivered incrementally during execution. |

---

## 2. Event Schema

Each line in a stream file is a transaction event. The schema is identical to Stage 2 `transactions.jsonl`.

```json
{
  "transaction_id": "string (UUID)",
  "account_id": "string (UUID, FK to accounts)",
  "transaction_date": "string (YYYY-MM-DD)",
  "transaction_time": "string (HH:MM:SS)",
  "transaction_type": "string (DEBIT|CREDIT|FEE|REVERSAL)",
  "merchant_category": "string (nullable)",
  "merchant_subcategory": "string (nullable)",
  "amount": "decimal",
  "currency": "string",
  "channel": "string (nullable)",
  "location": {
    "province": "string (nullable)",
    "city": "string (nullable)",
    "coordinates": "string (nullable)"
  },
  "metadata": {
    "device_id": "string (nullable)",
    "session_id": "string (nullable)",
    "retry_flag": "boolean"
  }
}
```

`merchant_subcategory` is present in all stream events (may be `null`). The same currency variant and type mismatch issues present in Stage 2 batch data may appear in stream events.

---

## 3. Polling Contract

Your pipeline must poll `/data/stream/` to detect new files.

**Requirements:**

- Track which files have already been processed (by filename or by sequence number)
- On each poll cycle, identify files not yet processed and process them in sequence order
- Do not reprocess files on subsequent poll cycles
- Poll interval must be short enough to meet the 5-minute SLA — a 60-second interval is sufficient and expected
- The pipeline must continue polling for the duration of the evaluation window; it must not exit after processing the first batch of files

**Recommended approach:** maintain a local state file or in-memory set of processed filenames. On each cycle, list `/data/stream/`, diff against processed set, process new files in ascending filename order.

---

## 4. Output: `current_balances`

**Path:** `/data/output/stream_gold/current_balances/`
**Format:** Delta Parquet
**Semantics:** Upsert — one row per `account_id`. Not an append table.

| Field | Type | Nullable | Description |
|---|---|---|---|
| `account_id` | STRING | No | Natural key. |
| `current_balance` | DECIMAL(18,2) | No | Most recent computed balance. Updated with each new event for this account. |
| `last_transaction_timestamp` | TIMESTAMP | No | Timestamp of the most recent event processed for this account. |
| `updated_at` | TIMESTAMP | No | When this row was last written. Must be within 5 minutes of the source event timestamp. |

**Merge key:** `account_id`

On each micro-batch, for each `account_id` in the batch: upsert the row with updated `current_balance`, `last_transaction_timestamp`, and `updated_at`. If no existing row, insert.

---

## 5. Output: `recent_transactions`

**Path:** `/data/output/stream_gold/recent_transactions/`
**Format:** Delta Parquet
**Semantics:** Merge/upsert keyed on `(account_id, transaction_id)`. Maintains last 50 transactions per account.

| Field | Type | Nullable | Description |
|---|---|---|---|
| `account_id` | STRING | No | Account reference. |
| `transaction_id` | STRING | No | Unique transaction identifier. |
| `transaction_timestamp` | TIMESTAMP | No | Timestamp of the transaction event. |
| `amount` | DECIMAL(18,2) | No | Transaction amount. |
| `transaction_type` | STRING | No | Values: `DEBIT`, `CREDIT`, `FEE`, `REVERSAL`. |
| `channel` | STRING | Yes | Nullable. |
| `updated_at` | TIMESTAMP | No | When this row was written by the streaming pipeline. |

**Merge key:** `(account_id, transaction_id)`

**Retention rule:** After each merge, retain only the 50 most recent rows (by `transaction_timestamp`) per `account_id`. Delete rows beyond position 50.

---

## 6. SLA Definition

The SLA is measured per event, not per file.

```
latency = updated_at (in output table) - transaction_timestamp (from source event)
```

| Latency | Score |
|---|---|
| ≤ 300 seconds | Full credit |
| 301–600 seconds | Partial credit |
| > 600 seconds | No credit |

`updated_at` must reflect when the pipeline wrote the row — not when the file arrived, not when the polling cycle started.

---

## 7. Output Directory Structure (Stage 3 additions)

```
/data/output/
  stream_gold/
    current_balances/     (Delta Parquet — 4 fields, upsert semantics)
      _delta_log/
      part-*.parquet
    recent_transactions/  (Delta Parquet — 7 fields, last-50 retention)
      _delta_log/
      part-*.parquet
```

Both tables must contain valid Delta Lake metadata (`_delta_log/`) alongside Parquet part files. Tables must be readable by `delta.load("path")` (PySpark) or `delta_scan("path")` (DuckDB + `delta` extension).

---

## 8. Constraints and Non-Requirements

- No Kafka, no Azure Event Hubs, no message queue infrastructure required or expected
- No exactly-once delivery guarantee is assessed — the scoring harness does not inject duplicate stream files
- Concurrent Spark session management (batch + stream in the same container) is the participant's responsibility; there is no prescribed approach
- Stream events reference accounts from the Stage 2 batch data; orphaned stream events (account not in batch data) may be discarded without penalty provided the batch `dq_report.json` already accounts for orphan handling

---

*End of document. Stream Interface Specification — DE Track Stage 3.*
