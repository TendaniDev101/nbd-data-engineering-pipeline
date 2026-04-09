# Stage 2 Specification Addendum — Data Engineering Track

**Stage:** 2 — Stress Test
**Window:** Days 8–14
**Released:** After Stage 1 submission closes
**Submission tag:** `stage2-submission`

---

## 1. What's Changed

Requirements have evolved. The fintech has updated their data feeds. Three changes apply simultaneously:

| Change | Detail |
|---|---|
| **Volume** | Data volume has tripled across all three source files |
| **Data quality** | Six categories of real-world quality issues are now present in the data |
| **Schema** | A new field, `merchant_subcategory`, has been added to the transaction feed |

All three changes arrive together. Your pipeline must handle them without a full rewrite.

---

## 2. New Data

Stage 2 data replaces Stage 1 data entirely. The three source files are the same — `customers.csv`, `accounts.csv`, `transactions.jsonl` — but larger and dirtier.

| File | Stage 1 Rows | Stage 2 Rows |
|---|---|---|
| `customers.csv` | ~80,000 | ~240,000 (3×) |
| `accounts.csv` | ~100,000 | ~300,000 (3×) |
| `transactions.jsonl` | ~1,000,000 | ~3,000,000 base (pre-duplicate injection) |

The file paths, formats, and delivery mechanism are unchanged. Mount the Stage 2 data volume at `/data/input/` as before.

**The 30-minute pipeline time limit still applies.** Approaches that worked on 1M records but cannot scale to 3M will fail.

---

## 3. Data Quality Issues

The fintech has updated their data feeds. Real-world quality issues are now present. Six issue categories have been injected across the source files.

Your pipeline must detect, handle, and report on each category. Exact injection rates are not disclosed — build detection logic that finds all instances, not logic tuned to an expected percentage.

| Issue Code | Affected File | Description |
|---|---|---|
| `DUPLICATE_DEDUPED` | `transactions.jsonl` | Duplicate transaction events with the same `transaction_id` but marginally different timestamps, simulating double-delivery from the fintech's event system (~5% of transactions). |
| `ORPHANED_ACCOUNT` | `transactions.jsonl` | Transactions referencing an `account_id` that has no matching record in `accounts.csv`. Cannot be resolved to a customer (~2% of transactions). |
| `TYPE_MISMATCH` | `transactions.jsonl` | The `amount` field is delivered as a string (`"349.50"`) rather than a numeric type (~3% of transactions). |
| `DATE_FORMAT` | `transactions.jsonl`, `accounts.csv`, `customers.csv` | Date fields contain a mix of formats within the same column: `YYYY-MM-DD`, `DD/MM/YYYY`, and Unix epoch integers (~4% of records across all files). |
| `CURRENCY_VARIANT` | `transactions.jsonl` | The `currency` field contains non-standard representations of South African Rand: `"R"`, `"rands"`, `710` (ISO 4217 numeric), `"zar"` (lowercase). All must be standardised to `"ZAR"` (~1% of transactions). |
| `NULL_REQUIRED` | `accounts.csv` | The `account_id` primary key field is null. These records have no valid identifier and cannot be loaded (~0.5% of account records). |

**Handling requirements:**

- Each issue category must have a defined handling rule: correct (normalise and retain), flag (retain with `dq_flag` set), quarantine (exclude from Silver/Gold with logged reason), or reject.
- Rules must be externalised in `config/dq_rules.yaml` — see Section 6.
- No record may be silently dropped. Every record is either loaded (clean or flagged) or explicitly quarantined with a logged reason.
- The `dq_flag` column in `fact_transactions` must be set to the relevant issue code (listed above) or `NULL` for clean records. No other values are accepted by the scoring harness.

---

## 4. New Field: `merchant_subcategory`

A new field has been added to the fintech transaction schema.

| Field | Type | Nullable | Notes |
|---|---|---|---|
| `merchant_subcategory` | STRING | Yes | Sub-classification within `merchant_category`. Present in Stage 2 and Stage 3 data. Absent from Stage 1 data. |

**Important distinctions:**

- In Stage 1 data, the field is **absent from the JSON object entirely** — it is not present as `null`, it is a missing key.
- In Stage 2 data, the field is present in transaction records but is `null` in approximately 30% of records even where the key exists.
- Your parser must handle both cases: missing key (Stage 1 compatibility) and explicit `null` (Stage 2).

**The pipeline must not break if `merchant_subcategory` is absent.** Stage 1 data processed through a Stage 2 pipeline must produce a valid `fact_transactions` output with `merchant_subcategory` as `NULL` throughout.

The field maps to `fact_transactions.merchant_subcategory` (position 9, nullable STRING) per `output_schema_spec.md` §2.

---

## 5. New Requirement: DQ Report

Your pipeline must produce a data quality summary at the following path:

```
/data/output/dq_report.json
```

The report must conform to the schema and format defined in `dq_report_template.json`. Key fields:

- `run_timestamp` — ISO 8601 UTC, set at pipeline start
- `stage` — `"2"`
- `source_record_counts` — raw record counts before any filtering
- `dq_issues` — one entry per issue type encountered; omit zero-count types
- `gold_layer_record_counts` — records written to each Gold table
- `execution_duration_seconds` — wall-clock seconds, start to finish

The scoring harness cross-references `records_affected` values against internally computed expected counts with ±5% tolerance per category. It also verifies that `handling_action` values in the report are consistent with your `dq_rules.yaml`.

The file must be present at exactly `/data/output/dq_report.json`. No other path is checked.

---

## 6. New Requirement: Config Externalisation

DQ handling rules must be defined in a configuration file, not hardcoded in pipeline logic.

**Required file:** `config/dq_rules.yaml`

This file must specify, for each of the six issue types:
- The detection approach (how the pipeline identifies the issue)
- The handling action (what it does with affected records)
- Any thresholds or parameters the rule depends on

The scoring system reads `config/dq_rules.yaml` and cross-references it against your `dq_report.json`. Inconsistencies between what the config declares and what the report records are flagged as correctness failures.

**Scored under maintainability.** The intent is that a DQ rule can be changed — swapping `QUARANTINED` to `NORMALISED` for orphaned accounts, for example — by editing the config file, not by modifying pipeline code.

---

## 7. Scoring Note

Your Stage 1 → Stage 2 code diff is measured as part of the maintainability assessment.

The scoring rubric distinguishes:

- **Targeted modification** — Stage 1 architecture intact, changes are additions and surgical edits. Scores higher.
- **Partial rewrite** — Core Stage 1 logic preserved but significant structural changes made. Scores moderately.
- **Full rewrite** — Stage 1 code substantially replaced. Scores lower, regardless of Stage 2 output correctness.

A correct Stage 2 output produced by a rewrite is worth less than a correct Stage 2 output produced by extending an extensible Stage 1 foundation. The diff is inspected; the design is assessed, not just the result.

---

## 8. Submission

Tag your submission commit as `stage2-submission`:

```bash
git tag stage2-submission
git push origin stage2-submission
```

All Stage 1 Docker constraints apply without change:
- Base image: `nedbank-de-challenge:base`
- Resource limits: 2 GB RAM, 2 vCPU
- Execution time limit: 30 minutes
- Input mount: `/data/input/`
- Output mount: `/data/output/`
- Entry point: `docker run ... nedbank-de-challenge:submission`

All Stage 1 validation queries must still pass on Stage 2 data with DQ handling applied.

---

*Stage 2 Specification Addendum — DE Track. Released after Stage 1 submission closes.*
