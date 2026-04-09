# Data Dictionary — DE Track Source Files

**Challenge:** Nedbank Data & Analytics Challenge — Data Engineering Track
**Document version:** 1.0
**Stage applicability:** Stages 1, 2, and 3 (differences noted per section)

---

## 1. Overview

### Source files

Three source files are provided at the start of Stage 1:

| File | Format | Stage 1 Row Count | Owner |
|---|---|---|---|
| `customers.csv` | CSV (comma-delimited, UTF-8) | ~80,000 | Bank (on-premises CRM export) |
| `accounts.csv` | CSV (comma-delimited, UTF-8) | ~100,000 | Fintech (daily batch extract) |
| `transactions.jsonl` | JSONL (newline-delimited JSON, UTF-8) | ~1,000,000 | Fintech (daily batch extract) |

### Business context

A major South African retail bank has partnered with a fintech to serve the unbanked and underbanked market. The fintech operates the virtual accounts and processes transactions. The bank holds the underlying customer identity and risk data. The bank's data engineering team (you) receives data from both owners and must build a clean, queryable, auditable gold layer for analytics, AI/ML, and compliance consumers.

The three files represent the three data owners:

- `customers.csv` — bank-owned customer master data (identity, KYC, segmentation)
- `accounts.csv` — fintech-owned virtual account records (account state, product, balances)
- `transactions.jsonl` — fintech-owned transaction events (activity ledger)

### Stage notes

- **Stage 1 data is clean.** No data quality issues are present. Use Stage 1 to build and validate your pipeline architecture.
- **Stage 2 introduces data quality issues** across multiple fields and file types, triples the volume, and adds a new field (`merchant_subcategory`) to `transactions.jsonl`. See Section 6 and Section 7 for details.
- **Stage 3 adds a real-time event stream** alongside the batch files. The stream uses the same JSONL transaction schema.

---

## 2. customers.csv

**Primary key:** `customer_id`
**Encoding:** UTF-8
**Delimiter:** `,`
**Header row:** Yes

| Field Name | Type | Nullable | Description | Sample Values | Notes |
|---|---|---|---|---|---|
| `customer_id` | STRING | No | Primary key. UUID format. Unique per customer. | `"a1b2c3d4-..."` | Referenced by `accounts.customer_ref` |
| `id_number` | STRING | No | South African national ID number. Masked to last 4 digits for privacy. | `"**********1234"` | Not a join key — do not use for matching |
| `first_name` | STRING | No | Customer given name. Drawn from SA name pool (Zulu, Xhosa, Sotho, Tswana, Afrikaans, English, Tsonga, Venda). | `"Thabo"`, `"Liezel"`, `"James"` | |
| `last_name` | STRING | No | Customer surname. SA surname pool. | `"Dlamini"`, `"Van der Merwe"`, `"Naidoo"` | |
| `dob` | STRING | No | Date of birth. Format: `YYYY-MM-DD` in Stage 1. | `"1987-04-15"` | Stage 2 introduces format variants. Used to derive `age_band` in Gold layer — do not copy raw `dob` to output. |
| `gender` | STRING | No | Gender identity. | `"M"`, `"F"`, `"NB"`, `"UNKNOWN"` | Distribution: M 45%, F 48%, NB 4%, UNKNOWN 3% |
| `province` | STRING | No | SA province of residence. | See province list below | Distribution is population-weighted (see note) |
| `income_band` | STRING | No | Gross monthly income classification. | `"LOW"`, `"LOWER_MIDDLE"`, `"MIDDLE"`, `"UPPER_MIDDLE"`, `"HIGH"` | Distribution: LOW 25%, LOWER_MIDDLE 30%, MIDDLE 25%, UPPER_MIDDLE 15%, HIGH 5% |
| `segment` | STRING | No | Nedbank customer segment. | `"MASS"`, `"EMERGING"`, `"MIDDLE_MKT"`, `"PROFESSIONAL"`, `"PRIVATE"` | Distribution: MASS 35%, EMERGING 30%, MIDDLE_MKT 20%, PROFESSIONAL 10%, PRIVATE 5% |
| `risk_score` | INTEGER | No | Risk score assigned to customer. | `1` through `10` | Normally distributed: mean 5, std 2. Clamped to [1, 10]. |
| `kyc_status` | STRING | No | Know Your Customer verification status. | `"VERIFIED"`, `"PENDING"`, `"FAILED"` | Distribution: VERIFIED 85%, PENDING 10%, FAILED 5% |
| `product_flags` | STRING | No | Pipe-delimited list of product codes held by the customer. | `"HL\|CC"`, `"SA\|CA\|INS"`, `"PL"` | Product codes: `HL` (Home Loan), `PL` (Personal Loan), `CC` (Credit Card), `SA` (Savings Account), `CA` (Current Account), `INV` (Investment), `INS` (Insurance) |

**Province values and approximate distribution:**

| Province | Approx. Share |
|---|---|
| Gauteng | 30% |
| Western Cape | 18% |
| KwaZulu-Natal | 15% |
| Eastern Cape | 10% |
| Limpopo | 8% |
| Mpumalanga | 6% |
| North West | 5% |
| Free State | 5% |
| Northern Cape | 3% |

---

## 3. accounts.csv

**Primary key:** `account_id`
**Foreign key:** `customer_ref` → `customers.customer_id`
**Encoding:** UTF-8
**Delimiter:** `,`
**Header row:** Yes

| Field Name | Type | Nullable | Description | Sample Values | Notes |
|---|---|---|---|---|---|
| `account_id` | STRING | No* | Primary key. UUID format. Unique per account. | `"f9e8d7c6-..."` | *Stage 2: ~0.5% of records have null `account_id`. These are invalid and must be handled by the pipeline. |
| `customer_ref` | STRING | No | Foreign key to `customers.customer_id`. Links the account to its owner. | `"a1b2c3d4-..."` | Renamed to `customer_id` in the Gold `dim_accounts` table. |
| `account_type` | STRING | No | Type of virtual account. | `"SAVINGS"`, `"TRANSACTIONAL"`, `"CREDIT"` | Distribution: SAVINGS 40%, TRANSACTIONAL 45%, CREDIT 15% |
| `account_status` | STRING | No | Current lifecycle state of the account. | `"ACTIVE"`, `"DORMANT"`, `"CLOSED"`, `"SUSPENDED"` | Distribution: ACTIVE 70%, DORMANT 15%, CLOSED 10%, SUSPENDED 5% |
| `open_date` | STRING | No | Date the account was opened. Format: `YYYY-MM-DD` in Stage 1. | `"2021-06-14"` | Range: 2018-01-01 to 2025-12-31. Stage 2 introduces format variants. |
| `product_tier` | STRING | No | Product tier assigned to the account. | `"BASIC"`, `"STANDARD"`, `"PREMIUM"` | Distribution: BASIC 50%, STANDARD 35%, PREMIUM 15% |
| `mobile_number` | STRING | Yes | SA mobile number associated with the account. | `"+27821234567"` | Null in ~15% of records. |
| `digital_channel` | STRING | No | Primary digital channel registered for the account. | `"APP"`, `"USSD"`, `"WEB"` | Distribution: APP 60%, USSD 30%, WEB 10% |
| `credit_limit` | DECIMAL | Yes | Approved credit limit. | `"15000.00"`, `"75000.00"` | Null for `SAVINGS` and `TRANSACTIONAL` account types. Range: 5,000–250,000 ZAR for `CREDIT` accounts. |
| `current_balance` | DECIMAL | No | Snapshot balance at time of batch extract. Two decimal places. | `"4231.50"`, `"0.00"` | Reflects balance at extract time, not real-time. |
| `last_activity_date` | STRING | Yes | Date of the most recent transaction on the account. Format: `YYYY-MM-DD` in Stage 1. | `"2025-11-03"` | May be null in Stage 2 (DQ-injected). |

**Cardinality note:** Approximately 1.25 accounts per customer on average. Distribution: 40% of customers have 1 account, 35% have 2, 20% have 3, 5% have 4.

---

## 4. transactions.jsonl

**Format:** JSONL — one complete JSON object per line. Each line is independently parseable.
**Primary key:** `transaction_id`
**Foreign key:** `account_id` → `accounts.account_id`

Each line contains a single transaction event. The top-level object has the following fields:

| Field Name | Type | Nullable | Description | Sample Values | Notes |
|---|---|---|---|---|---|
| `transaction_id` | STRING | No | Primary key. UUID format. Unique per transaction event. | `"3c7f1a2b-..."` | Stage 2 introduces ~5% duplicates (same `transaction_id`, marginally different timestamps). |
| `account_id` | STRING | No | Foreign key to `accounts.account_id`. | `"f9e8d7c6-..."` | Stage 2: ~2% orphaned (no matching account record). |
| `transaction_date` | STRING | No | Date of the transaction. Format: `YYYY-MM-DD` in Stage 1. | `"2025-03-22"` | Range: 2024-01-01 to 2025-12-31. Stage 2 introduces format variants. |
| `transaction_time` | STRING | No | Time of the transaction. Format: `HH:MM:SS`. | `"14:37:05"` | Combine with `transaction_date` to produce `transaction_timestamp` in Gold layer. |
| `transaction_type` | STRING | No | Classification of the transaction. | `"DEBIT"`, `"CREDIT"`, `"FEE"`, `"REVERSAL"` | Distribution: DEBIT 55%, CREDIT 30%, FEE 10%, REVERSAL 5% |
| `merchant_category` | STRING | Yes | MCC-style merchant category code. | `"GROCERY"`, `"FUEL"`, `"SALARY"`, `"ATM_WITHDRAWAL"` | 20 possible values — see full list below. Nullable where absent in source. |
| `merchant_subcategory` | STRING | Yes | Sub-classification within `merchant_category`. | `"Supermarket"`, `"Fast Food"`, `"Petrol"` | **ABSENT in Stage 1 data.** Present in Stage 2 and Stage 3. Pipelines must not fail if this field is missing from the source object. |
| `amount` | DECIMAL | No | Transaction amount in ZAR. Two decimal places. | `"349.50"`, `"12000.00"` | Log-normal distribution: median ~350, range 0.01–50,000. Stage 2: ~3% delivered as string type instead of numeric. |
| `currency` | STRING | No | Transaction currency. Should always be `"ZAR"`. | `"ZAR"` | Stage 2 introduces variants: `"R"`, `"rands"`, `710`, `"zar"` — all representing South African Rand. Gold layer must standardise all variants to `"ZAR"`. |
| `channel` | STRING | No | Transaction channel. | `"POS"`, `"APP"`, `"ATM"`, `"EFT"`, `"USSD"`, `"INTERNAL"` | Distribution: POS 35%, APP 30%, ATM 15%, EFT 10%, USSD 8%, INTERNAL 2% |
| `location.province` | STRING | Yes | SA province where the transaction occurred. Nested under `location` object. | `"Gauteng"`, `"Western Cape"` | Should match the province of the customer linked to the account. |
| `location.city` | STRING | Yes | City where the transaction occurred. Nested under `location` object. | `"Johannesburg"`, `"Cape Town"`, `"Durban"` | See province-to-city mapping in notes below. |
| `location.coordinates` | STRING | Yes | Approximate lat/lon. Nested under `location` object. | `"-26.2041,28.0473"` | Null in ~40% of records. |
| `metadata.device_id` | STRING | Yes | Device identifier for digital channel transactions. Nested under `metadata` object. | `"dev-a9f3..."` | Null in ~50% of records. |
| `metadata.session_id` | STRING | Yes | Session identifier. Nested under `metadata` object. | `"sess-7b12..."` | Null in ~40% of records. |
| `metadata.retry_flag` | BOOLEAN | No | Whether this event was a retry of a previously failed submission. Nested under `metadata` object. | `false`, `true` | True in ~2% of records. |

**Example record (Stage 1):**

```json
{
  "transaction_id": "3c7f1a2b-e4d5-4f6a-8b9c-0d1e2f3a4b5c",
  "account_id": "f9e8d7c6-b5a4-4321-9876-543210fedcba",
  "transaction_date": "2025-03-22",
  "transaction_time": "14:37:05",
  "transaction_type": "DEBIT",
  "merchant_category": "GROCERY",
  "amount": 349.50,
  "currency": "ZAR",
  "channel": "POS",
  "location": {
    "province": "Gauteng",
    "city": "Sandton",
    "coordinates": "-26.1076,28.0567"
  },
  "metadata": {
    "device_id": null,
    "session_id": null,
    "retry_flag": false
  }
}
```

**Note:** `merchant_subcategory` is absent from Stage 1 records entirely — it is not present as a null field, it is absent from the JSON object. Your parser must handle missing keys, not just null values.

**Merchant category values (20 total):**

`GROCERY`, `FUEL`, `RESTAURANT`, `RETAIL`, `HEALTHCARE`, `UTILITIES`, `TRANSPORT`, `ENTERTAINMENT`, `EDUCATION`, `INSURANCE`, `RENT`, `SALARY`, `ATM_WITHDRAWAL`, `TRANSFER_IN`, `TRANSFER_OUT`, `REVERSAL_CREDIT`, `REVERSAL_DEBIT`, `FEE_SERVICE`, `FEE_MONTHLY`, `FEE_TRANSACTION`

**Merchant subcategory values (Stage 2+, selected examples):**

| merchant_category | Possible subcategory values |
|---|---|
| GROCERY | Supermarket, Spaza Shop, Wholesale, Butchery |
| FUEL | Petrol, Diesel, LPG |
| RESTAURANT | Fast Food, Sit Down, Takeaway, Coffee Shop |
| RETAIL | Clothing, Electronics, Home Goods, Pharmacy |
| TRANSPORT | Taxi, Bus, Rideshare, Parking |
| SALARY | Monthly, Weekly, Contractor |
| UTILITIES | Electricity, Water, Municipal |

Subcategory is null in ~30% of Stage 2 records even where the field is present.

---

## 5. Relationships

```
customers.csv                accounts.csv              transactions.jsonl
─────────────────            ─────────────────         ──────────────────────
customer_id (PK) ────────── customer_ref (FK)
                             account_id (PK) ─────── account_id (FK)
```

**Cardinality:**

| Relationship | Cardinality | Detail |
|---|---|---|
| customers → accounts | 1 : 1..4 | Each customer has 1–4 accounts. Average ~1.25 accounts per customer. |
| accounts → transactions | 1 : 0..N | Active accounts generate the bulk of transactions. Dormant/Closed accounts have proportionally fewer. |
| Effective depth | Customer → ~1.25 accounts → ~10 transactions per account | Approximately 12.5 transactions per customer in Stage 1. |

**Gold layer join path:**

- `fact_transactions.account_sk` → `dim_accounts.account_sk` (via `transactions.account_id` → `accounts.account_id`)
- `fact_transactions.customer_sk` → `dim_customers.customer_sk` (via `dim_accounts.customer_id` → `dim_customers.customer_id`)
- `dim_accounts.customer_id` is renamed from `accounts.customer_ref` in the Gold layer

---

## 6. Stage Differences

| Aspect | Stage 1 | Stage 2 | Stage 3 |
|---|---|---|---|
| **customers.csv rows** | ~80,000 | ~240,000 (3×) | Same as Stage 2 |
| **accounts.csv rows** | ~100,000 | ~300,000 (3×) | Same as Stage 2 |
| **transactions.jsonl rows** | ~1,000,000 | ~3,000,000 base (pre-duplicate injection) | Stage 2 batch + streaming micro-batches |
| **Data quality** | Clean — no injected issues | 6 DQ issue types injected (see Section 7) | Same as Stage 2 batch; stream data may include quality variance |
| **`merchant_subcategory` field** | Absent from all records | Present in transaction records (nullable) | Present in both batch and stream records |
| **New data source** | None | None | Real-time JSONL event stream (micro-batch files, 5-minute intervals) |
| **New Gold tables** | None | None | `current_balances`, `recent_transactions` (stream gold) |
| **DQ report required** | No | Yes (`dq_report.json`) | Yes |
| **Pipeline time limit** | 30 minutes | 30 minutes | Batch: 30 minutes; Stream: 5-minute SLA per event |

---

## 7. Data Quality Notes (Stage 2)

The Stage 2 data reflects real-world quality issues that arise when integrating data across organisational boundaries — between a bank's on-premises systems and a fintech's event-driven platform. Your pipeline must detect, handle, and report on each issue category.

Six categories of data quality issues are present in Stage 2 data:

| Issue Code | Affected File(s) | Description |
|---|---|---|
| `DUPLICATE_DEDUPED` | `transactions.jsonl` | Duplicate transaction events with the same `transaction_id` but marginally different timestamps, simulating double-delivery from the fintech's event system. |
| `ORPHANED_ACCOUNT` | `transactions.jsonl` | Transactions referencing an `account_id` that has no matching record in `accounts.csv`. These cannot be resolved to a customer. |
| `TYPE_MISMATCH` | `transactions.jsonl` | The `amount` field is delivered as a string value rather than a numeric type (e.g., `"349.50"` instead of `349.50`). |
| `DATE_FORMAT` | `transactions.jsonl`, `accounts.csv`, `customers.csv` | Date fields contain a mix of formats within the same column: `YYYY-MM-DD`, `DD/MM/YYYY`, and Unix epoch integers. |
| `CURRENCY_VARIANT` | `transactions.jsonl` | The `currency` field contains non-standard values representing South African Rand: `"R"`, `"rands"`, `710` (ISO 4217 numeric), `"zar"` (lowercase). All must be standardised to `"ZAR"`. |
| `NULL_REQUIRED` | `accounts.csv` | The `account_id` primary key field is null. These records have no valid identifier and cannot be loaded as-is. |

**Implementation requirements:**

- Each issue category must have a defined handling rule (correct, flag, quarantine, or reject), externalised in a configuration file — not hardcoded.
- The pipeline must produce a `dq_report.json` summarising: count of records affected per issue category, handling action taken, and percentage of total records impacted.
- The `dq_flag` column in `fact_transactions` must be set to the relevant issue code for flagged records, or `NULL` for clean records. Only the 6 codes above are valid values — no custom codes.
- The pipeline must not silently drop records. Every record must be either loaded (clean or flagged) or explicitly quarantined with a logged reason.

**Exact injection rates are not disclosed.** The counts in Section 3.2 of the challenge brief are approximate. Your DQ report is scored against the actual generated counts — not the approximations. Build detection logic that identifies all instances, not logic tuned to an expected percentage.

---

*End of document.*
