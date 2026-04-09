-- =============================================================================
-- NEDBANK DATA TALENT CHALLENGE 2026 — DE TRACK
-- Scoring Validation Queries — Gold Layer (Stage 1 and Stage 2)
-- =============================================================================
--
-- These three queries are the automated scoring checks applied against your
-- pipeline's Gold layer output. Your pipeline must produce output that satisfies
-- all three to receive full correctness marks.
--
-- SCORING WEIGHT
--   Query 1 — Transaction Volume by Type  :  5 points  (tolerance: ±2%)
--   Query 2 — Zero Unlinked Accounts      :  5 points  (zero tolerance)
--   Query 3 — Province Distribution       :  5 points  (tolerance: ±5%)
--
-- EXPECTED VALUES
--   Exact expected row counts and totals are NOT published here. They will be
--   verified by the scoring harness against a known-good answer key generated
--   from the same source dataset that is provided to participants. You do not
--   need to hit a specific magic number — you need to build a correct pipeline.
--   The tolerances above exist to absorb minor floating-point rounding in
--   aggregates; they are not an invitation to produce approximate output.
--
-- HOW TO RUN LOCALLY (DuckDB)
--   1. Install DuckDB (https://duckdb.org/docs/installation) and the Delta
--      extension:
--        INSTALL delta; LOAD delta;
--   2. Set the GOLD_PATH variable below to match your output directory, e.g.:
--        SET VARIABLE gold_path = '/data/output/gold';
--   3. Run this file:
--        duckdb < validation_queries.sql
--   Alternatively, open the DuckDB CLI and run:
--        .read validation_queries.sql
--
--   If your output is plain Parquet (not Delta), replace delta_scan() calls
--   with parquet_scan('path/to/table/**/*.parquet').
--
-- NOTE FOR PARTICIPANTS
--   The scoring harness reads tables using the Delta format
--   (delta_scan / delta.load). Ensure your Gold layer output contains valid
--   Delta Lake metadata (_delta_log/) alongside the Parquet part files.
--   See output_schema_spec.md §7 for the required directory structure.
--
-- =============================================================================

-- Set the path to your Gold layer output root.
-- Adjust this to match your local or container mount point.
SET VARIABLE gold_path = '/data/output/gold';

-- =============================================================================
-- QUERY 1: Transaction Volume by Type
-- =============================================================================
--
-- WHAT IT CHECKS
--   Verifies that fact_transactions is fully populated and that all four
--   recognised transaction types are present with correct record counts.
--   Also surfaces total amounts per type as a secondary sanity check.
--
-- EXPECTED OUTPUT SHAPE
--   Exactly 4 rows, one per transaction type:
--     CREDIT   | <count> | <total_amount>
--     DEBIT    | <count> | <total_amount>
--     FEE      | <count> | <total_amount>
--     REVERSAL | <count> | <total_amount>
--
-- TOLERANCE
--   count per type: ±2% of the expected value (see scoring harness answer key)
--   total_amount:   informational only — not scored on this query
--
-- FAILURE MODES TO WATCH FOR
--   - Fewer than 4 rows: a transaction type was dropped or filtered out
--   - Row count well below expected: deduplication logic too aggressive
--   - Row count above expected: duplicates were not removed in the Silver layer
--   - NULL in transaction_type: type standardisation failed for some records

SELECT
    transaction_type,
    COUNT(*)                        AS record_count,
    SUM(amount)                     AS total_amount,
    ROUND(COUNT(*) * 100.0
          / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM delta_scan(getvariable('gold_path') || '/fact_transactions')
GROUP BY transaction_type
ORDER BY transaction_type;

-- =============================================================================
-- QUERY 2: Zero Unlinked Accounts
-- =============================================================================
--
-- WHAT IT CHECKS
--   Verifies referential integrity between dim_accounts and dim_customers.
--   Every account in the Gold layer must be linked to a known customer via
--   the customer_id field. An unlinked account indicates either:
--     (a) the Silver layer failed to validate the account-customer join, or
--     (b) dim_accounts.customer_id was not correctly populated from
--         accounts.csv.customer_ref (see output_schema_spec.md §3, GAP-026).
--
-- EXPECTED OUTPUT SHAPE
--   Exactly 1 row:
--     unlinked_accounts
--     -----------------
--     0
--
-- TOLERANCE
--   Zero tolerance. Any value > 0 is a hard failure (0 points for this query).
--
-- IMPORTANT SCHEMA NOTE (GAP-026)
--   This query joins on dim_accounts.customer_id. This field was added to the
--   dim_accounts schema at position 3 as a fix to the original spec. Your
--   Gold layer dim_accounts table must include this field — it is NOT optional.
--   See output_schema_spec.md §3 for derivation details (source field:
--   accounts.csv.customer_ref, renamed to customer_id in the Gold layer).

SELECT
    COUNT(*) AS unlinked_accounts
FROM delta_scan(getvariable('gold_path') || '/dim_accounts')   AS a
LEFT JOIN delta_scan(getvariable('gold_path') || '/dim_customers') AS c
       ON a.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- =============================================================================
-- QUERY 3: Province Distribution
-- =============================================================================
--
-- WHAT IT CHECKS
--   Verifies that dim_customers covers all 9 South African provinces and that
--   the distribution of accounts across provinces matches expectations.
--   A missing province indicates either a data generation issue (not your fault)
--   or that province records were dropped during transformation.
--
-- EXPECTED OUTPUT SHAPE
--   Exactly 9 rows, one per SA province (alphabetical order):
--     Eastern Cape          | <account_count>
--     Free State            | <account_count>
--     Gauteng               | <account_count>
--     KwaZulu-Natal         | <account_count>
--     Limpopo               | <account_count>
--     Mpumalanga            | <account_count>
--     North West            | <account_count>
--     Northern Cape         | <account_count>
--     Western Cape          | <account_count>
--
-- TOLERANCE
--   account_count per province: ±5% of the expected value (scoring harness
--   answer key). The wider tolerance reflects natural population variance in
--   the generated dataset.
--
-- FAILURE MODES TO WATCH FOR
--   - Fewer than 9 rows: one or more provinces were dropped or mis-labelled
--   - account_count far below expected: accounts were lost in transformation
--   - Province name mismatch: ensure province values are standardised to the
--     canonical names listed above (title-case, full name, no abbreviations)

SELECT
    c.province,
    COUNT(DISTINCT a.account_id) AS account_count
FROM delta_scan(getvariable('gold_path') || '/dim_accounts')   AS a
JOIN delta_scan(getvariable('gold_path') || '/dim_customers')  AS c
  ON a.customer_id = c.customer_id
GROUP BY c.province
ORDER BY c.province;

-- =============================================================================
-- STAGE 2 BONUS QUERY: DQ Report Record Count Reconciliation
-- (Optional — for participant self-check only; not scored via SQL)
-- =============================================================================
--
-- WHAT IT CHECKS
--   Reconciles the total flagged record count in your dq_report.json against
--   the count of non-null dq_flag values in fact_transactions. The two figures
--   must agree (within ±5%) for Stage 2 DQ scoring.
--
--   Note: This bonus query reads your Gold layer SQL table only. The JSON
--   report reconciliation is performed separately by the scoring harness
--   using Python (not DuckDB). Use this query to verify your pipeline's
--   internal consistency before submitting.
--
-- EXPECTED OUTPUT SHAPE
--   2 rows:
--     source              | flagged_count
--     --------------------|--------------
--     fact_transactions   | <n>
--     (compare manually against dq_report.json .summary.total_flagged_records)
--
-- STAGE AVAILABILITY
--   Stage 1: Not applicable (no DQ issues injected; dq_flag should be all NULL)
--   Stage 2: Use this to self-check before submission
--   Stage 3: Same as Stage 2

SELECT
    'fact_transactions'       AS source,
    COUNT(*)                  AS flagged_count
FROM delta_scan(getvariable('gold_path') || '/fact_transactions')
WHERE dq_flag IS NOT NULL

UNION ALL

SELECT
    'clean_records'           AS source,
    COUNT(*)                  AS clean_count
FROM delta_scan(getvariable('gold_path') || '/fact_transactions')
WHERE dq_flag IS NULL

ORDER BY source;

-- =============================================================================
-- END OF VALIDATION QUERIES
-- Document: validation_queries.sql
-- Spec references: de_challenge_design_spec.md §4.3; output_schema_spec.md §§2-4, 8
-- =============================================================================
