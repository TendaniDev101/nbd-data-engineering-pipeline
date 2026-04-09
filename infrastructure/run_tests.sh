#!/usr/bin/env bash
# =============================================================================
# run_tests.sh — Nedbank DE Challenge: Local Testing Harness
#
# Runs 7 checks against your pipeline submission before you push a stage tag.
# These checks mirror what the automated scoring system does. Fix any FAILs
# before submitting.
#
# Usage:
#   bash run_tests.sh [options]
#
# Options:
#   --stage N           Stage number to test: 1, 2, or 3 (default: 1)
#   --data-dir PATH     Host directory to mount as /data (default: ./sample)
#   --stream-dir PATH   Host directory to mount as /data/stream (Stage 3 only)
#   --image NAME        Docker image to test (default: my-submission:test)
#   --build             Build the image before testing (runs docker build .)
#   --timeout N         Per-container timeout in seconds (default: 900 = 15 min)
#   --help              Show this help message
#
# Examples:
#   bash run_tests.sh
#   bash run_tests.sh --stage 2 --data-dir /tmp/test-data --image my-sub:latest
#   bash run_tests.sh --stage 3 --stream-dir /tmp/stream-data --build
#
# Exit codes:
#   0   All checks for the active stage passed
#   1   One or more checks failed
# =============================================================================

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────────────────────
STAGE=1
DATA_DIR="./sample"
STREAM_DIR=""
IMAGE="my-submission:test"
DO_BUILD=false
TIMEOUT_SECS=900    # 15 minutes (scoring system uses 30; we're stricter locally)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Colour helpers ────────────────────────────────────────────────────────────
# Disable colour if output is not a terminal (e.g. CI logs)
if [ -t 1 ]; then
    GREEN="\033[0;32m"
    RED="\033[0;31m"
    YELLOW="\033[0;33m"
    BOLD="\033[1m"
    RESET="\033[0m"
else
    GREEN="" RED="" YELLOW="" BOLD="" RESET=""
fi

pass() { echo -e "  ${GREEN}[PASS]${RESET} $1"; }
fail() { echo -e "  ${RED}[FAIL]${RESET} $1"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "  ${YELLOW}[INFO]${RESET} $1"; }
header() { echo -e "\n${BOLD}$1${RESET}"; }

FAILURES=0

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --stage)       STAGE="$2";       shift 2 ;;
        --data-dir)    DATA_DIR="$2";    shift 2 ;;
        --stream-dir)  STREAM_DIR="$2";  shift 2 ;;
        --image)       IMAGE="$2";       shift 2 ;;
        --build)       DO_BUILD=true;    shift   ;;
        --timeout)     TIMEOUT_SECS="$2"; shift 2 ;;
        --help)
            sed -n '/^# Usage/,/^[^#]/p' "$0" | grep '^#' | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo "Unknown option: $1 (run with --help for usage)"
            exit 1
            ;;
    esac
done

# ── Validate stage argument ───────────────────────────────────────────────────
if [[ ! "$STAGE" =~ ^[123]$ ]]; then
    echo "Error: --stage must be 1, 2, or 3 (got: $STAGE)"
    exit 1
fi

# ── Resolve data directory ────────────────────────────────────────────────────
# If --data-dir was not explicitly set and the default ./sample doesn't exist,
# fall back gracefully but warn the user.
if [[ "$DATA_DIR" == "./sample" ]] && [[ ! -d "$DATA_DIR" ]]; then
    info "No --data-dir supplied and ./sample not found."
    info "Create a data directory with accounts.csv, transactions.jsonl,"
    info "customers.csv, and config/pipeline_config.yaml, then re-run with:"
    info "  bash run_tests.sh --data-dir /path/to/your/data"
    DATA_DIR=""
fi

# Convert to absolute path if provided
if [[ -n "$DATA_DIR" ]]; then
    DATA_DIR="$(cd "$DATA_DIR" && pwd)"
fi

if [[ -n "$STREAM_DIR" ]]; then
    STREAM_DIR="$(cd "$STREAM_DIR" && pwd)"
fi

# ── Summary banner ────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}=== Nedbank DE Challenge — Local Test Harness ===${RESET}"
echo "  Stage:      $STAGE"
echo "  Image:      $IMAGE"
echo "  Data dir:   ${DATA_DIR:-'(not set)'}"
[[ "$STAGE" == "3" ]] && echo "  Stream dir: ${STREAM_DIR:-'(not set)'}"
echo "  Timeout:    ${TIMEOUT_SECS}s"
echo ""

# =============================================================================
# CHECK 1: Docker image builds successfully
# =============================================================================
header "Check 1: Docker image builds"

if $DO_BUILD; then
    info "Building image from current directory..."
    if docker build -t "$IMAGE" . > /tmp/docker_build.log 2>&1; then
        pass "docker build succeeded"
    else
        fail "docker build failed — see /tmp/docker_build.log for details"
        echo ""
        echo "  Last 20 lines of build log:"
        tail -20 /tmp/docker_build.log | sed 's/^/    /'
        echo ""
        echo -e "${RED}Build failed. Fix the Dockerfile before running other checks.${RESET}"
        exit 1
    fi
else
    # Check that the image exists locally
    if docker image inspect "$IMAGE" > /dev/null 2>&1; then
        pass "Image '$IMAGE' found locally (use --build to rebuild)"
    else
        fail "Image '$IMAGE' not found. Build it first or pass --build"
        echo "  Hint: docker build -t $IMAGE ."
        FAILURES=$((FAILURES + 1))
        # Cannot proceed without an image
        echo ""
        echo -e "${RED}Cannot run remaining checks without a valid image.${RESET}"
        exit 1
    fi
fi

# =============================================================================
# CHECK 2: Container starts and exits with code 0
# =============================================================================
header "Check 2: Container runs and exits 0"

if [[ -z "$DATA_DIR" ]]; then
    fail "No data directory available — skipping container run"
    info "Provide sample data with --data-dir to enable this check"
else
    # Prepare a fresh, empty output directory so we test against a clean run
    OUTPUT_DIR="$(mktemp -d /tmp/de_test_output.XXXXXX)"
    mkdir -p "$OUTPUT_DIR"

    # Build the docker run command using the same security flags as the scorer
    DOCKER_RUN_CMD=(
        docker run --rm
        --network=none
        --memory=2g --memory-swap=2g
        --cpus=2
        --pids-limit=512
        --read-only
        --tmpfs /tmp:rw,size=512m
        --cap-drop=ALL
        --security-opt no-new-privileges
        -e PYTHONDONTWRITEBYTECODE=1
        -v "${DATA_DIR}/input:/data/input:ro"
        -v "${DATA_DIR}/config:/data/config:ro"
        -v "${OUTPUT_DIR}:/data/output:rw"
    )

    # Stage 3: also mount the stream directory
    if [[ "$STAGE" == "3" ]] && [[ -n "$STREAM_DIR" ]]; then
        DOCKER_RUN_CMD+=(-v "${STREAM_DIR}:/data/stream:ro")
    fi

    DOCKER_RUN_CMD+=("$IMAGE")

    info "Running container (timeout: ${TIMEOUT_SECS}s)..."

    # Use 'timeout' if available, otherwise fall back to plain docker run
    TIMED_OUT=false
    if command -v timeout > /dev/null 2>&1; then
        if timeout "$TIMEOUT_SECS" "${DOCKER_RUN_CMD[@]}" > /tmp/container_run.log 2>&1; then
            EXIT_CODE=0
        else
            EXIT_CODE=$?
            if [[ $EXIT_CODE -eq 124 ]]; then
                TIMED_OUT=true
            fi
        fi
    else
        if "${DOCKER_RUN_CMD[@]}" > /tmp/container_run.log 2>&1; then
            EXIT_CODE=0
        else
            EXIT_CODE=$?
        fi
    fi

    if $TIMED_OUT; then
        fail "Container timed out after ${TIMEOUT_SECS}s (exit code 124)"
        info "The scoring system allows 30 minutes. Optimise your pipeline."
    elif [[ $EXIT_CODE -eq 0 ]]; then
        pass "Container exited with code 0"
    elif [[ $EXIT_CODE -eq 137 ]]; then
        fail "Container killed by OOM (exit 137). Reduce memory usage."
        info "Hard limit is 2 GB. Use local[2] Spark, avoid .toPandas() on large frames."
    else
        fail "Container exited with code $EXIT_CODE (expected 0)"
        info "Check /tmp/container_run.log for the Python traceback"
        echo ""
        echo "  Last 30 lines of container output:"
        tail -30 /tmp/container_run.log | sed 's/^/    /'
    fi

    # ==========================================================================
    # CHECK 3: Output directory structure exists
    # ==========================================================================
    header "Check 3: Output directory structure"

    EXPECTED_DIRS=(
        "bronze"
        "silver"
        "gold"
    )

    ALL_DIRS_OK=true
    for dir in "${EXPECTED_DIRS[@]}"; do
        if [[ -d "$OUTPUT_DIR/$dir" ]]; then
            pass "/data/output/$dir/ exists"
        else
            fail "/data/output/$dir/ not found — your pipeline must create this directory"
            ALL_DIRS_OK=false
        fi
    done

    # Stage 3: stream_gold directories
    if [[ "$STAGE" == "3" ]]; then
        for dir in "stream_gold/current_balances" "stream_gold/recent_transactions"; do
            if [[ -d "$OUTPUT_DIR/$dir" ]]; then
                pass "/data/output/$dir/ exists"
            else
                fail "/data/output/$dir/ not found"
                ALL_DIRS_OK=false
            fi
        done
    fi

    # ==========================================================================
    # CHECK 4: Gold layer Delta tables are readable by DuckDB
    # ==========================================================================
    header "Check 4: Gold layer Delta tables readable by DuckDB"

    # We run DuckDB inside a fresh container using the base image so the
    # participant doesn't need DuckDB installed on their host machine.
    # If the base image is not available, we try a direct host duckdb call.

    GOLD_TABLES=("fact_transactions" "dim_accounts" "dim_customers")
    DUCKDB_AVAILABLE=false

    # Try to locate duckdb on the host first (fast path)
    if command -v duckdb > /dev/null 2>&1; then
        DUCKDB_AVAILABLE=true
    fi

    check_gold_table_duckdb() {
        local table="$1"
        local table_path="$OUTPUT_DIR/gold/$table"

        if [[ ! -d "$table_path" ]]; then
            fail "gold/$table/ directory not found"
            return
        fi

        if $DUCKDB_AVAILABLE; then
            # Run DuckDB directly on the host
            local row_count
            row_count=$(duckdb -c "INSTALL delta; LOAD delta; SELECT COUNT(*) FROM delta_scan('${table_path}');" 2>/dev/null | grep -E '^[0-9]+$' | head -1 || echo "ERROR")

            if [[ "$row_count" == "ERROR" ]] || [[ -z "$row_count" ]]; then
                fail "gold/$table/ exists but DuckDB could not read it as a Delta table"
                info "Ensure your pipeline writes valid Delta Lake format (delta_log/ must be present)"
            elif [[ "$row_count" -eq 0 ]]; then
                fail "gold/$table/ is a valid Delta table but contains 0 rows"
            else
                pass "gold/$table/ readable by DuckDB ($row_count rows)"
            fi
        else
            # Fall back to checking for Delta log presence
            if [[ -d "$table_path/_delta_log" ]]; then
                pass "gold/$table/ contains _delta_log/ (Delta format assumed valid; install duckdb for full check)"
            else
                fail "gold/$table/ found but _delta_log/ missing — not a valid Delta table"
                info "Write using delta format in PySpark: df.write.format('delta').save(path)"
            fi
        fi
    }

    for table in "${GOLD_TABLES[@]}"; do
        check_gold_table_duckdb "$table"
    done

    # ==========================================================================
    # CHECK 5: Validation queries execute without error
    # ==========================================================================
    header "Check 5: Validation queries execute without error"

    # These are the three queries from validation_queries.sql.
    # We check structural correctness (query runs, returns expected row counts)
    # rather than exact values (exact values require the full dataset answer key).

    if $DUCKDB_AVAILABLE; then
        # Build a DuckDB script that registers the Gold tables and runs all 3 queries
        DUCKDB_SCRIPT=$(cat <<DUCKDB_EOF
INSTALL delta;
LOAD delta;

-- Register Gold tables as views
CREATE VIEW fact_transactions AS SELECT * FROM delta_scan('${OUTPUT_DIR}/gold/fact_transactions');
CREATE VIEW dim_accounts      AS SELECT * FROM delta_scan('${OUTPUT_DIR}/gold/dim_accounts');
CREATE VIEW dim_customers     AS SELECT * FROM delta_scan('${OUTPUT_DIR}/gold/dim_customers');

-- Q1: Transaction volume by type (expected: 4 rows)
SELECT 'Q1' AS query, COUNT(*) AS result_rows FROM (
    SELECT transaction_type, COUNT(*) AS cnt, SUM(amount) AS total_amount
    FROM fact_transactions
    GROUP BY transaction_type
    ORDER BY transaction_type
);

-- Q2: Orphaned accounts (expected: 0)
SELECT 'Q2' AS query, COUNT(*) AS unlinked_accounts
FROM dim_accounts a
LEFT JOIN dim_customers c ON a.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Q3: Province distribution (expected: up to 9 rows)
SELECT 'Q3' AS query, COUNT(*) AS result_rows FROM (
    SELECT c.province, COUNT(DISTINCT a.account_id) AS account_count
    FROM dim_accounts a
    JOIN dim_customers c ON a.customer_id = c.customer_id
    GROUP BY c.province
    ORDER BY c.province
);
DUCKDB_EOF
)
        QUERY_OUTPUT=$(echo "$DUCKDB_SCRIPT" | duckdb 2>/tmp/duckdb_queries.log || true)

        if [[ $? -ne 0 ]] || grep -q "Error" /tmp/duckdb_queries.log 2>/dev/null; then
            fail "One or more validation queries failed to execute"
            info "See /tmp/duckdb_queries.log for the error"
            tail -10 /tmp/duckdb_queries.log | sed 's/^/    /'
        else
            # Q1: expect 4 rows (CREDIT, DEBIT, FEE, REVERSAL)
            Q1_ROWS=$(echo "$QUERY_OUTPUT" | grep "^Q1" | awk '{print $NF}' || echo "0")
            if [[ "$Q1_ROWS" == "4" ]]; then
                pass "Q1 (transaction_type distribution) returned 4 rows as expected"
            else
                fail "Q1 returned $Q1_ROWS rows (expected 4 — one per transaction type: CREDIT, DEBIT, FEE, REVERSAL)"
            fi

            # Q2: expect 0 orphaned accounts
            Q2_ORPHANS=$(echo "$QUERY_OUTPUT" | grep "^Q2" | awk '{print $NF}' || echo "-1")
            if [[ "$Q2_ORPHANS" == "0" ]]; then
                pass "Q2 (orphaned accounts) returned 0 — all accounts link to a customer"
            else
                fail "Q2 returned $Q2_ORPHANS orphaned accounts (expected 0) — check dim_accounts.customer_id join to dim_customers.customer_id"
            fi

            # Q3: expect up to 9 rows (9 SA provinces)
            Q3_ROWS=$(echo "$QUERY_OUTPUT" | grep "^Q3" | awk '{print $NF}' || echo "0")
            if [[ "$Q3_ROWS" -ge 1 ]] && [[ "$Q3_ROWS" -le 9 ]]; then
                pass "Q3 (province distribution) returned $Q3_ROWS province rows (expected ≤9)"
            elif [[ "$Q3_ROWS" -eq 0 ]]; then
                fail "Q3 returned 0 rows — province join produced no results"
            else
                fail "Q3 returned $Q3_ROWS rows (expected at most 9 — one per SA province)"
            fi
        fi
    else
        info "DuckDB not found on host — skipping query execution check"
        info "Install DuckDB (https://duckdb.org) to enable this check"
        info "The scoring system always runs these queries; install duckdb before submitting"
    fi

    # ==========================================================================
    # CHECK 6 (Stage 2+): DQ report exists and is valid JSON with required keys
    # ==========================================================================
    if [[ "$STAGE" -ge 2 ]]; then
        header "Check 6: DQ report (Stage 2+)"

        DQ_REPORT="$OUTPUT_DIR/dq_report.json"

        if [[ ! -f "$DQ_REPORT" ]]; then
            fail "dq_report.json not found at /data/output/dq_report.json"
            info "Your pipeline must write this file at Stage 2+"
        else
            # Validate it parses as JSON and contains required top-level keys
            if command -v python3 > /dev/null 2>&1; then
                VALIDATION_RESULT=$(python3 - <<PYEOF 2>&1
import json, sys

required_keys = [
    "total_records",
    "clean_records",
    "flagged_records",
    "flag_counts",
]

try:
    with open("$DQ_REPORT") as f:
        report = json.load(f)
except json.JSONDecodeError as e:
    print(f"INVALID_JSON: {e}")
    sys.exit(1)

missing = [k for k in required_keys if k not in report]
if missing:
    print(f"MISSING_KEYS: {', '.join(missing)}")
    sys.exit(1)

flag_counts = report.get("flag_counts", {})
if not isinstance(flag_counts, dict):
    print("INVALID_FLAG_COUNTS: flag_counts must be a JSON object")
    sys.exit(1)

# Verify all total_records >= flagged_records (basic sanity)
total = report.get("total_records", 0)
flagged = report.get("flagged_records", 0)
if total < flagged:
    print(f"SANITY_FAIL: total_records ({total}) < flagged_records ({flagged})")
    sys.exit(1)

print(f"OK: total={total}, clean={report.get('clean_records', 0)}, flagged={flagged}")
PYEOF
)
                if echo "$VALIDATION_RESULT" | grep -q "^OK:"; then
                    pass "dq_report.json is valid JSON with required keys ($VALIDATION_RESULT)"
                else
                    fail "dq_report.json validation failed: $VALIDATION_RESULT"
                fi
            else
                # Minimal check: file is non-empty and looks like JSON
                FIRST_CHAR=$(head -c 1 "$DQ_REPORT")
                if [[ "$FIRST_CHAR" == "{" ]]; then
                    pass "dq_report.json exists and appears to be a JSON object (install python3 for full validation)"
                else
                    fail "dq_report.json exists but does not start with '{' — may not be valid JSON"
                fi
            fi
        fi
    fi

    # ==========================================================================
    # CHECK 7 (Stage 3): Streaming output tables exist and are non-empty
    # ==========================================================================
    if [[ "$STAGE" -eq 3 ]]; then
        header "Check 7: Streaming tables (Stage 3)"

        STREAM_TABLES=("current_balances" "recent_transactions")

        for table in "${STREAM_TABLES[@]}"; do
            TABLE_PATH="$OUTPUT_DIR/stream_gold/$table"

            if [[ ! -d "$TABLE_PATH" ]]; then
                fail "stream_gold/$table/ not found"
                info "Your streaming pipeline must write to /data/output/stream_gold/$table/"
                continue
            fi

            if [[ ! -d "$TABLE_PATH/_delta_log" ]]; then
                fail "stream_gold/$table/ exists but has no _delta_log/ — not a Delta table"
                continue
            fi

            # Check for at least one parquet file (non-empty table)
            PARQUET_COUNT=$(find "$TABLE_PATH" -name "*.parquet" | wc -l)
            if [[ "$PARQUET_COUNT" -gt 0 ]]; then
                if $DUCKDB_AVAILABLE; then
                    ROW_COUNT=$(duckdb -c "INSTALL delta; LOAD delta; SELECT COUNT(*) FROM delta_scan('${TABLE_PATH}');" 2>/dev/null | grep -E '^[0-9]+$' | head -1 || echo "0")
                    if [[ "$ROW_COUNT" -gt 0 ]]; then
                        pass "stream_gold/$table/ is a non-empty Delta table ($ROW_COUNT rows)"
                    else
                        fail "stream_gold/$table/ is a Delta table but contains 0 rows"
                    fi
                else
                    pass "stream_gold/$table/ is a non-empty Delta table (${PARQUET_COUNT} parquet file(s))"
                fi
            else
                fail "stream_gold/$table/ has _delta_log/ but no parquet files — table is empty"
            fi
        done
    fi

    # Clean up temporary output directory
    rm -rf "$OUTPUT_DIR"
fi

# =============================================================================
# Final summary
# =============================================================================
echo ""
echo -e "${BOLD}=== Test Summary ===${RESET}"
echo "  Stage tested: $STAGE"

if [[ $FAILURES -eq 0 ]]; then
    echo -e "  ${GREEN}All checks passed.${RESET} You are ready to push your stage${STAGE}-submission tag."
    echo ""
    echo "  Next step:"
    echo "    git tag -a stage${STAGE}-submission -m \"Stage ${STAGE} submission\""
    echo "    git push origin stage${STAGE}-submission"
    exit 0
else
    echo -e "  ${RED}${FAILURES} check(s) failed.${RESET} Fix the issues above before submitting."
    echo ""
    echo "  The scoring system will record zero correctness points for any"
    echo "  stage where the container exits non-zero or outputs are absent."
    exit 1
fi
