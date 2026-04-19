"""
Gold layer: Join and aggregate Silver tables into the scored output schema.

Input paths (Silver layer output — read these, do not modify):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Output paths (your pipeline must create these directories):
  /data/output/gold/fact_transactions/     — 15 fields (see output_schema_spec.md §2)
  /data/output/gold/dim_accounts/          — 11 fields (see output_schema_spec.md §3)
  /data/output/gold/dim_customers/         — 9 fields  (see output_schema_spec.md §4)

Requirements:
  - Generate surrogate keys (_sk fields) that are unique, non-null, and stable
    across pipeline re-runs on the same input data. Use row_number() with a
    stable ORDER BY on the natural key, or sha2(natural_key, 256) cast to BIGINT.
  - Resolve all foreign key relationships:
      fact_transactions.account_sk  → dim_accounts.account_sk
      fact_transactions.customer_sk → dim_customers.customer_sk
      dim_accounts.customer_id      → dim_customers.customer_id
  - Rename accounts.customer_ref → dim_accounts.customer_id at this layer.
  - Derive dim_customers.age_band from dob (do not copy dob directly).
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.
  - At Stage 2, also write /data/output/dq_report.json summarising DQ outcomes.

See output_schema_spec.md for the complete field-by-field specification.
"""

import json
import os
import time
from datetime import datetime, timezone

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_CONFIG_PATH = "/data/config/pipeline_config.yaml"
DEFAULT_DQ_RULES_PATH = "/data/config/dq_rules.yaml"


def _resolve_config_path() -> str:
    explicit_path = os.environ.get("PIPELINE_CONFIG")
    if explicit_path:
        return explicit_path

    candidates = [
        DEFAULT_CONFIG_PATH,
        os.path.join(os.getcwd(), "config", "pipeline_config.yaml"),
        os.path.join(os.path.dirname(__file__), "..", "config", "pipeline_config.yaml"),
    ]

    for path in candidates:
        normalized = os.path.abspath(path)
        if os.path.exists(normalized):
            return normalized

    return DEFAULT_CONFIG_PATH


def _load_config() -> dict:
    config_path = _resolve_config_path()
    with open(config_path, "r", encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


def _resolve_dq_rules_path(config: dict) -> str:
    explicit_path = os.environ.get("PIPELINE_DQ_RULES")
    if explicit_path:
        return explicit_path

    dq_cfg = config.get("dq", {})
    configured_path = dq_cfg.get("rules_path") if isinstance(dq_cfg, dict) else None
    if configured_path:
        return configured_path

    candidates = [
        DEFAULT_DQ_RULES_PATH,
        os.path.join(os.getcwd(), "config", "dq_rules.yaml"),
        os.path.join(os.path.dirname(__file__), "..", "config", "dq_rules.yaml"),
    ]

    for path in candidates:
        normalized = os.path.abspath(path)
        if os.path.exists(normalized):
            return normalized

    return DEFAULT_DQ_RULES_PATH


def _load_dq_rules(config: dict) -> dict:
    rules_path = _resolve_dq_rules_path(config)
    if not os.path.exists(rules_path):
        return {}

    with open(rules_path, "r", encoding="utf-8") as rules_file:
        loaded = yaml.safe_load(rules_file) or {}
        return loaded if isinstance(loaded, dict) else {}


def _build_spark_session(spark_config: dict) -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    configured_jars = spark_config.get("spark.jars")
    delta_jars = os.environ.get("DELTA_JARS")
    merged_jars = ",".join([j for j in [configured_jars, delta_jars] if j])

    builder = (
        SparkSession.builder.master(spark_config.get("master", "local[2]"))
        .appName(spark_config.get("app_name", "nedbank-de-pipeline"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.host", spark_config.get("driver_host", "127.0.0.1"))
        .config("spark.driver.bindAddress", spark_config.get("driver_bind_address", "127.0.0.1"))
        .config("spark.local.hostname", spark_config.get("local_hostname", "localhost"))
        .config(
            "spark.sql.parquet.compression.codec",
            spark_config.get("spark.sql.parquet.compression.codec", "uncompressed"),
        )
    )
    if merged_jars:
        builder = builder.config("spark.jars", merged_jars)

    for key, value in spark_config.items():
        if key in {"master", "app_name", "spark.jars"}:
            continue
        builder = builder.config(key, str(value))

    return builder.getOrCreate()


def _write_delta(df, path: str) -> None:
    df.write.format("delta").mode("overwrite").save(path)


def _missing_string(column_name: str):
    return F.col(column_name).isNull() | (F.length(F.trim(F.col(column_name).cast("string"))) == 0)


def _stable_bigint_sk(column_name: str):
    return F.expr(
        f"cast(conv(substr(sha2(coalesce(cast({column_name} as string), ''), 256), 1, 15), 16, 10) as bigint)"
    )


def _collect_flag_counts(df) -> dict:
    rows = (
        df.filter(F.col("dq_flag").isNotNull())
        .groupBy("dq_flag")
        .count()
        .collect()
    )
    return {row["dq_flag"]: int(row["count"]) for row in rows if row["dq_flag"]}


def _write_dq_report(path: str, payload: dict) -> None:
    report_dir = os.path.dirname(path)
    if report_dir:
        os.makedirs(report_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as report_file:
        json.dump(payload, report_file, indent=2, sort_keys=False)


def run_provisioning():
    config = _load_config()
    dq_rules = _load_dq_rules(config)
    output_cfg = config["output"]
    spark_cfg = config.get("spark", {})

    bronze_root = output_cfg["bronze_path"]
    silver_root = output_cfg["silver_path"]
    gold_root = output_cfg["gold_path"]
    dq_report_path = output_cfg.get("dq_report_path", "/data/output/dq_report.json")

    spark = _build_spark_session(spark_cfg)

    try:
        bronze_accounts = spark.read.format("delta").load(os.path.join(bronze_root, "accounts"))
        bronze_customers = spark.read.format("delta").load(os.path.join(bronze_root, "customers"))
        bronze_transactions = spark.read.format("delta").load(os.path.join(bronze_root, "transactions"))

        silver_accounts = spark.read.format("delta").load(os.path.join(silver_root, "accounts"))
        silver_customers = spark.read.format("delta").load(os.path.join(silver_root, "customers"))
        silver_transactions = spark.read.format("delta").load(os.path.join(silver_root, "transactions"))

        age_years = F.floor(F.datediff(F.current_date(), F.col("dob")) / F.lit(365.25))
        dim_customers_base = silver_customers.filter(F.col("customer_id").isNotNull()).select(
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("gender").cast("string").alias("gender"),
            F.col("province").cast("string").alias("province"),
            F.col("income_band").cast("string").alias("income_band"),
            F.col("segment").cast("string").alias("segment"),
            F.col("risk_score").cast("int").alias("risk_score"),
            F.col("kyc_status").cast("string").alias("kyc_status"),
            F.when(age_years >= 65, F.lit("65+"))
            .when(age_years >= 56, F.lit("56-65"))
            .when(age_years >= 46, F.lit("46-55"))
            .when(age_years >= 36, F.lit("36-45"))
            .when(age_years >= 26, F.lit("26-35"))
            .when(age_years >= 18, F.lit("18-25"))
            .otherwise(F.lit(None).cast("string"))
            .alias("age_band"),
        )

        dim_customers = dim_customers_base.withColumn(
            "customer_sk",
            _stable_bigint_sk("customer_id"),
        ).select(
            "customer_sk",
            "customer_id",
            "gender",
            "province",
            "income_band",
            "segment",
            "risk_score",
            "kyc_status",
            "age_band",
        )

        customer_keys = dim_customers.select("customer_id")
        dim_accounts_base = (
            silver_accounts.filter(F.col("account_id").isNotNull() & F.col("customer_ref").isNotNull())
            .join(customer_keys, silver_accounts["customer_ref"] == customer_keys["customer_id"], "inner")
            .select(
                F.col("account_id").cast("string").alias("account_id"),
                F.col("customer_ref").cast("string").alias("customer_id"),
                F.col("account_type").cast("string").alias("account_type"),
                F.col("account_status").cast("string").alias("account_status"),
                F.col("open_date").cast("date").alias("open_date"),
                F.col("product_tier").cast("string").alias("product_tier"),
                F.col("digital_channel").cast("string").alias("digital_channel"),
                F.col("credit_limit").cast("decimal(18,2)").alias("credit_limit"),
                F.col("current_balance").cast("decimal(18,2)").alias("current_balance"),
                F.col("last_activity_date").cast("date").alias("last_activity_date"),
            )
        )

        dim_accounts = dim_accounts_base.withColumn(
            "account_sk",
            _stable_bigint_sk("account_id"),
        ).select(
            "account_sk",
            "account_id",
            "customer_id",
            "account_type",
            "account_status",
            "open_date",
            "product_tier",
            "digital_channel",
            "credit_limit",
            "current_balance",
            "last_activity_date",
        )

        accounts_for_fact = dim_accounts.select("account_id", "account_sk", "customer_id")
        customers_for_fact = dim_customers.select("customer_id", "customer_sk", "province")

        fact_base = (
            silver_transactions.alias("t")
            .join(accounts_for_fact.alias("a"), F.col("t.account_id") == F.col("a.account_id"), "inner")
            .join(customers_for_fact.alias("c"), F.col("a.customer_id") == F.col("c.customer_id"), "inner")
            .filter(
                F.col("t.transaction_id").isNotNull()
                & F.col("t.transaction_date").isNotNull()
                & F.col("t.transaction_timestamp").isNotNull()
                & F.col("t.transaction_type").isNotNull()
                & F.col("t.amount").isNotNull()
                & F.col("t.channel").isNotNull()
            )
            .select(
                F.col("t.transaction_id").cast("string").alias("transaction_id"),
                F.col("a.account_sk").cast("bigint").alias("account_sk"),
                F.col("c.customer_sk").cast("bigint").alias("customer_sk"),
                F.col("t.transaction_date").cast("date").alias("transaction_date"),
                F.col("t.transaction_timestamp").cast("timestamp").alias("transaction_timestamp"),
                F.col("t.transaction_type").cast("string").alias("transaction_type"),
                F.col("t.merchant_category").cast("string").alias("merchant_category"),
                F.col("t.merchant_subcategory").cast("string").alias("merchant_subcategory"),
                F.col("t.amount").cast("decimal(18,2)").alias("amount"),
                F.lit("ZAR").cast("string").alias("currency"),
                F.col("t.channel").cast("string").alias("channel"),
                F.col("c.province").cast("string").alias("province"),
                F.col("t.dq_flag").cast("string").alias("dq_flag"),
                F.col("t.ingestion_timestamp").cast("timestamp").alias("ingestion_timestamp"),
            )
        )

        fact_transactions = fact_base.withColumn(
            "transaction_sk",
            _stable_bigint_sk("transaction_id"),
        ).select(
            "transaction_sk",
            "transaction_id",
            "account_sk",
            "customer_sk",
            "transaction_date",
            "transaction_timestamp",
            "transaction_type",
            "merchant_category",
            "merchant_subcategory",
            "amount",
            "currency",
            "channel",
            "province",
            "dq_flag",
            "ingestion_timestamp",
        )

        _write_delta(dim_customers, os.path.join(gold_root, "dim_customers"))
        _write_delta(dim_accounts, os.path.join(gold_root, "dim_accounts"))
        _write_delta(fact_transactions, os.path.join(gold_root, "fact_transactions"))

        accounts_raw = int(bronze_accounts.count())
        customers_raw = int(bronze_customers.count())
        transactions_raw = int(bronze_transactions.count())

        tx_flag_counts = _collect_flag_counts(silver_transactions)
        account_null_cfg = dq_rules.get("account_null_required", {}) if isinstance(dq_rules, dict) else {}
        account_null_key = account_null_cfg.get("key_field", "account_id")
        account_null_issue_code = account_null_cfg.get("issue_code", "NULL_REQUIRED_ACCOUNT")
        account_null_count = (
            int(bronze_accounts.filter(_missing_string(account_null_key)).count())
            if account_null_key in bronze_accounts.columns
            else 0
        )
        if account_null_count > 0:
            tx_flag_counts[account_null_issue_code] = tx_flag_counts.get(account_null_issue_code, 0) + account_null_count

        rules = dq_rules.get("rules", {}) if isinstance(dq_rules, dict) else {}
        total_records = accounts_raw + customers_raw + transactions_raw
        flagged_records = int(sum(tx_flag_counts.values()))
        clean_records = max(total_records - flagged_records, 0)

        stage_env = os.environ.get("PIPELINE_STAGE")
        stage_value = stage_env if stage_env else ("2" if flagged_records > 0 else "1")

        run_start_epoch = float(os.environ.get("PIPELINE_RUN_START_EPOCH", str(time.time())))
        run_timestamp = os.environ.get(
            "PIPELINE_RUN_TIMESTAMP_UTC",
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        execution_duration = max(int(time.time() - run_start_epoch), 0)

        issue_order = dq_rules.get("dq_flag_priority", []) if isinstance(dq_rules, dict) else []
        issue_order = issue_order if isinstance(issue_order, list) else []

        ordered_issue_codes = []
        for issue_code in issue_order:
            if issue_code in tx_flag_counts and issue_code not in ordered_issue_codes:
                ordered_issue_codes.append(issue_code)
        for issue_code in sorted(tx_flag_counts.keys()):
            if issue_code not in ordered_issue_codes:
                ordered_issue_codes.append(issue_code)

        source_counts_lookup = {
            "accounts_raw": accounts_raw,
            "customers_raw": customers_raw,
            "transactions_raw": transactions_raw,
        }

        dq_issues = []
        for issue_code in ordered_issue_codes:
            issue_count = int(tx_flag_counts.get(issue_code, 0))
            if issue_count <= 0:
                continue

            if issue_code == account_null_issue_code:
                issue_type = account_null_cfg.get("issue_type", issue_code.lower())
                handling_action = account_null_cfg.get("handling_action", "EXCLUDED_NULL_PK")
                denominator_key = account_null_cfg.get("denominator", "accounts_raw")
            else:
                issue_cfg = rules.get(issue_code, {}) if isinstance(rules.get(issue_code, {}), dict) else {}
                issue_type = issue_cfg.get("issue_type", issue_code.lower())
                handling_action = issue_cfg.get("handling_action", "FLAGGED")
                denominator_key = issue_cfg.get("denominator", "transactions_raw")

            denominator = int(source_counts_lookup.get(denominator_key, transactions_raw))
            percentage_of_total = round((issue_count * 100.0 / denominator), 2) if denominator > 0 else 0.0
            records_in_output = (
                0
                if handling_action in {"QUARANTINED", "EXCLUDED_NULL_PK", "EXCLUDED_CAST_FAILED"}
                else issue_count
            )

            dq_issues.append(
                {
                    "issue_type": issue_type,
                    "records_affected": issue_count,
                    "percentage_of_total": percentage_of_total,
                    "handling_action": handling_action,
                    "records_in_output": records_in_output,
                }
            )

        dq_report = {
            "$schema": "nedbank-de-challenge/dq-report/v1",
            "run_timestamp": run_timestamp,
            "stage": str(stage_value),
            "source_record_counts": {
                "accounts_raw": accounts_raw,
                "transactions_raw": transactions_raw,
                "customers_raw": customers_raw,
            },
            "dq_issues": dq_issues,
            "gold_layer_record_counts": {
                "fact_transactions": int(fact_transactions.count()),
                "dim_accounts": int(dim_accounts.count()),
                "dim_customers": int(dim_customers.count()),
            },
            "execution_duration_seconds": execution_duration,
            "total_records": total_records,
            "clean_records": clean_records,
            "flagged_records": flagged_records,
            "flag_counts": tx_flag_counts,
        }

        _write_dq_report(dq_report_path, dq_report)
    finally:
        spark.stop()
