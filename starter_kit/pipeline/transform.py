"""
Silver layer: Clean and conform Bronze tables into validated Silver Delta tables.

Input paths (Bronze layer output — read these, do not modify):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Output paths (your pipeline must create these directories):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Requirements:
  - Deduplicate records within each table on natural keys
    (account_id, transaction_id, customer_id respectively).
  - Standardise data types (e.g. parse date strings to DATE, cast amounts to
    DECIMAL(18,2), normalise currency variants to "ZAR").
  - Apply DQ flagging to transactions:
      - Set dq_flag = NULL for clean records.
      - Set dq_flag to the appropriate issue code for flagged records.
      - Valid codes: ORPHANED_ACCOUNT, DUPLICATE_DEDUPED, TYPE_MISMATCH,
        DATE_FORMAT, CURRENCY_VARIANT, NULL_REQUIRED.
  - At Stage 2, load DQ rules from config/dq_rules.yaml rather than hardcoding.
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.

See output_schema_spec.md §8 for the full list of DQ flag values and their
definitions.
"""

import os

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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


def _string_col(name: str):
    return F.trim(F.col(name).cast("string"))


def _parse_date(name: str):
    raw = _string_col(name)
    epoch_seconds = F.when(
        F.length(raw) >= 13,
        (raw.cast("double") / F.lit(1000)).cast("bigint"),
    ).otherwise(raw.cast("bigint"))
    return F.coalesce(
        F.to_date(raw, "yyyy-MM-dd"),
        F.to_date(raw, "dd/MM/yyyy"),
        F.to_date(F.from_unixtime(epoch_seconds)),
    )


def _dedupe_on_key(df, key: str):
    dedupe_window = Window.partitionBy(key).orderBy(
        F.col("ingestion_timestamp").desc_nulls_last(),
        F.col(key).asc_nulls_last(),
    )
    return df.withColumn("_rn", F.row_number().over(dedupe_window)).filter(F.col("_rn") == 1).drop("_rn")


def _write_delta(df, path: str) -> None:
    df.write.format("delta").mode("overwrite").save(path)


def _missing_string(column_name: str):
    return F.col(column_name).isNull() | (F.length(F.trim(F.col(column_name).cast("string"))) == 0)


def _bool_or(expressions):
    result = F.lit(False)
    for expression in expressions:
        result = result | expression
    return result


def run_transformation():
    config = _load_config()
    dq_rules = _load_dq_rules(config)
    output_cfg = config["output"]
    spark_cfg = config.get("spark", {})

    bronze_root = output_cfg["bronze_path"]
    silver_root = output_cfg["silver_path"]

    spark = _build_spark_session(spark_cfg)

    try:
        bronze_accounts = spark.read.format("delta").load(os.path.join(bronze_root, "accounts"))
        bronze_customers = spark.read.format("delta").load(os.path.join(bronze_root, "customers"))
        bronze_transactions = spark.read.format("delta").load(os.path.join(bronze_root, "transactions"))

        accounts_typed = bronze_accounts.select(
            _string_col("account_id").alias("account_id"),
            _string_col("customer_ref").alias("customer_ref"),
            F.upper(_string_col("account_type")).alias("account_type"),
            F.upper(_string_col("account_status")).alias("account_status"),
            _parse_date("open_date").alias("open_date"),
            F.upper(_string_col("product_tier")).alias("product_tier"),
            _string_col("mobile_number").alias("mobile_number"),
            F.upper(_string_col("digital_channel")).alias("digital_channel"),
            F.col("credit_limit").cast("decimal(18,2)").alias("credit_limit"),
            F.col("current_balance").cast("decimal(18,2)").alias("current_balance"),
            _parse_date("last_activity_date").alias("last_activity_date"),
            F.col("ingestion_timestamp").cast("timestamp").alias("ingestion_timestamp"),
        )
        silver_accounts = _dedupe_on_key(accounts_typed, "account_id").filter(
            F.col("account_id").isNotNull() & F.col("customer_ref").isNotNull()
        )

        customers_typed = bronze_customers.select(
            _string_col("customer_id").alias("customer_id"),
            _string_col("id_number").alias("id_number"),
            _string_col("first_name").alias("first_name"),
            _string_col("last_name").alias("last_name"),
            _parse_date("dob").alias("dob"),
            F.upper(_string_col("gender")).alias("gender"),
            _string_col("province").alias("province"),
            F.upper(_string_col("income_band")).alias("income_band"),
            F.upper(_string_col("segment")).alias("segment"),
            F.col("risk_score").cast("int").alias("risk_score"),
            F.upper(_string_col("kyc_status")).alias("kyc_status"),
            _string_col("product_flags").alias("product_flags"),
            F.col("ingestion_timestamp").cast("timestamp").alias("ingestion_timestamp"),
        )
        silver_customers = _dedupe_on_key(customers_typed, "customer_id").filter(F.col("customer_id").isNotNull())

        merchant_subcategory_col = (
            _string_col("merchant_subcategory")
            if "merchant_subcategory" in bronze_transactions.columns
            else F.lit(None).cast("string")
        )
        province_col = (
            F.trim(F.col("location.province").cast("string"))
            if "location" in bronze_transactions.columns
            else F.lit(None).cast("string")
        )

        date_raw = _string_col("transaction_date")
        time_raw = _string_col("transaction_time")
        parsed_date = _parse_date("transaction_date")
        transaction_timestamp = F.to_timestamp(
            F.concat_ws(" ", F.date_format(parsed_date, "yyyy-MM-dd"), time_raw),
            "yyyy-MM-dd HH:mm:ss",
        )
        amount_decimal = F.col("amount").cast("decimal(18,2)")
        currency_raw = _string_col("currency")

        tx_working = bronze_transactions.select(
            _string_col("transaction_id").alias("transaction_id"),
            _string_col("account_id").alias("account_id"),
            date_raw.alias("transaction_date_raw"),
            parsed_date.alias("transaction_date"),
            time_raw.alias("transaction_time"),
            transaction_timestamp.alias("transaction_timestamp"),
            F.upper(_string_col("transaction_type")).alias("transaction_type"),
            F.upper(_string_col("merchant_category")).alias("merchant_category"),
            merchant_subcategory_col.alias("merchant_subcategory"),
            F.col("amount").cast("string").alias("amount_raw"),
            amount_decimal.alias("amount"),
            currency_raw.alias("currency_raw"),
            F.lit("ZAR").cast("string").alias("currency"),
            F.upper(_string_col("channel")).alias("channel"),
            province_col.alias("province"),
            F.col("ingestion_timestamp").cast("timestamp").alias("ingestion_timestamp"),
        )

        tx_key_window = Window.partitionBy("transaction_id").orderBy(
            F.col("ingestion_timestamp").desc_nulls_last(),
            F.col("transaction_timestamp").desc_nulls_last(),
        )
        tx_group_window = Window.partitionBy("transaction_id")

        tx_deduped = (
            tx_working.withColumn("_dup_count", F.count(F.lit(1)).over(tx_group_window))
            .withColumn("_rn", F.row_number().over(tx_key_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        valid_accounts = silver_accounts.select(F.col("account_id").alias("_valid_account_id"))
        tx_with_accounts = tx_deduped.join(
            valid_accounts,
            tx_deduped["account_id"] == valid_accounts["_valid_account_id"],
            "left",
        )

        rules = dq_rules.get("rules", {}) if isinstance(dq_rules, dict) else {}
        default_priority = [
            "NULL_REQUIRED",
            "TYPE_MISMATCH",
            "DATE_FORMAT",
            "CURRENCY_VARIANT",
            "ORPHANED_ACCOUNT",
            "DUPLICATE_DEDUPED",
        ]
        dq_priority = dq_rules.get("dq_flag_priority", default_priority)
        if not isinstance(dq_priority, list) or not dq_priority:
            dq_priority = default_priority

        available_columns = set(tx_with_accounts.columns)

        null_rule = rules.get("NULL_REQUIRED", {})
        default_null_fields = [
            "transaction_id",
            "account_id",
            "transaction_date_raw",
            "transaction_time",
            "transaction_type",
            "amount_raw",
            "currency_raw",
            "channel",
        ]
        null_fields = null_rule.get("fields", default_null_fields)
        null_fields = [field for field in null_fields if field in available_columns]
        null_required = _bool_or([_missing_string(field) for field in null_fields]) if null_fields else F.lit(False)

        type_rule = rules.get("TYPE_MISMATCH", {})
        amount_raw_field = type_rule.get("amount_raw_field", "amount_raw")
        amount_cast_field = type_rule.get("amount_cast_field", "amount")
        timestamp_field = type_rule.get("timestamp_field", "transaction_timestamp")
        timestamp_source_fields = type_rule.get("timestamp_source_fields", ["transaction_date", "transaction_time"])
        allowed_transaction_types = [str(value).upper() for value in type_rule.get("allowed_transaction_types", [])]
        allowed_channels = [str(value).upper() for value in type_rule.get("allowed_channels", [])]

        amount_cast_failed = (
            F.col(amount_raw_field).isNotNull() & F.col(amount_cast_field).isNull()
            if amount_raw_field in available_columns and amount_cast_field in available_columns
            else F.lit(False)
        )

        timestamp_source_present = F.lit(True)
        usable_timestamp_sources = [field for field in timestamp_source_fields if field in available_columns]
        for source_field in usable_timestamp_sources:
            timestamp_source_present = timestamp_source_present & F.col(source_field).isNotNull()
        timestamp_cast_failed = (
            timestamp_source_present & F.col(timestamp_field).isNull()
            if timestamp_field in available_columns and usable_timestamp_sources
            else F.lit(False)
        )

        invalid_transaction_type = (
            F.col("transaction_type").isNotNull() & (~F.col("transaction_type").isin(allowed_transaction_types))
            if "transaction_type" in available_columns and allowed_transaction_types
            else F.lit(False)
        )
        invalid_channel = (
            F.col("channel").isNotNull() & (~F.col("channel").isin(allowed_channels))
            if "channel" in available_columns and allowed_channels
            else F.lit(False)
        )
        type_mismatch = _bool_or([amount_cast_failed, timestamp_cast_failed, invalid_transaction_type, invalid_channel])

        date_rule = rules.get("DATE_FORMAT", {})
        date_source_field = date_rule.get("source_field", "transaction_date_raw")
        date_parsed_field = date_rule.get("parsed_field", "transaction_date")
        date_format_issue = (
            F.col(date_source_field).isNotNull() & F.col(date_parsed_field).isNull()
            if date_source_field in available_columns and date_parsed_field in available_columns
            else F.lit(False)
        )

        currency_rule = rules.get("CURRENCY_VARIANT", {})
        currency_source_field = currency_rule.get("source_field", "currency_raw")
        target_currency = str(currency_rule.get("target_value", "ZAR")).upper()
        currency_variant = (
            F.col(currency_source_field).isNotNull()
            & (F.upper(F.col(currency_source_field).cast("string")) != F.lit(target_currency))
            if currency_source_field in available_columns
            else F.lit(False)
        )

        orphan_rule = rules.get("ORPHANED_ACCOUNT", {})
        orphan_reference_field = orphan_rule.get("reference_present_field", "_valid_account_id")
        orphaned_account = (
            F.col(orphan_reference_field).isNull() if orphan_reference_field in available_columns else F.lit(False)
        )

        duplicate_rule = rules.get("DUPLICATE_DEDUPED", {})
        duplicate_count_field = duplicate_rule.get("duplicate_count_field", "_dup_count")
        duplicate_deduped = (
            F.col(duplicate_count_field) > F.lit(1) if duplicate_count_field in available_columns else F.lit(False)
        )

        def _enabled(rule_code: str) -> bool:
            configured = rules.get(rule_code, {})
            if not isinstance(configured, dict):
                return True
            return bool(configured.get("enabled", True))

        issue_expressions = {
            "NULL_REQUIRED": null_required if _enabled("NULL_REQUIRED") else F.lit(False),
            "TYPE_MISMATCH": type_mismatch if _enabled("TYPE_MISMATCH") else F.lit(False),
            "DATE_FORMAT": date_format_issue if _enabled("DATE_FORMAT") else F.lit(False),
            "CURRENCY_VARIANT": currency_variant if _enabled("CURRENCY_VARIANT") else F.lit(False),
            "ORPHANED_ACCOUNT": orphaned_account if _enabled("ORPHANED_ACCOUNT") else F.lit(False),
            "DUPLICATE_DEDUPED": duplicate_deduped if _enabled("DUPLICATE_DEDUPED") else F.lit(False),
        }

        dq_flag_expr = None
        for rule_code in dq_priority:
            issue_expr = issue_expressions.get(rule_code, F.lit(False))
            if dq_flag_expr is None:
                dq_flag_expr = F.when(issue_expr, F.lit(rule_code))
            else:
                dq_flag_expr = dq_flag_expr.when(issue_expr, F.lit(rule_code))
        dq_flag_expr = (
            dq_flag_expr.otherwise(F.lit(None).cast("string"))
            if dq_flag_expr is not None
            else F.lit(None).cast("string")
        )

        silver_transactions = (
            tx_with_accounts.withColumn(
                "dq_flag",
                dq_flag_expr.cast("string"),
            )
            .select(
                "transaction_id",
                "account_id",
                "transaction_date",
                "transaction_time",
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
        )

        _write_delta(silver_accounts, os.path.join(silver_root, "accounts"))
        _write_delta(silver_customers, os.path.join(silver_root, "customers"))
        _write_delta(silver_transactions, os.path.join(silver_root, "transactions"))
    finally:
        spark.stop()
