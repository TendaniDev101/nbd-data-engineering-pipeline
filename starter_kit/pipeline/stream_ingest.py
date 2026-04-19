"""
Stage 3 — Streaming extension: Process micro-batch JSONL files from /data/stream/.

This module is only used at Stage 3. You do not need to implement it for
Stage 1 or Stage 2.

Input paths:
  /data/stream/            — Directory of micro-batch JSONL files, arriving
                             during pipeline execution. The scoring system
                             drops new files here while your container runs.

Output paths (your pipeline must create these directories):
  /data/output/stream_gold/current_balances/    — 4 fields; upsert table
  /data/output/stream_gold/recent_transactions/ — 7 fields; last 50 per account

Requirements:
  - Poll /data/stream/ periodically for new files (the scoring system will
    deliver micro-batches over the course of the run).
  - Process each new file and merge results into the stream_gold tables.
  - current_balances: maintain one row per account_id (upsert/merge).
  - recent_transactions: maintain the 50 most recent transactions per account.
  - SLA: updated_at must be within 300 seconds of the source event timestamp
    for full credit; 300–600 seconds receives partial credit.
  - Write all stream_gold output as Delta Parquet tables.
  - The stream_ingest loop must terminate on its own when no new files have
    arrived for a reasonable quiesce period. The container has a 30-minute
    hard timeout — do not run indefinitely.

See output_schema_spec.md §5 and §6 for the full field-by-field specification
of current_balances and recent_transactions.

See docker_interface_contract.md §3 for the /data/stream/ mount details.
"""


import os
import time
from decimal import Decimal

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


DEFAULT_CONFIG_PATH = "/data/config/pipeline_config.yaml"


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


def _build_spark_session(spark_config: dict) -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    configured_jars = spark_config.get("spark.jars")
    delta_jars = os.environ.get("DELTA_JARS")
    merged_jars = ",".join([j for j in [configured_jars, delta_jars] if j])

    builder = (
        SparkSession.builder.master(spark_config.get("master", "local[2]"))
        .appName(spark_config.get("app_name", "nedbank-de-pipeline-stream"))
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


def _parse_date(col_expr):
    raw = F.trim(col_expr.cast("string"))
    epoch_seconds = F.when(
        F.length(raw) >= 13,
        (raw.cast("double") / F.lit(1000)).cast("bigint"),
    ).otherwise(raw.cast("bigint"))
    return F.coalesce(
        F.to_date(raw, "yyyy-MM-dd"),
        F.to_date(raw, "dd/MM/yyyy"),
        F.to_date(F.from_unixtime(epoch_seconds)),
    )


def _load_delta_or_empty(spark: SparkSession, path: str, schema: StructType):
    if os.path.isdir(os.path.join(path, "_delta_log")):
        return spark.read.format("delta").load(path)
    empty_columns = [F.lit(None).cast(field.dataType).alias(field.name) for field in schema.fields]
    return spark.range(0).select(*empty_columns)


def _write_delta(df, path: str) -> None:
    df.write.format("delta").mode("overwrite").save(path)


def _list_stream_files(stream_input_path: str):
    if not os.path.isdir(stream_input_path):
        return []
    candidates = [
        name
        for name in os.listdir(stream_input_path)
        if name.startswith("stream_") and name.endswith(".jsonl")
    ]
    return sorted(candidates)


def _process_stream_file(
    spark: SparkSession,
    file_path: str,
    current_balances_path: str,
    recent_transactions_path: str,
    gold_accounts_path: str,
) -> None:
    stream_df = spark.read.json(file_path)
    if stream_df.rdd.isEmpty():
        return

    date_parsed = _parse_date(F.col("transaction_date"))
    time_raw = _string_col("transaction_time")
    transaction_ts = F.to_timestamp(
        F.concat_ws(" ", F.date_format(date_parsed, "yyyy-MM-dd"), time_raw),
        "yyyy-MM-dd HH:mm:ss",
    )

    events = (
        stream_df.select(
            _string_col("account_id").alias("account_id"),
            _string_col("transaction_id").alias("transaction_id"),
            transaction_ts.alias("transaction_timestamp"),
            F.col("amount").cast(DecimalType(18, 2)).alias("amount"),
            F.upper(_string_col("transaction_type")).alias("transaction_type"),
            _string_col("channel").alias("channel"),
        )
        .filter(
            F.col("account_id").isNotNull()
            & (F.length(F.col("account_id")) > 0)
            & F.col("transaction_id").isNotNull()
            & (F.length(F.col("transaction_id")) > 0)
            & F.col("transaction_timestamp").isNotNull()
            & F.col("amount").isNotNull()
            & F.col("transaction_type").isNotNull()
        )
        .withColumn(
            "signed_amount",
            F.when(F.col("transaction_type").isin("DEBIT", "FEE"), -F.col("amount"))
            .when(F.col("transaction_type").isin("CREDIT", "REVERSAL"), F.col("amount"))
            .otherwise(F.lit(Decimal("0.00")).cast(DecimalType(18, 2))),
        )
        .withColumn("updated_at", F.col("transaction_timestamp"))
    )

    current_balances_schema = StructType(
        [
            StructField("account_id", StringType(), False),
            StructField("current_balance", DecimalType(18, 2), False),
            StructField("last_transaction_timestamp", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
        ]
    )
    recent_transactions_schema = StructType(
        [
            StructField("account_id", StringType(), False),
            StructField("transaction_id", StringType(), False),
            StructField("transaction_timestamp", TimestampType(), False),
            StructField("amount", DecimalType(18, 2), False),
            StructField("transaction_type", StringType(), False),
            StructField("channel", StringType(), True),
            StructField("updated_at", TimestampType(), False),
        ]
    )

    existing_balances = _load_delta_or_empty(spark, current_balances_path, current_balances_schema)
    existing_recent = _load_delta_or_empty(spark, recent_transactions_path, recent_transactions_schema)

    base_accounts = spark.read.format("delta").load(gold_accounts_path).select(
        F.col("account_id").cast("string").alias("account_id"),
        F.col("current_balance").cast(DecimalType(18, 2)).alias("base_balance"),
    )

    event_impact = events.groupBy("account_id").agg(
        F.sum("signed_amount").cast(DecimalType(18, 2)).alias("delta_sum"),
        F.max("transaction_timestamp").alias("last_transaction_timestamp"),
    )
    touched_accounts = event_impact.select("account_id").distinct()

    current_base = (
        touched_accounts.join(
            existing_balances.select(
                "account_id",
                F.col("current_balance").cast(DecimalType(18, 2)).alias("existing_balance"),
                F.col("last_transaction_timestamp").alias("existing_ts"),
            ),
            "account_id",
            "left",
        )
        .join(base_accounts, "account_id", "left")
        .join(event_impact, "account_id", "left")
    )

    current_updates = current_base.select(
        "account_id",
        (
            F.coalesce(F.col("existing_balance"), F.col("base_balance"), F.lit(Decimal("0.00")).cast(DecimalType(18, 2)))
            + F.coalesce(F.col("delta_sum"), F.lit(Decimal("0.00")).cast(DecimalType(18, 2)))
        ).cast(DecimalType(18, 2)).alias("current_balance"),
        F.greatest(F.col("existing_ts"), F.col("last_transaction_timestamp")).alias("last_transaction_timestamp"),
        F.col("last_transaction_timestamp").alias("updated_at"),
    )

    untouched_balances = existing_balances.join(touched_accounts, "account_id", "left_anti")
    combined_balances = untouched_balances.unionByName(current_updates)
    _write_delta(combined_balances, current_balances_path)

    dedupe_window = Window.partitionBy("account_id", "transaction_id").orderBy(
        F.col("transaction_timestamp").desc_nulls_last(),
        F.col("updated_at").desc_nulls_last(),
    )
    rank_window = Window.partitionBy("account_id").orderBy(
        F.col("transaction_timestamp").desc_nulls_last(),
        F.col("updated_at").desc_nulls_last(),
    )

    new_recent = events.select(
        "account_id",
        "transaction_id",
        "transaction_timestamp",
        "amount",
        "transaction_type",
        "channel",
        "updated_at",
    )

    recent_union = existing_recent.unionByName(new_recent)
    recent_deduped = (
        recent_union.withColumn("_dedupe_rn", F.row_number().over(dedupe_window))
        .filter(F.col("_dedupe_rn") == 1)
        .drop("_dedupe_rn")
    )
    recent_top50 = (
        recent_deduped.withColumn("_rn", F.row_number().over(rank_window))
        .filter(F.col("_rn") <= 50)
        .drop("_rn")
    )
    _write_delta(recent_top50, recent_transactions_path)


def run_stream_ingestion():
    config = _load_config()
    output_cfg = config["output"]
    spark_cfg = config.get("spark", {})
    streaming_cfg = config.get("streaming", {})

    stream_input_path = streaming_cfg.get("stream_input_path", "/data/stream")
    stream_gold_root = streaming_cfg.get(
        "stream_gold_path",
        os.path.join(os.path.dirname(output_cfg["gold_path"]), "stream_gold"),
    )
    poll_interval = int(streaming_cfg.get("poll_interval_seconds", 10))
    quiesce_timeout = int(streaming_cfg.get("quiesce_timeout_seconds", 60))

    if not os.path.isdir(stream_input_path):
        return

    current_balances_path = os.path.join(stream_gold_root, "current_balances")
    recent_transactions_path = os.path.join(stream_gold_root, "recent_transactions")
    gold_accounts_path = os.path.join(output_cfg["gold_path"], "dim_accounts")

    if not os.path.isdir(os.path.join(gold_accounts_path, "_delta_log")):
        return

    spark = _build_spark_session(spark_cfg)
    processed = set()
    last_new_file_time = time.time()

    try:
        while True:
            stream_files = _list_stream_files(stream_input_path)
            new_files = [file_name for file_name in stream_files if file_name not in processed]

            if new_files:
                for file_name in new_files:
                    _process_stream_file(
                        spark=spark,
                        file_path=os.path.join(stream_input_path, file_name),
                        current_balances_path=current_balances_path,
                        recent_transactions_path=recent_transactions_path,
                        gold_accounts_path=gold_accounts_path,
                    )
                    processed.add(file_name)
                last_new_file_time = time.time()
                continue

            if time.time() - last_new_file_time >= quiesce_timeout:
                break

            time.sleep(max(poll_interval, 1))
    finally:
        spark.stop()
