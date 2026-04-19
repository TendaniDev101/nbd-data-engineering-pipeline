"""
Bronze layer: Ingest raw source data into Delta Parquet tables.

Input paths (read-only mounts — do not write here):
  /data/input/accounts.csv
  /data/input/transactions.jsonl
  /data/input/customers.csv

Output paths (your pipeline must create these directories):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Requirements:
  - Preserve source data as-is; do not transform at this layer.
  - Add an `ingestion_timestamp` column (TIMESTAMP) recording when each
    record entered the Bronze layer. Use a consistent timestamp for the
    entire ingestion run (not per-row).
  - Write each table as a Delta Parquet table (not plain Parquet).
  - Read paths from config/pipeline_config.yaml — do not hardcode paths.
  - All paths are absolute inside the container (e.g. /data/input/accounts.csv).

Spark configuration tip:
  Run Spark in local[2] mode to stay within the 2-vCPU resource constraint.
  Configure Delta Lake using the builder pattern shown in the base image docs.
"""

import os
from datetime import datetime

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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


def _write_bronze_table(df, output_path: str, ingestion_time: datetime) -> None:
    (
        df.withColumn("ingestion_timestamp", F.lit(ingestion_time).cast("timestamp"))
        .write.format("delta")
        .mode("overwrite")
        .save(output_path)
    )


def run_ingestion():
    config = _load_config()
    input_cfg = config["input"]
    output_cfg = config["output"]
    spark_cfg = config.get("spark", {})

    bronze_root = output_cfg["bronze_path"]
    ingestion_time = datetime.utcnow()

    spark = _build_spark_session(spark_cfg)

    try:
        accounts_df = spark.read.option("header", "true").csv(input_cfg["accounts_path"])
        transactions_df = spark.read.json(input_cfg["transactions_path"])
        customers_df = spark.read.option("header", "true").csv(input_cfg["customers_path"])

        _write_bronze_table(accounts_df, os.path.join(bronze_root, "accounts"), ingestion_time)
        _write_bronze_table(transactions_df, os.path.join(bronze_root, "transactions"), ingestion_time)
        _write_bronze_table(customers_df, os.path.join(bronze_root, "customers"), ingestion_time)
    finally:
        spark.stop()
