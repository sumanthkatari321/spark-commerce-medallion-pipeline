"""CLI entry point for the bronze, silver, and gold Spark stages."""

import argparse
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from spark_pipeline.config import PipelineConfig, load_config
from spark_pipeline.transforms import build_daily_sales, clean_orders


def create_spark(config: PipelineConfig) -> SparkSession:
    return (
        SparkSession.builder.appName(config.app_name)
        .config("spark.sql.shuffle.partitions", config.shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def write_parquet(frame, path: str, partition_by: list[str] | None = None) -> None:
    writer = frame.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)


def run_bronze(spark: SparkSession, config: PipelineConfig):
    source = Path(config.source_path)
    if not source.exists():
        raise FileNotFoundError(f"Source file not found: {source}")
    bronze = (
        spark.read.option("header", True)
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssX")
        .csv(str(source))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
    )
    write_parquet(bronze, config.bronze_path)
    return bronze


def run_silver(spark: SparkSession, config: PipelineConfig):
    bronze = spark.read.parquet(config.bronze_path)
    valid, rejected = clean_orders(bronze, config.min_order_amount)
    write_parquet(valid, config.silver_path, ["status"])
    write_parquet(rejected, config.quarantine_path)
    return valid, rejected


def run_gold(spark: SparkSession, config: PipelineConfig):
    silver = spark.read.parquet(config.silver_path)
    daily_sales = build_daily_sales(silver)
    write_parquet(daily_sales, config.gold_path)
    return daily_sales


def write_audit(spark: SparkSession, config: PipelineConfig, stage: str, rows: int) -> None:
    audit = spark.createDataFrame(
        [(datetime.now(timezone.utc), stage, rows)],
        ["run_at_utc", "stage", "rows_written"],
    )
    audit.write.mode("append").parquet(config.audit_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Spark medallion pipeline")
    parser.add_argument("--config", default="config/pipeline.yaml")
    parser.add_argument("--stage", choices=["all", "bronze", "silver", "gold"], default="all")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    spark = create_spark(config)
    try:
        if args.stage in {"all", "bronze"}:
            bronze = run_bronze(spark, config)
            write_audit(spark, config, "bronze", bronze.count())
        if args.stage in {"all", "silver"}:
            silver, rejected = run_silver(spark, config)
            write_audit(spark, config, "silver", silver.count())
            write_audit(spark, config, "quarantine", rejected.count())
        if args.stage in {"all", "gold"}:
            gold = run_gold(spark, config)
            write_audit(spark, config, "gold", gold.count())
            gold.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
