"""Spark transformations used by the medallion layers."""

from pyspark.sql import DataFrame, functions as F


def clean_orders(bronze_orders: DataFrame, min_order_amount: float) -> tuple[DataFrame, DataFrame]:
    """Return valid orders and rejected rows with a data-quality reason."""
    typed = bronze_orders.select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.to_timestamp("order_timestamp").alias("order_timestamp"),
        F.trim("product_category").alias("product_category"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("double").alias("unit_price"),
        F.upper(F.trim("currency")).alias("currency"),
        F.lower(F.trim("status")).alias("status"),
        "ingested_at",
    ).withColumn("order_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))

    quality_reason = (
        F.when(F.col("order_id").isNull() | (F.col("order_id") == ""), "missing_order_id")
        .when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), "missing_customer_id")
        .when(F.col("order_timestamp").isNull(), "invalid_order_timestamp")
        .when(F.col("quantity") <= 0, "non_positive_quantity")
        .when(F.col("unit_price") <= 0, "non_positive_unit_price")
        .when(F.col("order_amount") < F.lit(min_order_amount), "below_minimum_order_amount")
        .when(~F.col("status").isin("completed", "cancelled", "refunded"), "unsupported_status")
    )
    evaluated = typed.withColumn("quality_reason", quality_reason)
    rejected = evaluated.filter(F.col("quality_reason").isNotNull())
    valid = (
        evaluated.filter(F.col("quality_reason").isNull())
        .drop("quality_reason")
        .dropDuplicates(["order_id"])
    )
    return valid, rejected


def build_daily_sales(silver_orders: DataFrame) -> DataFrame:
    """Build analytics-ready daily sales metrics from valid orders."""
    return (
        silver_orders.filter(F.col("status") == "completed")
        .withColumn("order_date", F.to_date("order_timestamp"))
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.countDistinct("customer_id").alias("customers"),
            F.round(F.sum("order_amount"), 2).alias("gross_revenue"),
            F.round(F.avg("order_amount"), 2).alias("average_order_value"),
        )
        .orderBy("order_date")
    )
