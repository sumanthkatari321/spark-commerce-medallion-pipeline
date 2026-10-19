from pyspark.sql import SparkSession, functions as F

from spark_pipeline.transforms import build_daily_sales, clean_orders


def test_clean_orders_quarantines_invalid_rows():
    spark = SparkSession.builder.master("local[1]").appName("pipeline-test").getOrCreate()
    try:
        source = spark.createDataFrame(
            [
                ("o-1", "c-1", "2026-10-01T10:00:00Z", "Books", "2", "10.0", "USD", "completed", None),
                ("o-2", "c-2", "2026-10-01T10:00:00Z", "Books", "1", "0", "USD", "completed", None),
            ],
            ["order_id", "customer_id", "order_timestamp", "product_category", "quantity", "unit_price", "currency", "status", "ingested_at"],
        )
        valid, rejected = clean_orders(source, min_order_amount=0.01)
        assert valid.count() == 1
        assert rejected.select("quality_reason").first()[0] == "non_positive_unit_price"
    finally:
        spark.stop()


def test_build_daily_sales_counts_completed_orders():
    spark = SparkSession.builder.master("local[1]").appName("pipeline-test").getOrCreate()
    try:
        orders = spark.createDataFrame(
            [
                ("o-1", "c-1", "2026-10-01 10:00:00", "completed", 20.0),
                ("o-2", "c-2", "2026-10-01 11:00:00", "cancelled", 30.0),
            ],
            ["order_id", "customer_id", "order_timestamp", "status", "order_amount"],
        ).withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
        result = build_daily_sales(orders).first()
        assert result.orders == 1
        assert result.gross_revenue == 20.0
    finally:
        spark.stop()
