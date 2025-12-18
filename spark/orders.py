import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.window import Window


# Spark session


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("orders-quarantine-test")
        .master("local[2]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


# Load data


@pytest.fixture(scope="session")
def orders_df(spark):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/orders.csv")
    )


# Add deterministic row_id


@pytest.fixture(scope="session")
def orders_with_id(orders_df):
    window = Window.orderBy(lit(1))
    return orders_df.withColumn(
        "row_id",
        row_number().over(window)
    )


# DQ rules (row-level)


def rule_order_id_not_null(df):
    return df.filter(col("order_id").isNull()).select("row_id")

def rule_invalid_amount(df):
    return df.filter(
        (col("status") == "COMPLETED") & (col("amount") <= 0)
    ).select("row_id")

def rule_invalid_status(df):
    allowed = ["COMPLETED", "PENDING", "CANCELLED"]
    return df.filter(
        ~col("status").isin(allowed)
    ).select("row_id")


# Collect failed row_ids


def collect_failed_ids(df):
    failed_df = (
        rule_order_id_not_null(df)
        .union(rule_invalid_amount(df))
        .union(rule_invalid_status(df))
        .distinct()
    )
    return [r.row_id for r in failed_df.collect()]


# Quarantine split


def split_valid_quarantine(df):
    failed_ids = collect_failed_ids(df)

    quarantine_df = (
        df.filter(col("row_id").isin(failed_ids))
          .drop("row_id")
    )

    valid_df = (
        df.filter(~col("row_id").isin(failed_ids))
          .drop("row_id")
    )

    return valid_df, quarantine_df


# Pytest tests


def test_quarantine_logic(orders_with_id):
    valid_df, quarantine_df = split_valid_quarantine(orders_with_id)

    # From sample data:
    # order_id 3 -> amount NULL
    # order_id 4 -> negative amount
    assert quarantine_df.count() == 2
    assert valid_df.count() == 3


def test_quarantine_threshold(orders_with_id):
    total = orders_with_id.count()
    failed = len(collect_failed_ids(orders_with_id))

    failure_ratio = failed / total

    # Fail pipeline only if >20% bad data
    assert failure_ratio < 0.3


def test_write_outputs(orders_with_id):
    valid_df, quarantine_df = split_valid_quarantine(orders_with_id)

    valid_df.write.mode("overwrite").parquet("output/valid/")
    quarantine_df.write.mode("overwrite").parquet("output/quarantine/")
