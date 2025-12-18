import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# -----------------------------
# Spark fixture (session scope)
# -----------------------------

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("pytest-orders")
        .master("local[2]")
        .getOrCreate()
    )
    yield spark
    spark.stop()

# -----------------------------
# Load orders data
# -----------------------------

@pytest.fixture(scope="session")
def orders_df(spark):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/orders.csv")
    )

# -----------------------------
# Tests
# -----------------------------

def test_order_id_not_null(orders_df):
    assert orders_df.filter(col("order_id").isNull()).count() == 0


def test_order_date_is_valid(orders_df):
    invalid_dates = orders_df.withColumn(
        "order_date_parsed",
        to_date(col("order_date"))
    ).filter(col("order_date_parsed").isNull())

    assert invalid_dates.count() == 0


def test_amount_positive_for_completed_orders(orders_df):
    invalid_amounts = orders_df.filter(
        (col("status") == "COMPLETED") & (col("amount") <= 0)
    )

    assert invalid_amounts.count() == 0


def test_status_allowed_values(orders_df):
    allowed = ["COMPLETED", "PENDING", "CANCELLED"]

    invalid_status = orders_df.filter(
        ~col("status").isin(allowed)
    )

    assert invalid_status.count() == 0


def test_detect_bad_rows(orders_df):
    bad_rows = orders_df.filter(
        col("amount").isNull() | (col("amount") < 0)
    )

    # From sample data: order_id 3 (null amount), order_id 4 (negative)
    assert bad_rows.count() == 2
