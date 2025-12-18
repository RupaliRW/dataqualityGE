from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit
import great_expectations as ge


# Spark session
spark = (
    SparkSession.builder
    .appName("Orders-Quarantine")
    .getOrCreate()
)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/orders.csv")
)


# Add stable row_id (required)


window = Window.orderBy(lit(1))
df_indexed = df.withColumn("row_id", row_number().over(window) - 1)


# Load GE Context (1.9.3)


context = ge.get_context()


# Register Spark datasource


datasource_name = "spark_ds"

if datasource_name not in [ds["name"] for ds in context.list_datasources()]:
    context.sources.add_or_update_spark(name=datasource_name)


# Create DataFrame Asset + BatchRequest


asset = context.sources[spark_ds].add_dataframe_asset(
    name="orders_asset"
)

batch_request = asset.build_batch_request(
    dataframe=df_indexed
)


# Create Expectation Suite (if missing)


suite_name = "orders_suite"

if suite_name not in context.list_expectation_suite_names():
    context.add_expectation_suite(suite_name)


# Get Validator (CORRECT API)


validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name
)


# Expectations


validator.expect_column_values_to_not_be_null(
    "order_id",
    result_format="COMPLETE"
)

validator.expect_column_values_to_be_between(
    "amount",
    min_value=0,
    strictly=True,
    result_format="COMPLETE"
)


# Validate


results = validator.validate()

validator.save_expectation_suite()


# Collect failed row_ids


failed_ids = set()

for r in results["results"]:
    idx = r["result"].get("unexpected_index_list")
    if idx:
        failed_ids.update(idx)


# Split data


quarantine_df = (
    df_indexed
    .filter(df_indexed.row_id.isin(list(failed_ids)))
    .drop("row_id")
)

valid_df = (
    df_indexed
    .filter(~df_indexed.row_id.isin(list(failed_ids)))
    .drop("row_id")
)


# Write outputs


valid_df.write.mode("overwrite").parquet("output/valid/")
quarantine_df.write.mode("overwrite").parquet("output/quarantine/")

print(
    f"Total: {df.count()}, "
    f"Quarantined: {quarantine_df.count()}"
)
