from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit

from great_expectations.validator.validator import Validator
from great_expectations.execution_engine import SparkDFExecutionEngine
from great_expectations.core.expectation_suite import ExpectationSuite


# Spark session

spark = SparkSession.builder.appName("Orders-Quarantine").getOrCreate()

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/orders.csv")
)


# Add stable row_id

window = Window.orderBy(lit(1))
df_indexed = df.withColumn("row_id", row_number().over(window) - 1)


# Create Expectation Suite

suite = ExpectationSuite(expectation_suite_name="orders_suite")


# Create Spark Execution Engine

engine = SparkDFExecutionEngine(spark=spark)


# Create Validator (THIS IS THE KEY)

validator = Validator(
    execution_engine=engine,
    expectation_suite=suite,
    batch_data=df_indexed
)


# Define expectations

validator.expect_column_values_to_not_be_null("order_id")

validator.expect_column_values_to_be_between(
    "amount",
    min_value=0,
    strictly=True
)


# Run validation

validation_result = validator.validate(result_format="COMPLETE")


# Collect failed row_ids

failed_indices = set()

for result in validation_result["results"]:
    if not result["success"]:
        failed_indices.update(
            result["result"].get("unexpected_index_list", [])
        )

failed_ids = list(failed_indices)


# Split quarantine vs valid

quarantine_df = (
    df_indexed
    .filter(df_indexed.row_id.isin(failed_ids))
    .drop("row_id")
)

valid_df = (
    df_indexed
    .filter(~df_indexed.row_id.isin(failed_ids))
    .drop("row_id")
)


# Write outputs

valid_df.write.mode("overwrite").parquet(
    "/tmp/ukus18novtmp/rupali/orders_valid/"
)

quarantine_df.write.mode("append").parquet(
    "/tmp/ukus18novtmp/rupali/orders_quarantine/"
)

valid_df.show(5)
quarantine_df.show(5)


# Metrics & SLA threshold

total = df.count()
bad = quarantine_df.count()

print(f"Total records: {total}, Quarantined: {bad}")

if total > 0 and bad / total > 0.05:
    raise Exception("Quarantine threshold exceeded")
