from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit
import great_expectations as ge


# Spark session

spark = SparkSession.builder.appName("Orders-Quarantine").getOrCreate()

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/orders.csv")
)


# Add row_id (stable indexing)

window = Window.orderBy(lit(1))

df_indexed = (
    df.withColumn("row_id", row_number().over(window) - 1)
)


# Great Expectations Context

context = ge.get_context()

context.sources.add_or_update_spark(
    name="spark_ds"
)

validator = context.sources.get("spark_ds").get_validator(
    batch_data=df_indexed,
    expectation_suite_name="orders_suite"
)


# Run validation

validation_result = validator.validate(
    result_format="COMPLETE"
)


# Collect failed row_ids

failed_indices = set()

for result in validation_result["results"]:
    if not result["success"]:
        unexpected_rows = result["result"].get("unexpected_index_list", [])
        failed_indices.update(unexpected_rows)

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


# Metrics & threshold

total = df.count()
bad = quarantine_df.count()

print(f"Total records: {total}, Quarantined: {bad}")

if total > 0 and bad / total > 0.05:
    raise Exception("Quarantine threshold exceeded")
