from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit
import great_expectations as ge


# Spark session

spark = SparkSession.builder \
    .appName("Orders-Quarantine") \
    .getOrCreate()

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/orders.csv")
)


# Add stable row_id

window = Window.orderBy(lit(1))
df_indexed = df.withColumn("row_id", row_number().over(window) - 1)


# Load GE DataContext (1.9.3 way)

context = ge.get_context(
    context_root_dir="great_expectations"
)


# Add Spark datasource

datasource = context.sources.add_or_update_spark(
    name="spark_ds"
)


# Create Validator (SAFE & SUPPORTED)

validator = datasource.get_validator(
    batch_data=df_indexed,
    expectation_suite_name="orders_suite"
)


# Expectations

validator.expect_column_values_to_not_be_null("order_id")
validator.expect_column_values_to_be_between(
    "amount", min_value=0, strictly=True
)


# Validate

results = validator.validate(result_format="COMPLETE")


# Collect failed rows

failed_ids = set()
for r in results["results"]:
    if not r["success"]:
        failed_ids.update(
            r["result"].get("unexpected_index_list", [])
        )


# Split data

quarantine_df = df_indexed.filter(
    df_indexed.row_id.isin(list(failed_ids))
).drop("row_id")

valid_df = df_indexed.filter(
    ~df_indexed.row_id.isin(list(failed_ids))
).drop("row_id")


# Write outputs

valid_df.write.mode("overwrite").parquet("output/valid/")
quarantine_df.write.mode("overwrite").parquet("output/quarantine/")

print(f"Total: {df.count()}, Quarantined: {quarantine_df.count()}")
