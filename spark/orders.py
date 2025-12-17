from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit
import great_expectations as ge


spark = SparkSession.builder.appName("Orders-Quarantine").getOrCreate()


df = spark.read.option("header", True).option("inferSchema", True).csv("data/orders.csv")


ge_df = ge.dataset.SparkDFDataset(df)

validation_result = ge_df.validate(
expectation_suite="getest/orders_suite.json",
result_format="COMPLETE"
)


failed_indices = set()


for result in validation_result["results"]:
    if not result["success"]:
        unexpected = result["result"].get("unexpected_index_list", [])
        failed_indices.update(unexpected)


        window = Window.orderBy(lit(1))


        df_indexed = df.withColumn(
        "row_id",
        row_number().over(window) - 1
        )


        failed_ids = list(failed_indices)


        quarantine_df = df_indexed.filter(
        df_indexed.row_id.isin(failed_ids)
        ).drop("row_id")


        valid_df = df_indexed.filter(
        ~df_indexed.row_id.isin(failed_ids)
        ).drop("row_id")


        valid_df.write.mode("overwrite").parquet("/tmp/ukus18novtmp/rupali/orders_valid/")
        quarantine_df.write.mode("append").parquet("/tmp/ukus18novtmp/rupali/orders_quarantine/")


        total = df.count()
        bad = quarantine_df.count()

if total > 0 and bad / total > 0.05:
    raise Exception("Quarantine threshold exceeded")