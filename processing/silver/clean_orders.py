
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper


def clean_orders(df: DataFrame) -> DataFrame:

    cleaned_df = df.filter(
        col("order_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("total_amount").isNotNull()
        & (col("total_amount") >= 0)
    )

    cleaned_df = cleaned_df.select(
        col("order_id").cast("integer").alias("order_id"),
        col("customer_id").cast("integer").alias("customer_id"),
        col("order_date").cast("timestamp").alias("order_date"),
        upper(trim(col("order_status"))).alias("order_status"),
        col("total_amount").cast("decimal(12,2)").alias("total_amount")
    )

    cleaned_df = cleaned_df.dropDuplicates(["order_id"])

    return cleaned_df
