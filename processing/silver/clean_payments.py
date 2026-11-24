
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper, to_date


def clean_payments(df: DataFrame) -> DataFrame:

    cleaned_df = df.filter(
        col("payment_id").isNotNull()
        & col("order_id").isNotNull()
        & col("amount").isNotNull()
        & (col("amount") >= 0)
    )

    cleaned_df = cleaned_df.select(
        trim(col("payment_id")).alias("payment_id"),
        col("order_id").cast("integer").alias("order_id"),
        upper(trim(col("payment_method"))).alias("payment_method"),
        upper(trim(col("payment_status"))).alias("payment_status"),
        col("amount").cast("decimal(12,2)").alias("amount"),
        upper(trim(col("currency"))).alias("currency"),
        to_date(col("payment_date")).alias("payment_date")
    )

    cleaned_df = cleaned_df.dropDuplicates(["payment_id"])

    return cleaned_df
