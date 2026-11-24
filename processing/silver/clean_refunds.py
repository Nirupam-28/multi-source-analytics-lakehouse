
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper, to_date


def clean_refunds(df: DataFrame) -> DataFrame:

    cleaned_df = df.filter(
        col("refund_id").isNotNull()
        & col("payment_id").isNotNull()
        & col("refund_amount").isNotNull()
        & (col("refund_amount") >= 0)
    )

    cleaned_df = cleaned_df.select(
        trim(col("refund_id")).alias("refund_id"),
        trim(col("payment_id")).alias("payment_id"),
        col("refund_amount").cast("decimal(12,2)").alias("refund_amount"),
        upper(trim(col("refund_status"))).alias("refund_status"),
        to_date(col("refund_date")).alias("refund_date")
    )

    cleaned_df = cleaned_df.dropDuplicates(["refund_id"])

    return cleaned_df
