
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    round as spark_round
)


def build_payment_summary(
    payments_df: DataFrame
) -> DataFrame:

    summary_df = (
        payments_df
        .groupBy(
            "payment_method",
            "payment_status"
        )
        .agg(
            count("payment_id").alias("total_payments"),

            spark_sum("amount").alias("total_amount"),

            spark_round(
                avg("amount"), 2
            ).alias("average_payment_amount")
        )
        .orderBy(
            "payment_method",
            "payment_status"
        )
    )

    return summary_df
