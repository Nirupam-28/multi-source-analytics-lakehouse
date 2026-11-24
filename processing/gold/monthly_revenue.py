
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    date_format,
    sum as spark_sum,
    count,
    avg,
    when,
    round as spark_round
)


def build_monthly_revenue(orders_df: DataFrame) -> DataFrame:

    monthly_df = (
        orders_df
        .withColumn(
            "revenue_month",
            date_format(col("order_date"), "yyyy-MM")
        )
        .groupBy("revenue_month")
        .agg(
            spark_sum("total_amount").alias("total_revenue"),

            count("order_id").alias("total_orders"),

            spark_round(
                avg("total_amount"), 2
            ).alias("average_order_value"),

            spark_sum(
                when(
                    col("order_status") == "COMPLETED", 1
                ).otherwise(0)
            ).alias("completed_orders"),

            spark_sum(
                when(
                    col("order_status") == "CANCELLED", 1
                ).otherwise(0)
            ).alias("cancelled_orders")
        )
        .orderBy("revenue_month")
    )

    return monthly_df
