
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    max as spark_max,
    round as spark_round
)


def build_customer_purchase_summary(
    customers_df: DataFrame,
    orders_df: DataFrame
) -> DataFrame:

    customer_orders = customers_df.join(
        orders_df,
        on="customer_id",
        how="left"
    )

    summary_df = (
        customer_orders
        .groupBy(
            customers_df["customer_id"],
            customers_df["first_name"],
            customers_df["last_name"],
            customers_df["email"],
            customers_df["city"],
            customers_df["country"]
        )
        .agg(
            count("order_id").alias("total_orders"),

            spark_sum("total_amount").alias("total_spent"),

            spark_round(
                avg("total_amount"), 2
            ).alias("average_order_value"),

            spark_max("order_date").alias("last_order_date")
        )
        .fillna({
            "total_orders": 0,
            "total_spent": 0
        })
    )

    return summary_df
