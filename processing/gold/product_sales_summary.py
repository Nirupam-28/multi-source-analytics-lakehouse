
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    avg,
    round as spark_round
)


def build_product_sales_summary(
    order_items_df: DataFrame
) -> DataFrame:

    sales_df = (
        order_items_df
        .withColumn(
            "line_revenue",
            col("quantity") * col("unit_price")
        )
        .groupBy("product_name")
        .agg(
            spark_sum("quantity").alias("total_units_sold"),

            count("order_item_id").alias("total_order_items"),

            spark_sum("line_revenue").alias("total_revenue"),

            spark_round(
                avg("unit_price"), 2
            ).alias("average_selling_price")
        )
        .orderBy(
            col("total_revenue").desc()
        )
    )

    return sales_df
