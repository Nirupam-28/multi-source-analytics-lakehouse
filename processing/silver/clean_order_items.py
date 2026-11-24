
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim


def clean_order_items(df: DataFrame) -> DataFrame:

    cleaned_df = df.filter(
        col("order_item_id").isNotNull()
        & col("order_id").isNotNull()
        & col("quantity").isNotNull()
        & col("unit_price").isNotNull()
        & (col("quantity") > 0)
        & (col("unit_price") >= 0)
    )

    cleaned_df = cleaned_df.select(
        col("order_item_id").cast("integer").alias("order_item_id"),
        col("order_id").cast("integer").alias("order_id"),
        trim(col("product_name")).alias("product_name"),
        col("quantity").cast("integer").alias("quantity"),
        col("unit_price").cast("decimal(12,2)").alias("unit_price")
    )

    cleaned_df = cleaned_df.dropDuplicates(["order_item_id"])

    return cleaned_df
