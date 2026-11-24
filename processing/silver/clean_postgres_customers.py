
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    lower
)


def clean_postgres_customers(df: DataFrame) -> DataFrame:

    cleaned_df = df.filter(
        col("customer_id").isNotNull()
    )

    cleaned_df = cleaned_df.select(
        col("customer_id").cast("integer").alias("customer_id"),

        trim(col("first_name")).alias("first_name"),

        trim(col("last_name")).alias("last_name"),

        lower(trim(col("email"))).alias("email"),

        trim(col("city")).alias("city"),

        trim(col("country")).alias("country"),

        col("created_at").cast("timestamp").alias("created_at")
    )

    cleaned_df = cleaned_df.dropDuplicates(
        ["customer_id"]
    )

    return cleaned_df
