
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    lower
)


def clean_api_customers(df: DataFrame) -> DataFrame:
    """
    Clean and standardize API customer records.
    """

    # Remove records without a customer ID.
    df = df.filter(
        col("id").isNotNull()
    )

    # Select relevant columns and standardize values.
    cleaned_df = df.select(
        col("id").cast("integer").alias("customer_id"),

        trim(col("firstName")).alias("first_name"),

        trim(col("lastName")).alias("last_name"),

        lower(trim(col("email"))).alias("email"),

        col("age").cast("integer").alias("age"),

        trim(col("gender")).alias("gender"),

        trim(col("phone")).alias("phone"),

        trim(col("address.city")).alias("city"),

        trim(col("address.state")).alias("state"),

        trim(col("address.country")).alias("country")
    )

    # Remove duplicate customers across ingestion snapshots.
    cleaned_df = cleaned_df.dropDuplicates(
        ["customer_id"]
    )

    return cleaned_df
