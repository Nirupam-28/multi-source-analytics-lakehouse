
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, col


def extract_json_records(df: DataFrame) -> DataFrame:
    """
    Extract business records from the data array
    inside our Bronze ingestion envelope.
    """

    return (
        df
        .select(explode(col("data")).alias("record"))
        .select("record.*")
    )
