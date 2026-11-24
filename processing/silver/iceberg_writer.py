
from pyspark.sql import DataFrame


def write_iceberg_table(
    df: DataFrame,
    table_name: str,
    mode: str = "replace"
):
    writer = (
        df.writeTo(table_name)
        .using("iceberg")
        .tableProperty("format-version", "2")
    )

    if mode == "replace":
        writer.createOrReplace()

    elif mode == "append":
        writer.append()

    elif mode == "create":
        writer.create()

    else:
        raise ValueError(
            f"Unsupported write mode: {mode}"
        )

    print(
        f"Successfully wrote {df.count()} records "
        f"to {table_name}"
    )
