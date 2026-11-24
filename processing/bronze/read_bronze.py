
from config.spark import create_spark_session
from processing.bronze.extract_records import extract_json_records


def read_api_customers(spark):

    raw_df = (
        spark.read
        .option("multiline", "true")
        .json("s3a://bronze/customers/")
    )

    return extract_json_records(raw_df)


def read_postgres_table(spark, table_name):

    allowed_tables = {
        "customers",
        "orders",
        "order_items"
    }

    if table_name not in allowed_tables:
        raise ValueError(f"Unsupported PostgreSQL table: {table_name}")

    raw_df = (
        spark.read
        .option("multiline", "true")
        .json(f"s3a://bronze/postgres/{table_name}/")
    )

    return extract_json_records(raw_df)


def read_payments(spark):

    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("s3a://bronze/files/payments.csv/")
    )


def read_refunds(spark):

    raw_df = (
        spark.read
        .option("multiline", "true")
        .json("s3a://bronze/files/refunds.json/")
    )

    return raw_df.selectExpr("explode(refunds) AS refund").select("refund.*")


def main():

    spark = create_spark_session("RetailPulseBronzeReader")

    try:

        datasets = {
            "api_customers": read_api_customers(spark),
            "postgres_customers": read_postgres_table(spark, "customers"),
            "orders": read_postgres_table(spark, "orders"),
            "order_items": read_postgres_table(spark, "order_items"),
            "payments": read_payments(spark),
            "refunds": read_refunds(spark)
        }

        for name, df in datasets.items():

            print(f"\n========== {name.upper()} ==========")

            df.printSchema()

            print("Record count:", df.count())

            df.show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
