
from config.iceberg_spark import create_iceberg_spark_session

from processing.bronze.read_bronze import read_api_customers
from processing.silver.clean_customers import clean_api_customers


def main():

    spark = create_iceberg_spark_session(
        "RetailPulseFirstIcebergTable"
    )

    try:
        # 1. Read customers from Bronze
        print("\nSTEP 1: Reading Bronze customers")

        bronze_df = read_api_customers(spark)

        print(f"Bronze record count: {bronze_df.count()}")

        # 2. Clean customer records
        print("\nSTEP 2: Cleaning customer data")

        silver_df = clean_api_customers(bronze_df)

        print(f"Silver record count: {silver_df.count()}")

        # 3. Write to Iceberg
        print("\nSTEP 3: Writing to Iceberg")

        table_name = "nessie.silver.api_customers"

        (
            silver_df.writeTo(table_name)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .createOrReplace()
        )

        print(f"SUCCESS: Created Iceberg table {table_name}")

        # 4. Read the table back
        print("\nSTEP 4: Reading Iceberg table")

        iceberg_df = spark.table(table_name)

        print(f"Iceberg record count: {iceberg_df.count()}")

        iceberg_df.printSchema()
        iceberg_df.show(5, truncate=False)

        # 5. Verify record count
        assert iceberg_df.count() == silver_df.count()

        print("\nFIRST ICEBERG TABLE TEST PASSED!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
