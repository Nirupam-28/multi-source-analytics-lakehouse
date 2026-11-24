
from config.spark import create_spark_session

from processing.bronze.read_bronze import read_api_customers

from processing.silver.clean_customers import (
    clean_api_customers
)


def test_clean_api_customers():

    spark = create_spark_session(
        "SilverCustomerCleaningTest"
    )

    try:

        bronze_df = read_api_customers(spark)

        print("\nBronze record count:")
        print(bronze_df.count())

        silver_df = clean_api_customers(bronze_df)

        print("\nSilver record count:")
        print(silver_df.count())

        print("\nSilver schema:")
        silver_df.printSchema()

        print("\nCleaned customer records:")
        silver_df.show(10, truncate=False)

        # Validate duplicate removal.
        assert bronze_df.count() == 416

        assert silver_df.count() == 208

        # Validate unique customer IDs.
        assert (
            silver_df.count()
            ==
            silver_df.select("customer_id").distinct().count()
        )

        # Validate standardized email addresses.
        assert "first_name" in silver_df.columns
        assert "last_name" in silver_df.columns

        print("\nSUCCESS: API customer Silver transformation passed!")

    finally:

        spark.stop()


if __name__ == "__main__":
    test_clean_api_customers()
