
from config.spark import create_spark_session

from processing.bronze.read_bronze import read_postgres_table

from processing.silver.clean_postgres_customers import (
    clean_postgres_customers
)


def test_clean_postgres_customers():

    spark = create_spark_session(
        "SilverPostgresCustomerTest"
    )

    try:

        bronze_df = read_postgres_table(
            spark,
            "customers"
        )

        silver_df = clean_postgres_customers(bronze_df)

        print("\nBronze customer count:", bronze_df.count())

        print("Silver customer count:", silver_df.count())

        print("\nSilver schema:")
        silver_df.printSchema()

        print("\nCleaned customers:")
        silver_df.show(10, truncate=False)

        assert bronze_df.count() == 10

        assert silver_df.count() == 10

        assert (
            silver_df.count()
            ==
            silver_df.select("customer_id").distinct().count()
        )

        assert "created_at" in silver_df.columns

        print("\nSUCCESS: PostgreSQL customer cleaning passed!")

    finally:
        spark.stop()


if __name__ == "__main__":
    test_clean_postgres_customers()
