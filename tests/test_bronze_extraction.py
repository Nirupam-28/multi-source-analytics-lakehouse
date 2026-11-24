
from config.spark import create_spark_session
from processing.bronze.extract_records import extract_json_records


def test_bronze_record_extraction():

    spark = create_spark_session(
        "BronzeRecordExtractionTest"
    )

    try:

        source_df = (
            spark.read
            .option("multiline", "true")
            .json("s3a://bronze/postgres/orders/")
        )

        print("\nOriginal Bronze schema:")
        source_df.printSchema()

        orders_df = extract_json_records(source_df)

        print("\nExtracted business schema:")
        orders_df.printSchema()

        print("\nExtracted orders:")
        orders_df.show(5, truncate=False)

        print("\nExtracted record count:")
        print(orders_df.count())

        assert orders_df.count() == 15

        assert "metadata" not in orders_df.columns
        assert "data" not in orders_df.columns

        assert "order_id" in orders_df.columns
        assert "customer_id" in orders_df.columns

        print("\nSUCCESS: Bronze record extraction passed!")

    finally:
        spark.stop()


if __name__ == "__main__":
    test_bronze_record_extraction()
