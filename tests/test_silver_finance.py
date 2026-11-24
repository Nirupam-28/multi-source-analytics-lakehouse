
from pyspark.sql.functions import col

from config.spark import create_spark_session

from processing.bronze.read_bronze import (
    read_payments,
    read_refunds
)

from processing.silver.clean_payments import clean_payments
from processing.silver.clean_refunds import clean_refunds


def test_clean_finance_data():

    spark = create_spark_session(
        "SilverFinanceCleaningTest"
    )

    try:

        payments_bronze = read_payments(spark)
        refunds_bronze = read_refunds(spark)

        payments_silver = clean_payments(payments_bronze)
        refunds_silver = clean_refunds(refunds_bronze)

        print("\nPayments Bronze:", payments_bronze.count())
        print("Payments Silver:", payments_silver.count())

        print("\nRefunds Bronze:", refunds_bronze.count())
        print("Refunds Silver:", refunds_silver.count())

        print("\nCleaned Payments:")
        payments_silver.show(truncate=False)

        print("\nCleaned Refunds:")
        refunds_silver.show(truncate=False)

        payments_silver.printSchema()
        refunds_silver.printSchema()

        assert payments_silver.count() == 8
        assert refunds_silver.count() == 3

        assert str(
            payments_silver.schema["amount"].dataType
        ) == "DecimalType(12,2)"

        assert str(
            refunds_silver.schema["refund_amount"].dataType
        ) == "DecimalType(12,2)"

        assert payments_silver.filter(
            col("amount") < 0
        ).count() == 0

        assert refunds_silver.filter(
            col("refund_amount") < 0
        ).count() == 0

        print("\nSUCCESS: Finance cleaning passed!")

    finally:
        spark.stop()


if __name__ == "__main__":
    test_clean_finance_data()
