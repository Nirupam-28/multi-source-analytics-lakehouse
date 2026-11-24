
from config.iceberg_spark import create_iceberg_spark_session

from processing.gold.payment_summary import (
    build_payment_summary
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulsePaymentSummary"
    )

    try:

        print("\nReading Silver payments...")

        payments_df = spark.table(
            "nessie.silver.payments"
        )

        print(f"Payment records: {payments_df.count()}")

        print("\nBuilding payment summary...")

        summary_df = build_payment_summary(
            payments_df
        )

        summary_df.show(truncate=False)

        print("\nPayment summary schema:")

        summary_df.printSchema()

        assert summary_df.count() > 0

        assert "total_payments" in summary_df.columns

        assert "total_amount" in summary_df.columns

        print("\nPAYMENT SUMMARY TEST PASSED!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
