
from config.iceberg_spark import create_iceberg_spark_session

from processing.gold.payment_summary import build_payment_summary
from processing.silver.iceberg_writer import write_iceberg_table


def main():

    spark = create_iceberg_spark_session(
        "RetailPulsePaymentSummaryWriter"
    )

    try:

        print("\nReading Silver payments...")

        payments_df = spark.table(
            "nessie.silver.payments"
        )

        print("\nBuilding payment summary...")

        summary_df = build_payment_summary(
            payments_df
        )

        print("\nWriting Gold Iceberg table...")

        write_iceberg_table(
            summary_df,
            "nessie.gold.payment_summary",
            mode="replace"
        )

        print("\nVerifying Gold table...")

        gold_df = spark.table(
            "nessie.gold.payment_summary"
        )

        gold_df.show(truncate=False)

        print(
            f"Payment summary records: {gold_df.count()}"
        )

        print("\nPAYMENT SUMMARY GOLD TABLE CREATED!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
