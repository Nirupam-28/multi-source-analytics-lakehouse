
from config.iceberg_spark import create_iceberg_spark_session

from processing.gold.customer_purchase_summary import (
    build_customer_purchase_summary
)

from processing.silver.iceberg_writer import write_iceberg_table


def main():

    spark = create_iceberg_spark_session(
        "RetailPulseCustomerSummaryWriter"
    )

    try:

        print("\nReading Silver tables...")

        customers_df = spark.table(
            "nessie.silver.postgres_customers"
        )

        orders_df = spark.table(
            "nessie.silver.orders"
        )

        print("\nBuilding customer purchase summary...")

        summary_df = build_customer_purchase_summary(
            customers_df,
            orders_df
        )

        print("\nWriting Gold Iceberg table...")

        write_iceberg_table(
            summary_df,
            "nessie.gold.customer_purchase_summary",
            mode="replace"
        )

        print("\nVerifying Gold table...")

        gold_df = spark.table(
            "nessie.gold.customer_purchase_summary"
        )

        gold_df.show(truncate=False)

        print(
            f"Customer summary records: {gold_df.count()}"
        )

        print("\nCUSTOMER PURCHASE SUMMARY GOLD TABLE CREATED!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
