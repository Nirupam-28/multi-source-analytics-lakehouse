
from config.iceberg_spark import create_iceberg_spark_session

from processing.gold.monthly_revenue import build_monthly_revenue
from processing.silver.iceberg_writer import write_iceberg_table


def main():

    spark = create_iceberg_spark_session(
        "RetailPulseMonthlyRevenueWriter"
    )

    try:

        print("\nReading Silver orders...")

        orders_df = spark.table(
            "nessie.silver.orders"
        )

        print("\nBuilding monthly revenue...")

        monthly_df = build_monthly_revenue(orders_df)

        print("\nWriting Gold Iceberg table...")

        write_iceberg_table(
            monthly_df,
            "nessie.gold.monthly_revenue",
            mode="replace"
        )

        print("\nVerifying Gold table...")

        gold_df = spark.table(
            "nessie.gold.monthly_revenue"
        )

        gold_df.show(truncate=False)

        print(
            f"Gold monthly revenue records: {gold_df.count()}"
        )

        print("\nMONTHLY REVENUE GOLD TABLE CREATED!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
