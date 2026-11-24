
from config.iceberg_spark import create_iceberg_spark_session

from processing.gold.monthly_revenue import build_monthly_revenue


def main():

    spark = create_iceberg_spark_session(
        "RetailPulseMonthlyRevenue"
    )

    try:

        print("\nReading Silver orders...")

        orders_df = spark.table(
            "nessie.silver.orders"
        )

        print(f"Silver orders: {orders_df.count()}")

        print("\nBuilding monthly revenue...")

        monthly_df = build_monthly_revenue(orders_df)

        monthly_df.show(truncate=False)

        print("\nMonthly revenue schema:")

        monthly_df.printSchema()

        assert monthly_df.count() > 0

        assert "total_revenue" in monthly_df.columns

        assert "total_orders" in monthly_df.columns

        print("\nMONTHLY REVENUE TRANSFORMATION PASSED!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
