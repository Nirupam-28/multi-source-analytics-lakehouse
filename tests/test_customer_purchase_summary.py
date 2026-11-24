
from config.iceberg_spark import create_iceberg_spark_session

from processing.gold.customer_purchase_summary import (
    build_customer_purchase_summary
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulseCustomerSummary"
    )

    try:

        print("\nReading Silver customers and orders...")

        customers_df = spark.table(
            "nessie.silver.postgres_customers"
        )

        orders_df = spark.table(
            "nessie.silver.orders"
        )

        print(f"Customers: {customers_df.count()}")
        print(f"Orders: {orders_df.count()}")

        print("\nBuilding customer purchase summary...")

        summary_df = build_customer_purchase_summary(
            customers_df,
            orders_df
        )

        summary_df.show(truncate=False)

        print("\nCustomer summary schema:")

        summary_df.printSchema()

        assert summary_df.count() == customers_df.count()

        assert "total_spent" in summary_df.columns

        assert "total_orders" in summary_df.columns

        print("\nCUSTOMER PURCHASE SUMMARY TEST PASSED!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
