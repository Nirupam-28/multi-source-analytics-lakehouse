
from config.iceberg_spark import create_iceberg_spark_session

from processing.gold.product_sales_summary import (
    build_product_sales_summary
)


def main():

    spark = create_iceberg_spark_session(
        "RetailPulseProductSales"
    )

    try:

        print("\nReading Silver order items...")

        order_items_df = spark.table(
            "nessie.silver.order_items"
        )

        print(
            f"Order item records: {order_items_df.count()}"
        )

        print("\nBuilding product sales summary...")

        sales_df = build_product_sales_summary(
            order_items_df
        )

        sales_df.show(truncate=False)

        print("\nProduct sales schema:")

        sales_df.printSchema()

        assert sales_df.count() > 0

        assert "total_units_sold" in sales_df.columns

        assert "total_revenue" in sales_df.columns

        print("\nPRODUCT SALES SUMMARY TEST PASSED!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
