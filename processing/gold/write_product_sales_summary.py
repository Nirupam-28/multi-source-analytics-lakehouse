
from config.iceberg_spark import create_iceberg_spark_session

from processing.gold.product_sales_summary import (
    build_product_sales_summary
)

from processing.silver.iceberg_writer import write_iceberg_table


def main():

    spark = create_iceberg_spark_session(
        "RetailPulseProductSalesWriter"
    )

    try:

        print("\nReading Silver order items...")

        order_items_df = spark.table(
            "nessie.silver.order_items"
        )

        print("\nBuilding product sales summary...")

        sales_df = build_product_sales_summary(
            order_items_df
        )

        print("\nWriting Gold Iceberg table...")

        write_iceberg_table(
            sales_df,
            "nessie.gold.product_sales_summary",
            mode="replace"
        )

        print("\nVerifying Gold table...")

        gold_df = spark.table(
            "nessie.gold.product_sales_summary"
        )

        gold_df.show(truncate=False)

        print(
            f"Product sales records: {gold_df.count()}"
        )

        print("\nPRODUCT SALES GOLD TABLE CREATED!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
