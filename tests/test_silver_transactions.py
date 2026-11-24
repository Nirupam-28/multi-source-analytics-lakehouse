
from config.spark import create_spark_session

from processing.bronze.read_bronze import read_postgres_table

from processing.silver.clean_orders import clean_orders
from processing.silver.clean_order_items import clean_order_items
from pyspark.sql.functions import col


def test_clean_transactions():

    spark = create_spark_session(
        "SilverTransactionCleaningTest"
    )

    try:

        orders_bronze = read_postgres_table(spark, "orders")

        items_bronze = read_postgres_table(spark, "order_items")

        orders_silver = clean_orders(orders_bronze)

        items_silver = clean_order_items(items_bronze)

        print("\nOrders Bronze:", orders_bronze.count())
        print("Orders Silver:", orders_silver.count())

        print("\nOrder Items Bronze:", items_bronze.count())
        print("Order Items Silver:", items_silver.count())

        print("\nOrders schema:")
        orders_silver.printSchema()

        print("\nOrder Items schema:")
        items_silver.printSchema()

        orders_silver.show(truncate=False)

        items_silver.show(truncate=False)

        assert orders_silver.count() == 15
        assert items_silver.count() == 15

        assert str(
            orders_silver.schema["total_amount"].dataType
        ) == "DecimalType(12,2)"

        assert str(
            items_silver.schema["unit_price"].dataType
        ) == "DecimalType(12,2)"

        assert orders_silver.filter(
            col("total_amount") < 0
        ).count() == 0

        assert items_silver.filter(
            col("quantity") <= 0
        ).count() == 0

        print("\nSUCCESS: Transaction cleaning passed!")

    finally:
        spark.stop()


if __name__ == "__main__":
    from pyspark.sql.functions import col
    test_clean_transactions()
