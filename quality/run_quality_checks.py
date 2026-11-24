
from config.spark import create_spark_session

from processing.bronze.read_bronze import (
    read_api_customers,
    read_postgres_table,
    read_payments,
    read_refunds
)

from processing.silver.clean_customers import clean_api_customers
from processing.silver.clean_postgres_customers import clean_postgres_customers
from processing.silver.clean_orders import clean_orders
from processing.silver.clean_order_items import clean_order_items
from processing.silver.clean_payments import clean_payments
from processing.silver.clean_refunds import clean_refunds

from quality.data_quality import (
    check_not_null,
    check_unique,
    check_non_negative,
    check_positive,
    validate_dataset
)


def main():

    spark = create_spark_session(
        "RetailPulseDataQuality"
    )

    try:

        datasets = {
            "api_customers": (
                clean_api_customers(
                    read_api_customers(spark)
                ),
                [
                    ("Customer ID not null", check_not_null, "customer_id"),
                    ("Customer ID unique", check_unique, "customer_id")
                ]
            ),

            "postgres_customers": (
                clean_postgres_customers(
                    read_postgres_table(spark, "customers")
                ),
                [
                    ("Customer ID not null", check_not_null, "customer_id"),
                    ("Customer ID unique", check_unique, "customer_id"),
                    ("Email not null", check_not_null, "email")
                ]
            ),

            "orders": (
                clean_orders(
                    read_postgres_table(spark, "orders")
                ),
                [
                    ("Order ID unique", check_unique, "order_id"),
                    ("Order amount non-negative", check_non_negative, "total_amount")
                ]
            ),

            "order_items": (
                clean_order_items(
                    read_postgres_table(spark, "order_items")
                ),
                [
                    ("Order item ID unique", check_unique, "order_item_id"),
                    ("Quantity positive", check_positive, "quantity"),
                    ("Unit price non-negative", check_non_negative, "unit_price")
                ]
            ),

            "payments": (
                clean_payments(read_payments(spark)),
                [
                    ("Payment ID unique", check_unique, "payment_id"),
                    ("Payment amount non-negative", check_non_negative, "amount")
                ]
            ),

            "refunds": (
                clean_refunds(read_refunds(spark)),
                [
                    ("Refund ID unique", check_unique, "refund_id"),
                    ("Refund amount non-negative", check_non_negative, "refund_amount")
                ]
            )
        }

        for dataset_name, (df, rules) in datasets.items():

            validate_dataset(
                df,
                dataset_name,
                rules
            )

        print("\nALL DATA QUALITY CHECKS PASSED!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
