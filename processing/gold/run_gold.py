
from config.iceberg_spark import create_iceberg_spark_session

from processing.gold.monthly_revenue import build_monthly_revenue
from processing.gold.customer_purchase_summary import (
    build_customer_purchase_summary
)
from processing.gold.payment_summary import build_payment_summary
from processing.gold.product_sales_summary import (
    build_product_sales_summary
)

from processing.silver.iceberg_writer import write_iceberg_table

from monitoring.pipeline_metrics import PipelineMetrics


def run_gold_pipeline(metrics):

    spark = create_iceberg_spark_session(
        "RetailPulseGoldPipeline"
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    try:

        print("\n========== GOLD PIPELINE STARTED ==========")

        # Read Silver tables
        print("\nReading Silver Iceberg tables...")

        orders_df = spark.table("nessie.silver.orders")

        customers_df = spark.table(
            "nessie.silver.postgres_customers"
        )

        payments_df = spark.table(
            "nessie.silver.payments"
        )

        order_items_df = spark.table(
            "nessie.silver.order_items"
        )

        # Build Gold datasets
        gold_datasets = {

            "monthly_revenue": build_monthly_revenue(
                orders_df
            ),

            "customer_purchase_summary": (
                build_customer_purchase_summary(
                    customers_df,
                    orders_df
                )
            ),

            "payment_summary": build_payment_summary(
                payments_df
            ),

            "product_sales_summary": (
                build_product_sales_summary(
                    order_items_df
                )
            )
        }

        # Write Gold Iceberg tables
        total_records = 0

        for dataset_name, df in gold_datasets.items():

            print(
                f"\n========== {dataset_name.upper()} =========="
            )

            table_name = f"nessie.gold.{dataset_name}"

            write_iceberg_table(
                df,
                table_name,
                mode="replace"
            )

            total_records += df.count()

        # Verify all Gold tables
        print("\n========== GOLD TABLE VERIFICATION ==========")

        spark.sql(
            "SHOW TABLES IN nessie.gold"
        ).show(truncate=False)

        for dataset_name in gold_datasets:

            table_name = f"nessie.gold.{dataset_name}"

            gold_df = spark.table(table_name)

            print(
                f"{dataset_name}: {gold_df.count()} records"
            )

        print("\n========== GOLD PIPELINE COMPLETED ==========")

        metrics.record_records(total_records)

        print("\nALL GOLD TABLES CREATED SUCCESSFULLY!")

    finally:
        spark.stop()

def main():
    with PipelineMetrics("gold", "analytics") as metrics:
        run_gold_pipeline(metrics)

if __name__ == "__main__":
    main()
