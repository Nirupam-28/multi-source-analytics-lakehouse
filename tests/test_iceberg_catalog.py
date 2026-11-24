
from config.iceberg_spark import create_iceberg_spark_session


def main():

    spark = create_iceberg_spark_session(
        "RetailPulseIcebergCatalogTest"
    )

    try:
        print("\nTesting Nessie catalog connection...")

        # Create Silver namespace
        spark.sql("""
            CREATE NAMESPACE IF NOT EXISTS nessie.silver
        """)

        print("SUCCESS: Silver namespace created!")

        # List namespaces
        print("\nAvailable namespaces:")

        spark.sql("""
            SHOW NAMESPACES IN nessie
        """).show(truncate=False)

        # List tables
        print("\nTables inside Silver namespace:")

        spark.sql("""
            SHOW TABLES IN nessie.silver
        """).show(truncate=False)

        print("\nICEBERG CATALOG TEST PASSED!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
