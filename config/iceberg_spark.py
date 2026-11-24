
import os

from pyspark.sql import SparkSession
from dotenv import load_dotenv


PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

load_dotenv(
    os.path.join(
        PROJECT_ROOT,
        "infrastructure",
        "docker",
        ".env"
    )
)


def create_iceberg_spark_session(
    app_name="RetailPulseIceberg"
):

    packages = ",".join([
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.110.0"
    ])

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        # Required libraries
        .config("spark.jars.packages", packages)

        # Iceberg and Nessie SQL extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
        )

        # Nessie catalog
        .config(
            "spark.sql.catalog.nessie",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            "spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog"
        )
        .config(
            "spark.sql.catalog.nessie.uri",
            os.getenv(
                "NESSIE_URI",
                "http://localhost:19120/api/v2"
            )
        )
        .config(
            "spark.sql.catalog.nessie.ref",
            "main"
        )
        .config(
            "spark.sql.catalog.nessie.warehouse",
            "s3a://silver/warehouse"
        )
        .config(
            "spark.sql.catalog.nessie.io-impl",
            "org.apache.iceberg.hadoop.HadoopFileIO"
        )

        # S3-compatible storage
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("S3_ENDPOINT_URL")
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.getenv("S3_ACCESS_KEY")
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv("S3_SECRET_KEY")
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            os.getenv("S3_REGION", "us-east-1")
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark
