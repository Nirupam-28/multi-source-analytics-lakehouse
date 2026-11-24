
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


def create_spark_session(app_name="RetailPulseLakehouse"):

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        # Download the S3A connector matching Hadoop 3.3.4.
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4"
        )

        # S3-compatible storage configuration.
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
