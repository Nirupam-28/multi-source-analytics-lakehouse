import json
import logging
import os
import uuid

from datetime import datetime, timezone
from decimal import Decimal

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

from config.storage import StorageClient


PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

load_dotenv(
    os.path.join(PROJECT_ROOT, "infrastructure", "docker", ".env")
)

TABLES = [
    "customers",
    "orders",
    "order_items"
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def json_serializer(value):

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, datetime):
        return value.isoformat()

    raise TypeError(
        f"Type {type(value).__name__} is not JSON serializable"
    )


def get_postgres_connection():

    return psycopg2.connect(
        host="localhost",
        port=5432,
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )


def extract_table(cursor, table_name):

    query = f"SELECT * FROM {table_name} ORDER BY 1"

    logger.info("Extracting table: %s", table_name)

    cursor.execute(query)

    records = cursor.fetchall()

    logger.info(
        "Extracted %s records from %s",
        len(records),
        table_name
    )

    return records


def ingest_postgres():

    run_id = str(uuid.uuid4())

    ingestion_time = datetime.now(timezone.utc)

    ingestion_date = ingestion_time.strftime("%Y-%m-%d")

    logger.info("Starting PostgreSQL ingestion")
    logger.info("Run ID: %s", run_id)

    storage = StorageClient()

    connection = get_postgres_connection()

    try:

        with connection.cursor(
            cursor_factory=RealDictCursor
        ) as cursor:

            for table_name in TABLES:

                records = extract_table(
                    cursor,
                    table_name
                )

                payload = {
                    "metadata": {
                        "source": "retailpulse_postgres",
                        "entity": table_name,
                        "run_id": run_id,
                        "ingestion_timestamp": ingestion_time.isoformat(),
                        "record_count": len(records)
                    },
                    "data": records
                }

                object_key = (
                    f"postgres/{table_name}/"
                    f"ingestion_date={ingestion_date}/"
                    f"run_id={run_id}/"
                    f"{table_name}.json"
                )

                storage.upload_bytes(
                    data=json.dumps(
                        payload,
                        default=json_serializer,
                        indent=2
                    ).encode("utf-8"),
                    bucket_name=storage.buckets["bronze"],
                    object_key=object_key,
                    content_type="application/json"
                )

        logger.info(
            "PostgreSQL ingestion completed successfully"
        )

    finally:

        connection.close()


if __name__ == "__main__":

    ingest_postgres()