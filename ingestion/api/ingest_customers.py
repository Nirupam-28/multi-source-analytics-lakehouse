import json
import logging
import uuid
from datetime import datetime, timezone

import requests

from config.storage import StorageClient
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


API_URL = "https://dummyjson.com/users"
PAGE_SIZE = 100

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def fetch_customers():
    all_customers = []
    skip = 0

    retry_strategy = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    while True:

        params = {
            "limit": PAGE_SIZE,
            "skip": skip
        }

        logger.info(
            "Fetching customers with skip=%s",
            skip
        )

        try:
            response = session.get(
                API_URL,
                params=params,
                timeout=30
            )

            response.raise_for_status()

            payload = response.json()

        except requests.RequestException as error:
            logger.error("API request failed: %s", error)
            raise

        customers = payload.get("users", [])

        if not customers:
            break

        all_customers.extend(customers)

        total = payload.get("total", 0)

        skip += len(customers)

        if skip >= total:
            break

    logger.info(
        "Successfully fetched %s customers",
        len(all_customers)
    )

    return all_customers


def ingest_customers():

    run_id = str(uuid.uuid4())

    ingestion_time = datetime.now(timezone.utc)

    logger.info("Starting customer ingestion")
    logger.info("Run ID: %s", run_id)

    customers = fetch_customers()

    payload = {
        "metadata": {
            "source": "dummyjson_api",
            "entity": "customers",
            "run_id": run_id,
            "ingestion_timestamp": ingestion_time.isoformat(),
            "record_count": len(customers)
        },
        "data": customers
    }

    ingestion_date = ingestion_time.strftime("%Y-%m-%d")

    object_key = (
        f"customers/ingestion_date={ingestion_date}/"
        f"run_id={run_id}/customers.json"
    )

    storage = StorageClient()

    storage.upload_bytes(
        data=json.dumps(payload, indent=2).encode("utf-8"),
        bucket_name=storage.buckets["bronze"],
        object_key=object_key,
        content_type="application/json"
    )

    logger.info("Customer ingestion completed")
    logger.info(
        "Total records ingested: %s",
        len(customers)
    )


if __name__ == "__main__":
    ingest_customers()