
import logging
import os
import uuid

from datetime import datetime, timezone

from config.storage import StorageClient


PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

DATA_FOLDER = os.path.join(
    PROJECT_ROOT,
    "ingestion",
    "files",
    "data"
)

SUPPORTED_EXTENSIONS = {
    ".csv": "text/csv",
    ".json": "application/json"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def discover_files():

    discovered_files = []

    for filename in os.listdir(DATA_FOLDER):

        file_path = os.path.join(
            DATA_FOLDER,
            filename
        )

        if not os.path.isfile(file_path):
            continue

        extension = os.path.splitext(filename)[1].lower()

        if extension in SUPPORTED_EXTENSIONS:
            discovered_files.append(file_path)

        else:
            logger.warning(
                "Skipping unsupported file: %s",
                filename
            )

    return discovered_files


def ingest_files():

    run_id = str(uuid.uuid4())

    ingestion_time = datetime.now(timezone.utc)

    ingestion_date = ingestion_time.strftime("%Y-%m-%d")

    logger.info("Starting file ingestion")
    logger.info("Run ID: %s", run_id)

    storage = StorageClient()

    files = discover_files()

    if not files:
        logger.warning("No supported files found")
        return

    for file_path in files:

        filename = os.path.basename(file_path)

        extension = os.path.splitext(filename)[1].lower()

        content_type = SUPPORTED_EXTENSIONS[extension]

        logger.info("Processing file: %s", filename)

        with open(file_path, "rb") as file:
            file_content = file.read()

        object_key = (
            f"files/{filename}/"
            f"ingestion_date={ingestion_date}/"
            f"run_id={run_id}/"
            f"{filename}"
        )

        storage.upload_bytes(
            data=file_content,
            bucket_name=storage.buckets["bronze"],
            object_key=object_key,
            content_type=content_type
        )

        logger.info(
            "Successfully ingested %s (%s bytes)",
            filename,
            len(file_content)
        )

    logger.info(
        "File ingestion completed successfully. Total files: %s",
        len(files)
    )


if __name__ == "__main__":
    ingest_files()
