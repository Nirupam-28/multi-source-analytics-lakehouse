
import hashlib
import os

from config.storage import StorageClient


PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

DATA_FOLDER = os.path.join(
    PROJECT_ROOT,
    "ingestion",
    "files",
    "data"
)


def calculate_sha256(content):

    return hashlib.sha256(content).hexdigest()


def test_file_integrity():

    storage = StorageClient()

    files = [
        "payments.csv",
        "refunds.json"
    ]

    for filename in files:

        local_path = os.path.join(
            DATA_FOLDER,
            filename
        )

        with open(local_path, "rb") as file:
            local_content = file.read()

        prefix = f"files/{filename}/"

        objects = storage.list_objects(
            storage.buckets["bronze"],
            prefix
        )

        matching_objects = [
            key for key in objects
            if key.endswith(f"/{filename}")
        ]

        assert matching_objects, (
            f"No Bronze object found for {filename}"
        )

        # Verify the latest uploaded object.
        object_key = sorted(matching_objects)[-1]

        response = storage.client.get_object(
            Bucket=storage.buckets["bronze"],
            Key=object_key
        )

        bronze_content = response["Body"].read()

        local_hash = calculate_sha256(local_content)
        bronze_hash = calculate_sha256(bronze_content)

        print(f"\nFile: {filename}")
        print(f"Bronze object: {object_key}")
        print(f"Local SHA256:  {local_hash}")
        print(f"Bronze SHA256: {bronze_hash}")

        assert local_hash == bronze_hash, (
            f"File integrity check failed: {filename}"
        )

        print("SUCCESS: File integrity verified!")


if __name__ == "__main__":
    test_file_integrity()
