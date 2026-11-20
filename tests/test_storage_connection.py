from config.storage import StorageClient


def test_storage_connection():

    storage = StorageClient()

    buckets = storage.list_buckets()

    assert "bronze" in buckets
    assert "silver" in buckets
    assert "gold" in buckets

    print("Storage connection test passed!")


if __name__ == "__main__":
    test_storage_connection()