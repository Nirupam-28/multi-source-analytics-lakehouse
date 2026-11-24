
from config.spark import create_spark_session


def test_spark_storage_connection():

    spark = create_spark_session()

    try:

        print("\nSpark version:", spark.version)

        hadoop_conf = (
            spark.sparkContext._jsc.hadoopConfiguration()
        )

        Path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path

        bronze_path = Path("s3a://bronze/")

        filesystem = bronze_path.getFileSystem(hadoop_conf)

        objects = filesystem.listStatus(bronze_path)

        print("\nBronze bucket contents:")

        for obj in objects:
            print(obj.getPath().toString())

        assert len(objects) > 0

        print("\nSUCCESS: Spark connected to Bronze storage!")

    finally:
        spark.stop()


if __name__ == "__main__":
    test_spark_storage_connection()
