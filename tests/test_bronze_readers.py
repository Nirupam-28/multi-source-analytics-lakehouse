
import pytest

from config.spark import create_spark_session

from processing.bronze.read_bronze import (
    read_api_customers,
    read_postgres_table,
    read_payments,
    read_refunds
)


@pytest.fixture(scope="module")
def spark():

    session = create_spark_session(
        "BronzeReaderTests"
    )

    yield session

    session.stop()


def test_api_customers(spark):

    df = read_api_customers(spark)

    assert df.count() == 416
    assert "id" in df.columns


def test_postgres_customers(spark):

    df = read_postgres_table(spark, "customers")

    assert df.count() == 10
    assert "customer_id" in df.columns


def test_orders(spark):

    df = read_postgres_table(spark, "orders")

    assert df.count() == 15


def test_order_items(spark):

    df = read_postgres_table(spark, "order_items")

    assert df.count() == 15


def test_payments(spark):

    df = read_payments(spark)

    assert df.count() == 8


def test_refunds(spark):

    df = read_refunds(spark)

    assert df.count() == 3


def test_invalid_postgres_table(spark):

    with pytest.raises(ValueError):

        read_postgres_table(spark, "employees")
