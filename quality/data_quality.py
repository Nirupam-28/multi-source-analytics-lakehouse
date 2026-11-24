
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def check_not_null(df: DataFrame, column: str) -> bool:

    invalid_count = df.filter(
        col(column).isNull()
    ).count()

    return invalid_count == 0


def check_unique(df: DataFrame, column: str) -> bool:

    total_count = df.count()

    unique_count = df.select(
        column
    ).distinct().count()

    return total_count == unique_count


def check_non_negative(df: DataFrame, column: str) -> bool:

    invalid_count = df.filter(
        col(column) < 0
    ).count()

    return invalid_count == 0


def check_positive(df: DataFrame, column: str) -> bool:

    invalid_count = df.filter(
        col(column) <= 0
    ).count()

    return invalid_count == 0


def validate_dataset(
    df: DataFrame,
    dataset_name: str,
    rules: list
):

    print(f"\nValidating dataset: {dataset_name}")

    failed_rules = []

    for rule_name, rule_function, column in rules:

        passed = rule_function(df, column)

        if passed:
            print(f"PASS: {rule_name}")

        else:
            print(f"FAIL: {rule_name}")
            failed_rules.append(rule_name)

    if failed_rules:

        raise ValueError(
            f"Data quality failed for {dataset_name}: "
            f"{failed_rules}"
        )

    print(f"SUCCESS: {dataset_name} passed all quality checks!")
