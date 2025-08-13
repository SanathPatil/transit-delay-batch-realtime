from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)

def check_nulls(df: DataFrame, table_name: str):
    """
    Check for null or empty values in fields that are defined as non-nullable in the schema.
    """
    non_nullable_fields = [field.name for field in df.schema.fields if not field.nullable]
    for col in non_nullable_fields:
        null_count = df.filter(df[col].isNull() | (df[col] == '')).count()
        if null_count > 0:
            raise ValueError(f"Column '{col}' in table '{table_name}' contains {null_count} null or empty values")
        else:
            logger.info(f"Column '{col}' in '{table_name}' passed null check")

def check_unique(df: DataFrame, key_columns: list, table_name: str):
    """
    Ensure key columns contain unique combinations (i.e., no duplicates).
    """
    if not key_columns:
        logger.warning(f"No primary key defined for table '{table_name}', skipping uniqueness check")
        return

    total_count = df.count()
    unique_count = df.select(key_columns).distinct().count()

    if total_count != unique_count:
        raise ValueError(
            f"Primary key uniqueness violation in '{table_name}': "
            f"{total_count - unique_count} duplicate rows found on keys {key_columns}"
        )
    else:
        logger.info(f"Primary key check passed for table '{table_name}'")

def log_row_count(df: DataFrame, table_name: str):
    """
    Log row count of DataFrame.
    """
    count = df.count()
    logger.info(f"Row count for table '{table_name}': {count}")


def check_foreign_key(
    child_df: DataFrame,
    parent_df: DataFrame,
    child_col: str,
    parent_col: str,
    child_table: str,
    parent_table: str
):
    """
    Check if all values in child_df[child_col] exist in parent_df[parent_col].
    """
    missing = (
        child_df.select(child_col).distinct()
        .join(parent_df.select(parent_col).distinct(), child_col, "left_anti")
    )

    count = missing.count()
    if count:
        sample = missing.limit(5).toPandas().to_dict("records")
        raise ValueError(
            f" Referential-Integrity violation: {count} '{child_col}' in '{child_table}' not found in '{parent_table}.{parent_col}'. "
            f"Sample: {sample}"
        )

    logger.info(f" Referential-Integrity check passed: '{child_table}.{child_col}' → '{parent_table}.{parent_col}'")


def check_referential_integrity(dataframes: dict):
    """
    Perform referential integrity checks between GTFS tables using known foreign key relationships.
    """
    check_foreign_key(
        child_df=dataframes["trips"],
        parent_df=dataframes["routes"],
        child_col="route_id",
        parent_col="route_id",
        child_table="trips",
        parent_table="routes"
    )