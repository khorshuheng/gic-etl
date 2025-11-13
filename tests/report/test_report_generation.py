import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round
from pyspark.sql.types import IntegerType
from pyspark.testing import assertDataFrameEqual

from gic.report.pricing import (
    generate_pricing_reconciliation_report,
    generate_top_performing_fund_report,
)


@pytest.mark.integration
def test_pricing_reconciliation_report_generation(
    spark_session: SparkSession, temp_sqlite_database_path: str, temp_dir: str
) -> None:
    datastore_url = f"jdbc:sqlite:{temp_sqlite_database_path}"
    dest_path = f"file://{temp_dir}/pricing_reconciliation_report"
    generate_pricing_reconciliation_report(
        spark_session=spark_session, datastore_url=datastore_url, dest_path=dest_path
    )
    report_df = spark_session.read.csv(dest_path, inferSchema=True, header=True)
    expected_df = (
        spark_session.createDataFrame(
            [
                ["Applebead", "Equities", "TJX", 2022, 11, 0.89],
            ],
            [
                "FUND NAME",
                "FINANCIAL TYPE",
                "INSTRUMENT IDENTIFIER",
                "YEAR",
                "MONTH",
                "PRICE BREAK",
            ],
        )
        .withColumn("YEAR", col("YEAR").cast(IntegerType()))
        .withColumn("MONTH", col("MONTH").cast(IntegerType()))
    )
    assertDataFrameEqual(
        report_df,
        expected_df,
    )


@pytest.mark.integration
def test_top_performing_fund_report_generation(
    spark_session: SparkSession, temp_sqlite_database_path: str, temp_dir: str
) -> None:
    datastore_url = f"jdbc:sqlite:{temp_sqlite_database_path}"
    dest_path = f"file://{temp_dir}/top_performing_fund_report"
    generate_top_performing_fund_report(
        spark_session=spark_session, datastore_url=datastore_url, dest_path=dest_path
    )
    report_df = spark_session.read.csv(
        dest_path, inferSchema=True, header=True
    ).withColumn("RATE OF RETURN", round(col("RATE OF RETURN"), 2))
    expected_df = (
        spark_session.createDataFrame(
            [
                [2022, 11, "Applebead", 1.85],
            ],
            [
                "YEAR",
                "MONTH",
                "FUND NAME",
                "RATE OF RETURN",
            ],
        )
        .withColumn("MONTH", col("MONTH").cast(IntegerType()))
        .withColumn("YEAR", col("YEAR").cast(IntegerType()))
    )
    assertDataFrameEqual(
        report_df,
        expected_df,
    )
