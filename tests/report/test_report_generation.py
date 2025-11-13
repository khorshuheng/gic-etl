import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.testing import assertDataFrameEqual

from gic.report.pricing import generate_pricing_reconciliation_report, Config


@pytest.mark.integration
def test_pricing_reconciliation_report_generation(
    spark_session: SparkSession, temp_sqlite_database_path: str, temp_dir: str
) -> None:
    src_url = f"jdbc:sqlite:{temp_sqlite_database_path}"
    dest_path = f"file://{temp_dir}/pricing_reconciliation_report"
    generate_pricing_reconciliation_report(
        spark_session, Config(src_url=src_url, dest_path=dest_path)
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
