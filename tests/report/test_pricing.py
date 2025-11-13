from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.testing import assertDataFrameEqual

from gic.report.pricing import (
    combined_instrument_pricing,
    eom_and_bom_pricing,
    pricing_reconciliation,
)


def test_combined_instrument_pricing(spark_session: SparkSession):
    bond_pricing = spark_session.createDataFrame(
        [
            ["2018-10-30", "ABCDEF", 98.12],
        ],
        ["DATETIME", "ISIN", "PRICE"],
    )
    equity_pricing = spark_session.createDataFrame(
        [
            ["10/1/2018", "AAA", 100.0],
        ],
        ["DATETIME", "SYMBOL", "PRICE"],
    )
    combined_pricing = combined_instrument_pricing(bond_pricing, equity_pricing)
    expected_df = spark_session.createDataFrame(
        [
            [datetime(2018, 10, 1).date(), "AAA", 100.0, "Equities"],
            [datetime(2018, 10, 31).date(), "ABCDEF", 98.12, "Government Bonds"],
        ],
        ["DATETIME", "INSTRUMENT IDENTIFIER", "PRICE", "FINANCIAL TYPE"],
    )
    assertDataFrameEqual(combined_pricing, expected_df)


def test_eom_and_bom_pricing(spark_session: SparkSession):
    instrument_pricing = spark_session.createDataFrame(
        [
            [datetime(2018, 9, 1).date(), "AAA", 60.0],
            [datetime(2018, 9, 30).date(), "AAA", 80.0],
            [datetime(2018, 10, 1).date(), "AAA", 80.0],
            [datetime(2018, 10, 31).date(), "AAA", 100.0],
            [datetime(2018, 10, 1).date(), "BBB", 70.0],
            [datetime(2018, 10, 31).date(), "BBB", 90.0],
        ],
        ["DATETIME", "INSTRUMENT IDENTIFIER", "PRICE"],
    )
    expected_df = (
        spark_session.createDataFrame(
            [
                [2018, 9, "AAA", 80.0, 60.0],
                [2018, 10, "AAA", 100.0, 80.0],
                [2018, 10, "BBB", 90.0, 70.0],
            ],
            ["YEAR", "MONTH", "INSTRUMENT IDENTIFIER", "EOM PRICE", "BOM PRICE"],
        )
        .withColumn("YEAR", col("YEAR").cast(IntegerType()))
        .withColumn("MONTH", col("MONTH").cast(IntegerType()))
    )
    eom_pricing_df = eom_and_bom_pricing(instrument_pricing)
    assertDataFrameEqual(eom_pricing_df, expected_df)


def test_pricing_reconciliation(spark_session: SparkSession):
    fund_positions = spark_session.createDataFrame(
        [
            ["Magnum", "Equities", "AAA", "", 100.0, 9, 2022],
        ],
        [
            "FUND NAME",
            "FINANCIAL TYPE",
            "SYMBOL",
            "SECURITY IDENTIFIER",
            "PRICE",
            "MONTH",
            "YEAR",
        ],
    )
    eom_pricing = spark_session.createDataFrame(
        [
            [2022, 9, datetime(2022, 9, 30).date(), "AAA", 80.0],
        ],
        ["YEAR", "MONTH", "DATETIME", "INSTRUMENT IDENTIFIER", "EOM PRICE"],
    )
    df = pricing_reconciliation(eom_pricing, fund_positions)
    expected_df = spark_session.createDataFrame(
        [
            ["Magnum", "Equities", "AAA", 2022, 9, 20.0],
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
    assertDataFrameEqual(df, expected_df)
