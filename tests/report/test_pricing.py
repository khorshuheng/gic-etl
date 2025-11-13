from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round
from pyspark.sql.types import IntegerType
from pyspark.testing import assertDataFrameEqual

from gic.report.pricing import (
    combined_instrument_pricing,
    eom_and_bom_pricing,
    pricing_reconciliation,
    fund_performance,
    fund_aggregated_market_value,
)


def test_combined_instrument_pricing(spark_session: SparkSession):
    bond_pricing = spark_session.createDataFrame(
        [
            ["2018-10-31", "ABCDEF", 98.12],
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
    fund_positions_df = spark_session.createDataFrame(
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
    eom_pricing_df = spark_session.createDataFrame(
        [
            [2022, 9, "AAA", 80.0],
        ],
        ["YEAR", "MONTH", "INSTRUMENT IDENTIFIER", "EOM PRICE"],
    )
    df = pricing_reconciliation(eom_pricing_df, fund_positions_df)
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


def test_aggregate_fund_market_value(spark_session: SparkSession):
    fund_positions_df = spark_session.createDataFrame(
        [
            ["Magnum", "Equities", "AAA", "", 100.0, 100, 20000.0, 9, 2022],
            ["Magnum", "Equities", "BBB", "", 150.0, 80, 18000.0, 9, 2022],
            ["Leeder", "Equities", "AAA", "", 120.0, 75, 15000.0, 9, 2022],
            ["Leeder", "Equities", "BBB", "", 110.0, 120, 17000.0, 9, 2022],
            ["Magnum", "Equities", "CCC", "", 100.0, 110, 16000.0, 10, 2022],
            ["Magnum", "Equities", "BBB", "", 120.0, 20, 13000.0, 10, 2022],
            ["Leeder", "Equities", "AAA", "", 90.0, 40, 14000.0, 10, 2022],
            ["Leeder", "Equities", "CCC", "", 80.0, 50, 18000.0, 10, 2022],
        ],
        [
            "FUND NAME",
            "FINANCIAL TYPE",
            "SYMBOL",
            "SECURITY IDENTIFIER",
            "PRICE",
            "QUANTITY",
            "REALISED P/L",
            "MONTH",
            "YEAR",
        ],
    )
    eom_and_bom_pricing_df = spark_session.createDataFrame(
        [
            [2022, 9, "AAA", 80.0, 70.0],
            [2022, 9, "BBB", 60.0, 90.0],
            [2022, 10, "AAA", 50.0, 90.0],
            [2022, 10, "BBB", 80.0, 70.0],
            [2022, 10, "CCC", 110.0, 60.0],
        ],
        ["YEAR", "MONTH", "INSTRUMENT IDENTIFIER", "EOM PRICE", "BOM PRICE"],
    )
    df = fund_aggregated_market_value(eom_and_bom_pricing_df, fund_positions_df)
    expected_df = spark_session.createDataFrame(
        [
            [2022, 9, "Magnum", 14200.0, 12800.0, 38000.0],
            [2022, 10, "Magnum", 8000.0, 13700.0, 29000.0],
            [2022, 9, "Leeder", 16050.0, 13200.0, 32000.0],
            [2022, 10, "Leeder", 6600.0, 7500.0, 32000.0],
        ],
        [
            "YEAR",
            "MONTH",
            "FUND NAME",
            "FUND MV START",
            "FUND MV END",
            "FUND REALISED P/L",
        ],
    )
    assertDataFrameEqual(df, expected_df)


def test_fund_performance(spark_session: SparkSession):
    fund_positions_df = spark_session.createDataFrame(
        [
            ["Magnum", "Equities", "AAA", "", 100.0, 100, 20000.0, 9, 2022],
            ["Magnum", "Equities", "BBB", "", 150.0, 80, 18000.0, 9, 2022],
            ["Leeder", "Equities", "AAA", "", 120.0, 75, 15000.0, 9, 2022],
            ["Leeder", "Equities", "BBB", "", 110.0, 120, 17000.0, 9, 2022],
            ["Magnum", "Equities", "CCC", "", 100.0, 110, 16000.0, 10, 2022],
            ["Magnum", "Equities", "BBB", "", 120.0, 20, 13000.0, 10, 2022],
            ["Leeder", "Equities", "AAA", "", 90.0, 40, 14000.0, 10, 2022],
            ["Leeder", "Equities", "CCC", "", 80.0, 50, 18000.0, 10, 2022],
        ],
        [
            "FUND NAME",
            "FINANCIAL TYPE",
            "SYMBOL",
            "SECURITY IDENTIFIER",
            "PRICE",
            "QUANTITY",
            "REALISED P/L",
            "MONTH",
            "YEAR",
        ],
    )
    eom_and_bom_pricing_df = spark_session.createDataFrame(
        [
            [2022, 9, "AAA", 80.0, 70.0],
            [2022, 9, "BBB", 60.0, 90.0],
            [2022, 10, "AAA", 50.0, 90.0],
            [2022, 10, "BBB", 80.0, 70.0],
            [2022, 10, "CCC", 110.0, 60.0],
        ],
        ["YEAR", "MONTH", "INSTRUMENT IDENTIFIER", "EOM PRICE", "BOM PRICE"],
    )
    df = fund_performance(eom_and_bom_pricing_df, fund_positions_df).withColumn(
        "RATE OF RETURN", round(col("RATE OF RETURN"), 2)
    )
    expected_df = spark_session.createDataFrame(
        [
            [2022, 9, "Magnum", 2.58],
            [2022, 10, "Magnum", 4.34],
            [2022, 9, "Leeder", 1.82],
            [2022, 10, "Leeder", 4.98],
        ],
        [
            "YEAR",
            "MONTH",
            "FUND NAME",
            "RATE OF RETURN",
        ],
    )
    assertDataFrameEqual(df, expected_df)
