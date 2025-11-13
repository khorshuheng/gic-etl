import dataclasses
from typing import Optional

from pyspark.sql import DataFrame, Window, SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    lit,
    rank,
    month,
    when,
    year,
    first_value,
    last_value,
)

from gic.constants.column import (
    INSTRUMENT_IDENTIFIER_COLUMN,
    FINANCIAL_TYPE_COLUMN,
    MONTH_COLUMN,
    EOM_PRICE_COLUMN,
    PRICE_BREAK_COLUMN,
    PRICE_COLUMN,
    BOM_PRICE_COLUMN,
)
from gic.constants.table import BOND_PRICES, EQUITY_PRICES, FUND_POSITIONS
from gic.ingestion.external_funds import YEAR_COLUMN, FUND_NAME_COLUMN, DATETIME_COLUMN


@dataclasses.dataclass
class Config:
    src_url: str
    dest_path: str
    year: Optional[int] = None
    month: Optional[int] = None


def combined_instrument_pricing(
    bond_pricing: DataFrame, equity_pricing: DataFrame
) -> DataFrame:
    bond_pricing = (
        bond_pricing.withColumn(
            DATETIME_COLUMN, to_date(col(DATETIME_COLUMN), "yyyy-MM-dd")
        )
        .withColumnRenamed("ISIN", INSTRUMENT_IDENTIFIER_COLUMN)
        .withColumn(FINANCIAL_TYPE_COLUMN, lit("Government Bonds"))
    )
    equity_pricing = (
        equity_pricing.withColumn(
            DATETIME_COLUMN, to_date(col(DATETIME_COLUMN), "M/d/yyyy")
        )
        .withColumnRenamed("SYMBOL", INSTRUMENT_IDENTIFIER_COLUMN)
        .withColumn(FINANCIAL_TYPE_COLUMN, lit("Equities"))
    )
    return bond_pricing.unionAll(equity_pricing)


def eom_and_bom_pricing(pricing: DataFrame) -> DataFrame:
    pricing = pricing.withColumn(YEAR_COLUMN, year(col(DATETIME_COLUMN)))
    pricing = pricing.withColumn(MONTH_COLUMN, month(col(DATETIME_COLUMN)))
    unbounded_window = (
        Window.partitionBy(INSTRUMENT_IDENTIFIER_COLUMN, MONTH_COLUMN, YEAR_COLUMN)
        .orderBy(col(DATETIME_COLUMN))
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    date_window = Window.partitionBy(
        INSTRUMENT_IDENTIFIER_COLUMN, MONTH_COLUMN, YEAR_COLUMN
    ).orderBy(col(DATETIME_COLUMN).desc())
    pricing = (
        pricing.withColumn(
            EOM_PRICE_COLUMN, last_value(col(PRICE_COLUMN)).over(unbounded_window)
        )
        .withColumn(
            BOM_PRICE_COLUMN, first_value(col(PRICE_COLUMN)).over(unbounded_window)
        )
        .withColumn("_RANK", rank().over(date_window))
        .filter(col("_RANK") == 1)
    )
    return pricing.select(
        YEAR_COLUMN,
        MONTH_COLUMN,
        INSTRUMENT_IDENTIFIER_COLUMN,
        EOM_PRICE_COLUMN,
        BOM_PRICE_COLUMN,
    )


def pricing_reconciliation(
    eom_pricing_df: DataFrame, fund_position: DataFrame
) -> DataFrame:
    fund_position = fund_position.withColumn(
        INSTRUMENT_IDENTIFIER_COLUMN,
        when(col(FINANCIAL_TYPE_COLUMN) == "Equities", col("SYMBOL")).otherwise(
            col("SECURITY IDENTIFIER")
        ),
    )
    eom_pricing_df = eom_pricing_df.withColumnRenamed("PRICE", EOM_PRICE_COLUMN)
    return (
        fund_position.join(
            eom_pricing_df,
            (
                eom_pricing_df[INSTRUMENT_IDENTIFIER_COLUMN]
                == fund_position[INSTRUMENT_IDENTIFIER_COLUMN]
            )
            & (eom_pricing_df[MONTH_COLUMN] == fund_position[MONTH_COLUMN])
            & (eom_pricing_df[YEAR_COLUMN] == fund_position[YEAR_COLUMN]),
        )
        .withColumn(PRICE_BREAK_COLUMN, fund_position["PRICE"] - col(EOM_PRICE_COLUMN))
        .select(
            fund_position[FUND_NAME_COLUMN],
            fund_position[FINANCIAL_TYPE_COLUMN],
            fund_position[INSTRUMENT_IDENTIFIER_COLUMN],
            fund_position[YEAR_COLUMN],
            fund_position[MONTH_COLUMN],
            col(PRICE_BREAK_COLUMN),
        )
    )


def get_combined_instrument_pricing_df(
    spark_session: SparkSession, src_url: str
) -> DataFrame:
    bond_prices_df = spark_session.read.jdbc(url=src_url, table=BOND_PRICES)
    equity_prices_df = spark_session.read.jdbc(url=src_url, table=EQUITY_PRICES)
    return combined_instrument_pricing(bond_prices_df, equity_prices_df)


def get_fund_position_df(spark_session: SparkSession, src_url: str) -> DataFrame:
    fund_position_df = spark_session.read.jdbc(url=src_url, table=FUND_POSITIONS)
    return fund_position_df


def generate_pricing_reconciliation_report(spark_session: SparkSession, config: Config):
    combined_instrument_pricing_df = get_combined_instrument_pricing_df(
        spark_session, config.src_url
    )
    fund_position_df = get_fund_position_df(spark_session, config.src_url)
    eom_pricing_df = eom_and_bom_pricing(combined_instrument_pricing_df)
    report_df = pricing_reconciliation(eom_pricing_df, fund_position_df)
    report_df.write.csv(path=config.dest_path, mode="overwrite", header=True)
