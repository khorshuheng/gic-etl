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
    sum,
)

from gic.constants.column import (
    INSTRUMENT_IDENTIFIER_COLUMN,
    FINANCIAL_TYPE_COLUMN,
    MONTH_COLUMN,
    EOM_PRICE_COLUMN,
    PRICE_BREAK_COLUMN,
    PRICE_COLUMN,
    BOM_PRICE_COLUMN,
    RATE_OF_RETURN_COLUMN,
    QUANTITY_COLUMN,
    REALISED_PL_COLUMN,
    FUND_MV_START_COLUMN,
    FUND_MV_END_COLUMN,
    FUND_REALISED_PL_COLUMN,
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


def with_instrument_identifier_col(fund_position_df: DataFrame) -> DataFrame:
    return fund_position_df.withColumn(
        INSTRUMENT_IDENTIFIER_COLUMN,
        when(col(FINANCIAL_TYPE_COLUMN) == "Equities", col("SYMBOL")).otherwise(
            col("SECURITY IDENTIFIER")
        ),
    )


def pricing_reconciliation(
    eom_pricing_df: DataFrame, fund_position: DataFrame
) -> DataFrame:
    fund_position = with_instrument_identifier_col(fund_position)
    eom_pricing_df = eom_pricing_df.withColumnRenamed(PRICE_COLUMN, EOM_PRICE_COLUMN)
    return (
        fund_position.join(
            eom_pricing_df, [YEAR_COLUMN, MONTH_COLUMN, INSTRUMENT_IDENTIFIER_COLUMN]
        )
        .withColumn(
            PRICE_BREAK_COLUMN, fund_position[PRICE_COLUMN] - col(EOM_PRICE_COLUMN)
        )
        .select(
            fund_position[FUND_NAME_COLUMN],
            fund_position[FINANCIAL_TYPE_COLUMN],
            fund_position[INSTRUMENT_IDENTIFIER_COLUMN],
            fund_position[YEAR_COLUMN],
            fund_position[MONTH_COLUMN],
            col(PRICE_BREAK_COLUMN),
        )
    )


def fund_aggregated_market_value(
    eom_and_bom_pricing_df: DataFrame, fund_position_df: DataFrame
) -> DataFrame:
    fund_position_df = with_instrument_identifier_col(fund_position_df)
    return (
        fund_position_df.join(
            eom_and_bom_pricing_df,
            [YEAR_COLUMN, MONTH_COLUMN, INSTRUMENT_IDENTIFIER_COLUMN],
        )
        .groupby(
            fund_position_df[YEAR_COLUMN],
            fund_position_df[MONTH_COLUMN],
            fund_position_df[FUND_NAME_COLUMN],
        )
        .agg(
            sum(col(BOM_PRICE_COLUMN) * col(QUANTITY_COLUMN)).alias(
                FUND_MV_START_COLUMN
            ),
            sum(col(EOM_PRICE_COLUMN) * col(QUANTITY_COLUMN)).alias(FUND_MV_END_COLUMN),
            sum(col(REALISED_PL_COLUMN)).alias(FUND_REALISED_PL_COLUMN),
        )
    )


def fund_performance(
    eom_and_bom_pricing_df: DataFrame, fund_position_df: DataFrame
) -> DataFrame:
    fund_aggregated_market_value_df = fund_aggregated_market_value(
        eom_and_bom_pricing_df, fund_position_df
    )
    fund_performance_df = fund_aggregated_market_value_df.withColumn(
        RATE_OF_RETURN_COLUMN,
        (
            col(FUND_MV_END_COLUMN)
            - col(FUND_MV_START_COLUMN)
            + col(FUND_REALISED_PL_COLUMN)
        )
        / col(FUND_MV_START_COLUMN),
    )
    return fund_performance_df.select(
        col(YEAR_COLUMN),
        col(MONTH_COLUMN),
        col(FUND_NAME_COLUMN),
        col(RATE_OF_RETURN_COLUMN),
    )


def top_performing_fund(fund_performance_df: DataFrame) -> DataFrame:
    window = Window.partitionBy(col(YEAR_COLUMN), col(MONTH_COLUMN)).orderBy(
        col(RATE_OF_RETURN_COLUMN).desc()
    )
    return (
        fund_performance_df.withColumn(
            "_RANK",
            rank().over(window),
        )
        .filter(col("_RANK") == 1)
        .drop("_RANK")
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


def generate_top_performing_fund_report(spark_session: SparkSession, config: Config):
    combined_instrument_pricing_df = get_combined_instrument_pricing_df(
        spark_session, config.src_url
    )
    fund_position_df = get_fund_position_df(spark_session, config.src_url)
    eom_and_bom_pricing_df = eom_and_bom_pricing(combined_instrument_pricing_df)
    fund_performance_df = fund_performance(eom_and_bom_pricing_df, fund_position_df)
    top_performing_fund_df = top_performing_fund(fund_performance_df)
    top_performing_fund_df.write.csv(
        path=config.dest_path, mode="overwrite", header=True
    )
