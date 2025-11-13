from functools import partial
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import input_file_name, col, from_unixtime
from pyspark.sql.types import StructType, StructField

from gic.constants.column import (
    TIMESTAMP_COLUMN,
    MONTH_COLUMN,
    YEAR_COLUMN,
    SRC_FILE_COLUMN,
    FUND_NAME_COLUMN,
)
from gic.constants.table import FUND_POSITIONS
from gic.ingestion.parser import parse_fund_ingestion_timestamp, parse_fund_name


def write_batch_to_datastore(
    batch_df: DataFrame,
    _batch_id: str,
    data_store_url: str,
):
    batch_df.coalesce(1).write.jdbc(
        url=data_store_url, mode="append", table=FUND_POSITIONS
    )


def ingest_external_funds(
    spark_session: SparkSession,
    data_store_url: str,
    external_fund_src: str,
    checkpoint: str,
):
    dest_schema: StructType = spark_session.read.jdbc(
        url=data_store_url, table=FUND_POSITIONS
    ).schema
    non_derived_fields: List[StructField] = [
        field
        for field in dest_schema.fields
        if field.name not in {TIMESTAMP_COLUMN, MONTH_COLUMN, YEAR_COLUMN}
    ]
    src_schema = StructType(non_derived_fields)
    df = spark_session.readStream.csv(
        path=external_fund_src, header=True, schema=src_schema, inferSchema=False
    )
    df = df.withColumn(SRC_FILE_COLUMN, input_file_name())
    df = df.withColumn(FUND_NAME_COLUMN, parse_fund_name(col(SRC_FILE_COLUMN)))
    df = df.withColumn(
        TIMESTAMP_COLUMN, parse_fund_ingestion_timestamp(col(SRC_FILE_COLUMN))
    )
    df = df.withColumn(MONTH_COLUMN, from_unixtime(col(TIMESTAMP_COLUMN), "M"))
    df = df.withColumn(YEAR_COLUMN, from_unixtime(col(TIMESTAMP_COLUMN), "yyyy"))
    df = df.drop(SRC_FILE_COLUMN)
    query = (
        df.writeStream.foreachBatch(
            partial(write_batch_to_datastore, data_store_url=data_store_url)
        )
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
