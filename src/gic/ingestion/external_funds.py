import dataclasses
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import input_file_name, udf
from pyspark.sql.types import StructType, StructField, StringType

EXTERNAL_FUNDS = "external_funds"
INGESTION_DATE_COLUMN = "ingestion_date"


@dataclasses.dataclass
class Config:
    src_dir: str
    dest_url: str


@udf(returnType=StringType())
def derive_ingestion_date_from_filename(filename: str) -> str:
    return filename


def ingest_external_funds(spark_session: SparkSession, config: Config):
    dest_schema: StructType = spark_session.read.jdbc(
        url=config.dest_url, table=EXTERNAL_FUNDS
    ).schema
    non_derived_fields: List[StructField] = [
        field
        for field in dest_schema.fields
        if field.name not in {INGESTION_DATE_COLUMN}
    ]
    src_schema = StructType(non_derived_fields)
    df = spark_session.read.csv(path=config.src_dir, header=True, schema=src_schema)
    df.withColumn("src_file", input_file_name())
    df.withColumn(
        INGESTION_DATE_COLUMN,
    )
    df = derive_fund_name_from_filename(df)
    df.write.jdbc(url=config.dest_url, mode="append", table=EXTERNAL_FUNDS)
