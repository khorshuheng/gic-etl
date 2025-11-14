import sys

from pyspark.sql import SparkSession

from gic.config import parse_config
from gic.ingestion.external_funds import (
    ingest_external_funds,
)


def create_spark_session(spark_jars: str) -> SparkSession:
    spark_session = (
        SparkSession.builder.master("local[*]")
        .config("spark.jars", spark_jars)
        .config("spark.driver.extraClassPath", spark_jars)
        .getOrCreate()
    )

    return spark_session


def main():
    spark_session = None
    try:
        config = parse_config()
        spark_jars = config.datastore.driver_jar_path
        spark_session = create_spark_session(spark_jars)
        ingest_external_funds(
            spark_session=spark_session,
            data_store_url=config.datastore.url,
            external_fund_src=config.ingestion.external_funds.src,
            checkpoint=config.ingestion.checkpoint,
        )

    except Exception as e:
        print(f"Error while running job: {e}", file=sys.stderr)

    finally:
        if spark_session:
            spark_session.stop()
            print("Spark Session stopped.")


if __name__ == "__main__":
    main()
