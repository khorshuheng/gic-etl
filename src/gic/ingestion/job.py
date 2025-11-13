import sys

from pyspark.sql import SparkSession

from gic.ingestion.external_funds import (
    ingest_external_funds,
    Config as IngestionConfig,
)

import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--src-dir", required=True, type=str)
    parser.add_argument("-d", "--dest-url", required=True, type=str)
    parser.add_argument(
        "-j",
        "--jars",
        required=False,
        type=str,
        default="jars/sqlite-jdbc-3.51.0.0.jar",
    )
    return parser.parse_args()


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
        config = parse_args()
        spark_jars = config.jars
        spark_session = create_spark_session(spark_jars)
        ingest_external_funds(
            spark_session,
            IngestionConfig(
                src_dir=config.src_dir,
                dest_url=config.dest_url,
            ),
        )

    except Exception as e:
        print(f"Error while running job: {e}", file=sys.stderr)

    finally:
        if spark_session:
            spark_session.stop()
            print("Spark Session stopped.")


if __name__ == "__main__":
    main()
