import sys

from pyspark.sql import SparkSession

from src.ingestion.external_funds import ingest_external_funds, Config as IngestionConfig

import argparse

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--src-dir", required=True, type=str)
    parser.add_argument("-d", "--dest-url", required=True, type=str)
    return parser.parse_args()


def create_spark_session() -> SparkSession:
    spark_session = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.jars", "jars/sqlite-jdbc-3.51.0.0.jar") \
        .config("spark.driver.extraClassPath", "jars/sqlite-jdbc-3.51.0.0.jar") \
        .getOrCreate()

    return spark_session

if __name__ == "__main__":
    spark_session = None
    try:
        spark_session = create_spark_session()
        config = parse_args()
        ingest_external_funds(spark_session, IngestionConfig(
            src_dir=config.src_dir,
            dest_url=config.dest_url,
        ))

    except Exception as e:
        print(f"Error while running job: {e}", file=sys.stderr)

    finally:
        if spark_session:
            spark_session.stop()
            print("Spark Session stopped.")