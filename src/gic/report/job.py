import sys

from pyspark.sql import SparkSession

from gic.config import parse_config
from gic.report.pricing import (
    generate_pricing_reconciliation_report,
    generate_top_performing_fund_report,
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
        generate_pricing_reconciliation_report(
            spark_session=spark_session,
            datastore_url=config.datastore.url,
            dest_path=config.report.pricing_reconciliation.dest,
        )
        generate_top_performing_fund_report(
            spark_session=spark_session,
            datastore_url=config.datastore.url,
            dest_path=config.report.top_performing_funds.dest,
        )

    except Exception as e:
        print(f"Error while running job: {e}", file=sys.stderr)

    finally:
        if spark_session:
            spark_session.stop()
            print("Spark Session stopped.")


if __name__ == "__main__":
    main()
