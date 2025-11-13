import pytest
from pyspark.sql import SparkSession

import sqlite3

from gic.ingestion.external_funds import ingest_external_funds, Config


@pytest.mark.integration
def test_external_fund_ingestion(
    pytestconfig, temp_sqlite_database_path: str, spark_session: SparkSession
) -> None:
    root = pytestconfig.rootpath.resolve()
    src_dir = (root / "tests" / "data" / "external-funds").resolve()
    ingest_external_funds(
        spark_session,
        Config(
            src_dir=str(src_dir),
            dest_url=f"jdbc:sqlite:{temp_sqlite_database_path}",
        ),
    )
    with sqlite3.connect(temp_sqlite_database_path) as conn:
        cursor = conn.cursor()
        ingested_rows_count = cursor.execute(
            """
        SELECT COUNT(1) FROM fund_positions
        """
        ).fetchone()[0]
        assert ingested_rows_count == 2
