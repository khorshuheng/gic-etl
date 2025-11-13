import os
import tempfile

import pytest
from pyspark.sql import SparkSession


def pytest_addoption(parser):
    parser.addoption(
        "--sqlite-jdbc-path",
        action="store",
        help="path to sqlite jdbc driver",
    )


@pytest.fixture(scope="session")
def spark_session(pytestconfig):
    sqlite_jar_name = "sqlite-jdbc-3.51.0.0.jar"
    project_root = pytestconfig.rootpath.resolve()
    default_sqlite_jdbc_path = os.path.join(project_root, "jars", sqlite_jar_name)
    spark_jars_file_dir = (
        pytestconfig.getoption("--sqlite-jdbc-path") or default_sqlite_jdbc_path
    )

    spark_session = (
        SparkSession.builder.master("local[*]")
        .config("spark.jars", spark_jars_file_dir)
        .config("spark.driver.extraClassPath", spark_jars_file_dir)
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


@pytest.fixture()
def temp_dir():
    return tempfile.mkdtemp()
