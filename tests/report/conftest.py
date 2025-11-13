import os
import sqlite3
import subprocess
import tempfile

import pytest


def insert_mock_fund_position_data(db_path: str):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        rows = [
            (
                "Equities",
                "TJX",
                "TJX Companies",
                80.0,
                12225.84,
                100.0,
                2970435.98,
                "Applebead",
                1669737600,
                2022,
                11,
            )
        ]

        cursor.executemany(
            """
        INSERT INTO fund_positions 
        (
            "FINANCIAL TYPE",
            "SYMBOL",
            "SECURITY NAME",
            "PRICE",
            "REALISED P/L",
            "QUANTITY",
            "MARKET VALUE",
            "FUND NAME",
            "TIMESTAMP",
            "YEAR",
            "MONTH"
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            rows,
        )


@pytest.fixture(scope="package")
def temp_sqlite_database_path(pytestconfig):
    fd, db_path = tempfile.mkstemp()
    os.close(fd)
    db_url = f"sqlite://{db_path}"
    try:
        subprocess.run(
            ["atlas", "migrate", "apply", "--url", db_url],
            check=True,
            capture_output=True,
            text=True,
            cwd=pytestconfig.rootpath.resolve(),
        )
    except subprocess.CalledProcessError as e:
        print("Atlas migration failed:\n", e.stdout, e.stderr)
        raise
    insert_mock_fund_position_data(db_path)
    yield db_path
    os.remove(db_path)
