import os
import subprocess
import tempfile

import pytest


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
    yield db_path
    os.remove(db_path)
