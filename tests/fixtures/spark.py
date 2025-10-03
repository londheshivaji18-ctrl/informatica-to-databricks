from __future__ import annotations

import os
import sys
from ctypes import create_unicode_buffer, windll
from pathlib import Path
from typing import Iterator

import pytest
from pyspark.sql import SparkSession


def _short_path(path: str) -> str:
    buf = create_unicode_buffer(260)
    result = windll.kernel32.GetShortPathNameW(path, buf, len(buf))  # type: ignore[attr-defined]
    if result:
        return buf.value
    return path


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    python_exec = _short_path(sys.executable)
    os.environ["PYSPARK_PYTHON"] = python_exec
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec
    local_dir = Path('tmp') / 'spark_local'
    local_dir.mkdir(parents=True, exist_ok=True)
    os.environ["PYSPARK_LOCAL_DIRS"] = str(local_dir.resolve())
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("unit-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.pyspark.python", python_exec)
        .config("spark.pyspark.driver.python", python_exec)
    )
    spark = builder.getOrCreate()
    yield spark
    spark.stop()
