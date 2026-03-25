"""
Shared fixtures for multi-backend DQE testing.

Backends are organized into tiers:
  - Tier 1 (embedded):      duckdb, polars, pandas, datafusion, sqlite
  - Tier 2 (docker db):     postgres, mysql, clickhouse
  - Tier 3 (docker complex): trino, risingwave, pyspark

Tests auto-skip backends that aren't available (missing pip package or unreachable service).
"""

import pytest
import ibis
import pandas as pd
from typing import Any, Tuple, Optional

import dqe

# ── Standard test dataset ────────────────────────────────────────
TEST_DATA = {
    "id": [1, 2, 3, 4, 5],
    "age": [25.0, 30.0, 15.0, None, 99.0],
    "status": ["active", "inactive", "active", "pending", "active"],
    "email": ["test@example.com", "user@domain.org", "hello@world.net", "invalid", "valid@email.com"],
}

# ── Connection configs for Docker services ───────────────────────
DOCKER_CONFIGS = {
    "postgres": {
        "host": "localhost",
        "port": 5432,
        "user": "dqe",
        "password": "dqe_test",
        "database": "dqe_test",
    },
    "mysql": {
        "host": "localhost",
        "port": 3306,
        "user": "dqe",
        "password": "dqe_test",
        "database": "dqe_test",
    },
    "clickhouse": {
        "host": "localhost",
        "port": 8123,
        "user": "dqe",
        "password": "dqe_test",
    },
    "trino": {
        "host": "localhost",
        "port": 8080,
        "user": "dqe",
    },
    "risingwave": {
        "host": "localhost",
        "port": 4566,
        "user": "root",
        "database": "dev",
    },
}


# ── Backend setup functions ──────────────────────────────────────
# Each returns an Ibis connection with the test table already created.

def _setup_duckdb() -> Tuple[Any, str]:
    con = ibis.duckdb.connect()
    con.create_table("users", ibis.memtable(pd.DataFrame(TEST_DATA)))
    return con, "users"


def _setup_polars() -> Tuple[Any, str]:
    import polars as pl
    df = pl.DataFrame(TEST_DATA)
    con = ibis.polars.connect()
    con.create_table("users", df)
    return con, "users"


def _setup_pandas() -> Tuple[Any, str]:
    # Route through DuckDB (ibis 9.x+ removed native pandas backend)
    con = ibis.duckdb.connect()
    con.create_table("users", ibis.memtable(pd.DataFrame(TEST_DATA)))
    return con, "users"


def _setup_datafusion() -> Tuple[Any, str]:
    con = ibis.datafusion.connect()
    con.create_table("users", ibis.memtable(pd.DataFrame(TEST_DATA)))
    return con, "users"


def _setup_sqlite() -> Tuple[Any, str]:
    con = ibis.sqlite.connect()
    df = pd.DataFrame(TEST_DATA)
    # SQLite doesn't support create_table from memtable directly;
    # we use DuckDB to produce a pyarrow table, then insert via raw SQL path
    con.raw_sql("""
        CREATE TABLE users (
            id INTEGER,
            age REAL,
            status TEXT,
            email TEXT
        )
    """)
    for _, row in df.iterrows():
        age_val = "NULL" if pd.isna(row["age"]) else row["age"]
        con.raw_sql(
            f"INSERT INTO users VALUES ({row['id']}, {age_val}, '{row['status']}', '{row['email']}')"
        )
    return con, "users"


def _setup_pyspark() -> Tuple[Any, str]:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("dqe_test") \
        .master("local[*]") \
        .config("spark.driver.memory", "512m") \
        .getOrCreate()
    spark_df = spark.createDataFrame(pd.DataFrame(TEST_DATA))
    spark_df.createOrReplaceTempView("users")
    con = ibis.pyspark.connect(spark)
    return con, "users"


def _setup_postgres() -> Tuple[Any, str]:
    cfg = DOCKER_CONFIGS["postgres"]
    con = ibis.postgres.connect(**cfg)
    _seed_sql_backend(con, "users")
    return con, "users"


def _setup_mysql() -> Tuple[Any, str]:
    cfg = DOCKER_CONFIGS["mysql"]
    con = ibis.mysql.connect(**cfg)
    _seed_sql_backend(con, "users")
    return con, "users"


def _setup_clickhouse() -> Tuple[Any, str]:
    cfg = DOCKER_CONFIGS["clickhouse"]
    con = ibis.clickhouse.connect(**cfg)
    _seed_clickhouse(con, "users")
    return con, "users"


def _setup_trino() -> Tuple[Any, str]:
    cfg = DOCKER_CONFIGS["trino"]
    con = ibis.trino.connect(**cfg, catalog="memory", schema="default")
    _seed_trino(con, "users")
    return con, "users"


def _setup_risingwave() -> Tuple[Any, str]:
    cfg = DOCKER_CONFIGS["risingwave"]
    con = ibis.risingwave.connect(**cfg)
    _seed_sql_backend(con, "users")
    return con, "users"


# ── Data seeding helpers ─────────────────────────────────────────

def _seed_sql_backend(con, table_name: str):
    """Seed test data into a SQL backend (Postgres, MySQL, RisingWave)."""
    try:
        con.raw_sql(f"DROP TABLE IF EXISTS {table_name}")
    except Exception:
        pass
    con.raw_sql(f"""
        CREATE TABLE {table_name} (
            id INTEGER,
            age DOUBLE PRECISION,
            status VARCHAR(50),
            email VARCHAR(255)
        )
    """)
    df = pd.DataFrame(TEST_DATA)
    for _, row in df.iterrows():
        age_val = "NULL" if pd.isna(row["age"]) else row["age"]
        con.raw_sql(
            f"INSERT INTO {table_name} VALUES ({row['id']}, {age_val}, '{row['status']}', '{row['email']}')"
        )


def _seed_clickhouse(con, table_name: str):
    """Seed test data into ClickHouse (uses Nullable types and MergeTree engine)."""
    try:
        con.raw_sql(f"DROP TABLE IF EXISTS {table_name}")
    except Exception:
        pass
    con.raw_sql(f"""
        CREATE TABLE {table_name} (
            id Int32,
            age Nullable(Float64),
            status String,
            email String
        ) ENGINE = MergeTree() ORDER BY id
    """)
    df = pd.DataFrame(TEST_DATA)
    for _, row in df.iterrows():
        age_val = "NULL" if pd.isna(row["age"]) else row["age"]
        con.raw_sql(
            f"INSERT INTO {table_name} VALUES ({row['id']}, {age_val}, '{row['status']}', '{row['email']}')"
        )


def _seed_trino(con, table_name: str):
    """Seed test data into Trino memory catalog."""
    try:
        con.raw_sql(f"DROP TABLE IF EXISTS {table_name}")
    except Exception:
        pass
    con.raw_sql(f"""
        CREATE TABLE {table_name} (
            id INTEGER,
            age DOUBLE,
            status VARCHAR,
            email VARCHAR
        )
    """)
    df = pd.DataFrame(TEST_DATA)
    for _, row in df.iterrows():
        age_val = "NULL" if pd.isna(row["age"]) else row["age"]
        con.raw_sql(
            f"INSERT INTO {table_name} VALUES ({row['id']}, {age_val}, '{row['status']}', '{row['email']}')"
        )


# ── Backend registry ─────────────────────────────────────────────
# Maps backend name → (setup_fn, required_import, tier, pytest_marker)

BACKENDS = {
    # Tier 1: Embedded
    "duckdb":     (_setup_duckdb,     "duckdb",          "embedded"),
    "polars":     (_setup_polars,     "polars",          "embedded"),
    "pandas":     (_setup_pandas,     "pandas",          "embedded"),
    "datafusion": (_setup_datafusion, "datafusion",      "embedded"),
    "sqlite":     (_setup_sqlite,     "sqlite3",         "embedded"),
    # Tier 2: Docker DB
    "postgres":   (_setup_postgres,   "psycopg2",        "docker"),
    "mysql":      (_setup_mysql,      "pymysql",         "docker"),
    "clickhouse": (_setup_clickhouse, "clickhouse_connect", "docker"),
    # Tier 3: Docker Complex
    "trino":      (_setup_trino,      "trino",           "docker_complex"),
    "risingwave": (_setup_risingwave, "psycopg2",        "docker_complex"),
    "pyspark":    (_setup_pyspark,    "pyspark",         "docker_complex"),
}


def _try_setup_backend(name: str) -> Optional[Tuple[Any, str]]:
    """Attempt to set up a backend, returning None if it's unavailable."""
    setup_fn, required_import, tier = BACKENDS[name]

    # Check if the required Python package is installed
    try:
        __import__(required_import)
    except ImportError:
        return None

    # Attempt connection — catches Docker-not-running scenarios
    try:
        return setup_fn()
    except Exception:
        return None


# ── pytest fixtures ──────────────────────────────────────────────

def pytest_addoption(parser):
    parser.addoption(
        "--backend",
        action="append",
        default=None,
        help="Specific backend(s) to test (can be repeated). Default: all available.",
    )
    parser.addoption(
        "--tier",
        action="append",
        default=None,
        help="Backend tier(s) to test: embedded, docker, docker_complex. Can be repeated.",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "embedded: Tier 1 embedded backends (no Docker)")
    config.addinivalue_line("markers", "docker: Tier 2 Docker database backends")
    config.addinivalue_line("markers", "docker_complex: Tier 3 complex Docker backends")


def _get_requested_backends(config) -> list:
    """Determine which backends to test based on CLI options."""
    explicit = config.getoption("--backend")
    if explicit:
        return [b for b in explicit if b in BACKENDS]

    tiers = config.getoption("--tier")
    if tiers:
        tier_set = set(tiers)
        return [name for name, (_, _, tier) in BACKENDS.items() if tier in tier_set]

    # Default: all backends
    return list(BACKENDS.keys())


@pytest.fixture(params=list(BACKENDS.keys()))
def backend_table(request):
    """
    Parametrized fixture that yields (context, table, backend_name) for each backend.
    Automatically skips backends that are unavailable or not requested.
    """
    backend_name = request.param
    _, _, tier = BACKENDS[backend_name]

    # Check if this backend was requested via CLI options
    requested = _get_requested_backends(request.config)
    if backend_name not in requested:
        pytest.skip(f"Backend '{backend_name}' not in requested set")

    result = _try_setup_backend(backend_name)
    if result is None:
        pytest.skip(f"Backend '{backend_name}' is not available (missing package or unreachable service)")

    con, table_name = result
    context = dqe.Context()
    context.add_data_source("test_db", backend=backend_name, connection=con)
    table = context.get_table("test_db", table_name)

    return context, table, backend_name
