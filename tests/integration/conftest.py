"""Integration fixtures for real database tests."""

from __future__ import annotations

import shutil
import subprocess
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from atlas.config import AtlasConnectionConfig, PrivacyMode
from atlas.connectors.mssql import MSSQLConnector
from atlas.connectors.mysql import MySQLConnector
from atlas.connectors.postgresql import PostgreSQLConnector


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--update-baseline",
        action="store_true",
        default=False,
        help="Rewrite approved SVG baselines.",
    )

POSTGRES_URL = "postgresql://atlas_test:atlas_test@localhost:5433/atlas_test"
MYSQL_URL = "mysql://atlas:atlas_pass@localhost:3307/atlas_test"
MARIADB_URL = "mysql://atlas:atlas_pass@localhost:3308/atlas_test"
MSSQL_URL = "mssql://sa:AtlasStrongPass%21123@127.0.0.1:1434/atlas_test"

POSTGRES_SETUP_SQL = """
CREATE SCHEMA IF NOT EXISTS atlas_test;
SET search_path = atlas_test;

DROP TABLE IF EXISTS empty_table CASCADE;
DROP TABLE IF EXISTS large_events CASCADE;
DROP TABLE IF EXISTS audit_log CASCADE;
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS categories CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_order_summary;
DROP VIEW IF EXISTS v_active_customers;

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    cpf VARCHAR(14),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    active BOOLEAN NOT NULL DEFAULT TRUE
);
COMMENT ON TABLE customers IS 'Customer registry';
COMMENT ON COLUMN customers.email IS 'Customer email address';

CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    slug VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    category_id INTEGER NOT NULL REFERENCES categories(id),
    name VARCHAR(255) NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE products IS 'Product catalog';
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_products_price ON products(price);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    total NUMERIC(12,2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    notes TEXT
);
COMMENT ON TABLE orders IS 'Customer orders';
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_customer_status ON orders(customer_id, status);
CREATE INDEX idx_orders_status_partial ON orders(status) WHERE status <> 'cancelled';

CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products(id),
    quantity INTEGER NOT NULL DEFAULT 1,
    unit_price NUMERIC(10,2) NOT NULL
);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);

CREATE TABLE audit_log (
    id BIGSERIAL PRIMARY KEY,
    customer_id INTEGER,
    action VARCHAR(50) NOT NULL,
    payload JSONB,
    occurred_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE empty_table (
    id SERIAL PRIMARY KEY,
    placeholder TEXT
);

CREATE TABLE large_events (
    id BIGSERIAL PRIMARY KEY,
    customer_id INTEGER,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB,
    occurred_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE VIEW v_active_customers AS
SELECT id, name, email
FROM customers
WHERE active = TRUE;

CREATE MATERIALIZED VIEW mv_order_summary AS
SELECT DATE_TRUNC('day', created_at) AS day_bucket,
       COUNT(*) AS total_orders,
       SUM(total) AS total_amount
FROM orders
GROUP BY 1;

INSERT INTO categories (name, slug) VALUES
    ('Electronics', 'electronics'),
    ('Apparel', 'apparel');

INSERT INTO customers (name, email, cpf) VALUES
    ('Alice', 'alice@example.com', '123.456.789-00'),
    ('Bruno', 'bruno@example.com', NULL),
    ('Carla', 'carla@example.com', '987.654.321-00');

INSERT INTO products (category_id, name, price, metadata) VALUES
    (1, 'Notebook Pro', 4500.00, '{"tier":"premium"}'),
    (1, 'Mouse USB', 89.90, '{"wireless":false}'),
    (2, 'T-Shirt M', 59.90, '{"size":"M"}');

INSERT INTO orders (customer_id, status, total, notes) VALUES
    (1, 'paid', 4589.90, 'priority'),
    (1, 'pending', 89.90, NULL),
    (2, 'shipped', 4500.00, 'gift wrap'),
    (3, 'cancelled', 59.90, NULL);

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 4500.00),
    (1, 2, 1, 89.90),
    (2, 2, 1, 89.90),
    (3, 1, 1, 4500.00),
    (4, 3, 1, 59.90);

INSERT INTO audit_log (customer_id, action, payload) VALUES
    (1, 'login', '{"ip":"127.0.0.1"}'),
    (2, 'logout', '{"ip":"127.0.0.2"}');

INSERT INTO large_events (customer_id, event_type, payload)
SELECT ((gs - 1) % 3) + 1,
       CASE WHEN gs % 2 = 0 THEN 'page_view' ELSE 'checkout' END,
       jsonb_build_object('ordinal', gs)
FROM generate_series(1, 15000) AS gs;

REFRESH MATERIALIZED VIEW mv_order_summary;
ANALYZE atlas_test.customers;
ANALYZE atlas_test.products;
ANALYZE atlas_test.orders;
ANALYZE atlas_test.order_items;
ANALYZE atlas_test.audit_log;
ANALYZE atlas_test.large_events;
"""


def _run_compose(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", "-f", str(cwd / "tests" / "integration" / "docker-compose.yml"), *args],
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
    )


@pytest.fixture(scope="session")
def integration_root(repo_root: Path, tests_tmp_root: Path) -> Path:
    target = tests_tmp_root / "phase_1_integration"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)
    return repo_root


@pytest.fixture(scope="session")
def postgres_service(integration_root: Path) -> Generator[None, None, None]:
    result = _run_compose("up", "-d", "postgres", cwd=integration_root)
    if result.returncode != 0:
        pytest.skip(f"Unable to start PostgreSQL docker service: {result.stderr.strip()}")
    try:
        yield
    finally:
        _run_compose("down", "-v", cwd=integration_root)


@pytest.fixture(scope="session")
def pg_psycopg2(postgres_service):
    try:
        import psycopg2  # type: ignore[import-not-found]
    except ImportError:
        pytest.skip("psycopg2 is not installed in the active environment.")
    return psycopg2


@pytest.fixture(scope="session")
def pg_test_db(pg_psycopg2) -> Generator[None, None, None]:
    last_error: Exception | None = None
    for _ in range(30):
        try:
            connection = pg_psycopg2.connect(
                host="localhost",
                port=5433,
                dbname="atlas_test",
                user="atlas_test",
                password="atlas_test",
            )
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(POSTGRES_SETUP_SQL)
            connection.close()
            break
        except Exception as exc:  # pragma: no cover - only on environment timing issues
            last_error = exc
            time.sleep(1)
    else:
        pytest.skip(f"PostgreSQL docker service did not become ready: {last_error}")

    yield


@pytest.fixture(scope="session")
def pg_config(pg_test_db) -> AtlasConnectionConfig:
    return AtlasConnectionConfig.from_url(POSTGRES_URL, privacy_mode=PrivacyMode.normal)


@pytest.fixture(scope="session")
def pg_config_masked(pg_test_db) -> AtlasConnectionConfig:
    return AtlasConnectionConfig.from_url(POSTGRES_URL, privacy_mode=PrivacyMode.masked)


@pytest.fixture(scope="session")
def pg_config_stats_only(pg_test_db) -> AtlasConnectionConfig:
    return AtlasConnectionConfig.from_url(POSTGRES_URL, privacy_mode=PrivacyMode.stats_only)


@pytest.fixture(scope="session")
def pg_config_no_samples(pg_test_db) -> AtlasConnectionConfig:
    return AtlasConnectionConfig.from_url(POSTGRES_URL, privacy_mode=PrivacyMode.no_samples)


@pytest.fixture()
def pg_connector(pg_config: AtlasConnectionConfig) -> Generator[PostgreSQLConnector, None, None]:
    connector = PostgreSQLConnector(pg_config)
    connector.connect()
    try:
        yield connector
    finally:
        connector.disconnect()


@pytest.fixture()
def pg_connector_masked(
    pg_config_masked: AtlasConnectionConfig,
) -> Generator[PostgreSQLConnector, None, None]:
    connector = PostgreSQLConnector(pg_config_masked)
    connector.connect()
    try:
        yield connector
    finally:
        connector.disconnect()


@pytest.fixture()
def pg_connector_stats_only(
    pg_config_stats_only: AtlasConnectionConfig,
) -> Generator[PostgreSQLConnector, None, None]:
    connector = PostgreSQLConnector(pg_config_stats_only)
    connector.connect()
    try:
        yield connector
    finally:
        connector.disconnect()


@pytest.fixture()
def pg_connector_no_samples(
    pg_config_no_samples: AtlasConnectionConfig,
) -> Generator[PostgreSQLConnector, None, None]:
    connector = PostgreSQLConnector(pg_config_no_samples)
    connector.connect()
    try:
        yield connector
    finally:
        connector.disconnect()


@pytest.fixture(scope="session")
def mysql_service(integration_root: Path) -> Generator[None, None, None]:
    result = _run_compose("up", "-d", "mysql", cwd=integration_root)
    if result.returncode != 0:
        pytest.skip(f"Unable to start MySQL docker service: {result.stderr.strip()}")
    try:
        yield
    finally:
        _run_compose("down", "-v", cwd=integration_root)


@pytest.fixture(scope="session")
def mariadb_service(integration_root: Path) -> Generator[None, None, None]:
    result = _run_compose("up", "-d", "mariadb", cwd=integration_root)
    if result.returncode != 0:
        pytest.skip(f"Unable to start MariaDB docker service: {result.stderr.strip()}")
    try:
        yield
    finally:
        _run_compose("down", "-v", cwd=integration_root)


@pytest.fixture(scope="session")
def mysql_driver(mysql_service):
    try:
        import mysql.connector  # type: ignore[import-untyped]
    except ImportError:
        pytest.skip("mysql-connector-python is not installed in the active environment.")
    return mysql.connector


def _wait_for_mysql_ready(
    mysql_connector: Any,
    *,
    port: int,
) -> None:
    last_error: Exception | None = None
    for _ in range(40):
        try:
            connection = mysql_connector.connect(
                host="127.0.0.1",
                port=port,
                user="atlas",
                password="atlas_pass",
                database="atlas_test",
                autocommit=True,
            )
            connection.close()
            return
        except Exception as exc:  # pragma: no cover - environment timing only
            last_error = exc
            time.sleep(1)
    pytest.skip(f"MySQL service on port {port} did not become ready: {last_error}")


@pytest.fixture(scope="session")
def mysql_test_db(mysql_driver) -> Generator[None, None, None]:
    _wait_for_mysql_ready(mysql_driver, port=3307)
    yield


@pytest.fixture(scope="session")
def mariadb_test_db(mysql_driver, mariadb_service) -> Generator[None, None, None]:
    _wait_for_mysql_ready(mysql_driver, port=3308)
    yield


@pytest.fixture(scope="session")
def mysql_config(mysql_test_db) -> AtlasConnectionConfig:
    return AtlasConnectionConfig.from_url(MYSQL_URL, privacy_mode=PrivacyMode.normal)


@pytest.fixture(scope="session")
def mysql_config_masked(mysql_test_db) -> AtlasConnectionConfig:
    return AtlasConnectionConfig.from_url(MYSQL_URL, privacy_mode=PrivacyMode.masked)


@pytest.fixture(scope="session")
def mariadb_config(mariadb_test_db) -> AtlasConnectionConfig:
    return AtlasConnectionConfig.from_url(MARIADB_URL, privacy_mode=PrivacyMode.normal)


@pytest.fixture()
def mysql_connector(mysql_config: AtlasConnectionConfig) -> Generator[MySQLConnector, None, None]:
    connector = MySQLConnector(mysql_config)
    connector.connect()
    try:
        yield connector
    finally:
        connector.disconnect()


@pytest.fixture()
def mysql_connector_masked(
    mysql_config_masked: AtlasConnectionConfig,
) -> Generator[MySQLConnector, None, None]:
    connector = MySQLConnector(mysql_config_masked)
    connector.connect()
    try:
        yield connector
    finally:
        connector.disconnect()


@pytest.fixture()
def mariadb_connector(
    mariadb_config: AtlasConnectionConfig,
) -> Generator[MySQLConnector, None, None]:
    connector = MySQLConnector(mariadb_config)
    connector.connect()
    try:
        yield connector
    finally:
        connector.disconnect()


@pytest.fixture(scope="session")
def mssql_service(integration_root: Path) -> Generator[None, None, None]:
    result = _run_compose("up", "-d", "mssql", cwd=integration_root)
    if result.returncode != 0:
        pytest.skip(f"Unable to start SQL Server docker service: {result.stderr.strip()}")
    try:
        yield
    finally:
        _run_compose("down", "-v", cwd=integration_root)


@pytest.fixture(scope="session")
def mssql_driver(mssql_service):
    try:
        import pyodbc
    except ImportError:
        pytest.skip("pyodbc is not installed in the active environment.")
    return pyodbc


def _build_mssql_test_connection_string(*, database: str) -> str:
    return (
        "DRIVER=/opt/homebrew/lib/libtdsodbc.so;"
        "SERVER=127.0.0.1;"
        "PORT=1434;"
        f"DATABASE={database};"
        "UID=sa;"
        "PWD=AtlasStrongPass!123;"
        "TDS_Version=7.4;"
        "ClientCharset=UTF-8;"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=5"
    )


def _execute_mssql_script(connection: Any, script_path: Path) -> None:
    content = script_path.read_text(encoding="utf-8")
    batches: list[str] = []
    current_lines: list[str] = []
    for line in content.splitlines():
        if line.strip().upper() == "GO":
            if current_lines:
                batches.append("\n".join(current_lines).strip())
                current_lines = []
            continue
        current_lines.append(line)
    if current_lines:
        batches.append("\n".join(current_lines).strip())

    cursor = connection.cursor()
    try:
        for batch in batches:
            if batch:
                cursor.execute(batch)
        if not bool(getattr(connection, "autocommit", False)):
            connection.commit()
    finally:
        cursor.close()


@pytest.fixture(scope="session")
def mssql_test_db(mssql_driver, integration_root: Path) -> Generator[None, None, None]:
    last_error: Exception | None = None
    for _ in range(60):
        try:
            master_connection = mssql_driver.connect(
                _build_mssql_test_connection_string(database="master"),
                autocommit=True,
            )
            master_connection.close()
            break
        except Exception as exc:  # pragma: no cover - environment timing only
            last_error = exc
            time.sleep(2)
    else:
        pytest.skip(f"SQL Server docker service did not become ready: {last_error}")

    setup_connection = mssql_driver.connect(
        _build_mssql_test_connection_string(database="master"),
        autocommit=True,
    )
    try:
        _execute_mssql_script(
            setup_connection,
            integration_root / "tests" / "integration" / "mssql_setup.sql",
        )
    except Exception as exc:
        setup_connection.close()
        pytest.skip(f"Unable to initialize SQL Server test schema: {exc}")
    setup_connection.close()
    yield


@pytest.fixture(scope="session")
def mssql_config(mssql_test_db) -> AtlasConnectionConfig:
    return AtlasConnectionConfig.from_url(
        MSSQL_URL,
        privacy_mode=PrivacyMode.normal,
        connect_args={"driver": "/opt/homebrew/lib/libtdsodbc.so"},
    )


@pytest.fixture(scope="session")
def mssql_config_masked(mssql_test_db) -> AtlasConnectionConfig:
    return AtlasConnectionConfig.from_url(
        MSSQL_URL,
        privacy_mode=PrivacyMode.masked,
        connect_args={"driver": "/opt/homebrew/lib/libtdsodbc.so"},
    )


@pytest.fixture()
def mssql_connector(
    mssql_config: AtlasConnectionConfig,
) -> Generator[MSSQLConnector, None, None]:
    connector = MSSQLConnector(mssql_config)
    connector.connect()
    try:
        yield connector
    finally:
        connector.disconnect()


@pytest.fixture()
def mssql_connector_masked(
    mssql_config_masked: AtlasConnectionConfig,
) -> Generator[MSSQLConnector, None, None]:
    connector = MSSQLConnector(mssql_config_masked)
    connector.connect()
    try:
        yield connector
    finally:
        connector.disconnect()
