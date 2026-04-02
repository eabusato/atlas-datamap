"""Helpers shared by Phase 7 integration tests."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from atlas.analysis.classifier import TableClassifier
from atlas.config import AtlasConnectionConfig
from atlas.connectors import get_connector
from atlas.introspection.runner import IntrospectionRunner
from atlas.types import IntrospectionResult


def sqlite_url(db_path: Path) -> str:
    """Build a SQLite URL for the provided local fixture path."""

    return f"sqlite:///{db_path.as_posix()}"


def build_phase7_sqlite_fixture(db_path: Path) -> None:
    """Create a deterministic SQLite fixture for search and reporting flows."""

    if db_path.exists():
        db_path.unlink()

    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE customer_accounts (
                id INTEGER PRIMARY KEY,
                email_address TEXT NOT NULL,
                account_status TEXT NOT NULL
            );

            CREATE TABLE fact_orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                total_amount NUMERIC NOT NULL,
                payment_status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (customer_id) REFERENCES customer_accounts(id)
            );

            CREATE TABLE order_items (
                id INTEGER PRIMARY KEY,
                order_id INTEGER NOT NULL,
                sku_code TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                FOREIGN KEY (order_id) REFERENCES fact_orders(id)
            );

            CREATE TABLE log_payment_history (
                id INTEGER PRIMARY KEY,
                order_id INTEGER NOT NULL,
                payment_event TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                FOREIGN KEY (order_id) REFERENCES fact_orders(id)
            );

            CREATE TABLE config_settings (
                id INTEGER PRIMARY KEY,
                setting_key TEXT NOT NULL,
                setting_value TEXT NOT NULL
            );

            INSERT INTO customer_accounts (id, email_address, account_status) VALUES
                (1, 'alice@example.com', 'active'),
                (2, 'bruno@example.com', 'active');

            INSERT INTO fact_orders (id, customer_id, total_amount, payment_status, created_at) VALUES
                (10, 1, 150.25, 'paid', '2026-01-01T10:00:00'),
                (11, 2, 80.00, 'pending', '2026-01-02T11:00:00');

            INSERT INTO order_items (id, order_id, sku_code, quantity) VALUES
                (100, 10, 'SKU-001', 2),
                (101, 11, 'SKU-002', 1);

            INSERT INTO log_payment_history (id, order_id, payment_event, occurred_at) VALUES
                (200, 10, 'gateway_authorized', '2026-01-01T10:01:00'),
                (201, 10, 'gateway_captured', '2026-01-01T10:02:00'),
                (202, 11, 'gateway_pending', '2026-01-02T11:01:00');

            INSERT INTO config_settings (id, setting_key, setting_value) VALUES
                (300, 'billing.currency', 'BRL'),
                (301, 'billing.retry_window', '30');
            """
        )
        connection.commit()
    finally:
        connection.close()


def introspect_phase7_sqlite(db_path: Path) -> IntrospectionResult:
    """Introspect and classify a Phase 7 SQLite fixture."""

    config = AtlasConnectionConfig.from_url(sqlite_url(db_path))
    connector = get_connector(config)
    result = IntrospectionRunner(config, connector).run()
    TableClassifier().classify_all(result)
    return result
