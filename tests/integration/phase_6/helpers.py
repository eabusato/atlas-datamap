"""Integration helpers for Phase 6 analysis heuristics."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from atlas.config import AtlasConnectionConfig
from atlas.connectors import get_connector
from atlas.introspection import IntrospectionRunner
from atlas.types import ColumnStats, IntrospectionResult


def build_analysis_sqlite_fixture(db_path: Path) -> None:
    if db_path.exists():
        db_path.unlink()
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE customers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                segment TEXT,
                created_at DATETIME NOT NULL
            );

            CREATE TABLE dim_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                category TEXT,
                sku TEXT NOT NULL UNIQUE
            );

            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                total NUMERIC NOT NULL,
                created_at DATETIME NOT NULL,
                FOREIGN KEY (customer_id) REFERENCES customers(id)
            );
            CREATE INDEX idx_orders_customer ON orders(customer_id);

            CREATE TABLE tags (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE order_tags (
                order_id INTEGER NOT NULL,
                tag_id INTEGER NOT NULL,
                created_at DATETIME,
                PRIMARY KEY (order_id, tag_id),
                FOREIGN KEY (order_id) REFERENCES orders(id),
                FOREIGN KEY (tag_id) REFERENCES tags(id)
            );

            CREATE TABLE audit_log (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                event_type TEXT NOT NULL,
                occurred_at DATETIME NOT NULL,
                processed_at DATETIME,
                payload TEXT,
                FOREIGN KEY (customer_id) REFERENCES customers(id)
            );

            CREATE TABLE customer_settings (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                setting_key TEXT NOT NULL,
                setting_value TEXT,
                description TEXT,
                FOREIGN KEY (customer_id) REFERENCES customers(id)
            );

            CREATE TABLE stg_orders_raw (
                id INTEGER PRIMARY KEY,
                batch_id TEXT,
                load_date DATETIME,
                customer_id INTEGER,
                total NUMERIC,
                payload_raw TEXT
            );

            CREATE TABLE fact_sales (
                id INTEGER PRIMARY KEY,
                order_id INTEGER NOT NULL,
                customer_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                total_amount NUMERIC NOT NULL,
                quantity INTEGER NOT NULL,
                occurred_at DATETIME NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders(id),
                FOREIGN KEY (customer_id) REFERENCES customers(id),
                FOREIGN KEY (product_id) REFERENCES dim_products(id)
            );
            CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id);
            CREATE INDEX idx_fact_sales_order ON fact_sales(order_id);

            CREATE TABLE unresolved_links (
                customer_id INTEGER,
                external_id INTEGER,
                value TEXT,
                notes TEXT
            );

            CREATE TABLE empty_archive (
                id INTEGER,
                info TEXT
            );

            CREATE TABLE wide_metrics (
                id INTEGER PRIMARY KEY,
                c01 TEXT, c02 TEXT, c03 TEXT, c04 TEXT, c05 TEXT, c06 TEXT, c07 TEXT, c08 TEXT, c09 TEXT, c10 TEXT,
                c11 TEXT, c12 TEXT, c13 TEXT, c14 TEXT, c15 TEXT, c16 TEXT, c17 TEXT, c18 TEXT, c19 TEXT, c20 TEXT,
                c21 TEXT, c22 TEXT, c23 TEXT, c24 TEXT, c25 TEXT, c26 TEXT, c27 TEXT, c28 TEXT, c29 TEXT, c30 TEXT,
                c31 TEXT, c32 TEXT, c33 TEXT, c34 TEXT, c35 TEXT, c36 TEXT, c37 TEXT, c38 TEXT, c39 TEXT, c40 TEXT,
                c41 TEXT, c42 TEXT, c43 TEXT, c44 TEXT, c45 TEXT, c46 TEXT, c47 TEXT, c48 TEXT, c49 TEXT, c50 TEXT,
                c51 TEXT, c52 TEXT
            );

            CREATE VIEW customer_emails AS
            SELECT id, email
            FROM customers;

            WITH RECURSIVE seq(x) AS (
                SELECT 1
                UNION ALL
                SELECT x + 1 FROM seq WHERE x < 600
            )
            INSERT INTO customers (id, name, email, segment, created_at)
            SELECT
                x,
                'Customer ' || x,
                'customer' || x || '@example.com',
                CASE WHEN x % 2 = 0 THEN 'enterprise' ELSE 'retail' END,
                '2024-01-01 00:00:00'
            FROM seq;

            WITH RECURSIVE seq(x) AS (
                SELECT 1
                UNION ALL
                SELECT x + 1 FROM seq WHERE x < 20
            )
            INSERT INTO dim_products (id, name, description, category, sku)
            SELECT x, 'Product ' || x, 'Desc ' || x, 'cat-' || ((x % 4) + 1), 'SKU-' || x
            FROM seq;

            INSERT INTO tags (id, name) VALUES (1, 'vip'), (2, 'new'), (3, 'fraud-watch');

            WITH RECURSIVE seq(x) AS (
                SELECT 1
                UNION ALL
                SELECT x + 1 FROM seq WHERE x < 200
            )
            INSERT INTO orders (id, customer_id, status, total, created_at)
            SELECT x, ((x - 1) % 600) + 1, 'paid', (x * 10.5), '2024-01-02 10:00:00'
            FROM seq;

            INSERT INTO order_tags (order_id, tag_id, created_at) VALUES
                (1, 1, '2024-01-02 10:00:00'),
                (1, 2, '2024-01-02 10:05:00'),
                (2, 3, '2024-01-03 11:00:00'),
                (3, 1, '2024-01-03 12:00:00'),
                (4, 2, '2024-01-03 13:00:00');

            WITH RECURSIVE seq(x) AS (
                SELECT 1
                UNION ALL
                SELECT x + 1 FROM seq WHERE x < 400
            )
            INSERT INTO audit_log (id, customer_id, action, event_type, occurred_at, processed_at, payload)
            SELECT
                x,
                ((x - 1) % 600) + 1,
                CASE WHEN x % 2 = 0 THEN 'login' ELSE 'logout' END,
                CASE WHEN x % 3 = 0 THEN 'security' ELSE 'session' END,
                '2024-01-04 09:00:00',
                '2024-01-04 09:05:00',
                '{"ordinal":' || x || '}'
            FROM seq;

            INSERT INTO customer_settings (id, customer_id, setting_key, setting_value, description) VALUES
                (1, 1, 'locale', 'pt-BR', 'Preferred language'),
                (2, 2, 'timezone', 'America/Sao_Paulo', 'Preferred timezone'),
                (3, 3, 'currency', 'BRL', 'Preferred currency'),
                (4, 4, 'notifications', 'enabled', 'Notification mode');

            INSERT INTO stg_orders_raw (id, batch_id, load_date, customer_id, total, payload_raw) VALUES
                (1, 'b1', '2024-01-05 10:00:00', 1, 10.5, '{}'),
                (2, 'b1', '2024-01-05 10:00:00', 2, 21.0, '{}'),
                (3, 'b2', '2024-01-06 10:00:00', 3, 31.5, '{}');

            WITH RECURSIVE seq(x) AS (
                SELECT 1
                UNION ALL
                SELECT x + 1 FROM seq WHERE x < 120
            )
            INSERT INTO fact_sales (id, order_id, customer_id, product_id, total_amount, quantity, occurred_at)
            SELECT
                x,
                ((x - 1) % 200) + 1,
                ((x - 1) % 600) + 1,
                ((x - 1) % 20) + 1,
                x * 15.75,
                (x % 5) + 1,
                '2024-01-07 14:00:00'
            FROM seq;

            INSERT INTO unresolved_links (customer_id, external_id, value, notes) VALUES
                (1, 9001, 'legacy', NULL),
                (2, 9002, 'legacy', 'check'),
                (NULL, 9003, 'legacy', NULL);

            INSERT INTO wide_metrics (id) VALUES (1);
            """
        )
        connection.commit()
    finally:
        connection.close()


def introspect_analysis_fixture(db_path: Path) -> IntrospectionResult:
    config = AtlasConnectionConfig.from_url(f"sqlite:///{db_path.as_posix()}")
    connector = get_connector(config)
    return IntrospectionRunner(config, connector).run()


def attach_fill_rate_stats(result: IntrospectionResult) -> None:
    customers = result.get_table("main", "customers")
    if customers is not None:
        for column in customers.columns:
            if column.name == "segment":
                column.stats = ColumnStats(row_count=600, null_count=120)
            else:
                column.stats = ColumnStats(row_count=600, null_count=0)
    unresolved = result.get_table("main", "unresolved_links")
    if unresolved is not None:
        for column in unresolved.columns:
            column.stats = ColumnStats(row_count=3, null_count=2 if column.name != "external_id" else 0)
