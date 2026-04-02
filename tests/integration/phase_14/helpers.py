"""Shared helpers for Phase 14 public API and regression tests."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from atlas import AtlasConnectionConfig
from atlas.ai import AIConfig, LocalLLMClient
from atlas.ai.types import ModelInfo
from atlas.config import PrivacyMode


class DeterministicLocalClient(LocalLLMClient):
    """Deterministic local client used by Phase 14 regression tests."""

    def __init__(self) -> None:
        super().__init__(
            AIConfig(
                provider="openai_compatible",
                model="deterministic-test-model",
                base_url="http://127.0.0.1:1",
            )
        )
        self.generate_calls = 0

    def is_available(self) -> bool:
        return True

    def get_model_info(self) -> ModelInfo:
        return ModelInfo(
            provider_name="openai_compatible",
            model_name=self.config.model,
            is_local=True,
            version="test",
        )

    def generate(
        self,
        prompt: str,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> str:
        del max_tokens, temperature
        self.generate_calls += 1
        lowered = prompt.lower()
        if "search_terms" in lowered and "semantic_terms" in lowered:
            return json.dumps(
                {
                    "search_terms": ["customer", "orders"],
                    "semantic_terms": ["sales", "billing", "orders"],
                    "reasoning": "Customer orders live in the operational sales tables.",
                    "suggested_query": "SELECT * FROM main.orders LIMIT 20",
                }
            )
        if "table context" in lowered:
            if "table: main.orders" in lowered:
                return json.dumps(
                    {
                        "short_description": "Customer orders",
                        "detailed_description": "Operational sales orders placed by customers.",
                        "probable_domain": "sales",
                        "probable_role": "transaction ledger",
                        "confidence": 0.96,
                    }
                )
            if "table: main.customers" in lowered:
                return json.dumps(
                    {
                        "short_description": "Customer registry",
                        "detailed_description": "Primary customer master data used by orders and invoices.",
                        "probable_domain": "crm",
                        "probable_role": "reference entity",
                        "confidence": 0.92,
                    }
                )
            return json.dumps(
                {
                    "short_description": "Support table",
                    "detailed_description": "Supporting operational structure for the fictional bank fixture.",
                    "probable_domain": "operations",
                    "probable_role": "support entity",
                    "confidence": 0.75,
                }
            )
        if "column context" in lowered:
            if "column: email" in lowered:
                return json.dumps(
                    {
                        "short_description": "Customer email",
                        "detailed_description": "Electronic mail address for customer contact.",
                        "probable_role": "contact identifier",
                        "confidence": 0.93,
                    }
                )
            if "column: total_amount" in lowered:
                return json.dumps(
                    {
                        "short_description": "Order total",
                        "detailed_description": "Monetary amount charged for the order.",
                        "probable_role": "financial measure",
                        "confidence": 0.9,
                    }
                )
            return json.dumps(
                {
                    "short_description": "Support attribute",
                    "detailed_description": "Operational attribute inferred from sanitized metadata.",
                    "probable_role": "support attribute",
                    "confidence": 0.74,
                }
            )
        return json.dumps({"message": "ok"})


def create_phase14_sqlite_db(db_path: Path) -> Path:
    """Create a stable SQLite fixture for the Phase 14 public API tests."""

    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE customers (
                id INTEGER PRIMARY KEY,
                external_ref TEXT NOT NULL UNIQUE,
                full_name TEXT NOT NULL,
                email TEXT NOT NULL,
                segment TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL REFERENCES customers(id),
                account_number TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                opened_at TEXT NOT NULL
            );

            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL REFERENCES customers(id),
                account_id INTEGER REFERENCES accounts(id),
                status TEXT NOT NULL,
                total_amount NUMERIC NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE order_items (
                id INTEGER PRIMARY KEY,
                order_id INTEGER NOT NULL REFERENCES orders(id),
                sku TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price NUMERIC NOT NULL
            );

            CREATE TABLE invoices (
                id INTEGER PRIMARY KEY,
                order_id INTEGER NOT NULL REFERENCES orders(id),
                invoice_number TEXT NOT NULL UNIQUE,
                issued_at TEXT NOT NULL,
                paid_at TEXT
            );

            CREATE INDEX idx_accounts_customer ON accounts(customer_id);
            CREATE INDEX idx_orders_customer_status ON orders(customer_id, status);
            CREATE INDEX idx_order_items_order ON order_items(order_id);
            CREATE INDEX idx_invoices_order ON invoices(order_id);

            INSERT INTO customers (id, external_ref, full_name, email, segment, created_at) VALUES
                (1, 'CUST-001', 'Alice Walker', 'alice@example.com', 'retail', '2026-01-03T10:00:00Z'),
                (2, 'CUST-002', 'Bruno Lima', 'bruno@example.com', 'enterprise', '2026-01-04T12:15:00Z');

            INSERT INTO accounts (id, customer_id, account_number, status, opened_at) VALUES
                (10, 1, 'ACC-1001', 'active', '2026-01-03T10:05:00Z'),
                (11, 2, 'ACC-2001', 'active', '2026-01-04T12:20:00Z');

            INSERT INTO orders (id, customer_id, account_id, status, total_amount, created_at) VALUES
                (100, 1, 10, 'paid', 450.75, '2026-02-01T09:00:00Z'),
                (101, 1, 10, 'pending', 99.90, '2026-02-03T14:30:00Z'),
                (102, 2, 11, 'paid', 1200.00, '2026-02-04T16:00:00Z');

            INSERT INTO order_items (id, order_id, sku, quantity, unit_price) VALUES
                (1000, 100, 'CARD-GOLD', 1, 450.75),
                (1001, 101, 'INSURANCE-BASIC', 1, 99.90),
                (1002, 102, 'LOAN-SETUP', 2, 600.00);

            INSERT INTO invoices (id, order_id, invoice_number, issued_at, paid_at) VALUES
                (5000, 100, 'INV-100', '2026-02-01T09:05:00Z', '2026-02-01T09:10:00Z'),
                (5001, 102, 'INV-102', '2026-02-04T16:05:00Z', '2026-02-04T16:30:00Z');
            """
        )
        connection.commit()
    finally:
        connection.close()

    return db_path


def make_phase14_config(
    db_path: Path,
    *,
    privacy_mode: PrivacyMode = PrivacyMode.masked,
) -> AtlasConnectionConfig:
    """Return a stable SQLite Atlas config for Phase 14 tests."""

    return AtlasConnectionConfig.from_url(
        f"sqlite:///{db_path.resolve().as_posix()}",
        privacy_mode=privacy_mode,
    )
