"""Build a complete end-to-end Atlas showcase with a complex fictional database."""

from __future__ import annotations

import argparse
import json
import random
import shutil
import sqlite3
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from atlas import Atlas, AtlasConnectionConfig, IntrospectionResult
from atlas.ai import AIConfig, AIConnectionError, LocalLLMClient, SemanticCache, build_client
from atlas.analysis import AnomalyDetector, TableClassifier, TableScorer
from atlas.export.diff import SnapshotDiff
from atlas.export.diff_report import SnapshotDiffReport
from atlas.export.report import HTMLReportGenerator
from atlas.export.report_executive import ExecutiveReportGenerator
from atlas.export.snapshot import AtlasSnapshot
from atlas.export.standalone import StandaloneHTMLBuilder
from atlas.export.structured import StructuredExporter
from atlas.history import AtlasHistory
from atlas.search import AtlasDiscovery, AtlasSearch
from atlas.sigilo.panel import PanelBuilder


def _json_dump(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _write_text(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _write_json(path: Path, payload: Any) -> Path:
    return _write_text(path, _json_dump(payload))


@dataclass(slots=True)
class ShowcasePaths:
    root: Path
    generated: Path
    databases: Path
    scans: Path
    semantic: Path
    exports: Path
    reports: Path
    history: Path
    diff: Path
    queries: Path


def _make_paths(root: Path) -> ShowcasePaths:
    generated = root / "generated"
    return ShowcasePaths(
        root=root,
        generated=generated,
        databases=generated / "databases",
        scans=generated / "scans",
        semantic=generated / "semantic",
        exports=generated / "exports",
        reports=generated / "reports",
        history=generated / "history",
        diff=generated / "diff",
        queries=generated / "queries",
    )


def _reset_sqlite_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def _create_showcase_schema(connection: sqlite3.Connection, *, include_chargebacks: bool, include_marketing_events: bool) -> None:
    connection.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE regions (
            id INTEGER PRIMARY KEY,
            region_code TEXT NOT NULL UNIQUE,
            region_name TEXT NOT NULL,
            currency_code TEXT NOT NULL
        );

        CREATE TABLE customer_segments (
            id INTEGER PRIMARY KEY,
            segment_code TEXT NOT NULL UNIQUE,
            segment_name TEXT NOT NULL,
            risk_profile TEXT NOT NULL
        );

        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            customer_number TEXT NOT NULL UNIQUE,
            full_name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            tax_id TEXT NOT NULL,
            segment_id INTEGER NOT NULL REFERENCES customer_segments(id),
            region_id INTEGER NOT NULL REFERENCES regions(id),
            status TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE customer_addresses (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES customers(id),
            address_type TEXT NOT NULL,
            street TEXT NOT NULL,
            city TEXT NOT NULL,
            state_code TEXT NOT NULL,
            postal_code TEXT NOT NULL,
            country_code TEXT NOT NULL
        );

        CREATE TABLE customer_preferences (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES customers(id),
            marketing_opt_in INTEGER NOT NULL,
            preferred_channel TEXT NOT NULL,
            preferred_language TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES customers(id),
            account_number TEXT NOT NULL UNIQUE,
            account_type TEXT NOT NULL,
            status TEXT NOT NULL,
            opened_at TEXT NOT NULL,
            risk_score REAL NOT NULL
        );

        CREATE TABLE account_limits (
            id INTEGER PRIMARY KEY,
            account_id INTEGER NOT NULL REFERENCES accounts(id),
            daily_limit NUMERIC NOT NULL,
            monthly_limit NUMERIC NOT NULL,
            currency_code TEXT NOT NULL,
            effective_at TEXT NOT NULL
        );

        CREATE TABLE cards (
            id INTEGER PRIMARY KEY,
            account_id INTEGER NOT NULL REFERENCES accounts(id),
            card_token TEXT NOT NULL UNIQUE,
            card_brand TEXT NOT NULL,
            card_status TEXT NOT NULL,
            issued_at TEXT NOT NULL
        );

        CREATE TABLE devices (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES customers(id),
            device_fingerprint TEXT NOT NULL UNIQUE,
            platform TEXT NOT NULL,
            trusted INTEGER NOT NULL,
            first_seen_at TEXT NOT NULL
        );

        CREATE TABLE sessions (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES customers(id),
            device_id INTEGER REFERENCES devices(id),
            session_token TEXT NOT NULL UNIQUE,
            started_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            status TEXT NOT NULL
        );

        CREATE TABLE merchants (
            id INTEGER PRIMARY KEY,
            merchant_code TEXT NOT NULL UNIQUE,
            merchant_name TEXT NOT NULL,
            category_code TEXT NOT NULL,
            region_id INTEGER NOT NULL REFERENCES regions(id),
            created_at TEXT NOT NULL
        );

        CREATE TABLE merchant_locations (
            id INTEGER PRIMARY KEY,
            merchant_id INTEGER NOT NULL REFERENCES merchants(id),
            location_code TEXT NOT NULL UNIQUE,
            city TEXT NOT NULL,
            state_code TEXT NOT NULL,
            country_code TEXT NOT NULL
        );

        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            sku TEXT NOT NULL UNIQUE,
            product_name TEXT NOT NULL,
            product_family TEXT NOT NULL,
            active INTEGER NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES customers(id),
            account_id INTEGER NOT NULL REFERENCES accounts(id),
            merchant_id INTEGER REFERENCES merchants(id),
            order_number TEXT NOT NULL UNIQUE,
            order_status TEXT NOT NULL,
            order_channel TEXT NOT NULL,
            total_amount NUMERIC NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE order_items (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL REFERENCES orders(id),
            product_id INTEGER NOT NULL REFERENCES products(id),
            quantity INTEGER NOT NULL,
            unit_price NUMERIC NOT NULL,
            discount_amount NUMERIC NOT NULL DEFAULT 0
        );

        CREATE TABLE invoices (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL REFERENCES orders(id),
            invoice_number TEXT NOT NULL UNIQUE,
            issued_at TEXT NOT NULL,
            due_at TEXT NOT NULL,
            invoice_status TEXT NOT NULL
        );

        CREATE TABLE invoice_items (
            id INTEGER PRIMARY KEY,
            invoice_id INTEGER NOT NULL REFERENCES invoices(id),
            order_item_id INTEGER NOT NULL REFERENCES order_items(id),
            line_amount NUMERIC NOT NULL,
            tax_amount NUMERIC NOT NULL
        );

        CREATE TABLE payments (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL REFERENCES orders(id),
            invoice_id INTEGER REFERENCES invoices(id),
            account_id INTEGER NOT NULL REFERENCES accounts(id),
            merchant_id INTEGER REFERENCES merchants(id),
            payment_reference TEXT NOT NULL UNIQUE,
            payment_method TEXT NOT NULL,
            payment_status TEXT NOT NULL,
            amount NUMERIC NOT NULL,
            settled_amount NUMERIC NOT NULL,
            created_at TEXT NOT NULL,
            settled_at TEXT
        );

        CREATE TABLE payment_events (
            id INTEGER PRIMARY KEY,
            payment_id INTEGER NOT NULL REFERENCES payments(id),
            event_type TEXT NOT NULL,
            event_status TEXT NOT NULL,
            gateway_code TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE refunds (
            id INTEGER PRIMARY KEY,
            payment_id INTEGER NOT NULL REFERENCES payments(id),
            refund_reference TEXT NOT NULL UNIQUE,
            amount NUMERIC NOT NULL,
            refund_status TEXT NOT NULL,
            requested_at TEXT NOT NULL,
            completed_at TEXT
        );

        CREATE TABLE ledger_accounts (
            id INTEGER PRIMARY KEY,
            ledger_code TEXT NOT NULL UNIQUE,
            ledger_name TEXT NOT NULL,
            ledger_type TEXT NOT NULL,
            currency_code TEXT NOT NULL
        );

        CREATE TABLE ledger_entries (
            id INTEGER PRIMARY KEY,
            payment_id INTEGER REFERENCES payments(id),
            refund_id INTEGER REFERENCES refunds(id),
            ledger_account_id INTEGER NOT NULL REFERENCES ledger_accounts(id),
            ledger_direction TEXT NOT NULL,
            amount NUMERIC NOT NULL,
            posted_at TEXT NOT NULL,
            narrative TEXT
        );

        CREATE TABLE risk_cases (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES customers(id),
            account_id INTEGER REFERENCES accounts(id),
            case_number TEXT NOT NULL UNIQUE,
            case_status TEXT NOT NULL,
            opened_at TEXT NOT NULL,
            resolved_at TEXT
        );

        CREATE TABLE risk_alerts (
            id INTEGER PRIMARY KEY,
            risk_case_id INTEGER NOT NULL REFERENCES risk_cases(id),
            payment_id INTEGER REFERENCES payments(id),
            severity TEXT NOT NULL,
            rule_name TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE compliance_reviews (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES customers(id),
            review_type TEXT NOT NULL,
            review_status TEXT NOT NULL,
            reviewed_at TEXT NOT NULL
        );

        CREATE TABLE support_tickets (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES customers(id),
            account_id INTEGER REFERENCES accounts(id),
            payment_id INTEGER REFERENCES payments(id),
            ticket_number TEXT NOT NULL UNIQUE,
            category TEXT NOT NULL,
            priority TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE support_messages (
            id INTEGER PRIMARY KEY,
            ticket_id INTEGER NOT NULL REFERENCES support_tickets(id),
            sender_role TEXT NOT NULL,
            message_body TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE audit_events (
            id INTEGER PRIMARY KEY,
            entity_type TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            actor TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE config_settings (
            id INTEGER PRIMARY KEY,
            setting_key TEXT NOT NULL UNIQUE,
            setting_value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE fx_rates (
            id INTEGER PRIMARY KEY,
            base_currency TEXT NOT NULL,
            quote_currency TEXT NOT NULL,
            rate NUMERIC NOT NULL,
            valid_at TEXT NOT NULL
        );

        CREATE TABLE daily_balances (
            id INTEGER PRIMARY KEY,
            account_id INTEGER NOT NULL REFERENCES accounts(id),
            balance_date TEXT NOT NULL,
            available_balance NUMERIC NOT NULL,
            ledger_balance NUMERIC NOT NULL,
            UNIQUE(account_id, balance_date)
        );

        CREATE TABLE raw_import_batches (
            id INTEGER PRIMARY KEY,
            source_name TEXT NOT NULL,
            file_name TEXT NOT NULL,
            loaded_at TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            raw_payload TEXT
        );

        CREATE VIEW vw_customer_value AS
        SELECT
            c.id AS customer_id,
            c.full_name,
            COUNT(DISTINCT o.id) AS order_count,
            COALESCE(SUM(p.amount), 0) AS total_paid
        FROM customers c
        LEFT JOIN orders o ON o.customer_id = c.id
        LEFT JOIN payments p ON p.order_id = o.id
        GROUP BY c.id, c.full_name;

        CREATE VIEW vw_payment_failures AS
        SELECT
            p.id AS payment_id,
            p.payment_reference,
            p.payment_status,
            pe.gateway_code,
            pe.created_at
        FROM payments p
        JOIN payment_events pe ON pe.payment_id = p.id
        WHERE p.payment_status IN ('failed', 'reversed');

        CREATE VIEW vw_support_pressure AS
        SELECT
            t.category,
            t.priority,
            COUNT(*) AS ticket_count
        FROM support_tickets t
        GROUP BY t.category, t.priority;
        """
    )
    if include_chargebacks:
        connection.executescript(
            """
            CREATE TABLE chargebacks (
                id INTEGER PRIMARY KEY,
                payment_id INTEGER NOT NULL REFERENCES payments(id),
                risk_case_id INTEGER REFERENCES risk_cases(id),
                chargeback_reference TEXT NOT NULL UNIQUE,
                dispute_reason TEXT NOT NULL,
                chargeback_status TEXT NOT NULL,
                amount NUMERIC NOT NULL,
                opened_at TEXT NOT NULL,
                resolved_at TEXT
            );

            CREATE VIEW vw_chargeback_exposure AS
            SELECT
                c.chargeback_status,
                COUNT(*) AS total_cases,
                SUM(c.amount) AS total_amount
            FROM chargebacks c
            GROUP BY c.chargeback_status;
            """
        )
    if include_marketing_events:
        connection.executescript(
            """
            CREATE TABLE marketing_events (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL REFERENCES customers(id),
                campaign_code TEXT NOT NULL,
                channel TEXT NOT NULL,
                event_status TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )

    connection.executescript(
        """
        CREATE INDEX idx_customers_segment ON customers(segment_id);
        CREATE INDEX idx_customers_region ON customers(region_id);
        CREATE INDEX idx_addresses_customer ON customer_addresses(customer_id);
        CREATE INDEX idx_preferences_customer ON customer_preferences(customer_id);
        CREATE INDEX idx_accounts_customer ON accounts(customer_id);
        CREATE INDEX idx_limits_account ON account_limits(account_id);
        CREATE INDEX idx_cards_account ON cards(account_id);
        CREATE INDEX idx_devices_customer ON devices(customer_id);
        CREATE INDEX idx_sessions_customer ON sessions(customer_id);
        CREATE INDEX idx_sessions_device ON sessions(device_id);
        CREATE INDEX idx_merchants_region ON merchants(region_id);
        CREATE INDEX idx_locations_merchant ON merchant_locations(merchant_id);
        CREATE INDEX idx_orders_customer ON orders(customer_id);
        CREATE INDEX idx_orders_account ON orders(account_id);
        CREATE INDEX idx_orders_merchant ON orders(merchant_id);
        CREATE INDEX idx_order_items_order ON order_items(order_id);
        CREATE INDEX idx_invoices_order ON invoices(order_id);
        CREATE INDEX idx_invoice_items_invoice ON invoice_items(invoice_id);
        CREATE INDEX idx_invoice_items_order_item ON invoice_items(order_item_id);
        CREATE INDEX idx_payments_order ON payments(order_id);
        CREATE INDEX idx_payments_account ON payments(account_id);
        CREATE INDEX idx_payment_events_payment ON payment_events(payment_id);
        CREATE INDEX idx_refunds_payment ON refunds(payment_id);
        CREATE INDEX idx_ledger_entries_payment ON ledger_entries(payment_id);
        CREATE INDEX idx_ledger_entries_refund ON ledger_entries(refund_id);
        CREATE INDEX idx_ledger_entries_account ON ledger_entries(ledger_account_id);
        CREATE INDEX idx_risk_cases_customer ON risk_cases(customer_id);
        CREATE INDEX idx_risk_alerts_case ON risk_alerts(risk_case_id);
        CREATE INDEX idx_compliance_reviews_customer ON compliance_reviews(customer_id);
        CREATE INDEX idx_support_tickets_customer ON support_tickets(customer_id);
        CREATE INDEX idx_support_messages_ticket ON support_messages(ticket_id);
        CREATE INDEX idx_daily_balances_account ON daily_balances(account_id);
        """
    )
    if include_chargebacks:
        connection.execute("CREATE INDEX idx_chargebacks_payment ON chargebacks(payment_id)")
    if include_marketing_events:
        connection.execute("CREATE INDEX idx_marketing_events_customer ON marketing_events(customer_id)")


def _populate_showcase_data(connection: sqlite3.Connection, *, include_chargebacks: bool, include_marketing_events: bool) -> None:
    rng = random.Random(20260402 + int(include_chargebacks) * 100)

    regions = [
        (1, "BR-SE", "Southeast Brazil", "BRL"),
        (2, "BR-S", "South Brazil", "BRL"),
        (3, "US-NE", "Northeast US", "USD"),
        (4, "EU-W", "Western Europe", "EUR"),
    ]
    connection.executemany(
        "INSERT INTO regions (id, region_code, region_name, currency_code) VALUES (?, ?, ?, ?)",
        regions,
    )

    segments = [
        (1, "retail", "Retail", "standard"),
        (2, "premium", "Premium", "elevated"),
        (3, "enterprise", "Enterprise", "managed"),
        (4, "partner", "Partner", "review"),
    ]
    connection.executemany(
        "INSERT INTO customer_segments (id, segment_code, segment_name, risk_profile) VALUES (?, ?, ?, ?)",
        segments,
    )

    for product_id in range(1, 16):
        family = ["card", "insurance", "loan", "cashback", "fx"][product_id % 5]
        connection.execute(
            """
            INSERT INTO products (id, sku, product_name, product_family, active, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                product_id,
                f"SKU-{product_id:04d}",
                f"{family.title()} Product {product_id}",
                family,
                1,
                f"2026-01-{(product_id % 28) + 1:02d}T08:00:00Z",
            ),
        )

    merchant_rows = []
    location_rows = []
    for merchant_id in range(1, 21):
        region_id = (merchant_id % len(regions)) + 1
        merchant_rows.append(
            (
                merchant_id,
                f"MER-{merchant_id:03d}",
                f"Merchant {merchant_id}",
                ["retail", "travel", "food", "subscription", "insurance"][merchant_id % 5],
                region_id,
                f"2026-01-{(merchant_id % 28) + 1:02d}T10:00:00Z",
            )
        )
        location_rows.append(
            (
                merchant_id,
                merchant_id,
                f"LOC-{merchant_id:03d}",
                ["Sao Paulo", "Rio de Janeiro", "Curitiba", "Boston", "Porto"][merchant_id % 5],
                ["SP", "RJ", "PR", "MA", "PT"][merchant_id % 5],
                ["BR", "BR", "BR", "US", "PT"][merchant_id % 5],
            )
        )
    connection.executemany(
        """
        INSERT INTO merchants (id, merchant_code, merchant_name, category_code, region_id, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        merchant_rows,
    )
    connection.executemany(
        """
        INSERT INTO merchant_locations (id, merchant_id, location_code, city, state_code, country_code)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        location_rows,
    )

    account_id = 1
    card_id = 1
    device_id = 1
    session_id = 1
    order_id = 1
    order_item_id = 1
    invoice_id = 1
    invoice_item_id = 1
    payment_id = 1
    payment_event_id = 1
    refund_id = 1
    ledger_entry_id = 1
    risk_case_id = 1
    risk_alert_id = 1
    compliance_id = 1
    support_ticket_id = 1
    support_message_id = 1
    marketing_event_id = 1
    chargeback_id = 1
    daily_balance_id = 1
    audit_event_id = 1

    ledger_accounts = [
        (1, "CASH", "Cash Settlement", "asset", "BRL"),
        (2, "REV", "Revenue", "revenue", "BRL"),
        (3, "FEE", "Fees", "revenue", "BRL"),
        (4, "REFUND", "Refund Liability", "liability", "BRL"),
        (5, "CBACK", "Chargeback Reserve", "liability", "BRL"),
    ]
    connection.executemany(
        """
        INSERT INTO ledger_accounts (id, ledger_code, ledger_name, ledger_type, currency_code)
        VALUES (?, ?, ?, ?, ?)
        """,
        ledger_accounts,
    )

    for customer_id in range(1, 91):
        region_id = (customer_id % len(regions)) + 1
        segment_id = (customer_id % len(segments)) + 1
        created_day = (customer_id % 28) + 1
        full_name = f"Customer {customer_id:03d}"
        customer_number = f"CUST-{customer_id:05d}"
        email = f"customer{customer_id:03d}@aurora.example"
        connection.execute(
            """
            INSERT INTO customers (
                id, customer_number, full_name, email, tax_id, segment_id, region_id, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                customer_id,
                customer_number,
                full_name,
                email,
                f"TAX-{customer_id:011d}",
                segment_id,
                region_id,
                "active" if customer_id % 9 != 0 else "under_review",
                f"2026-01-{created_day:02d}T09:00:00Z",
            ),
        )
        connection.execute(
            """
            INSERT INTO customer_addresses (
                customer_id, address_type, street, city, state_code, postal_code, country_code
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                customer_id,
                "home",
                f"{customer_id} Showcase Avenue",
                ["Sao Paulo", "Rio de Janeiro", "Curitiba", "Boston", "Lisbon"][customer_id % 5],
                ["SP", "RJ", "PR", "MA", "LS"][customer_id % 5],
                f"{10000 + customer_id:05d}",
                ["BR", "BR", "BR", "US", "PT"][customer_id % 5],
            ),
        )
        connection.execute(
            """
            INSERT INTO customer_preferences (
                customer_id, marketing_opt_in, preferred_channel, preferred_language, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                customer_id,
                1 if customer_id % 4 != 0 else 0,
                ["email", "sms", "push"][customer_id % 3],
                ["en", "pt-BR", "es"][customer_id % 3],
                f"2026-02-{(customer_id % 28) + 1:02d}T10:00:00Z",
            ),
        )

        for account_offset in range(2):
            this_account_id = account_id
            account_id += 1
            opened_day = ((customer_id + account_offset) % 28) + 1
            connection.execute(
                """
                INSERT INTO accounts (
                    id, customer_id, account_number, account_type, status, opened_at, risk_score
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    this_account_id,
                    customer_id,
                    f"ACC-{customer_id:05d}-{account_offset + 1}",
                    ["checking", "savings"][account_offset],
                    "active" if customer_id % 7 != 0 else "restricted",
                    f"2026-01-{opened_day:02d}T08:30:00Z",
                    round(0.15 + (customer_id % 10) * 0.08, 2),
                ),
            )
            connection.execute(
                """
                INSERT INTO account_limits (
                    account_id, daily_limit, monthly_limit, currency_code, effective_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    this_account_id,
                    5000 + customer_id * 10 + account_offset * 500,
                    25000 + customer_id * 25 + account_offset * 2500,
                    "BRL",
                    f"2026-02-{opened_day:02d}T09:00:00Z",
                ),
            )
            connection.execute(
                """
                INSERT INTO cards (
                    id, account_id, card_token, card_brand, card_status, issued_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    card_id,
                    this_account_id,
                    f"TOK-{this_account_id:06d}",
                    ["visa", "mastercard", "elo"][this_account_id % 3],
                    "active",
                    f"2026-01-{opened_day:02d}T10:00:00Z",
                ),
            )
            card_id += 1
            connection.execute(
                """
                INSERT INTO daily_balances (
                    id, account_id, balance_date, available_balance, ledger_balance
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    daily_balance_id,
                    this_account_id,
                    "2026-03-01",
                    round(1200 + customer_id * 37.5 + account_offset * 400, 2),
                    round(1180 + customer_id * 36.2 + account_offset * 390, 2),
                ),
            )
            daily_balance_id += 1

        connection.execute(
            """
            INSERT INTO devices (
                id, customer_id, device_fingerprint, platform, trusted, first_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                device_id,
                customer_id,
                f"DEV-{customer_id:05d}",
                ["ios", "android", "web"][customer_id % 3],
                1 if customer_id % 5 != 0 else 0,
                f"2026-02-{(customer_id % 28) + 1:02d}T11:00:00Z",
            ),
        )
        connection.execute(
            """
            INSERT INTO sessions (
                id, customer_id, device_id, session_token, started_at, expires_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_id,
                customer_id,
                device_id,
                f"SES-{customer_id:07d}",
                f"2026-03-{(customer_id % 28) + 1:02d}T12:00:00Z",
                f"2026-03-{(customer_id % 28) + 1:02d}T18:00:00Z",
                "expired" if customer_id % 6 == 0 else "closed",
            ),
        )
        device_id += 1
        session_id += 1

        if customer_id % 11 == 0:
            connection.execute(
                """
                INSERT INTO compliance_reviews (
                    id, customer_id, review_type, review_status, reviewed_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    compliance_id,
                    customer_id,
                    ["kyc_refresh", "sanctions", "pep_review"][customer_id % 3],
                    "completed" if customer_id % 22 != 0 else "pending",
                    f"2026-03-{(customer_id % 28) + 1:02d}T13:00:00Z",
                ),
            )
            compliance_id += 1

        if customer_id % 13 == 0:
            connection.execute(
                """
                INSERT INTO risk_cases (
                    id, customer_id, account_id, case_number, case_status, opened_at, resolved_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    risk_case_id,
                    customer_id,
                    (customer_id - 1) * 2 + 1,
                    f"RISK-{risk_case_id:05d}",
                    "open" if customer_id % 26 != 0 else "closed",
                    f"2026-03-{(customer_id % 28) + 1:02d}T15:00:00Z",
                    None if customer_id % 26 != 0 else f"2026-03-{(customer_id % 28) + 2:02d}T15:00:00Z",
                ),
            )
            case_payment_id = max(1, payment_id - 1)
            connection.execute(
                """
                INSERT INTO risk_alerts (
                    id, risk_case_id, payment_id, severity, rule_name, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    risk_alert_id,
                    risk_case_id,
                    case_payment_id,
                    ["medium", "high", "critical"][customer_id % 3],
                    ["velocity_spike", "merchant_mismatch", "device_reuse"][customer_id % 3],
                    f"2026-03-{(customer_id % 28) + 1:02d}T16:00:00Z",
                ),
            )
            risk_case_id += 1
            risk_alert_id += 1

        if include_marketing_events and customer_id % 3 == 0:
            connection.execute(
                """
                INSERT INTO marketing_events (
                    id, customer_id, campaign_code, channel, event_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    marketing_event_id,
                    customer_id,
                    f"CMP-{(customer_id % 9) + 1:02d}",
                    ["email", "push", "sms"][customer_id % 3],
                    "delivered" if customer_id % 6 != 0 else "clicked",
                    f"2026-02-{(customer_id % 28) + 1:02d}T17:00:00Z",
                ),
            )
            marketing_event_id += 1

        order_count = 1 + (customer_id % 4)
        for order_offset in range(order_count):
            this_order_id = order_id
            order_id += 1
            order_total = 0.0
            account_ref = (customer_id - 1) * 2 + 1 + (order_offset % 2)
            merchant_ref = ((customer_id + order_offset) % 20) + 1
            created_day = ((customer_id + order_offset * 2) % 28) + 1
            connection.execute(
                """
                INSERT INTO orders (
                    id, customer_id, account_id, merchant_id, order_number, order_status,
                    order_channel, total_amount, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    this_order_id,
                    customer_id,
                    account_ref,
                    merchant_ref,
                    f"ORD-{this_order_id:06d}",
                    ["paid", "pending", "fulfilled", "cancelled"][this_order_id % 4],
                    ["app", "web", "partner"][this_order_id % 3],
                    0,
                    f"2026-03-{created_day:02d}T09:30:00Z",
                ),
            )
            item_count = 1 + (this_order_id % 3)
            for item_offset in range(item_count):
                product_ref = ((this_order_id + item_offset) % 15) + 1
                quantity = 1 + (item_offset % 2)
                unit_price = round(49 + (product_ref * 13.75), 2)
                discount = 5.0 if this_order_id % 5 == 0 and item_offset == 0 else 0.0
                line_total = quantity * unit_price - discount
                order_total += line_total
                connection.execute(
                    """
                    INSERT INTO order_items (
                        id, order_id, product_id, quantity, unit_price, discount_amount
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        order_item_id,
                        this_order_id,
                        product_ref,
                        quantity,
                        unit_price,
                        discount,
                    ),
                )
                order_item_id += 1
            connection.execute(
                "UPDATE orders SET total_amount = ? WHERE id = ?",
                (round(order_total, 2), this_order_id),
            )

            this_invoice_id = invoice_id
            invoice_id += 1
            connection.execute(
                """
                INSERT INTO invoices (
                    id, order_id, invoice_number, issued_at, due_at, invoice_status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    this_invoice_id,
                    this_order_id,
                    f"INV-{this_invoice_id:06d}",
                    f"2026-03-{created_day:02d}T10:00:00Z",
                    f"2026-03-{((created_day + 5 - 1) % 28) + 1:02d}T10:00:00Z",
                    ["issued", "paid", "overdue"][this_invoice_id % 3],
                ),
            )
            order_item_rows = connection.execute(
                "SELECT id, quantity, unit_price, discount_amount FROM order_items WHERE order_id = ?",
                (this_order_id,),
            ).fetchall()
            for row in order_item_rows:
                line_amount = round(row[1] * row[2] - row[3], 2)
                tax_amount = round(line_amount * 0.08, 2)
                connection.execute(
                    """
                    INSERT INTO invoice_items (
                        id, invoice_id, order_item_id, line_amount, tax_amount
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        invoice_item_id,
                        this_invoice_id,
                        row[0],
                        line_amount,
                        tax_amount,
                    ),
                )
                invoice_item_id += 1

            status = ["authorized", "settled", "failed", "reversed"][this_order_id % 4]
            settled_amount = round(order_total if status in {"authorized", "settled"} else 0.0, 2)
            this_payment_id = payment_id
            payment_id += 1
            connection.execute(
                """
                INSERT INTO payments (
                    id, order_id, invoice_id, account_id, merchant_id, payment_reference,
                    payment_method, payment_status, amount, settled_amount, created_at, settled_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    this_payment_id,
                    this_order_id,
                    this_invoice_id,
                    account_ref,
                    merchant_ref,
                    f"PAY-{this_payment_id:07d}",
                    ["card", "pix", "bank_transfer"][this_payment_id % 3],
                    status,
                    round(order_total, 2),
                    settled_amount,
                    f"2026-03-{created_day:02d}T11:00:00Z",
                    None if status in {"failed", "reversed"} else f"2026-03-{created_day:02d}T11:05:00Z",
                ),
            )
            connection.execute(
                """
                INSERT INTO payment_events (
                    id, payment_id, event_type, event_status, gateway_code, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    payment_event_id,
                    this_payment_id,
                    "authorization",
                    status,
                    ["GATE-A", "GATE-B", "GATE-C"][this_payment_id % 3],
                    f"2026-03-{created_day:02d}T11:01:00Z",
                ),
            )
            payment_event_id += 1

            ledger_account_ref = 1 if status in {"authorized", "settled"} else 5
            for direction, amount in (("debit", round(order_total, 2)), ("credit", round(order_total, 2))):
                connection.execute(
                    """
                    INSERT INTO ledger_entries (
                        id, payment_id, refund_id, ledger_account_id, ledger_direction, amount, posted_at, narrative
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ledger_entry_id,
                        this_payment_id,
                        None,
                        ledger_account_ref if direction == "debit" else 2,
                        direction,
                        amount,
                        f"2026-03-{created_day:02d}T11:10:00Z",
                        f"Posting for payment {this_payment_id}",
                    ),
                )
                ledger_entry_id += 1

            if this_payment_id % 9 == 0:
                connection.execute(
                    """
                    INSERT INTO refunds (
                        id, payment_id, refund_reference, amount, refund_status, requested_at, completed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        refund_id,
                        this_payment_id,
                        f"REF-{refund_id:07d}",
                        round(order_total * 0.25, 2),
                        "completed",
                        f"2026-03-{created_day:02d}T13:00:00Z",
                        f"2026-03-{created_day:02d}T13:20:00Z",
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO ledger_entries (
                        id, payment_id, refund_id, ledger_account_id, ledger_direction, amount, posted_at, narrative
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ledger_entry_id,
                        None,
                        refund_id,
                        4,
                        "debit",
                        round(order_total * 0.25, 2),
                        f"2026-03-{created_day:02d}T13:25:00Z",
                        f"Refund posting {refund_id}",
                    ),
                )
                ledger_entry_id += 1
                refund_id += 1

            if include_chargebacks and this_payment_id % 17 == 0:
                open_case = risk_case_id
                connection.execute(
                    """
                    INSERT INTO risk_cases (
                        id, customer_id, account_id, case_number, case_status, opened_at, resolved_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        open_case,
                        customer_id,
                        account_ref,
                        f"RISK-{open_case:05d}",
                        "open",
                        f"2026-03-{created_day:02d}T14:00:00Z",
                        None,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO chargebacks (
                        id, payment_id, risk_case_id, chargeback_reference, dispute_reason,
                        chargeback_status, amount, opened_at, resolved_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        chargeback_id,
                        this_payment_id,
                        open_case,
                        f"CBK-{chargeback_id:07d}",
                        ["fraud", "duplicate", "service_not_received"][chargeback_id % 3],
                        "open" if chargeback_id % 2 else "resolved",
                        round(order_total * 0.4, 2),
                        f"2026-03-{created_day:02d}T14:10:00Z",
                        None if chargeback_id % 2 else f"2026-03-{created_day:02d}T18:00:00Z",
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO risk_alerts (
                        id, risk_case_id, payment_id, severity, rule_name, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        risk_alert_id,
                        open_case,
                        this_payment_id,
                        "critical",
                        "chargeback_detected",
                        f"2026-03-{created_day:02d}T14:15:00Z",
                    ),
                )
                risk_case_id += 1
                risk_alert_id += 1
                chargeback_id += 1

            if this_payment_id % 8 == 0:
                connection.execute(
                    """
                    INSERT INTO support_tickets (
                        id, customer_id, account_id, payment_id, ticket_number, category, priority, status, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        support_ticket_id,
                        customer_id,
                        account_ref,
                        this_payment_id,
                        f"TIC-{support_ticket_id:06d}",
                        ["payment", "account", "refund", "fraud"][support_ticket_id % 4],
                        ["low", "medium", "high"][support_ticket_id % 3],
                        "closed" if support_ticket_id % 2 else "open",
                        f"2026-03-{created_day:02d}T12:30:00Z",
                    ),
                )
                for sender_role in ("customer", "agent"):
                    connection.execute(
                        """
                        INSERT INTO support_messages (
                            id, ticket_id, sender_role, message_body, created_at
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            support_message_id,
                            support_ticket_id,
                            sender_role,
                            f"Showcase message {support_message_id} for ticket {support_ticket_id}",
                            f"2026-03-{created_day:02d}T12:{31 + support_message_id % 20:02d}:00Z",
                        ),
                    )
                    support_message_id += 1
                support_ticket_id += 1

            connection.execute(
                """
                INSERT INTO audit_events (
                    id, entity_type, entity_id, event_type, actor, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    audit_event_id,
                    "payment",
                    this_payment_id,
                    "payment_created",
                    "atlas_demo_seed",
                    f"2026-03-{created_day:02d}T11:02:00Z",
                ),
            )
            audit_event_id += 1

    config_rows = [
        ("feature.payments.retry_window", "48h", "2026-03-01T09:00:00Z"),
        ("feature.risk.auto_hold_threshold", "0.82", "2026-03-01T09:00:00Z"),
        ("feature.support.default_priority", "medium", "2026-03-01T09:00:00Z"),
        ("feature.fx.primary_source", "ecb", "2026-03-01T09:00:00Z"),
    ]
    connection.executemany(
        "INSERT INTO config_settings (setting_key, setting_value, updated_at) VALUES (?, ?, ?)",
        config_rows,
    )

    fx_rows = []
    fx_id = 1
    for base, quote, rate in (("BRL", "USD", 0.19), ("BRL", "EUR", 0.17), ("USD", "BRL", 5.25), ("EUR", "BRL", 5.85)):
        fx_rows.append((fx_id, base, quote, rate, "2026-03-01T00:00:00Z"))
        fx_id += 1
    connection.executemany(
        "INSERT INTO fx_rates (id, base_currency, quote_currency, rate, valid_at) VALUES (?, ?, ?, ?, ?)",
        fx_rows,
    )

    for batch_id in range(1, 7):
        raw_payload = None if batch_id % 2 == 0 else json.dumps({"batch": batch_id, "status": "loaded", "seed": rng.randint(1000, 9999)})
        connection.execute(
            """
            INSERT INTO raw_import_batches (
                id, source_name, file_name, loaded_at, row_count, raw_payload
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                batch_id,
                ["partner_api", "legacy_csv", "risk_feed"][batch_id % 3],
                f"batch_{batch_id:03d}.csv",
                f"2026-03-{batch_id:02d}T05:00:00Z",
                rng.randint(100, 900),
                raw_payload,
            ),
        )

    connection.commit()


def create_showcase_database(path: Path, *, version: str) -> Path:
    include_chargebacks = version == "v2"
    include_marketing_events = version == "v1"
    connection = _reset_sqlite_db(path)
    try:
        _create_showcase_schema(
            connection,
            include_chargebacks=include_chargebacks,
            include_marketing_events=include_marketing_events,
        )
        _populate_showcase_data(
            connection,
            include_chargebacks=include_chargebacks,
            include_marketing_events=include_marketing_events,
        )
    finally:
        connection.close()
    return path


def _config_text(db_path: Path) -> str:
    return "\n".join(
        [
            "[connection]",
            'engine = "sqlite"',
            f'database = "{db_path.resolve().as_posix()}"',
            "",
            "[analysis]",
            'privacy_mode = "masked"',
            "sample_limit = 12",
            "",
        ]
    )


def _ollama_config_text(
    db_path: Path,
    *,
    model: str,
    base_url: str,
) -> str:
    return "\n".join(
        [
            "[connection]",
            'engine = "sqlite"',
            f'database = "{db_path.resolve().as_posix()}"',
            "",
            "[analysis]",
            'privacy_mode = "masked"',
            "sample_limit = 12",
            "",
            "[ai]",
            'provider = "ollama"',
            f'model = "{model}"',
            f'base_url = "{base_url}"',
            "temperature = 0.1",
            "max_tokens = 300",
            "timeout_seconds = 60.0",
            "",
        ]
    )


def _prepare_result(result: IntrospectionResult) -> IntrospectionResult:
    TableClassifier().classify_all(result)
    TableScorer(result).score_all()
    return result


def _set_database_name(result: IntrospectionResult, name: str) -> IntrospectionResult:
    result.database = name
    return result


def _save_search_outputs(result: IntrospectionResult, output_dir: Path) -> dict[str, str]:
    search = AtlasSearch(result)
    discovery = AtlasDiscovery(result)
    search_hits = search.search_schema("chargeback payment dispute")
    info_table = result.get_table("main", "payments")
    discovery_result = discovery.find_likely_location("where are risk alerts tracked?")
    payload = {
        "search_payment_dispute.txt": "\n".join(
            f"[{item.entity_type.value}] {item.qualified_name} score={item.score:.1f} reason={item.reason}"
            for item in search_hits[:12]
        ),
        "discovery_risk_alerts.json": _json_dump(discovery_result.to_dict()),
        "info_payments.json": _json_dump(info_table.to_dict() if info_table is not None else {}),
    }
    for filename, content in payload.items():
        _write_text(output_dir / filename, content)
    return dict(payload)


def _save_reports(result: IntrospectionResult, reports_dir: Path, *, include_sigilo: bool) -> dict[str, Path]:
    health_report = reports_dir / "aurora_health_report.html"
    HTMLReportGenerator(result).generate(health_report, include_sigilo=include_sigilo)
    executive_report = reports_dir / "aurora_executive_report.html"
    ExecutiveReportGenerator(result).export(executive_report)
    return {
        "health_report": health_report,
        "executive_report": executive_report,
    }


def _save_structured_exports(
    result: IntrospectionResult,
    semantics: dict[str, object] | None,
    exports_dir: Path,
) -> dict[str, Path]:
    exporter = StructuredExporter(result, semantics=semantics)
    outputs = {
        "dictionary.json": exporter.export_json(),
        "tables.csv": exporter.export_csv_tables(),
        "columns.csv": exporter.export_csv_columns(),
        "dictionary.md": exporter.export_markdown(),
    }
    written: dict[str, Path] = {}
    for filename, payload in outputs.items():
        path = exports_dir / filename
        _write_text(path, payload)
        written[filename] = path
    return written


def _save_panel_html(svg_bytes: bytes, output_path: Path, *, db_name: str) -> Path:
    html = PanelBuilder(svg_bytes, db_name=db_name).build_html()
    return _write_text(output_path, html)


def _resolve_ollama_client(ai_config: AIConfig) -> LocalLLMClient:
    client = build_client(ai_config)
    if not client.is_available():
        raise RuntimeError(
            "Ollama is not reachable at the configured base_url. "
            "Start Ollama, pull the configured model, and rerun with --enable-ollama."
        )
    return client


def _enrich_result(
    atlas: Atlas,
    result: IntrospectionResult,
    cache_dir: Path,
    *,
    client: LocalLLMClient,
) -> IntrospectionResult:
    prepared = IntrospectionResult.from_dict(result.to_dict())
    return atlas.enrich(
        prepared,
        client=client,
        cache=SemanticCache(cache_dir),
        tables_only=False,
        parallel_workers=4,
        force=True,
    )


def _save_qa_outputs(
    atlas: Atlas,
    result: IntrospectionResult,
    output_dir: Path,
    *,
    client: LocalLLMClient,
) -> dict[str, Path]:
    question = "Where are payment disputes tracked?"
    answer = atlas.ask(result, question, client=client)
    answer_path = _write_json(output_dir / "ask_payment_disputes.json", answer.to_dict())
    human_text = "\n".join(
        [
            f"Question: {answer.question}",
            f"Reasoning: {answer.reasoning}",
            f"Confidence: {answer.confidence:.2f}",
            "",
            "Candidates:",
            *[
                f"- {candidate.qualified_name} (final={candidate.final_score:.2f}, structural={candidate.structural_score:.2f}, semantic={candidate.semantic_score:.2f}, heuristic={candidate.heuristic_score:.2f})"
                for candidate in answer.candidates
            ],
        ]
    )
    text_path = _write_text(output_dir / "ask_payment_disputes.txt", human_text)
    return {"json": answer_path, "text": text_path}


def _snapshot_with_analysis(result: IntrospectionResult, svg_text: str) -> AtlasSnapshot:
    scores = [item.to_dict() for item in TableScorer(result).score_all()]
    anomalies = [item.to_dict() for item in AnomalyDetector().detect(result)]
    return AtlasSnapshot.from_result(
        result,
        sigil_svg=svg_text,
        sigil_payload=result.to_json(indent=None),
        scores=scores,
        anomalies=anomalies,
    )


def _save_history_snapshots(history_dir: Path, snapshots: list[AtlasSnapshot]) -> list[Path]:
    history = AtlasHistory(history_dir)
    saved: list[Path] = []
    for index, snapshot in enumerate(snapshots):
        created_at = snapshot.manifest.created_at
        try:
            dt = datetime.fromisoformat(created_at.replace("Z", "+00:00")) + timedelta(seconds=index)
            snapshot.manifest.created_at = dt.astimezone(UTC).isoformat().replace("+00:00", "Z")
        except ValueError:
            pass
        name = history.build_snapshot_name(snapshot.manifest.database, snapshot.manifest.created_at)
        saved.append(snapshot.save(history_dir / name))
    listing = [
        {
            "file": path.name,
            "created_at": manifest.created_at,
            "database": manifest.database,
            "engine": manifest.engine,
        }
        for path, manifest in history.list_snapshots()
    ]
    _write_json(history_dir / "history_list.json", listing)
    return saved


def build_showcase(
    output_root: Path,
    *,
    enable_ollama: bool = False,
    ollama_model: str = "qwen2.5:1.5b",
    ollama_base_url: str = "http://127.0.0.1:11434",
) -> dict[str, object]:
    paths = _make_paths(output_root)
    if paths.generated.exists():
        shutil.rmtree(paths.generated)
    for directory in asdict(paths).values():
        if isinstance(directory, Path):
            directory.mkdir(parents=True, exist_ok=True)

    db_v1 = create_showcase_database(paths.databases / "aurora_demo_v1.db", version="v1")
    db_v2 = create_showcase_database(paths.databases / "aurora_demo_v2.db", version="v2")
    _write_text(paths.generated / "atlas.toml", _config_text(db_v1))
    _write_text(
        paths.generated / "atlas.ai.ollama.toml",
        _ollama_config_text(db_v1, model=ollama_model, base_url=ollama_base_url),
    )

    atlas_v1 = Atlas(AtlasConnectionConfig.from_url(f"sqlite:///{db_v1.resolve().as_posix()}"))
    atlas_v2 = Atlas(AtlasConnectionConfig.from_url(f"sqlite:///{db_v2.resolve().as_posix()}"))

    result_v1 = _set_database_name(_prepare_result(atlas_v1.scan()), "aurora_demo_v1")
    result_v2 = _set_database_name(_prepare_result(atlas_v2.scan()), "aurora_demo_v2")

    sigilo_v1 = atlas_v1.build_sigilo(result_v1, style="network", layout="circular")
    scan_artifacts = atlas_v1.save_scan_artifacts(result_v1, sigilo_v1, paths.scans, stem="aurora_demo_v1")
    panel_path = _save_panel_html(sigilo_v1.svg_bytes, paths.scans / "aurora_demo_v1_panel.html", db_name=result_v1.database)

    base_snapshot = _snapshot_with_analysis(result_v1, sigilo_v1.to_svg_text())
    base_snapshot_path = base_snapshot.save(paths.scans / "aurora_demo_v1")

    report_paths = _save_reports(result_v1, paths.reports, include_sigilo=True)
    standalone_path = StandaloneHTMLBuilder(
        sigilo_v1.to_svg_text(),
        db_name=result_v1.database,
        has_semantics=False,
        include_semantics=False,
    ).export(paths.exports / "aurora_demo_v1_standalone.html")
    export_paths = _save_structured_exports(result_v1, None, paths.exports)
    search_payloads = _save_search_outputs(result_v1, paths.queries)

    sigilo_v2 = atlas_v2.build_sigilo(result_v2, style="network", layout="circular")
    snapshot_v2 = _snapshot_with_analysis(result_v2, sigilo_v2.to_svg_text())
    snapshot_v2_path = snapshot_v2.save(paths.diff / "aurora_demo_v2")

    diff = SnapshotDiff.compare(base_snapshot, snapshot_v2)
    diff_report_path = SnapshotDiffReport().write(base_snapshot, snapshot_v2, diff, paths.diff / "aurora_demo_diff.html")

    semantic_outputs: dict[str, object] = {}
    history_snapshots: list[AtlasSnapshot] = [base_snapshot, snapshot_v2]
    if enable_ollama:
        ai_config = AIConfig(
            provider="ollama",
            model=ollama_model,
            base_url=ollama_base_url,
            temperature=0.1,
            max_tokens=300,
            timeout_seconds=60.0,
        )
        try:
            ollama_client = _resolve_ollama_client(ai_config)
        except AIConnectionError as exc:
            raise RuntimeError(
                "Atlas could not reach Ollama. Start Ollama, pull the configured model, "
                "and rerun with --enable-ollama."
            ) from exc

        enriched_result = _prepare_result(
            _enrich_result(atlas_v1, result_v1, paths.semantic / ".cache", client=ollama_client)
        )
        enriched_sigilo = atlas_v1.build_sigilo(enriched_result, style="network", layout="circular")
        semantic_artifacts = atlas_v1.save_scan_artifacts(
            enriched_result,
            enriched_sigilo,
            paths.semantic,
            stem="aurora_demo_v1_semantic",
        )
        semantic_snapshot = _snapshot_with_analysis(enriched_result, enriched_sigilo.to_svg_text())
        semantic_snapshot_path = semantic_snapshot.save(paths.semantic / "aurora_demo_v1_semantic")
        semantic_export_paths = _save_structured_exports(
            enriched_result,
            semantic_snapshot.semantics,
            paths.semantic / "exports",
        )
        semantic_standalone_path = StandaloneHTMLBuilder(
            enriched_sigilo.to_svg_text(),
            db_name=enriched_result.database,
            has_semantics=bool(semantic_snapshot.semantics),
            include_semantics=True,
        ).export(paths.semantic / "aurora_demo_v1_semantic_standalone.html")
        semantic_report = ExecutiveReportGenerator(
            enriched_result,
            scores=semantic_snapshot.scores,
            anomalies=semantic_snapshot.anomalies,
            semantics=semantic_snapshot.semantics,
        ).export(paths.semantic / "aurora_demo_v1_semantic_executive.html")
        qa_paths = _save_qa_outputs(atlas_v1, enriched_result, paths.semantic, client=ollama_client)
        history_snapshots.append(semantic_snapshot)
        semantic_outputs = {
            "snapshot": str(semantic_snapshot_path),
            "svg": str(semantic_artifacts.svg_path),
            "standalone_html": str(semantic_standalone_path),
            "executive_report": str(semantic_report),
            "export_outputs": {name: str(path) for name, path in semantic_export_paths.items()},
            "qa_outputs": {name: str(path) for name, path in qa_paths.items()},
            "model": ollama_model,
            "base_url": ollama_base_url,
        }

    history_saved = _save_history_snapshots(paths.history, history_snapshots)

    summary = {
        "database": result_v1.database,
        "engine": result_v1.engine,
        "counts": {
            "schemas": len(result_v1.schemas),
            "tables": result_v1.total_tables,
            "views": result_v1.total_views,
            "columns": result_v1.total_columns,
        },
        "outputs": {
            "scan_svg": str(scan_artifacts.svg_path),
            "scan_sigil": str(scan_artifacts.sigil_path),
            "scan_meta": str(scan_artifacts.meta_json_path),
            "scan_panel_html": str(panel_path),
            "scan_snapshot": str(base_snapshot_path),
            "standalone_html": str(standalone_path),
            "health_report": str(report_paths["health_report"]),
            "executive_report": str(report_paths["executive_report"]),
            "diff_snapshot_v2": str(snapshot_v2_path),
            "diff_report": str(diff_report_path),
            "history": [str(path) for path in history_saved],
        },
        "search_outputs": {name: str(paths.queries / name) for name in search_payloads},
        "export_outputs": {name: str(path) for name, path in export_paths.items()},
        "semantic_outputs": semantic_outputs,
        "notes": {
            "ollama_stage": (
                "Run this script again with --enable-ollama to generate semantic enrichment and QA "
                "artifacts through a real local Ollama model."
            ),
            "open_command": (
                "The showcase includes a ready-made panel HTML, but the atlas open command still "
                "serves the SVG over a local HTTP server."
            ),
            "recommended_ollama_model": (
                "The showcase defaults to qwen2.5:1.5b as a small, practical local model for "
                "structured metadata prompts on Apple Silicon."
            ),
        },
    }
    _write_json(paths.generated / "showcase_manifest.json", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="Showcase root directory. Generated assets are written under ./generated.",
    )
    parser.add_argument(
        "--enable-ollama",
        action="store_true",
        help="Generate semantic enrichment and QA artifacts through a real local Ollama model.",
    )
    parser.add_argument(
        "--ollama-model",
        default="qwen2.5:1.5b",
        help="Ollama model name used when --enable-ollama is set.",
    )
    parser.add_argument(
        "--ollama-base-url",
        default="http://127.0.0.1:11434",
        help="Ollama base URL used when --enable-ollama is set.",
    )
    args = parser.parse_args()
    try:
        manifest = build_showcase(
            args.output_dir.resolve(),
            enable_ollama=args.enable_ollama,
            ollama_model=args.ollama_model,
            ollama_base_url=args.ollama_base_url,
        )
    except Exception as exc:
        parser.exit(1, f"atlas showcase failed: {exc}\n")
    print(_json_dump(manifest))


if __name__ == "__main__":
    main()
