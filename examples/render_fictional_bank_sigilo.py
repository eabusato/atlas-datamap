"""Render a rich fictional banking database sigilo to SVG."""

from __future__ import annotations

import sys
from pathlib import Path

from atlas.sigilo.datamap import DatamapSigiloBuilder
from atlas.types import (
    ColumnInfo,
    ForeignKeyInfo,
    IndexInfo,
    IntrospectionResult,
    SchemaInfo,
    TableInfo,
    TableType,
)


def _fk(
    source_schema: str,
    source_table: str,
    source_columns: list[str],
    target_schema: str,
    target_table: str,
    target_columns: list[str],
    *,
    name: str,
    on_delete: str | None = None,
    inferred: bool = False,
) -> ForeignKeyInfo:
    return ForeignKeyInfo(
        name=name,
        source_schema=source_schema,
        source_table=source_table,
        source_columns=source_columns,
        target_schema=target_schema,
        target_table=target_table,
        target_columns=target_columns,
        on_delete=on_delete,
        is_inferred=inferred,
    )


def _index(schema: str, table: str, name: str, columns: list[str], *, primary: bool = False) -> IndexInfo:
    return IndexInfo(name=name, table=table, schema=schema, columns=columns, is_primary=primary)


def build_result() -> IntrospectionResult:
    core_tables = [
        TableInfo(
            name="customers",
            schema="core",
            table_type=TableType.TABLE,
            row_count_estimate=180_000,
            size_bytes=52_428_800,
            comment="Retail and business customer registry.",
            columns=[
                ColumnInfo("id", "uuid", is_primary_key=True, is_nullable=False),
                ColumnInfo("segment", "varchar(32)", is_nullable=False),
                ColumnInfo("full_name", "varchar(160)", is_nullable=False),
                ColumnInfo("email", "varchar(255)", is_nullable=False),
                ColumnInfo("phone_number", "varchar(32)"),
                ColumnInfo("tax_document", "varchar(24)", is_nullable=False),
                ColumnInfo("risk_rating", "smallint", is_nullable=False),
                ColumnInfo("status", "varchar(16)", is_nullable=False),
                ColumnInfo("created_at", "timestamp", is_nullable=False),
            ],
            indexes=[
                _index("core", "customers", "customers_pkey", ["id"], primary=True),
                _index("core", "customers", "idx_customers_document", ["tax_document"]),
            ],
        ),
        TableInfo(
            name="branches",
            schema="core",
            table_type=TableType.TABLE,
            row_count_estimate=320,
            size_bytes=327_680,
            comment="Physical branch and service hub catalog.",
            columns=[
                ColumnInfo("id", "uuid", is_primary_key=True, is_nullable=False),
                ColumnInfo("region_code", "varchar(12)", is_nullable=False),
                ColumnInfo("display_name", "varchar(120)", is_nullable=False),
                ColumnInfo("manager_name", "varchar(120)"),
                ColumnInfo("opened_at", "date"),
                ColumnInfo("is_digital", "boolean", is_nullable=False),
            ],
            indexes=[_index("core", "branches", "branches_pkey", ["id"], primary=True)],
        ),
        TableInfo(
            name="accounts",
            schema="core",
            table_type=TableType.TABLE,
            row_count_estimate=420_000,
            size_bytes=124_780_544,
            comment="Current, savings and settlement accounts.",
            columns=[
                ColumnInfo("id", "uuid", is_primary_key=True, is_nullable=False),
                ColumnInfo("customer_id", "uuid", is_foreign_key=True, is_nullable=False),
                ColumnInfo("branch_id", "uuid", is_foreign_key=True, is_nullable=False),
                ColumnInfo("account_number", "varchar(24)", is_nullable=False),
                ColumnInfo("account_type", "varchar(20)", is_nullable=False),
                ColumnInfo("currency_code", "char(3)", is_nullable=False),
                ColumnInfo("available_balance", "numeric(18,2)", is_nullable=False),
                ColumnInfo("ledger_balance", "numeric(18,2)", is_nullable=False),
                ColumnInfo("status", "varchar(16)", is_nullable=False),
                ColumnInfo("opened_at", "timestamp", is_nullable=False),
                ColumnInfo("closed_at", "timestamp"),
            ],
            foreign_keys=[
                _fk("core", "accounts", ["customer_id"], "core", "customers", ["id"], name="fk_accounts_customer", on_delete="RESTRICT"),
                _fk("core", "accounts", ["branch_id"], "core", "branches", ["id"], name="fk_accounts_branch"),
            ],
            indexes=[
                _index("core", "accounts", "accounts_pkey", ["id"], primary=True),
                _index("core", "accounts", "idx_accounts_customer", ["customer_id"]),
                _index("core", "accounts", "idx_accounts_branch", ["branch_id"]),
            ],
        ),
        TableInfo(
            name="cards",
            schema="core",
            table_type=TableType.TABLE,
            row_count_estimate=260_000,
            size_bytes=83_886_080,
            comment="Debit and credit cards linked to accounts.",
            columns=[
                ColumnInfo("id", "uuid", is_primary_key=True, is_nullable=False),
                ColumnInfo("account_id", "uuid", is_foreign_key=True, is_nullable=False),
                ColumnInfo("customer_id", "uuid", is_foreign_key=True, is_nullable=False),
                ColumnInfo("masked_pan", "varchar(32)", is_nullable=False),
                ColumnInfo("brand", "varchar(24)", is_nullable=False),
                ColumnInfo("card_type", "varchar(16)", is_nullable=False),
                ColumnInfo("issued_at", "timestamp", is_nullable=False),
                ColumnInfo("expires_at", "date", is_nullable=False),
                ColumnInfo("status", "varchar(16)", is_nullable=False),
            ],
            foreign_keys=[
                _fk("core", "cards", ["account_id"], "core", "accounts", ["id"], name="fk_cards_account", on_delete="CASCADE"),
                _fk("core", "cards", ["customer_id"], "core", "customers", ["id"], name="fk_cards_customer", on_delete="CASCADE"),
            ],
            indexes=[
                _index("core", "cards", "cards_pkey", ["id"], primary=True),
                _index("core", "cards", "idx_cards_account", ["account_id"]),
            ],
        ),
    ]

    ledger_tables = [
        TableInfo(
            name="transactions",
            schema="ledger",
            table_type=TableType.TABLE,
            row_count_estimate=14_800_000,
            size_bytes=1_877_426_176,
            comment="Posted transactions emitted by all banking rails.",
            columns=[
                ColumnInfo("id", "uuid", is_primary_key=True, is_nullable=False),
                ColumnInfo("account_id", "uuid", is_foreign_key=True, is_nullable=False),
                ColumnInfo("card_id", "uuid", is_foreign_key=True),
                ColumnInfo("channel_code", "varchar(24)", is_nullable=False),
                ColumnInfo("direction", "varchar(8)", is_nullable=False),
                ColumnInfo("amount", "numeric(18,2)", is_nullable=False),
                ColumnInfo("currency_code", "char(3)", is_nullable=False),
                ColumnInfo("merchant_name", "varchar(140)"),
                ColumnInfo("reference_code", "varchar(48)", is_nullable=False),
                ColumnInfo("posted_at", "timestamp", is_nullable=False),
                ColumnInfo("settlement_day", "date"),
                ColumnInfo("status", "varchar(16)", is_nullable=False),
            ],
            foreign_keys=[
                _fk("ledger", "transactions", ["account_id"], "core", "accounts", ["id"], name="fk_transactions_account", on_delete="CASCADE"),
                _fk("ledger", "transactions", ["card_id"], "core", "cards", ["id"], name="fk_transactions_card"),
            ],
            indexes=[
                _index("ledger", "transactions", "transactions_pkey", ["id"], primary=True),
                _index("ledger", "transactions", "idx_transactions_account", ["account_id"]),
                _index("ledger", "transactions", "idx_transactions_posted_at", ["posted_at"]),
            ],
        ),
        TableInfo(
            name="transaction_splits",
            schema="ledger",
            table_type=TableType.TABLE,
            row_count_estimate=28_600_000,
            size_bytes=2_347_892_736,
            comment="Double-entry breakdown for each posted transaction.",
            columns=[
                ColumnInfo("id", "uuid", is_primary_key=True, is_nullable=False),
                ColumnInfo("transaction_id", "uuid", is_foreign_key=True, is_nullable=False),
                ColumnInfo("debit_account_id", "uuid", is_foreign_key=True, is_nullable=False),
                ColumnInfo("credit_account_id", "uuid", is_foreign_key=True, is_nullable=False),
                ColumnInfo("split_role", "varchar(16)", is_nullable=False),
                ColumnInfo("amount", "numeric(18,2)", is_nullable=False),
                ColumnInfo("book_code", "varchar(20)", is_nullable=False),
                ColumnInfo("created_at", "timestamp", is_nullable=False),
            ],
            foreign_keys=[
                _fk("ledger", "transaction_splits", ["transaction_id"], "ledger", "transactions", ["id"], name="fk_splits_transaction", on_delete="CASCADE"),
                _fk("ledger", "transaction_splits", ["debit_account_id"], "core", "accounts", ["id"], name="fk_splits_debit_account"),
                _fk("ledger", "transaction_splits", ["credit_account_id"], "core", "accounts", ["id"], name="fk_splits_credit_account"),
            ],
            indexes=[
                _index("ledger", "transaction_splits", "transaction_splits_pkey", ["id"], primary=True),
                _index("ledger", "transaction_splits", "idx_splits_transaction", ["transaction_id"]),
            ],
        ),
        TableInfo(
            name="loans",
            schema="ledger",
            table_type=TableType.TABLE,
            row_count_estimate=85_000,
            size_bytes=31_457_280,
            comment="Loan contracts and lifecycle state.",
            columns=[
                ColumnInfo("id", "uuid", is_primary_key=True, is_nullable=False),
                ColumnInfo("customer_id", "uuid", is_foreign_key=True, is_nullable=False),
                ColumnInfo("servicing_account_id", "uuid", is_foreign_key=True, is_nullable=False),
                ColumnInfo("principal_amount", "numeric(18,2)", is_nullable=False),
                ColumnInfo("interest_rate", "numeric(6,4)", is_nullable=False),
                ColumnInfo("installment_count", "integer", is_nullable=False),
                ColumnInfo("granted_at", "timestamp", is_nullable=False),
                ColumnInfo("maturity_day", "date", is_nullable=False),
                ColumnInfo("status", "varchar(16)", is_nullable=False),
            ],
            foreign_keys=[
                _fk("ledger", "loans", ["customer_id"], "core", "customers", ["id"], name="fk_loans_customer"),
                _fk("ledger", "loans", ["servicing_account_id"], "core", "accounts", ["id"], name="fk_loans_account"),
            ],
            indexes=[_index("ledger", "loans", "loans_pkey", ["id"], primary=True)],
        ),
    ]

    risk_tables = [
        TableInfo(
            name="fraud_alerts",
            schema="risk",
            table_type=TableType.TABLE,
            row_count_estimate=1_450_000,
            size_bytes=188_743_680,
            comment="Risk events and analyst triage records.",
            columns=[
                ColumnInfo("id", "uuid", is_primary_key=True, is_nullable=False),
                ColumnInfo("transaction_id", "uuid", is_foreign_key=True, is_nullable=False),
                ColumnInfo("account_id", "uuid", is_foreign_key=True, is_nullable=False),
                ColumnInfo("rule_code", "varchar(40)", is_nullable=False),
                ColumnInfo("severity", "varchar(12)", is_nullable=False),
                ColumnInfo("status", "varchar(16)", is_nullable=False),
                ColumnInfo("triggered_at", "timestamp", is_nullable=False),
                ColumnInfo("resolved_at", "timestamp"),
                ColumnInfo("assigned_team", "varchar(40)"),
            ],
            foreign_keys=[
                _fk("risk", "fraud_alerts", ["transaction_id"], "ledger", "transactions", ["id"], name="fk_alerts_transaction"),
                _fk("risk", "fraud_alerts", ["account_id"], "core", "accounts", ["id"], name="fk_alerts_account"),
            ],
            indexes=[
                _index("risk", "fraud_alerts", "fraud_alerts_pkey", ["id"], primary=True),
                _index("risk", "fraud_alerts", "idx_alerts_status", ["status"]),
            ],
        ),
        TableInfo(
            name="device_sessions",
            schema="risk",
            table_type=TableType.TABLE,
            row_count_estimate=3_200_000,
            size_bytes=442_499_072,
            comment="Session/device graph used for fraud correlation.",
            columns=[
                ColumnInfo("id", "uuid", is_primary_key=True, is_nullable=False),
                ColumnInfo("customer_id", "uuid", is_foreign_key=True, is_nullable=False),
                ColumnInfo("device_fingerprint", "varchar(128)", is_nullable=False),
                ColumnInfo("ip_address", "varchar(64)", is_nullable=False),
                ColumnInfo("user_agent_hash", "varchar(96)", is_nullable=False),
                ColumnInfo("first_seen_at", "timestamp", is_nullable=False),
                ColumnInfo("last_seen_at", "timestamp", is_nullable=False),
                ColumnInfo("trust_score", "numeric(5,2)", is_nullable=False),
            ],
            foreign_keys=[
                _fk("risk", "device_sessions", ["customer_id"], "core", "customers", ["id"], name="fk_sessions_customer"),
            ],
            indexes=[_index("risk", "device_sessions", "device_sessions_pkey", ["id"], primary=True)],
        ),
    ]

    reporting_tables = [
        TableInfo(
            name="daily_liquidity",
            schema="reporting",
            table_type=TableType.MATERIALIZED_VIEW,
            row_count_estimate=3_650,
            size_bytes=2_621_440,
            comment="Materialized daily liquidity aggregate.",
            columns=[
                ColumnInfo("day_bucket", "date", is_nullable=False),
                ColumnInfo("branch_id", "uuid", is_foreign_key=True, is_nullable=False),
                ColumnInfo("debits_total", "numeric(18,2)", is_nullable=False),
                ColumnInfo("credits_total", "numeric(18,2)", is_nullable=False),
                ColumnInfo("net_position", "numeric(18,2)", is_nullable=False),
            ],
            foreign_keys=[
                _fk("reporting", "daily_liquidity", ["branch_id"], "core", "branches", ["id"], name="fk_liquidity_branch", inferred=True),
            ],
        ),
        TableInfo(
            name="customer_360",
            schema="reporting",
            table_type=TableType.VIEW,
            row_count_estimate=180_000,
            size_bytes=16_777_216,
            comment="Unified customer view for service and growth teams.",
            columns=[
                ColumnInfo("customer_id", "uuid", is_nullable=False, is_foreign_key=True),
                ColumnInfo("segment", "varchar(32)", is_nullable=False),
                ColumnInfo("account_count", "integer", is_nullable=False),
                ColumnInfo("card_count", "integer", is_nullable=False),
                ColumnInfo("last_transaction_at", "timestamp"),
                ColumnInfo("open_alert_count", "integer", is_nullable=False),
            ],
            foreign_keys=[
                _fk("reporting", "customer_360", ["customer_id"], "core", "customers", ["id"], name="fk_customer_360_customer", inferred=True),
            ],
        ),
    ]

    return IntrospectionResult(
        database="aurora_bank",
        engine="postgresql",
        host="cluster-prd-01",
        schemas=[
            SchemaInfo(name="core", engine="postgresql", tables=core_tables),
            SchemaInfo(name="ledger", engine="postgresql", tables=ledger_tables),
            SchemaInfo(name="risk", engine="postgresql", tables=risk_tables),
            SchemaInfo(name="reporting", engine="postgresql", tables=reporting_tables),
        ],
    )


def render(output_path: Path) -> Path:
    svg = (
        DatamapSigiloBuilder(build_result())
        .set_style("network")
        .set_layout("circular")
        .build()
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(svg)
    return output_path


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    target = Path(args[0]) if args else Path(__file__).with_name("fictional_bank.system.svg")
    rendered = render(target)
    print(f"[atlas] wrote {rendered}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
