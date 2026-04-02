"""Helpers shared by Phase 10 integration tests."""

from __future__ import annotations

from pathlib import Path

from atlas.types import IntrospectionResult
from tests.integration.phase_7.helpers import build_phase7_sqlite_fixture, introspect_phase7_sqlite


def build_phase10_result(db_path: Path) -> IntrospectionResult:
    """Create a deterministic introspection result with semantic enrichment hints."""

    build_phase7_sqlite_fixture(db_path)
    result = introspect_phase7_sqlite(db_path)

    customer_accounts = result.get_table("main", "customer_accounts")
    fact_orders = result.get_table("main", "fact_orders")
    order_items = result.get_table("main", "order_items")
    payment_history = result.get_table("main", "log_payment_history")
    config_settings = result.get_table("main", "config_settings")
    assert customer_accounts is not None
    assert fact_orders is not None
    assert order_items is not None
    assert payment_history is not None
    assert config_settings is not None

    customer_accounts.semantic_short = "Customer accounts"
    customer_accounts.semantic_detailed = "Master data for registered customer profiles and status."
    customer_accounts.semantic_domain = "crm"
    customer_accounts.semantic_role = "dimension"
    customer_accounts.semantic_confidence = 0.92
    customer_accounts.heuristic_type = "dimension"
    customer_accounts.heuristic_confidence = 0.91
    customer_accounts.relevance_score = 0.78

    fact_orders.semantic_short = "Customer orders"
    fact_orders.semantic_detailed = "Transactional order headers for checkout and order lifecycle."
    fact_orders.semantic_domain = "sales"
    fact_orders.semantic_role = "transaction_header"
    fact_orders.semantic_confidence = 0.96
    fact_orders.heuristic_type = "fact"
    fact_orders.heuristic_confidence = 0.94
    fact_orders.relevance_score = 0.93

    order_items.semantic_short = "Order line items"
    order_items.semantic_detailed = "Detailed product lines that belong to an order."
    order_items.semantic_domain = "sales"
    order_items.semantic_role = "transaction_line"
    order_items.semantic_confidence = 0.9
    order_items.heuristic_type = "bridge"
    order_items.heuristic_confidence = 0.86
    order_items.relevance_score = 0.71

    payment_history.semantic_short = "Payment history"
    payment_history.semantic_detailed = "Historical payment gateway events and lifecycle records."
    payment_history.semantic_domain = "billing"
    payment_history.semantic_role = "event_log"
    payment_history.semantic_confidence = 0.94
    payment_history.heuristic_type = "event"
    payment_history.heuristic_confidence = 0.92
    payment_history.relevance_score = 0.88

    config_settings.semantic_short = "Runtime settings"
    config_settings.semantic_detailed = "Application and billing configuration settings."
    config_settings.semantic_domain = "platform"
    config_settings.semantic_role = "configuration"
    config_settings.semantic_confidence = 0.78
    config_settings.heuristic_type = "config"
    config_settings.heuristic_confidence = 0.82
    config_settings.relevance_score = 0.55

    for table in result.all_tables():
        for column in table.columns:
            if column.name == "email_address":
                column.semantic_short = "Customer email address"
                column.semantic_detailed = "Primary email used to contact a customer."
                column.semantic_role = "customer_email"
                column.semantic_confidence = 0.95
            elif column.name == "payment_status":
                column.semantic_short = "Payment status"
                column.semantic_detailed = "Current billing settlement state of the order."
                column.semantic_role = "payment_status"
                column.semantic_confidence = 0.9
            elif column.name == "payment_event":
                column.semantic_short = "Payment event"
                column.semantic_detailed = "Gateway event describing the payment history step."
                column.semantic_role = "payment_event"
                column.semantic_confidence = 0.94
            elif column.name == "setting_key":
                column.semantic_short = "Configuration key"
                column.semantic_detailed = "Unique key for one platform setting."
                column.semantic_role = "config_key"
                column.semantic_confidence = 0.83

    return result
