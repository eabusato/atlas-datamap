"""Structured export helpers for Atlas results."""

from __future__ import annotations

import csv
import json
from io import StringIO

from atlas.types import AtlasType, ColumnInfo, IntrospectionResult, TableInfo


class StructuredExporter:
    """Export Atlas metadata into JSON, CSV, and Markdown formats."""

    def __init__(
        self,
        result: IntrospectionResult,
        semantics: dict[str, object] | None = None,
    ) -> None:
        self._result = result
        self._semantics = semantics or {}
        tables_raw = self._semantics.get("tables", {})
        columns_raw = self._semantics.get("columns", {})
        self._table_semantics_map = (
            dict(tables_raw) if isinstance(tables_raw, dict) else {}
        )
        self._column_semantics_map = (
            dict(columns_raw) if isinstance(columns_raw, dict) else {}
        )

    def export_json(self) -> str:
        """Return a stable JSON export with injected semantic_data blocks."""

        payload = self._result.to_dict()
        for schema_payload in payload["schemas"]:
            assert isinstance(schema_payload, dict)
            tables_payload = schema_payload.get("tables", [])
            assert isinstance(tables_payload, list)
            for table_payload in tables_payload:
                assert isinstance(table_payload, dict)
                qualified_table = f"{table_payload['schema']}.{table_payload['name']}"
                table_semantics = self._table_semantics(qualified_table)
                if table_semantics:
                    table_payload["semantic_data"] = table_semantics
                columns_payload = table_payload.get("columns", [])
                assert isinstance(columns_payload, list)
                for column_payload in columns_payload:
                    assert isinstance(column_payload, dict)
                    qualified_column = f"{qualified_table}.{column_payload['name']}"
                    column_semantics = self._column_semantics(qualified_column)
                    if column_semantics:
                        column_payload["semantic_data"] = column_semantics
        return json.dumps(payload, ensure_ascii=False, indent=2)

    def export_csv_tables(self) -> str:
        """Return one row per table."""

        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerow(
            [
                "Schema",
                "Table",
                "Physical Type",
                "Estimated Rows",
                "Size Bytes",
                "Comment",
                "Semantic Summary",
                "Semantic Domain",
                "Semantic Role",
                "Semantic Confidence",
            ]
        )
        for table in self._iter_tables():
            semantics = self._table_semantics(table.qualified_name)
            writer.writerow(
                [
                    table.schema,
                    table.name,
                    table.table_type.value,
                    table.row_count_estimate,
                    table.size_bytes,
                    table.comment or "",
                    semantics.get("semantic_short", ""),
                    semantics.get("semantic_domain", ""),
                    semantics.get("semantic_role", ""),
                    semantics.get("semantic_confidence", ""),
                ]
            )
        return buffer.getvalue()

    def export_csv_columns(self) -> str:
        """Return one row per column."""

        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerow(
            [
                "Schema",
                "Table",
                "Column",
                "Native Type",
                "Canonical Type",
                "Nullable",
                "Primary Key",
                "Foreign Key",
                "Indexed",
                "Distinct Count",
                "Null Rate",
                "Comment",
                "Semantic Summary",
                "Semantic Role",
                "Semantic Confidence",
            ]
        )
        for table in self._iter_tables():
            for column in table.columns:
                semantics = self._column_semantics(f"{table.qualified_name}.{column.name}")
                writer.writerow(
                    [
                        table.schema,
                        table.name,
                        column.name,
                        column.native_type,
                        self._canonical_name(column),
                        str(column.is_nullable).lower(),
                        str(column.is_primary_key).lower(),
                        str(column.is_foreign_key).lower(),
                        str(column.is_indexed).lower(),
                        column.stats.distinct_count,
                        f"{column.stats.null_rate:.4f}",
                        column.comment or "",
                        semantics.get("semantic_short", ""),
                        semantics.get("semantic_role", ""),
                        semantics.get("semantic_confidence", ""),
                    ]
                )
        return buffer.getvalue()

    def export_markdown(self) -> str:
        """Return a human-readable Markdown data dictionary."""

        lines = [
            f"# Atlas Export: {self._result.database}",
            "",
            f"- Engine: `{self._result.engine}`",
            f"- Schemas: `{len(self._result.schemas)}`",
            f"- Tables: `{self._result.total_tables}`",
            f"- Columns: `{self._result.total_columns}`",
            "",
        ]
        for schema in sorted(self._result.schemas, key=lambda item: item.name):
            lines.extend(
                [
                    f"## Schema `{schema.name}`",
                    "",
                    f"Tables: `{len(schema.tables)}`",
                    "",
                ]
            )
            for table in sorted(schema.tables, key=lambda item: item.name):
                semantics = self._table_semantics(table.qualified_name)
                lines.extend(
                    [
                        f"### Table `{table.qualified_name}`",
                        "",
                        f"- Type: `{table.table_type.value}`",
                        f"- Estimated rows: `{table.row_count_estimate}`",
                        f"- Size bytes: `{table.size_bytes}`",
                        f"- Columns: `{len(table.columns)}`",
                    ]
                )
                if table.comment:
                    lines.append(f"- Comment: {table.comment}")
                if semantics.get("semantic_short"):
                    lines.append(f"- Semantic summary: {semantics['semantic_short']}")
                if semantics.get("semantic_domain"):
                    lines.append(f"- Semantic domain: `{semantics['semantic_domain']}`")
                if semantics.get("semantic_role"):
                    lines.append(f"- Semantic role: `{semantics['semantic_role']}`")
                lines.extend(
                    [
                        "",
                        "| Column | Native Type | Canonical Type | Nullable | PK | FK | Indexed | Comment | Semantic Summary | Semantic Role |",
                        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
                    ]
                )
                for column in table.columns:
                    column_semantics = self._column_semantics(f"{table.qualified_name}.{column.name}")
                    lines.append(
                        "| {name} | {native} | {canonical} | {nullable} | {pk} | {fk} | {indexed} | {comment} | {summary} | {role} |".format(
                            name=column.name,
                            native=column.native_type,
                            canonical=self._canonical_name(column),
                            nullable="yes" if column.is_nullable else "no",
                            pk="yes" if column.is_primary_key else "no",
                            fk="yes" if column.is_foreign_key else "no",
                            indexed="yes" if column.is_indexed else "no",
                            comment=column.comment or "",
                            summary=column_semantics.get("semantic_short", ""),
                            role=column_semantics.get("semantic_role", ""),
                        )
                    )
                lines.append("")
        return "\n".join(lines).rstrip() + "\n"

    def _iter_tables(self) -> list[TableInfo]:
        return sorted(self._result.all_tables(), key=lambda item: (item.schema, item.name))

    def _table_semantics(self, qualified_name: str) -> dict[str, object]:
        payload = self._table_semantics_map.get(qualified_name, {})
        if isinstance(payload, dict) and payload:
            return {
                key: value
                for key, value in payload.items()
                if value not in (None, "", 0.0)
            }
        table = next(
            (item for item in self._result.all_tables() if item.qualified_name == qualified_name),
            None,
        )
        if table is None:
            return {}
        return self._table_semantics_from_table(table)

    def _column_semantics(self, qualified_name: str) -> dict[str, object]:
        payload = self._column_semantics_map.get(qualified_name, {})
        if isinstance(payload, dict) and payload:
            return {
                key: value
                for key, value in payload.items()
                if value not in (None, "", 0.0)
            }
        column = self._find_column(qualified_name)
        if column is None:
            return {}
        return self._column_semantics_from_column(column)

    @staticmethod
    def _table_semantics_from_table(table: TableInfo) -> dict[str, object]:
        payload: dict[str, object] = {}
        if table.semantic_short:
            payload["semantic_short"] = table.semantic_short
        if table.semantic_detailed:
            payload["semantic_detailed"] = table.semantic_detailed
        if table.semantic_domain:
            payload["semantic_domain"] = table.semantic_domain
        if table.semantic_role:
            payload["semantic_role"] = table.semantic_role
        if table.semantic_confidence > 0.0:
            payload["semantic_confidence"] = table.semantic_confidence
        return payload

    @staticmethod
    def _column_semantics_from_column(column: ColumnInfo) -> dict[str, object]:
        payload: dict[str, object] = {}
        if column.semantic_short:
            payload["semantic_short"] = column.semantic_short
        if column.semantic_detailed:
            payload["semantic_detailed"] = column.semantic_detailed
        if column.semantic_role:
            payload["semantic_role"] = column.semantic_role
        if column.semantic_confidence > 0.0:
            payload["semantic_confidence"] = column.semantic_confidence
        return payload

    def _find_column(self, qualified_name: str) -> ColumnInfo | None:
        parts = qualified_name.split(".")
        if len(parts) != 3:
            return None
        schema_name, table_name, column_name = parts
        table = self._result.get_table(schema_name, table_name)
        if table is None:
            return None
        return next((column for column in table.columns if column.name == column_name), None)

    @staticmethod
    def _canonical_name(column: ColumnInfo) -> str:
        canonical = column.canonical_type
        if canonical is None:
            return ""
        if isinstance(canonical, AtlasType):
            return canonical.value
        return str(canonical)
