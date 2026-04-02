"""Persistent semantic cache for Atlas local AI enrichment."""

from __future__ import annotations

import hashlib
import json
import threading
from pathlib import Path
from typing import Any

from atlas.types import ColumnInfo, TableInfo

_CACHE_VERSION = 1
_CACHE_FILENAME = ".semantic_cache.json"


class SemanticCache:
    """JSON-backed semantic cache keyed by qualified table and column identities."""

    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir
        self.cache_file = cache_dir / _CACHE_FILENAME
        self._lock = threading.RLock()
        self._store: dict[str, Any] = {"version": _CACHE_VERSION, "tables": {}}
        self.load()

    def load(self) -> None:
        """Load cache contents from disk, resetting to empty if corrupted."""

        with self._lock:
            if not self.cache_file.exists():
                self._store = {"version": _CACHE_VERSION, "tables": {}}
                return
            try:
                payload = json.loads(self.cache_file.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                self._store = {"version": _CACHE_VERSION, "tables": {}}
                return
            if not isinstance(payload, dict):
                self._store = {"version": _CACHE_VERSION, "tables": {}}
                return
            tables = payload.get("tables")
            if not isinstance(tables, dict):
                tables = {}
            self._store = {
                "version": int(payload.get("version", _CACHE_VERSION)),
                "tables": tables,
            }

    def save(self) -> None:
        """Persist cache contents atomically to disk."""

        with self._lock:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            tmp_path = self.cache_file.with_suffix(f"{self.cache_file.suffix}.tmp")
            tmp_path.write_text(
                json.dumps(self._store, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            tmp_path.replace(self.cache_file)

    def build_table_signature(self, table: TableInfo) -> str:
        """Return a deterministic structural signature for a table."""

        parts = [table.schema, table.name]
        for column in sorted(table.columns, key=lambda item: item.name):
            parts.extend(
                [
                    column.name,
                    column.native_type,
                    str(bool(column.is_nullable)),
                    str(bool(column.is_primary_key)),
                    str(bool(column.is_foreign_key)),
                ]
            )
        return hashlib.md5("|".join(parts).encode("utf-8"), usedforsecurity=False).hexdigest()

    def build_column_signature(self, table: TableInfo, column: ColumnInfo) -> str:
        """Return a deterministic structural signature for a single column."""

        parts = [
            table.schema,
            table.name,
            column.name,
            column.native_type,
            str(bool(column.is_nullable)),
            str(bool(column.is_primary_key)),
            str(bool(column.is_foreign_key)),
        ]
        return hashlib.md5("|".join(parts).encode("utf-8"), usedforsecurity=False).hexdigest()

    def get_table_payload(self, table: TableInfo) -> dict[str, Any] | None:
        """Return cached table semantics when the signature is still valid."""

        with self._lock:
            record = self._table_record(table)
            if record is None:
                return None
            if record.get("signature") != self.build_table_signature(table):
                return None
            payload = record.get("table_payload")
            return payload if isinstance(payload, dict) else None

    def put_table_payload(self, table: TableInfo, payload: dict[str, Any]) -> None:
        """Store validated table semantics under the current structural signature."""

        with self._lock:
            signature = self.build_table_signature(table)
            qualified_name = table.qualified_name
            existing = self._store["tables"].get(qualified_name)
            columns: dict[str, Any] = {}
            if isinstance(existing, dict) and existing.get("signature") == signature:
                maybe_columns = existing.get("columns")
                if isinstance(maybe_columns, dict):
                    columns = maybe_columns
            self._store["tables"][qualified_name] = {
                "signature": signature,
                "table_payload": dict(payload),
                "columns": columns,
            }

    def get_column_payload(self, table: TableInfo, column: ColumnInfo) -> dict[str, Any] | None:
        """Return cached column semantics when both table and column signatures still match."""

        with self._lock:
            record = self._table_record(table)
            if record is None:
                return None
            if record.get("signature") != self.build_table_signature(table):
                return None
            columns = record.get("columns")
            if not isinstance(columns, dict):
                return None
            column_record = columns.get(column.name)
            if not isinstance(column_record, dict):
                return None
            if column_record.get("signature") != self.build_column_signature(table, column):
                return None
            payload = column_record.get("payload")
            return payload if isinstance(payload, dict) else None

    def put_column_payload(
        self,
        table: TableInfo,
        column: ColumnInfo,
        payload: dict[str, Any],
    ) -> None:
        """Store validated column semantics under the current table and column signatures."""

        with self._lock:
            qualified_name = table.qualified_name
            table_signature = self.build_table_signature(table)
            existing = self._store["tables"].get(qualified_name)
            if not isinstance(existing, dict) or existing.get("signature") != table_signature:
                existing = {
                    "signature": table_signature,
                    "table_payload": None,
                    "columns": {},
                }
            columns = existing.get("columns")
            if not isinstance(columns, dict):
                columns = {}
            columns[column.name] = {
                "signature": self.build_column_signature(table, column),
                "payload": dict(payload),
            }
            existing["columns"] = columns
            self._store["tables"][qualified_name] = existing

    def invalidate_table(self, table: TableInfo) -> None:
        """Drop every cached semantic payload for a table."""

        with self._lock:
            self._store["tables"].pop(table.qualified_name, None)

    def _table_record(self, table: TableInfo) -> dict[str, Any] | None:
        record = self._store["tables"].get(table.qualified_name)
        return record if isinstance(record, dict) else None
