"""Prompt execution, JSON extraction, and semantic metadata mutation."""

from __future__ import annotations

import contextlib
import json
import re
import threading
import time
from collections.abc import Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Literal

from atlas.ai.cache import SemanticCache
from atlas.ai.client import LocalLLMClient
from atlas.ai.prompts import COLUMN_PROMPT_TEMPLATE, TABLE_PROMPT_TEMPLATE
from atlas.ai.sampler import SamplePreparer
from atlas.ai.types import AIConnectionError, AIGenerationError, AITimeoutError
from atlas.config import PrivacyMode
from atlas.connectors.base import BaseConnector
from atlas.types import ColumnInfo, SchemaInfo, TableInfo

_MARKDOWN_JSON_PATTERN = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL)


class ResponseParser:
    """Extract and decode JSON payloads from imperfect LLM responses."""

    @staticmethod
    def extract_json(raw_text: str) -> dict[str, Any]:
        text = raw_text.strip()
        if not text:
            raise AIGenerationError("AI returned an empty response.")

        try:
            decoded = json.loads(text)
        except json.JSONDecodeError:
            decoded = None
        if isinstance(decoded, dict):
            return decoded

        markdown_match = _MARKDOWN_JSON_PATTERN.search(text)
        if markdown_match is not None:
            candidate = markdown_match.group(1)
            try:
                decoded_markdown = json.loads(candidate)
            except json.JSONDecodeError:
                decoded_markdown = None
            if isinstance(decoded_markdown, dict):
                return decoded_markdown

        balanced = ResponseParser._extract_balanced_json_object(text)
        if balanced is not None:
            try:
                decoded_balanced = json.loads(balanced)
            except json.JSONDecodeError:
                decoded_balanced = None
            if isinstance(decoded_balanced, dict):
                return decoded_balanced

        snippet = text[:100].replace("\n", " ")
        raise AIGenerationError(f"Failed to extract valid JSON from AI output: {snippet!r}")

    @staticmethod
    def _extract_balanced_json_object(text: str) -> str | None:
        start = text.find("{")
        if start == -1:
            return None

        depth = 0
        in_string = False
        escaped = False
        for index in range(start, len(text)):
            char = text[index]
            if in_string:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    in_string = False
                continue

            if char == '"':
                in_string = True
            elif char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return text[start : index + 1]

        return None


class SemanticEnricher:
    """Concrete semantic enricher for tables and columns."""

    def __init__(
        self,
        client: LocalLLMClient,
        sampler: SamplePreparer | None = None,
        cache: SemanticCache | None = None,
    ) -> None:
        self.client = client
        self.sampler = sampler or SamplePreparer()
        self.cache = cache
        self._connector_lock = threading.RLock()

    def _execute_prompt(self, template: str, context: dict[str, Any]) -> dict[str, Any]:
        prompt = template.format(**context)
        delay_seconds = 1.0
        last_timeout: AITimeoutError | None = None

        for attempt in range(3):
            try:
                raw = self.client.generate(prompt)
                return ResponseParser.extract_json(raw)
            except AITimeoutError as exc:
                last_timeout = exc
                if attempt == 2:
                    break
                time.sleep(delay_seconds)
                delay_seconds *= 2.0
            except (AIConnectionError, AIGenerationError):
                raise

        if last_timeout is not None:
            raise last_timeout
        raise AIGenerationError("Prompt execution failed without a timeout cause.")

    @staticmethod
    def _optional_text(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _confidence(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _apply_table_payload(self, table: TableInfo, payload: dict[str, Any]) -> dict[str, Any]:
        table.semantic_short = self._optional_text(payload.get("short_description"))
        table.semantic_detailed = self._optional_text(payload.get("detailed_description"))
        table.semantic_domain = self._optional_text(payload.get("probable_domain"))
        table.semantic_role = self._optional_text(payload.get("probable_role"))
        table.semantic_confidence = self._confidence(payload.get("confidence"))
        return payload

    def _apply_column_payload(
        self,
        column: ColumnInfo,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        column.semantic_short = self._optional_text(payload.get("short_description"))
        column.semantic_detailed = self._optional_text(payload.get("detailed_description"))
        column.semantic_role = self._optional_text(payload.get("probable_role"))
        column.semantic_confidence = self._confidence(payload.get("confidence"))
        return payload

    @staticmethod
    def _humanize_identifier(value: str) -> str:
        label = value.replace("_", " ").replace("-", " ").strip()
        return " ".join(part.capitalize() for part in label.split()) or value

    def _infer_column_payload(
        self,
        table: TableInfo,
        column: ColumnInfo,
        sample_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        context = self.sampler.prepare_column_context(column, sample_rows, PrivacyMode.normal)
        pattern = str(context.get("pattern", "none")).strip().lower()
        name = column.name.lower()
        native_type = column.native_type.lower()
        table_label = table.semantic_short or self._humanize_identifier(table.name)

        role = "attribute"
        short = self._humanize_identifier(column.name)
        detailed = f"{short} recorded for {table_label.lower()}."
        confidence = 0.36

        if column.is_primary_key:
            role = "identifier"
            short = "Primary identifier"
            detailed = f"Stable primary key for {table_label.lower()}."
            confidence = 0.86
        elif column.is_foreign_key:
            role = "foreign_key"
            short = f"{self._humanize_identifier(column.name)} reference"
            detailed = f"Relationship key that links {table_label.lower()} to another table."
            confidence = 0.82
        elif "email" in pattern or "email" in name:
            role = "email"
            short = "Email address"
            detailed = f"Contact email stored for {table_label.lower()}."
            confidence = 0.8 if "email" in pattern else 0.72
        elif "uuid" in pattern or name.endswith("_uuid") or name == "uuid":
            role = "uuid_identifier"
            short = "UUID identifier"
            detailed = f"External UUID used to identify {table_label.lower()} records."
            confidence = 0.78
        elif "date" in name or "time" in name or pattern == "iso_date" or "timestamp" in native_type:
            role = "event_time"
            short = f"{self._humanize_identifier(column.name)} timestamp"
            detailed = f"Time marker associated with {table_label.lower()}."
            confidence = 0.74
        elif any(token in name for token in ("amount", "balance", "total", "price", "fee", "tax")):
            role = "monetary_value"
            short = f"{self._humanize_identifier(column.name)} amount"
            detailed = f"Monetary measure tracked for {table_label.lower()}."
            confidence = 0.73
        elif any(token in name for token in ("status", "state")):
            role = "status_flag"
            short = f"{self._humanize_identifier(column.name)} status"
            detailed = f"Lifecycle state recorded for {table_label.lower()}."
            confidence = 0.71
        elif any(token in name for token in ("code", "reference", "ref")):
            role = "reference_code"
            short = f"{self._humanize_identifier(column.name)} code"
            detailed = f"Business code or reference used by {table_label.lower()}."
            confidence = 0.66
        elif any(token in name for token in ("name", "title", "label")):
            role = "descriptor"
            short = self._humanize_identifier(column.name)
            detailed = f"Readable descriptor for {table_label.lower()}."
            confidence = 0.61
        elif any(token in name for token in ("count", "qty", "quantity")):
            role = "quantity"
            short = self._humanize_identifier(column.name)
            detailed = f"Count-like measure stored for {table_label.lower()}."
            confidence = 0.63
        elif native_type in {"boolean", "bool", "bit"} or name.startswith(("is_", "has_", "can_")):
            role = "boolean_flag"
            short = self._humanize_identifier(column.name)
            detailed = f"Boolean flag for {table_label.lower()}."
            confidence = 0.67
        elif any(token in name for token in ("phone", "mobile", "whatsapp")):
            role = "phone"
            short = "Phone number"
            detailed = f"Phone contact stored for {table_label.lower()}."
            confidence = 0.68
        elif any(token in name for token in ("address", "street", "city", "postal", "zip")):
            role = "address"
            short = self._humanize_identifier(column.name)
            detailed = f"Address-related attribute for {table_label.lower()}."
            confidence = 0.64
        elif column.canonical_type is not None and column.canonical_type.value in {
            "integer",
            "bigint",
            "smallint",
            "decimal",
            "numeric",
            "float",
            "double",
        }:
            role = "numeric_measure"
            short = self._humanize_identifier(column.name)
            detailed = f"Numeric measure used by {table_label.lower()}."
            confidence = 0.58

        if table.semantic_role and role == "attribute":
            detailed = (
                f"{short} used inside a table classified as {table.semantic_role.replace('_', ' ')}."
            )
            confidence = max(confidence, 0.44)

        payload = {
            "short_description": short,
            "detailed_description": detailed,
            "probable_role": role,
            "confidence": max(0.0, min(0.95, confidence)),
        }
        return payload

    def _resolve_sample_rows_or_connector(
        self,
        sample_rows_or_connector: Sequence[dict[str, Any]] | BaseConnector | None,
    ) -> tuple[list[dict[str, Any]] | None, BaseConnector | None]:
        if sample_rows_or_connector is None:
            return None, None
        if isinstance(sample_rows_or_connector, BaseConnector):
            return None, sample_rows_or_connector
        return list(sample_rows_or_connector), None

    def _get_table_sample_rows(
        self,
        table: TableInfo,
        connector: BaseConnector | None,
        privacy_mode: PrivacyMode,
    ) -> list[dict[str, Any]]:
        if connector is None or not privacy_mode.allows_samples:
            return []
        sample_limit = min(getattr(connector.config, "sample_limit", 20), 20)
        with self._connector_guard():
            return connector.get_sample_rows(
                table.schema,
                table.name,
                limit=sample_limit,
                privacy_mode=privacy_mode,
            )

    def _get_column_sample_rows(
        self,
        table: TableInfo,
        column: ColumnInfo,
        connector: BaseConnector | None,
        privacy_mode: PrivacyMode,
    ) -> list[dict[str, Any]]:
        if connector is None or not privacy_mode.allows_samples:
            return []
        sample_limit = min(getattr(connector.config, "sample_limit", 20), 20)
        with self._connector_guard():
            return connector.get_sample_rows(
                table.schema,
                table.name,
                columns=[column.name],
                limit=sample_limit,
                privacy_mode=privacy_mode,
            )

    @contextlib.contextmanager
    def _connector_guard(self) -> Iterator[None]:
        with self._connector_lock:
            yield

    def enrich_table(
        self,
        table: TableInfo,
        sample_rows_or_connector: Sequence[dict[str, Any]] | BaseConnector | None,
        privacy_mode: PrivacyMode,
        *,
        force_recompute: bool = False,
    ) -> dict[str, Any]:
        cached_payload = None
        if self.cache is not None and not force_recompute:
            cached_payload = self.cache.get_table_payload(table)
        if cached_payload is not None:
            return self._apply_table_payload(table, cached_payload)

        sample_rows, connector = self._resolve_sample_rows_or_connector(sample_rows_or_connector)
        if sample_rows is None:
            try:
                sample_rows = self._get_table_sample_rows(table, connector, privacy_mode)
            except Exception:
                sample_rows = []
        context = self.sampler.prepare_table_context(table, sample_rows, privacy_mode)
        payload = self._execute_prompt(TABLE_PROMPT_TEMPLATE, context)
        if self.cache is not None:
            self.cache.put_table_payload(table, payload)
        return self._apply_table_payload(table, payload)

    def enrich_column(
        self,
        table: TableInfo,
        column: ColumnInfo,
        sample_rows_or_connector: Sequence[dict[str, Any]] | BaseConnector | None,
        privacy_mode: PrivacyMode,
        *,
        force_recompute: bool = False,
    ) -> dict[str, Any]:
        cached_payload = None
        if self.cache is not None and not force_recompute:
            cached_payload = self.cache.get_column_payload(table, column)
        if cached_payload is not None:
            return self._apply_column_payload(column, cached_payload)

        sample_rows, connector = self._resolve_sample_rows_or_connector(sample_rows_or_connector)
        if sample_rows is None:
            try:
                sample_rows = self._get_column_sample_rows(table, column, connector, privacy_mode)
            except Exception:
                sample_rows = []
        context = self.sampler.prepare_column_context(column, sample_rows, privacy_mode)
        context["schema"] = table.schema
        context["table_name"] = table.name
        payload = self._execute_prompt(COLUMN_PROMPT_TEMPLATE, context)
        if self.cache is not None:
            self.cache.put_column_payload(table, column, payload)
        return self._apply_column_payload(column, payload)

    def infer_columns(
        self,
        table: TableInfo,
        sample_rows_or_connector: Sequence[dict[str, Any]] | BaseConnector | None,
        privacy_mode: PrivacyMode,
        *,
        selected_column_names: set[str] | None = None,
    ) -> TableInfo:
        sample_rows, connector = self._resolve_sample_rows_or_connector(sample_rows_or_connector)
        if sample_rows is None:
            try:
                sample_rows = self._get_table_sample_rows(table, connector, privacy_mode)
            except Exception:
                sample_rows = []

        for column in table.columns:
            if selected_column_names is not None and column.name not in selected_column_names:
                continue
            payload = self._infer_column_payload(table, column, sample_rows)
            self._apply_column_payload(column, payload)
        return table

    def enrich_schema(
        self,
        schema: SchemaInfo,
        connector: BaseConnector | None,
        privacy_mode: PrivacyMode,
        *,
        parallel_workers: int = 4,
        force_recompute: bool = False,
        tables_only: bool = False,
        column_mode: Literal["infer", "full", "skip"] = "full",
        selected_columns_by_table: dict[tuple[str, str], set[str] | None] | None = None,
        on_table_complete: Any | None = None,
        on_column_complete: Any | None = None,
    ) -> SchemaInfo:
        """Enrich every table in a schema using table-level concurrency."""

        if parallel_workers < 1:
            raise ValueError("parallel_workers must be >= 1")
        total_tables = len(schema.tables)
        if total_tables == 0:
            if self.cache is not None:
                self.cache.save()
            return schema

        effective_column_mode: Literal["infer", "full", "skip"]
        effective_column_mode = "skip" if tables_only else column_mode

        def _worker(table: TableInfo, table_index: int) -> TableInfo:
            try:
                self.enrich_table(
                    table,
                    connector,
                    privacy_mode,
                    force_recompute=force_recompute,
                )
            except (AIConnectionError, AIGenerationError, AITimeoutError) as exc:
                table.semantic_short = "Semantic analysis failed"
                table.semantic_detailed = str(exc).strip() or "Unknown AI enrichment error."
                table.semantic_confidence = 0.0
            selected_columns = None
            if selected_columns_by_table is not None:
                selected_columns = selected_columns_by_table.get((table.schema, table.name))
            if effective_column_mode == "full":
                total_columns = len(table.columns)
                for column_index, column in enumerate(table.columns, start=1):
                    if selected_columns is not None and column.name not in selected_columns:
                        continue
                    try:
                        self.enrich_column(
                            table,
                            column,
                            connector,
                            privacy_mode,
                            force_recompute=force_recompute,
                        )
                    except (AIConnectionError, AIGenerationError, AITimeoutError):
                        continue
                    if on_column_complete is not None:
                        on_column_complete(
                            table,
                            column,
                            column_index,
                            total_columns,
                            table_index,
                            total_tables,
                        )
            elif effective_column_mode == "infer":
                self.infer_columns(
                    table,
                    connector,
                    privacy_mode,
                    selected_column_names=selected_columns,
                )
            return table

        completed = 0
        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            futures = {
                executor.submit(_worker, table, table_index): table
                for table_index, table in enumerate(schema.tables, start=1)
            }
            for future in as_completed(futures):
                table = future.result()
                completed += 1
                if on_table_complete is not None:
                    on_table_complete(table, completed, total_tables)
        if self.cache is not None:
            self.cache.save()
        return schema
