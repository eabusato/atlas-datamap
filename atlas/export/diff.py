"""Structural diff engine for Atlas snapshots."""

from __future__ import annotations

from dataclasses import dataclass, field

from atlas.export.snapshot import AtlasSnapshot
from atlas.types import ForeignKeyInfo, TableInfo

_VOLUME_THRESHOLD_ROWS = 1_000
_VOLUME_THRESHOLD_RATIO = 0.20


@dataclass(frozen=True, slots=True)
class ColumnTypeChange:
    """Mutation of a physical column type between snapshots."""

    table: str
    column: str
    old_type: str
    new_type: str

    def to_dict(self) -> dict[str, object]:
        return {
            "table": self.table,
            "column": self.column,
            "old_type": self.old_type,
            "new_type": self.new_type,
        }


@dataclass(frozen=True, slots=True)
class VolumeChange:
    """Significant table row estimate change between snapshots."""

    table: str
    old_rows: int
    new_rows: int
    percent_change: float

    def to_dict(self) -> dict[str, object]:
        return {
            "table": self.table,
            "old_rows": self.old_rows,
            "new_rows": self.new_rows,
            "percent_change": self.percent_change,
        }


@dataclass(slots=True)
class SchemaDiff:
    """Complete structural drift result between two Atlas snapshots."""

    added_tables: list[str] = field(default_factory=list)
    removed_tables: list[str] = field(default_factory=list)
    added_columns: dict[str, list[str]] = field(default_factory=dict)
    removed_columns: dict[str, list[str]] = field(default_factory=dict)
    type_changes: list[ColumnTypeChange] = field(default_factory=list)
    volume_changes: list[VolumeChange] = field(default_factory=list)
    new_relations: list[ForeignKeyInfo] = field(default_factory=list)
    removed_relations: list[ForeignKeyInfo] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return any(
            (
                self.added_tables,
                self.removed_tables,
                self.added_columns,
                self.removed_columns,
                self.type_changes,
                self.volume_changes,
                self.new_relations,
                self.removed_relations,
            )
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "added_tables": list(self.added_tables),
            "removed_tables": list(self.removed_tables),
            "added_columns": {key: list(value) for key, value in self.added_columns.items()},
            "removed_columns": {key: list(value) for key, value in self.removed_columns.items()},
            "type_changes": [item.to_dict() for item in self.type_changes],
            "volume_changes": [item.to_dict() for item in self.volume_changes],
            "new_relations": [item.to_dict() for item in self.new_relations],
            "removed_relations": [item.to_dict() for item in self.removed_relations],
        }


def _table_index(snapshot: AtlasSnapshot) -> dict[str, TableInfo]:
    return {
        table.qualified_name: table
        for schema in snapshot.result.schemas
        for table in schema.tables
    }


def _relation_signature(fk: ForeignKeyInfo) -> tuple[str, str, tuple[str, ...], str, str, tuple[str, ...]]:
    return (
        fk.name,
        fk.source_schema,
        tuple(fk.source_columns),
        fk.target_schema,
        fk.target_table,
        tuple(fk.target_columns),
    )


def _relation_sort_key(fk: ForeignKeyInfo) -> tuple[str, str, str]:
    return (
        f"{fk.source_schema}.{fk.source_table}",
        f"{fk.target_schema}.{fk.target_table}",
        fk.name,
    )


class SnapshotDiff:
    """Compare two snapshots without touching the original database."""

    @staticmethod
    def compare(before: AtlasSnapshot, after: AtlasSnapshot) -> SchemaDiff:
        diff = SchemaDiff()
        before_tables = _table_index(before)
        after_tables = _table_index(after)

        before_names = set(before_tables)
        after_names = set(after_tables)

        diff.added_tables = sorted(after_names - before_names)
        diff.removed_tables = sorted(before_names - after_names)

        for qualified_name in sorted(before_names & after_names):
            before_table = before_tables[qualified_name]
            after_table = after_tables[qualified_name]

            before_columns = {column.name: column for column in before_table.columns}
            after_columns = {column.name: column for column in after_table.columns}
            before_column_names = set(before_columns)
            after_column_names = set(after_columns)

            added_columns = sorted(after_column_names - before_column_names)
            removed_columns = sorted(before_column_names - after_column_names)
            if added_columns:
                diff.added_columns[qualified_name] = added_columns
            if removed_columns:
                diff.removed_columns[qualified_name] = removed_columns

            for column_name in sorted(before_column_names & after_column_names):
                before_column = before_columns[column_name]
                after_column = after_columns[column_name]
                if before_column.native_type != after_column.native_type:
                    diff.type_changes.append(
                        ColumnTypeChange(
                            table=qualified_name,
                            column=column_name,
                            old_type=before_column.native_type,
                            new_type=after_column.native_type,
                        )
                    )

            old_rows = before_table.row_count_estimate
            new_rows = after_table.row_count_estimate
            if old_rows > _VOLUME_THRESHOLD_ROWS and old_rows > 0:
                delta_ratio = (new_rows - old_rows) / old_rows
                if abs(delta_ratio) >= _VOLUME_THRESHOLD_RATIO:
                    diff.volume_changes.append(
                        VolumeChange(
                            table=qualified_name,
                            old_rows=old_rows,
                            new_rows=new_rows,
                            percent_change=round(delta_ratio * 100.0, 2),
                        )
                    )

        before_relations = {
            _relation_signature(fk): fk
            for table in before.result.all_tables()
            for fk in table.foreign_keys
        }
        after_relations = {
            _relation_signature(fk): fk
            for table in after.result.all_tables()
            for fk in table.foreign_keys
        }
        diff.new_relations = sorted(
            [after_relations[key] for key in after_relations.keys() - before_relations.keys()],
            key=_relation_sort_key,
        )
        diff.removed_relations = sorted(
            [before_relations[key] for key in before_relations.keys() - after_relations.keys()],
            key=_relation_sort_key,
        )
        diff.type_changes.sort(key=lambda item: (item.table, item.column))
        diff.volume_changes.sort(key=lambda item: (item.table, item.percent_change))
        return diff
