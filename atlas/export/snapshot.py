"""Artifact persistence helpers and Atlas snapshot archive support."""

from __future__ import annotations

import json
import re
import unicodedata
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from atlas.analysis import AnomalyDetector, TableScorer
from atlas.types import IntrospectionResult
from atlas.version import ATLAS_VERSION

_STEM_MAX_LENGTH = 64
_SNAPSHOT_EXTENSION = ".atlas"
_SNAPSHOT_FORMAT_VERSION = "1.0"
_MANIFEST_FILE = "manifest.json"
_SCHEMA_FILE = "schema.json"
_SIGIL_FILE = "sigilo.sigil"
_SVG_FILE = "sigilo.svg"
_SCORES_FILE = "scores.json"
_ANOMALIES_FILE = "anomalies.json"
_SEMANTICS_FILE = "semantics.json"
_REQUIRED_SNAPSHOT_FILES = (
    _MANIFEST_FILE,
    _SCHEMA_FILE,
    _SIGIL_FILE,
    _SVG_FILE,
    _SCORES_FILE,
    _ANOMALIES_FILE,
)


@dataclass(slots=True)
class ScanArtifacts:
    """Paths written by a successful scan execution."""

    svg_path: Path
    meta_json_path: Path
    sigil_path: Path


def sanitize_stem(raw_stem: str) -> str:
    """Normalize an artifact stem to a compact ASCII-safe filename."""

    normalized = unicodedata.normalize("NFKD", raw_stem)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    replaced = re.sub(r"[\\/:\s]+", "_", ascii_only)
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", replaced).strip("._-")
    compact = re.sub(r"_+", "_", cleaned)
    stem = compact[:_STEM_MAX_LENGTH].rstrip("._-")
    return stem or "atlas_scan"


def artifact_paths(
    result: IntrospectionResult,
    output_dir: str | Path,
    *,
    stem: str | None = None,
) -> ScanArtifacts:
    """Resolve the three canonical output paths for a scan result."""

    target_dir = Path(output_dir)
    effective_stem = sanitize_stem(stem or result.database)
    return ScanArtifacts(
        svg_path=target_dir / f"{effective_stem}.svg",
        meta_json_path=target_dir / f"{effective_stem}_meta.json",
        sigil_path=target_dir / f"{effective_stem}.sigil",
    )


def save_artifacts(
    result: IntrospectionResult,
    svg_bytes: bytes,
    output_dir: str | Path,
    *,
    stem: str | None = None,
) -> ScanArtifacts:
    """Persist SVG, compact sigil JSON, and pretty metadata JSON."""

    artifacts = artifact_paths(result, output_dir, stem=stem)
    artifacts.svg_path.parent.mkdir(parents=True, exist_ok=True)

    sigil_payload = json.dumps(
        result.to_dict(),
        ensure_ascii=False,
        separators=(",", ":"),
    )
    artifacts.svg_path.write_bytes(svg_bytes)
    artifacts.sigil_path.write_text(sigil_payload, encoding="utf-8")
    artifacts.meta_json_path.write_text(result.to_json(indent=2), encoding="utf-8")
    return artifacts


@dataclass(slots=True)
class SnapshotManifest:
    """Small metadata header for rapid snapshot discovery."""

    format_version: str
    atlas_version: str
    created_at: str
    database: str
    engine: str
    schema_count: int
    table_count: int
    has_semantics: bool
    has_anomalies: bool
    has_scores: bool
    contents: list[str]

    def to_dict(self) -> dict[str, object]:
        return {
            "format_version": self.format_version,
            "atlas_version": self.atlas_version,
            "created_at": self.created_at,
            "database": self.database,
            "engine": self.engine,
            "schema_count": self.schema_count,
            "table_count": self.table_count,
            "has_semantics": self.has_semantics,
            "has_anomalies": self.has_anomalies,
            "has_scores": self.has_scores,
            "contents": list(self.contents),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> SnapshotManifest:
        contents_raw = payload.get("contents", [])
        contents = (
            [str(item) for item in contents_raw]
            if isinstance(contents_raw, list)
            else []
        )
        return cls(
            format_version=str(payload.get("format_version", _SNAPSHOT_FORMAT_VERSION)),
            atlas_version=str(payload.get("atlas_version", "unknown")),
            created_at=str(payload.get("created_at", "")),
            database=str(payload.get("database", "")),
            engine=str(payload.get("engine", "")),
            schema_count=_coerce_int(payload.get("schema_count", 0)),
            table_count=_coerce_int(payload.get("table_count", 0)),
            has_semantics=bool(payload.get("has_semantics", False)),
            has_anomalies=bool(payload.get("has_anomalies", False)),
            has_scores=bool(payload.get("has_scores", False)),
            contents=contents,
        )


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _coerce_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _extract_semantics_payload(result: IntrospectionResult) -> dict[str, object] | None:
    tables: dict[str, dict[str, object]] = {}
    columns: dict[str, dict[str, object]] = {}
    for table in result.all_tables():
        table_payload: dict[str, object] = {}
        if table.semantic_short:
            table_payload["semantic_short"] = table.semantic_short
        if table.semantic_detailed:
            table_payload["semantic_detailed"] = table.semantic_detailed
        if table.semantic_domain:
            table_payload["semantic_domain"] = table.semantic_domain
        if table.semantic_role:
            table_payload["semantic_role"] = table.semantic_role
        if table.semantic_confidence > 0.0:
            table_payload["semantic_confidence"] = table.semantic_confidence
        if table_payload:
            tables[table.qualified_name] = table_payload

        for column in table.columns:
            column_payload: dict[str, object] = {}
            if column.semantic_short:
                column_payload["semantic_short"] = column.semantic_short
            if column.semantic_detailed:
                column_payload["semantic_detailed"] = column.semantic_detailed
            if column.semantic_role:
                column_payload["semantic_role"] = column.semantic_role
            if column.semantic_confidence > 0.0:
                column_payload["semantic_confidence"] = column.semantic_confidence
            if column_payload:
                columns[f"{table.qualified_name}.{column.name}"] = column_payload
    if not tables and not columns:
        return None
    return {"tables": tables, "columns": columns}


@dataclass(slots=True)
class AtlasSnapshot:
    """Portable offline snapshot of a fully analyzed Atlas result."""

    manifest: SnapshotManifest
    result: IntrospectionResult
    sigil_svg: str
    sigil_payload: str
    scores: list[dict[str, object]]
    anomalies: list[dict[str, object]]
    semantics: dict[str, object] | None = None

    @property
    def metadata(self) -> SnapshotManifest:
        """Convenience alias for callers that expect snapshot metadata."""

        return self.manifest

    @classmethod
    def from_result(
        cls,
        result: IntrospectionResult,
        *,
        sigil_svg: str,
        sigil_payload: str,
        scores: list[dict[str, object]] | None = None,
        anomalies: list[dict[str, object]] | None = None,
        semantics: dict[str, object] | None = None,
        created_at: str | None = None,
    ) -> AtlasSnapshot:
        snapshot_scores = (
            list(scores)
            if scores is not None
            else [score.to_dict() for score in TableScorer(result).score_all()]
        )
        snapshot_anomalies = (
            list(anomalies)
            if anomalies is not None
            else [anomaly.to_dict() for anomaly in AnomalyDetector().detect(result)]
        )
        snapshot_semantics = semantics if semantics is not None else _extract_semantics_payload(result)
        contents = list(_REQUIRED_SNAPSHOT_FILES)
        if snapshot_semantics:
            contents.append(_SEMANTICS_FILE)
        manifest = SnapshotManifest(
            format_version=_SNAPSHOT_FORMAT_VERSION,
            atlas_version=ATLAS_VERSION,
            created_at=created_at or result.introspected_at or _utc_now_iso(),
            database=result.database,
            engine=result.engine,
            schema_count=len(result.schemas),
            table_count=result.total_tables + result.total_views,
            has_semantics=bool(snapshot_semantics),
            has_anomalies=bool(snapshot_anomalies),
            has_scores=bool(snapshot_scores),
            contents=contents,
        )
        return cls(
            manifest=manifest,
            result=IntrospectionResult.from_dict(result.to_dict()),
            sigil_svg=str(sigil_svg),
            sigil_payload=str(sigil_payload),
            scores=snapshot_scores,
            anomalies=snapshot_anomalies,
            semantics=snapshot_semantics,
        )

    def _snapshot_path(self, path: str | Path) -> Path:
        target = Path(path)
        if target.suffix != _SNAPSHOT_EXTENSION:
            target = target.with_suffix(_SNAPSHOT_EXTENSION)
        return target

    def _build_manifest(self) -> SnapshotManifest:
        contents = list(_REQUIRED_SNAPSHOT_FILES)
        if self.semantics:
            contents.append(_SEMANTICS_FILE)
        return SnapshotManifest(
            format_version=self.manifest.format_version or _SNAPSHOT_FORMAT_VERSION,
            atlas_version=self.manifest.atlas_version or ATLAS_VERSION,
            created_at=self.manifest.created_at or _utc_now_iso(),
            database=self.result.database,
            engine=self.result.engine,
            schema_count=len(self.result.schemas),
            table_count=self.result.total_tables + self.result.total_views,
            has_semantics=bool(self.semantics),
            has_anomalies=bool(self.anomalies),
            has_scores=bool(self.scores),
            contents=contents,
        )

    def save(self, path: str | Path) -> Path:
        """Persist the snapshot to a ZIP-backed ``.atlas`` archive."""

        snapshot_path = self._snapshot_path(path)
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        manifest = self._build_manifest()
        self.manifest = manifest

        with zipfile.ZipFile(snapshot_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(
                _MANIFEST_FILE,
                json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2),
            )
            archive.writestr(_SCHEMA_FILE, self.result.to_json(indent=2))
            archive.writestr(_SIGIL_FILE, self.sigil_payload)
            archive.writestr(_SVG_FILE, self.sigil_svg)
            archive.writestr(
                _SCORES_FILE,
                json.dumps(self.scores, ensure_ascii=False, indent=2),
            )
            archive.writestr(
                _ANOMALIES_FILE,
                json.dumps(self.anomalies, ensure_ascii=False, indent=2),
            )
            if self.semantics is not None:
                archive.writestr(
                    _SEMANTICS_FILE,
                    json.dumps(self.semantics, ensure_ascii=False, indent=2),
                )
        return snapshot_path

    @classmethod
    def load(cls, path: str | Path) -> AtlasSnapshot:
        """Load a full snapshot archive from disk."""

        snapshot_path = Path(path)
        if not snapshot_path.is_file():
            raise FileNotFoundError(f"Snapshot archive not found: {snapshot_path}")
        try:
            with zipfile.ZipFile(snapshot_path, "r") as archive:
                names = set(archive.namelist())
                missing = [name for name in _REQUIRED_SNAPSHOT_FILES if name not in names]
                if missing:
                    raise ValueError(
                        "Snapshot archive is missing required files: "
                        + ", ".join(sorted(missing))
                    )

                manifest = SnapshotManifest.from_dict(
                    json.loads(archive.read(_MANIFEST_FILE).decode("utf-8"))
                )
                result = IntrospectionResult.from_json(
                    archive.read(_SCHEMA_FILE).decode("utf-8")
                )
                sigil_payload = archive.read(_SIGIL_FILE).decode("utf-8")
                sigil_svg = archive.read(_SVG_FILE).decode("utf-8")

                raw_scores = json.loads(archive.read(_SCORES_FILE).decode("utf-8"))
                if not isinstance(raw_scores, list):
                    raise ValueError("scores.json must contain a JSON array.")
                scores = [item for item in raw_scores if isinstance(item, dict)]

                raw_anomalies = json.loads(archive.read(_ANOMALIES_FILE).decode("utf-8"))
                if not isinstance(raw_anomalies, list):
                    raise ValueError("anomalies.json must contain a JSON array.")
                anomalies = [item for item in raw_anomalies if isinstance(item, dict)]

                semantics: dict[str, object] | None = None
                if _SEMANTICS_FILE in names:
                    raw_semantics = json.loads(archive.read(_SEMANTICS_FILE).decode("utf-8"))
                    if isinstance(raw_semantics, dict):
                        semantics = raw_semantics
                    else:
                        raise ValueError("semantics.json must contain a JSON object.")

                return cls(
                    manifest=manifest,
                    result=result,
                    sigil_svg=sigil_svg,
                    sigil_payload=sigil_payload,
                    scores=scores,
                    anomalies=anomalies,
                    semantics=semantics,
                )
        except zipfile.BadZipFile as exc:
            raise ValueError(f"Invalid or corrupted .atlas archive: {snapshot_path}") from exc
        except json.JSONDecodeError as exc:
            raise ValueError(f"Snapshot archive contains invalid JSON: {snapshot_path}") from exc

    @classmethod
    def peek_manifest(cls, path: str | Path) -> SnapshotManifest:
        """Read only the snapshot manifest without loading the heavy payload."""

        snapshot_path = Path(path)
        if not snapshot_path.is_file():
            raise FileNotFoundError(f"Snapshot archive not found: {snapshot_path}")
        try:
            with zipfile.ZipFile(snapshot_path, "r") as archive:
                if _MANIFEST_FILE not in archive.namelist():
                    raise ValueError("Snapshot archive is missing manifest.json.")
                payload = json.loads(archive.read(_MANIFEST_FILE).decode("utf-8"))
                if not isinstance(payload, dict):
                    raise ValueError("Snapshot manifest must be a JSON object.")
                return SnapshotManifest.from_dict(payload)
        except zipfile.BadZipFile as exc:
            raise ValueError(f"Invalid or corrupted .atlas archive: {snapshot_path}") from exc
        except json.JSONDecodeError as exc:
            raise ValueError(f"Snapshot manifest contains invalid JSON: {snapshot_path}") from exc
