"""Local snapshot history management."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from atlas.export.snapshot import AtlasSnapshot, SnapshotManifest, sanitize_stem


def _parse_iso_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


class AtlasHistory:
    """Manage a directory of Atlas snapshot archives."""

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)

    def build_snapshot_name(self, database: str, created_at: str) -> str:
        stamp = _parse_iso_timestamp(created_at).strftime("%Y%m%d_%H%M%S")
        return f"{sanitize_stem(database)}_{stamp}.atlas"

    def list_snapshots(self) -> list[tuple[Path, SnapshotManifest]]:
        if not self.directory.is_dir():
            return []
        snapshots: list[tuple[Path, SnapshotManifest]] = []
        for candidate in sorted(self.directory.glob("*.atlas")):
            try:
                manifest = AtlasSnapshot.peek_manifest(candidate)
            except Exception:
                continue
            snapshots.append((candidate, manifest))
        snapshots.sort(key=lambda item: (item[1].created_at, item[0].name), reverse=True)
        return snapshots

    def latest(self) -> Path | None:
        snapshots = self.list_snapshots()
        if not snapshots:
            return None
        return snapshots[0][0]

    def resolve_snapshot(self, reference: str) -> Path:
        snapshots = self.list_snapshots()
        if not snapshots:
            raise ValueError(f"No snapshots found in {self.directory}.")

        if reference == "latest":
            latest = self.latest()
            if latest is None:
                raise ValueError(f"No snapshots found in {self.directory}.")
            return latest

        exact_name = reference if reference.endswith(".atlas") else f"{reference}.atlas"
        exact_path = self.directory / exact_name
        if exact_path.is_file():
            return exact_path

        matches: list[Path] = []
        for path, manifest in snapshots:
            manifest_date = ""
            try:
                manifest_date = _parse_iso_timestamp(manifest.created_at).strftime("%Y%m%d")
            except ValueError:
                manifest_date = manifest.created_at[:8]
            if manifest_date == reference:
                matches.append(path)

        if not matches:
            raise ValueError(f"Snapshot reference {reference!r} could not be resolved.")
        if len(matches) > 1:
            raise ValueError(f"Snapshot reference {reference!r} is ambiguous.")
        return matches[0]
