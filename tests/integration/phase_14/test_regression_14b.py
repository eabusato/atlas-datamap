"""Phase 14B regression coverage for the public Atlas SDK."""

from __future__ import annotations

from pathlib import Path

import pytest

from atlas import Atlas, PrivacyMode
from atlas.ai import SemanticCache
from atlas.connectors.base import PrivacyViolationError
from tests.integration.phase_14.helpers import (
    DeterministicLocalClient,
    create_phase14_sqlite_db,
    make_phase14_config,
)
from tests.support.svg_baseline import assert_svg_matches_baseline

pytestmark = [pytest.mark.integration, pytest.mark.phase_14b]


@pytest.fixture()
def regression_db_path(phase_tmp_dir: Path) -> Path:
    return create_phase14_sqlite_db(phase_tmp_dir / "sdk_regression.sqlite")


@pytest.fixture()
def baseline_update(request: pytest.FixtureRequest) -> bool:
    return bool(request.config.getoption("--update-baseline"))


def test_public_api_compact_sigilo_matches_svg_baseline(
    regression_db_path: Path,
    baseline_update: bool,
) -> None:
    atlas = Atlas(make_phase14_config(regression_db_path))
    result = atlas.scan()

    artifact = atlas.build_sigilo(result, style="compact")

    assert_svg_matches_baseline(
        artifact.to_svg_text(),
        Path("tests/baselines/phase_14/public_api_sqlite_compact.svg"),
        update=baseline_update,
    )


def test_public_api_network_sigilo_matches_svg_baseline(
    regression_db_path: Path,
    baseline_update: bool,
) -> None:
    atlas = Atlas(make_phase14_config(regression_db_path))
    result = atlas.scan()

    artifact = atlas.build_sigilo(result, style="network")

    assert_svg_matches_baseline(
        artifact.to_svg_text(),
        Path("tests/baselines/phase_14/public_api_sqlite_network.svg"),
        update=baseline_update,
    )


def test_public_api_snapshot_flow_stays_roundtrip_safe(
    regression_db_path: Path,
    phase_tmp_dir: Path,
) -> None:
    atlas = Atlas(make_phase14_config(regression_db_path))
    result = atlas.scan()
    artifact = atlas.build_sigilo(result, style="compact")

    snapshot = atlas.create_snapshot(result, artifact)
    saved = snapshot.save(phase_tmp_dir / "snapshot_roundtrip")
    loaded = snapshot.load(saved)

    assert loaded.result.database == result.database
    assert loaded.result.total_tables == result.total_tables
    assert "main.orders" in loaded.sigil_payload


def test_public_api_masked_mode_hides_sensitive_sample_values(regression_db_path: Path) -> None:
    atlas = Atlas(make_phase14_config(regression_db_path, privacy_mode=PrivacyMode.masked))

    with atlas.connector.session():
        rows = atlas.connector.get_sample_rows("main", "customers", limit=2)

    assert rows
    assert rows[0]["email"] == "***"
    assert rows[0]["full_name"] in {"Alice Walker", "Bruno Lima"}


def test_public_api_stats_only_enrich_does_not_request_live_samples(
    regression_db_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    atlas = Atlas(make_phase14_config(regression_db_path, privacy_mode=PrivacyMode.stats_only))
    result = atlas.scan()

    def _forbidden(*args: object, **kwargs: object) -> object:
        raise AssertionError("stats_only should not request sample rows")

    monkeypatch.setattr(atlas.connector, "get_sample_rows", _forbidden)

    enriched = atlas.enrich(
        result,
        client=DeterministicLocalClient(),
        tables_only=False,
    )

    assert enriched.get_table("main", "orders") is not None
    orders = enriched.get_table("main", "orders")
    assert orders is not None
    assert orders.semantic_short == "Customer orders"


def test_public_api_no_samples_still_blocks_direct_sampling(regression_db_path: Path) -> None:
    atlas = Atlas(make_phase14_config(regression_db_path, privacy_mode=PrivacyMode.no_samples))

    with pytest.raises(PrivacyViolationError), atlas.connector.session():
        atlas.connector.get_sample_rows("main", "customers", limit=1)


def test_public_api_enrich_cache_is_stable_across_two_runs(
    regression_db_path: Path,
    phase_tmp_dir: Path,
) -> None:
    atlas = Atlas(make_phase14_config(regression_db_path))
    first = atlas.scan()
    second = atlas.scan()
    client = DeterministicLocalClient()
    cache = SemanticCache(phase_tmp_dir / ".semantic_cache")

    atlas.enrich(first, client=client, cache=cache, tables_only=True)
    first_calls = client.generate_calls
    atlas.enrich(second, client=client, cache=cache, tables_only=True)

    assert first_calls > 0
    assert client.generate_calls == first_calls
    orders = second.get_table("main", "orders")
    assert orders is not None
    assert orders.semantic_domain == "sales"


def test_public_api_ask_works_without_persisted_embeddings(regression_db_path: Path) -> None:
    atlas = Atlas(make_phase14_config(regression_db_path))
    result = atlas.scan()

    answer = atlas.ask(
        result,
        "which table stores customer orders and billing totals?",
        client=DeterministicLocalClient(),
        embeddings_path=None,
    )

    assert answer.candidates
    assert answer.candidates[0].qualified_name == "main.orders"
    assert answer.reasoning
