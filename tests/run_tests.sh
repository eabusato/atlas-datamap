#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-$(command -v python || command -v python3.12 || command -v python3.11 || command -v python3)}"
PHASE_FILTER="${ATLAS_TEST_PHASES:-ALL}"

run_pytest() {
  local label="$1"
  shift
  echo "[atlas-tests] Running ${label}..."
  "$PYTHON_BIN" -m pytest "$@"
  echo "[atlas-tests] ${label} is green."
}

run_phase_0a() {
  echo "[atlas-tests] Verifying Phase 0A packaging and CLI contracts."
  run_pytest "Phase 0A unit suite" --no-cov tests/test_version.py
  run_pytest "Phase 0A integration suite" --no-cov tests/integration/phase_0/test_0a_packaging.py
}

run_phase_0b() {
  echo "[atlas-tests] Re-checking historical Phase 0A coverage before Phase 0B."
  run_pytest "Phase 0B unit suite" --no-cov tests/test_config.py
  run_pytest "Phase 0B integration suite" --no-cov tests/integration/phase_0/test_0b_config.py
}

run_phase_0c() {
  echo "[atlas-tests] Re-checking historical Phase 0A/0B coverage before Phase 0C."
  run_pytest "Phase 0C unit suite" --no-cov tests/test_types.py tests/test_base_connector.py tests/test_connectors_factory.py
  run_pytest "Phase 0C integration suite" --no-cov tests/integration/phase_0/test_0c_connectors.py
}

run_phase_1a() {
  echo "[atlas-tests] Re-checking historical Phase 0 coverage before Phase 1A."
  run_pytest "Phase 1A unit suite" --no-cov tests/test_postgresql_1a.py
  run_pytest "Phase 1A integration suite" --no-cov tests/integration/phase_1/test_1a_connection.py
}

run_phase_1b() {
  echo "[atlas-tests] Re-checking historical Phase 0/1A coverage before Phase 1B."
  run_pytest "Phase 1B unit suite" --no-cov tests/test_postgresql_1b.py
  run_pytest "Phase 1B integration suite" --no-cov tests/integration/phase_1/test_1b_tables_columns.py
}

run_phase_1c() {
  echo "[atlas-tests] Re-checking historical Phase 0/1A/1B coverage before Phase 1C."
  run_pytest "Phase 1C unit suite" --no-cov tests/test_postgresql_1c.py
  run_pytest "Phase 1C integration suite" --no-cov tests/integration/phase_1/test_1c_relationships_indexes.py
}

run_phase_1d() {
  echo "[atlas-tests] Re-checking historical Phase 0/1A/1B/1C coverage before Phase 1D."
  run_pytest "Phase 1D unit suite" --no-cov tests/test_postgresql_1d.py
  run_pytest "Phase 1D integration suite" --no-cov tests/integration/phase_1/test_1d_sampling_stats.py
}

run_phase_2a() {
  echo "[atlas-tests] Re-checking historical Phase 0 and Phase 1 coverage before Phase 2A."
  run_pytest "Phase 2A unit suite" --no-cov tests/test_mysql_2a.py
  run_pytest "Phase 2A integration suite" --no-cov tests/integration/phase_2/test_2a_mysql.py
}

run_phase_2b() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, and Phase 2A coverage before Phase 2B."
  run_pytest "Phase 2B unit suite" --no-cov tests/test_mssql_2b.py
  run_pytest "Phase 2B integration suite" --no-cov tests/integration/phase_2/test_2b_mssql.py
}

run_phase_2c() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2A, and Phase 2B coverage before Phase 2C."
  run_pytest "Phase 2C unit suite" --no-cov tests/test_type_mapping.py
  run_pytest "Phase 2C integration suite" --no-cov tests/integration/phase_2/test_2c_type_mapping.py
}

run_phase_3a() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, and Phase 2 coverage before Phase 3A."
  run_pytest "Phase 3A unit suite" --no-cov tests/test_c_render.py
  run_pytest "Phase 3A integration suite" --no-cov tests/integration/phase_3/test_3a_c_library.py
}

run_phase_3b() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, and Phase 3A coverage before Phase 3B."
  run_pytest "Phase 3B unit suite" --no-cov tests/test_sigilo_3b.py
  run_pytest "Phase 3B integration suite" --no-cov tests/integration/phase_3/test_3b_sigilo.py
}

run_phase_3c() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, and Phase 3A/3B coverage before Phase 3C."
  run_pytest "Phase 3C unit suite" --no-cov tests/test_build_3c.py
  run_pytest "Phase 3C integration suite" --no-cov tests/integration/phase_3/test_3c_build_flow.py
}

run_phase_4a() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, and Phase 3 coverage before Phase 4A."
  run_pytest "Phase 4A unit suite" --no-cov tests/test_datamap_4a.py
  run_pytest "Phase 4A integration suite" --no-cov tests/integration/phase_4/test_4a_datamap.py
}

run_phase_4b() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, and Phase 4A coverage before Phase 4B."
  run_pytest "Phase 4B unit suite" --no-cov tests/test_hover_4b.py
  run_pytest "Phase 4B integration suite" --no-cov tests/integration/phase_4/test_4b_hover.py
}

run_phase_4c() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, and Phase 4A/4B coverage before Phase 4C."
  run_pytest "Phase 4C unit suite" --no-cov tests/test_layout_4c.py
  run_pytest "Phase 4C integration suite" --no-cov tests/integration/phase_4/test_4c_layout.py
}

run_phase_5a() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, and Phase 4 coverage before Phase 5A."
  run_pytest "Phase 5A unit suite" --no-cov tests/test_scan_5a.py
  run_pytest "Phase 5A integration suite" --no-cov tests/integration/phase_5/test_5a_scan.py
}

run_phase_5b() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, and Phase 5A coverage before Phase 5B."
  run_pytest "Phase 5B unit suite" --no-cov tests/test_open_5b.py
  run_pytest "Phase 5B integration suite" --no-cov tests/integration/phase_5/test_5b_open.py
}

run_phase_5c() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, and Phase 5A/5B coverage before Phase 5C."
  run_pytest "Phase 5C unit suite" --no-cov tests/test_info_5c.py
  run_pytest "Phase 5C integration suite" --no-cov tests/integration/phase_5/test_5c_info.py
}

run_phase_6a() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, and Phase 5 coverage before Phase 6A."
  run_pytest "Phase 6A unit suite" --no-cov tests/test_classifier_6a.py
  run_pytest "Phase 6A integration suite" --no-cov tests/integration/phase_6/test_6a_classifier.py
}

run_phase_6b() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, and Phase 6A coverage before Phase 6B."
  run_pytest "Phase 6B unit suite" --no-cov tests/test_scorer_6b.py
  run_pytest "Phase 6B integration suite" --no-cov tests/integration/phase_6/test_6b_scorer.py
}

run_phase_6c() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, and Phase 6A/6B coverage before Phase 6C."
  run_pytest "Phase 6C unit suite" --no-cov tests/test_anomalies_6c.py
  run_pytest "Phase 6C integration suite" --no-cov tests/integration/phase_6/test_6c_anomalies.py
}

run_phase_7a() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, and Phase 6 coverage before Phase 7A."
  run_pytest "Phase 7A unit suite" --no-cov tests/test_search_7a.py
  run_pytest "Phase 7A integration suite" --no-cov tests/integration/phase_7/test_search_7a.py
}

run_phase_7b() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, and Phase 7A coverage before Phase 7B."
  run_pytest "Phase 7B unit suite" --no-cov tests/test_discovery_7b.py
  run_pytest "Phase 7B integration suite" --no-cov tests/integration/phase_7/test_discovery_7b.py
}

run_phase_7c() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, and Phase 7A/7B coverage before Phase 7C."
  run_pytest "Phase 7C unit suite" --no-cov tests/test_report_7c.py
  run_pytest "Phase 7C integration suite" --no-cov tests/integration/phase_7/test_report_7c.py
}

run_phase_8a() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, and Phase 7 coverage before Phase 8A."
  run_pytest "Phase 8A integration suite" --no-cov tests/integration/phase_8/test_client_8a.py
}

run_phase_8b() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, Phase 7, and Phase 8A coverage before Phase 8B."
  run_pytest "Phase 8B integration suite" --no-cov tests/integration/phase_8/test_sampler_8b.py
}

run_phase_8c() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, Phase 7, and Phase 8A/8B coverage before Phase 8C."
  run_pytest "Phase 8C integration suite" --no-cov tests/integration/phase_8/test_enricher_8c.py
}

run_phase_9a() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, Phase 7, and Phase 8 coverage before Phase 9A."
  run_pytest "Phase 9A integration suite" --no-cov tests/integration/phase_9/test_enricher_9a.py
}

run_phase_9b() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, Phase 7, Phase 8, and Phase 9A coverage before Phase 9B."
  run_pytest "Phase 9B integration suite" --no-cov tests/integration/phase_9/test_sigilo_semantic_9b.py
}

run_phase_9c() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, Phase 7, Phase 8, and Phase 9A/9B coverage before Phase 9C."
  run_pytest "Phase 9C integration suite" --no-cov tests/integration/phase_9/test_cli_enrich_9c.py
}

run_phase_10a() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, Phase 7, Phase 8, and Phase 9 coverage before Phase 10A."
  run_pytest "Phase 10A integration suite" --no-cov tests/integration/phase_10/test_qa_10a.py
}

run_phase_10b() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, Phase 7, Phase 8, Phase 9, and Phase 10A coverage before Phase 10B."
  run_pytest "Phase 10B integration suite" --no-cov tests/integration/phase_10/test_vector_10b.py
}

run_phase_10c() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, Phase 7, Phase 8, Phase 9, and Phase 10A/10B coverage before Phase 10C."
  run_pytest "Phase 10C integration suite" --no-cov tests/integration/phase_10/test_cli_ask_10c.py
}

run_phase_11a() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, Phase 7, Phase 8, Phase 9, and Phase 10 coverage before Phase 11A."
  run_pytest "Phase 11A integration suite" --no-cov tests/integration/phase_11/test_snapshot_11a.py
}

run_phase_11b() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, Phase 7, Phase 8, Phase 9, Phase 10, and Phase 11A coverage before Phase 11B."
  run_pytest "Phase 11B integration suite" --no-cov tests/integration/phase_11/test_snapshot_diff_11b.py
}

run_phase_11c() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, Phase 7, Phase 8, Phase 9, Phase 10, and Phase 11A/11B coverage before Phase 11C."
  run_pytest "Phase 11C integration suite" --no-cov tests/integration/phase_11/test_cli_history_11c.py
}

run_phase_12a() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, Phase 7, Phase 8, Phase 9, Phase 10, and Phase 11 coverage before Phase 12A."
  run_pytest "Phase 12A integration suite" --no-cov tests/integration/phase_12/test_standalone_12a.py
}

run_phase_12b() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, Phase 7, Phase 8, Phase 9, Phase 10, Phase 11, and Phase 12A coverage before Phase 12B."
  run_pytest "Phase 12B integration suite" --no-cov tests/integration/phase_12/test_structured_12b.py
}

run_phase_12c() {
  echo "[atlas-tests] Re-checking historical Phase 0, Phase 1, Phase 2, Phase 3, Phase 4, Phase 5, Phase 6, Phase 7, Phase 8, Phase 9, Phase 10, Phase 11, and Phase 12A/12B coverage before Phase 12C."
  run_pytest "Phase 12C integration suite" --no-cov tests/integration/phase_12/test_report_exec_12c.py
}

run_phase_13a() {
  echo "[atlas-tests] Re-checking historical Phase 0 through Phase 12 coverage before Phase 13A."
  run_pytest "Phase 13A integration suite" --no-cov tests/integration/phase_13/test_sqlite_13a.py
}

run_phase_13b() {
  echo "[atlas-tests] Re-checking historical Phase 0 through Phase 12 and Phase 13A coverage before Phase 13B."
  run_pytest "Phase 13B integration suite" --no-cov tests/integration/phase_13/test_mariadb_13b.py
}

run_phase_13c() {
  echo "[atlas-tests] Re-checking historical Phase 0 through Phase 12 and Phase 13A/13B coverage before Phase 13C."
  run_pytest "Phase 13C integration suite" --no-cov tests/integration/phase_13/test_generic_13c.py
}

run_phase_14a() {
  echo "[atlas-tests] Re-checking historical Phase 0 through Phase 13 coverage before Phase 14A."
  run_pytest "Phase 14A integration suite" --no-cov tests/integration/phase_14/test_public_api_14a.py
}

run_phase_14b() {
  echo "[atlas-tests] Re-checking historical Phase 0 through Phase 13 and Phase 14A coverage before Phase 14B."
  run_pytest "Phase 14B integration suite" --no-cov tests/integration/phase_14/test_regression_14b.py
}

run_phase_14c() {
  echo "[atlas-tests] Re-checking historical Phase 0 through Phase 13, Phase 14A, and Phase 14B coverage before Phase 14C."
  run_pytest "Phase 14C integration suite" --no-cov tests/integration/phase_14/test_distribution_14c.py
}

run_full_phase_0_regression() {
  echo "[atlas-tests] Running full Phase 0 regression suite with informational coverage."
  run_pytest "Phase 0 full regression suite" --cov-fail-under=0 tests/integration/phase_0 tests/test_version.py tests/test_config.py tests/test_types.py tests/test_base_connector.py tests/test_connectors_factory.py
}

run_full_phase_1_regression() {
  echo "[atlas-tests] Running full Phase 1 regression suite with coverage."
  run_pytest "Phase 0 and 1 full regression suite" tests
}

run_full_phase_2_regression() {
  echo "[atlas-tests] Running full Phase 2 regression suite with coverage."
  run_pytest "Phase 0, 1, and 2 full regression suite" tests
}

run_full_phase_3_regression() {
  echo "[atlas-tests] Running full Phase 3 regression suite with coverage."
  run_pytest "Phase 0, 1, 2, and 3 full regression suite" tests
}

run_full_phase_4_regression() {
  echo "[atlas-tests] Running full Phase 4 regression suite with coverage."
  run_pytest "Phase 0, 1, 2, 3, and 4 full regression suite" tests
}

run_full_phase_5_regression() {
  echo "[atlas-tests] Running full Phase 5 regression suite with coverage."
  run_pytest "Phase 0, 1, 2, 3, 4, and 5 full regression suite" tests
}

run_full_phase_6_regression() {
  echo "[atlas-tests] Running full Phase 6 regression suite with coverage."
  run_pytest "Phase 0, 1, 2, 3, 4, 5, and 6 full regression suite" tests
}

run_full_phase_12_regression() {
  echo "[atlas-tests] Running full Phase 12 regression suite with coverage."
  run_pytest "Full Atlas regression suite through Phase 12" tests
}

run_full_phase_13_regression() {
  echo "[atlas-tests] Running full Phase 13 regression suite with coverage."
  run_pytest "Full Atlas regression suite through Phase 13" tests
}

run_full_phase_14_regression() {
  echo "[atlas-tests] Running full Phase 14 regression suite with coverage."
  run_pytest "Full Atlas regression suite through Phase 14" tests
}

run_full_phase_7_regression() {
  echo "[atlas-tests] Running full Phase 7 regression suite with coverage."
  run_pytest "Phase 0, 1, 2, 3, 4, 5, 6, and 7 full regression suite" tests
}

run_full_phase_8_regression() {
  echo "[atlas-tests] Running full Phase 8 regression suite with coverage."
  run_pytest "Phase 0, 1, 2, 3, 4, 5, 6, 7, and 8 full regression suite" tests
}

run_full_phase_9_regression() {
  echo "[atlas-tests] Running full Phase 9 regression suite with coverage."
  run_pytest "Phase 0, 1, 2, 3, 4, 5, 6, 7, 8, and 9 full regression suite" tests
}

run_full_phase_10_regression() {
  echo "[atlas-tests] Running full Phase 10 regression suite with coverage."
  run_pytest "Phase 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, and 10 full regression suite" tests
}

run_full_phase_11_regression() {
  echo "[atlas-tests] Running full Phase 11 regression suite with coverage."
  run_pytest "Phase 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, and 11 full regression suite" tests
}

echo "[atlas-tests] Using Python interpreter: $PYTHON_BIN"
echo "[atlas-tests] Ensuring editable development installation is available."
echo "[atlas-tests] Bootstrapping build backend tooling."
PIP_BOOTSTRAP_ARGS=(install --upgrade "setuptools>=77" "wheel>=0.42")
if [ -z "${VIRTUAL_ENV:-}" ]; then
  PIP_BOOTSTRAP_ARGS=(install --user --upgrade "setuptools>=77" "wheel>=0.42")
fi
"$PYTHON_BIN" -m pip "${PIP_BOOTSTRAP_ARGS[@]}"
if ! "$PYTHON_BIN" -c "import atlas, atlas.sigilo, build, mypy, pytest, ruff, sqlalchemy" >/dev/null 2>&1; then
  "$PYTHON_BIN" -m pip install --no-build-isolation -e ".[dev]"
else
  echo "[atlas-tests] Reusing the current editable development environment."
fi

case "$PHASE_FILTER" in
  0A)
    run_phase_0a
    ;;
  0B)
    run_phase_0b
    ;;
  0C)
    run_phase_0c
    ;;
  1A)
    run_phase_1a
    ;;
  1B)
    run_phase_1b
    ;;
  1C)
    run_phase_1c
    ;;
  1D)
    run_phase_1d
    ;;
  2A)
    run_phase_2a
    ;;
  2B)
    run_phase_2b
    ;;
  2C)
    run_phase_2c
    ;;
  3A)
    run_phase_3a
    ;;
  3B)
    run_phase_3b
    ;;
  3C)
    run_phase_3c
    ;;
  4A)
    run_phase_4a
    ;;
  4B)
    run_phase_4b
    ;;
  4C)
    run_phase_4c
    ;;
  5A)
    run_phase_5a
    ;;
  5B)
    run_phase_5b
    ;;
  5C)
    run_phase_5c
    ;;
  6A)
    run_phase_6a
    ;;
  6B)
    run_phase_6b
    ;;
  6C)
    run_phase_6c
    ;;
  7A)
    run_phase_7a
    ;;
  7B)
    run_phase_7b
    ;;
  7C)
    run_phase_7c
    ;;
  8A)
    run_phase_8a
    ;;
  8B)
    run_phase_8b
    ;;
  8C)
    run_phase_8c
    ;;
  9A)
    run_phase_9a
    ;;
  9B)
    run_phase_9b
    ;;
  9C)
    run_phase_9c
    ;;
  10A)
    run_phase_10a
    ;;
  10B)
    run_phase_10b
    ;;
  10C)
    run_phase_10c
    ;;
  11A)
    run_phase_11a
    ;;
  11B)
    run_phase_11b
    ;;
  11C)
    run_phase_11c
    ;;
  12A)
    run_phase_12a
    ;;
  12B)
    run_phase_12b
    ;;
  12C)
    run_phase_12c
    ;;
  13A)
    run_phase_13a
    ;;
  13B)
    run_phase_13b
    ;;
  13C)
    run_phase_13c
    ;;
  14A)
    run_phase_14a
    ;;
  14B)
    run_phase_14b
    ;;
  14C)
    run_phase_14c
    ;;
  ALL)
    run_full_phase_14_regression
    echo "[atlas-tests] Current full Phase 14 suite is green."
    ;;
  HISTORICAL)
    run_phase_0a
    run_phase_0b
    run_phase_0c
    run_phase_1a
    run_phase_1b
    run_phase_1c
    run_phase_1d
    run_phase_2a
    run_phase_2b
    run_phase_2c
    run_phase_3a
    run_phase_3b
    run_phase_3c
    run_phase_4a
    run_phase_4b
    run_phase_4c
    run_phase_5a
    run_phase_5b
    run_phase_5c
    run_phase_6a
    run_phase_6b
    run_phase_6c
    run_phase_7a
    run_phase_7b
    run_phase_7c
    run_phase_8a
    run_phase_8b
    run_phase_8c
    run_phase_9a
    run_phase_9b
    run_phase_9c
    run_phase_10a
    run_phase_10b
    run_phase_10c
    run_phase_11a
    run_phase_11b
    run_phase_11c
    run_phase_12a
    run_phase_12b
    run_phase_12c
    run_phase_13a
    run_phase_13b
    run_phase_13c
    run_phase_14a
    run_phase_14b
    run_phase_14c
    run_full_phase_0_regression
    run_full_phase_1_regression
    run_full_phase_2_regression
    run_full_phase_3_regression
    run_full_phase_4_regression
    run_full_phase_5_regression
    run_full_phase_6_regression
    run_full_phase_7_regression
    run_full_phase_8_regression
    run_full_phase_9_regression
    run_full_phase_10_regression
    run_full_phase_11_regression
    run_full_phase_12_regression
    run_full_phase_13_regression
    run_full_phase_14_regression
    echo "[atlas-tests] Historical and current Phase 0 through Phase 14 suites remain green."
    ;;
  *)
    echo "[atlas-tests] Unsupported ATLAS_TEST_PHASES value: $PHASE_FILTER" >&2
    exit 1
    ;;
esac
