PYTHON ?= $(shell command -v python3.12 || command -v python3.11 || command -v python3)
PIP := $(PYTHON) -m pip
PYTEST := $(PYTHON) -m pytest
RUFF := $(PYTHON) -m ruff
MYPY := $(PYTHON) -m mypy
BUILD := $(PYTHON) -m build
PDOC := $(PYTHON) -m pdoc
TWINE := $(PYTHON) -m twine

SRC_DIR := atlas
TESTS_DIR := tests
DIST_DIR := dist
DOCS_DIR := docs/api
C_BUILD_HELPER := $(PYTHON) atlas/_c/build_lib.py

.PHONY: build check-dist build-c build-c-make docs clean-c test test-cli test-sigilo test-c-smoke test-integration test-all lint fmt typecheck clean install-dev help

install-dev:
	@echo "[atlas] Installing editable development environment..."
	$(PIP) install -e ".[dev]"
	@echo "[atlas] Editable installation completed."

build:
	@echo "[atlas] Building wheel and sdist..."
	$(BUILD) --no-isolation --outdir $(DIST_DIR)
	@echo "[atlas] Build completed in $(DIST_DIR)/."

check-dist:
	@echo "[atlas] Validating distribution metadata..."
	$(TWINE) check $(DIST_DIR)/*
	@echo "[atlas] Distribution metadata is valid."

docs:
	@echo "[atlas] Generating API documentation with pdoc..."
	@$(PYTHON) -c "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('pdoc') else 1)" || (echo "[atlas] pdoc is not installed. Run 'make install-dev' or install the 'dev' extra." && exit 1)
	$(PDOC) atlas --output-directory $(DOCS_DIR) --docformat google
	@echo "[atlas] Documentation generated in $(DOCS_DIR)/."

build-c:
	@echo "[atlas] Building libatlas_sigilo..."
	$(C_BUILD_HELPER) build
	@echo "[atlas] Native sigilo build completed."

build-c-make:
	@echo "[atlas] Building libatlas_sigilo via Makefile fallback..."
	$(C_BUILD_HELPER) build --prefer-make
	@echo "[atlas] Makefile fallback build completed."

test:
	@echo "[atlas] Running unit tests..."
	$(PYTEST) $(TESTS_DIR) -m "not integration" --tb=short
	@echo "[atlas] Unit tests completed."

test-cli:
	@echo "[atlas] Running Phase 5 CLI suites..."
	$(PYTEST) tests/test_scan_5a.py tests/test_open_5b.py tests/test_info_5c.py tests/integration/phase_5 --tb=short
	@echo "[atlas] Phase 5 CLI suites completed."

test-sigilo: build-c
	@echo "[atlas] Running sigilo unit and integration tests..."
	$(PYTEST) tests/test_c_render.py tests/test_sigilo_3b.py tests/test_build_3c.py tests/integration/phase_3 --tb=short
	@echo "[atlas] Sigilo tests completed."

test-c-smoke:
	@echo "[atlas] Running standalone C smoke test..."
	bash tests/test_c_library.sh
	@echo "[atlas] Standalone C smoke test completed."

test-integration:
	@echo "[atlas] Running integration tests..."
	$(PYTEST) $(TESTS_DIR)/integration --tb=short
	@echo "[atlas] Integration tests completed."

test-all:
	@echo "[atlas] Running the full test suite..."
	$(PYTEST) $(TESTS_DIR) --tb=short
	@echo "[atlas] Full test suite completed."

lint:
	@echo "[atlas] Running Ruff checks..."
	$(RUFF) check $(SRC_DIR) $(TESTS_DIR)
	$(RUFF) format --check $(SRC_DIR) $(TESTS_DIR)
	@echo "[atlas] Ruff checks completed."

fmt:
	@echo "[atlas] Formatting code..."
	$(RUFF) format $(SRC_DIR) $(TESTS_DIR)
	$(RUFF) check --fix $(SRC_DIR) $(TESTS_DIR)
	@echo "[atlas] Formatting completed."

typecheck:
	@echo "[atlas] Running mypy..."
	$(MYPY) $(SRC_DIR)
	@echo "[atlas] Type checking completed."

clean:
	@echo "[atlas] Cleaning generated artifacts..."
	$(C_BUILD_HELPER) clean || true
	rm -rf $(DIST_DIR) build .pytest_cache .mypy_cache .ruff_cache htmlcov
	rm -rf *.egg-info .coverage coverage.xml
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.so" -delete
	find . -type f -name "*.dylib" -delete
	find . -type f -name "*.dll" -delete
	@echo "[atlas] Clean completed."

clean-c:
	@echo "[atlas] Cleaning libatlas_sigilo artifacts..."
	$(C_BUILD_HELPER) clean
	@echo "[atlas] Native sigilo artifacts removed."

help:
	@echo ""
	@echo "Atlas Datamap targets"
	@echo ""
	@echo "  make install-dev      Install editable package with dev dependencies"
	@echo "  make build            Build wheel and sdist"
	@echo "  make check-dist       Validate built artifacts with twine check"
	@echo "  make build-c          Build the native sigilo library"
	@echo "  make build-c-make     Force the Makefile fallback build"
	@echo "  make docs             Generate API documentation with pdoc"
	@echo "  make test             Run unit tests"
	@echo "  make test-cli         Run the Phase 5 CLI suites"
	@echo "  make test-sigilo      Run the sigilo-focused suites"
	@echo "  make test-c-smoke     Run the standalone C smoke test"
	@echo "  make test-integration Run integration tests"
	@echo "  make test-all         Run every test"
	@echo "  make lint             Run Ruff lint and format checks"
	@echo "  make fmt              Auto-format code with Ruff"
	@echo "  make typecheck        Run mypy"
	@echo "  make clean-c          Remove native sigilo build artifacts"
	@echo "  make clean            Remove generated artifacts"
	@echo "  make help             Show this help"
	@echo ""
