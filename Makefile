# ---------------------------------------------------------------------------
# GnitzDB Makefile.  Run `make help` for the target list.
# ---------------------------------------------------------------------------

SHELL         := bash
.SHELLFLAGS   := -eu -o pipefail -c          # fail on errors *inside* pipelines too
.DEFAULT_GOAL := help

# Knobs — override on the command line, e.g.
#   make bench WORKERS=4 PERF=1
#   make test T=some_test_name
#   make e2e WORKERS=1 K='joins and not slow'
WORKERS    ?= 1                              # e2e overrides this to 4 (see below)
CLIENTS    ?= 1
FULL       ?=
PERF       ?=
PERF_DWARF ?=
T          ?=                                # cargo test name filter
K          ?=                                # pytest -k expression

.PHONY: all help \
        test rust-engine-test fmt fmt-check clippy check verify \
        server release-server pyext pyext-release e2e e2e-release release-test \
        clean distclean \
        bench bench-full bench-sweep bench-perf bench-perf-dwarf bench-profile profiling-server

all: test

help: ## Show this help
	@grep -hE '^[a-zA-Z][a-zA-Z0-9_-]*:.*?##' $(MAKEFILE_LIST) \
		| sort | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Tests & quality gates
# ---------------------------------------------------------------------------

test: ## Run all Rust workspace tests (gnitz-py excluded — pyo3 extension can't link a test harness)
	cd crates && cargo test --workspace --exclude gnitz-py $(T)

rust-engine-test: ## Run only the gnitz-engine tests (faster inner loop)
	cd crates && cargo test -p gnitz-engine $(T)

fmt: ## Format the whole workspace
	cd crates && cargo fmt --all

fmt-check: ## Check formatting without writing (CI gate)
	cd crates && cargo fmt --all --check

clippy: ## Lint the workspace; warnings are errors
	cd crates && cargo clippy --workspace --all-targets -- -D warnings

check: ## Fast type-check without producing binaries
	cd crates && cargo check --workspace

verify: fmt-check clippy test ## Pre-commit gate: format + lint + tests

# ---------------------------------------------------------------------------
# Build & end-to-end
# ---------------------------------------------------------------------------

server: ## Build the debug server binary -> ./gnitz-server
	cd crates && cargo build -p gnitz-engine --bin gnitz-server
	cp crates/target/debug/gnitz-server gnitz-server

release-server: ## Build the release server binary -> ./gnitz-server-release
	cd crates && cargo build --release -p gnitz-engine --bin gnitz-server
	cp crates/target/release/gnitz-server gnitz-server-release

pyext: ## Build & install the Python extension (debug) into the uv venv
	cd crates/gnitz-py && uv run maturin develop

pyext-release: ## Build & install the Python extension (release) into the uv venv
	cd crates/gnitz-py && uv run maturin develop --release

e2e: WORKERS = 4
e2e: server pyext ## Run the Python E2E suite (debug server, W=4; override WORKERS=/K=)
	cd crates/gnitz-py && GNITZ_WORKERS=$(WORKERS) uv run pytest tests/ -v $(if $(K),-k '$(K)')

e2e-release: WORKERS = 4
e2e-release: release-server pyext ## Run the E2E suite against the release server
	cd crates/gnitz-py && GNITZ_SERVER_BIN=../../gnitz-server-release GNITZ_WORKERS=$(WORKERS) \
		uv run pytest tests/ -v $(if $(K),-k '$(K)')

release-test: e2e-release ## Validate the release build end-to-end

# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------

clean: ## Remove built binaries + per-run scratch data (keeps post-mortem logs)
	@echo "Removing server binaries and per-run scratch data..."
	@rm -f gnitz-server gnitz-server-release gnitz-server-profiling
	@rm -rf ~/git/gnitz/tmp/gnitz_*

distclean: clean ## clean + cargo target cache + post-mortem logs
	cd crates && cargo clean
	@rm -f ~/git/gnitz/tmp/*.log

# ---------------------------------------------------------------------------
# Benchmarks — SQL-level performance suite (release server + release planner)
# ---------------------------------------------------------------------------

bench: release-server pyext-release ## Run the SQL benchmark suite
	cd crates/gnitz-py && uv run python ../../benchmarks/run.py \
		$(if $(FULL),--full) \
		--workers=$(WORKERS) --clients=$(CLIENTS) \
		$(if $(PERF),--perf --perf-stat) \
		$(if $(PERF_DWARF),--perf-dwarf)

bench-full: WORKERS = 4
bench-full: FULL    = 1
bench-full: bench ## Full benchmark mode, 4 workers

bench-sweep: release-server pyext-release ## Sweep workers×clients over {1,2,4}
	cd crates/gnitz-py && uv run python ../../benchmarks/run.py --full --workers=1,2,4 --clients=1,2,4

bench-perf: WORKERS = 4
bench-perf: FULL    = 1
bench-perf: PERF    = 1
bench-perf: bench ## Full + perf record + perf stat

bench-perf-dwarf: WORKERS = 4
bench-perf-dwarf: FULL       = 1
bench-perf-dwarf: PERF_DWARF = 1
bench-perf-dwarf: bench ## Full + perf with DWARF call graphs

profiling-server: ## Build frame-pointer release server -> ./gnitz-server-profiling (accurate perf call graphs)
	cd crates && RUSTFLAGS="-C force-frame-pointers=yes" CARGO_TARGET_DIR=target/profiling \
		cargo build --release -p gnitz-engine --bin gnitz-server
	cp crates/target/profiling/release/gnitz-server gnitz-server-profiling

bench-profile: profiling-server pyext-release ## Profile incremental view maintenance under perf (frame pointers, W=4)
	cd crates/gnitz-py && GNITZ_SERVER_BIN=$(abspath gnitz-server-profiling) \
		uv run python ../../benchmarks/run.py --full --workers=4 --perf -k view_maintenance
	cd crates/gnitz-py && uv run python ../../benchmarks/report.py
