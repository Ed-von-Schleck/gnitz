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

# The instruction set every build targets. Canonical copy lives in
# crates/.cargo/config.toml; it is restated here because Cargo's RUSTFLAGS
# environment variable REPLACES the config's rustflags rather than appending, so
# any recipe that sets RUSTFLAGS must carry this or it profiles a binary built
# against a different instruction set than the one `make release-server` ships.
# For a machine-specific build use `make bench-native`, which exports RUSTFLAGS.
TARGET_CPU ?= x86-64-v3

.PHONY: all help \
        test rust-engine-test fmt fmt-check clippy check verify \
        server release-server checked-server pyext pyext-release e2e e2e-tls e2e-release \
        e2e-checked e2e-debug release-test \
        clean distclean \
        bench bench-full bench-features bench-txn bench-sweep bench-sweep-dwarf \
        bench-perf bench-perf-dwarf bench-native bench-profile profiling-server profiling-server-dwarf

all: test

help: ## Show this help
	@grep -hE '^[a-zA-Z][a-zA-Z0-9_-]*:.*?##' $(MAKEFILE_LIST) \
		| sort | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Tests & quality gates
# ---------------------------------------------------------------------------

test: server ## Run all Rust workspace tests incl. gnitz-sql/gnitz-core integration (gnitz-py excluded — pyo3 extension can't link a test harness)
	cd crates && cargo test --workspace --exclude gnitz-py --features gnitz-sql/integration --features gnitz-core/integration --features gnitz-tokio/integration $(T)

rust-engine-test: ## Run only the gnitz-store + gnitz-server tests (faster inner loop)
	cd crates && cargo test -p gnitz-store -p gnitz-server $(T)

fmt: ## Format the whole workspace
	cd crates && cargo fmt --all

fmt-check: ## Check formatting without writing (CI gate)
	cd crates && cargo fmt --all --check

clippy: ## Lint the workspace incl. integration tests; warnings are errors
	cd crates && cargo clippy --workspace --all-targets --features gnitz-sql/integration --features gnitz-core/integration --features gnitz-tokio/integration -- -D warnings

check: ## Fast type-check without producing binaries
	cd crates && cargo check --workspace

verify: fmt-check clippy test ## Pre-commit gate: format + lint + tests

# ---------------------------------------------------------------------------
# Build & end-to-end
# ---------------------------------------------------------------------------

server: ## Build the debug server binary -> ./gnitz-server
	cd crates && cargo build -p gnitz-server
	cp crates/target/debug/gnitz-server gnitz-server

release-server: ## Build the release server binary -> ./gnitz-server-release
	cd crates && cargo build --release -p gnitz-server
	cp crates/target/release/gnitz-server gnitz-server-release

checked-server: ## Build the release server WITH the Z-set layout verifiers -> ./gnitz-server-checked
	cd crates && cargo build --profile release-checked -p gnitz-server
	cp crates/target/release-checked/gnitz-server gnitz-server-checked

pyext: ## Build & install the Python extension (debug) into the uv venv
	cd crates/gnitz-py && uv run maturin develop

pyext-release: ## Build & install the Python extension (release) into the uv venv
	cd crates/gnitz-py && uv run maturin develop --release

e2e: WORKERS = 4
e2e: server pyext ## Run the Python E2E suite (debug server, W=4; override WORKERS=/K=)
	cd crates/gnitz-py && GNITZ_WORKERS=$(WORKERS) uv run pytest tests/ -v $(if $(K),-k '$(K)')

e2e-tls: WORKERS = 4
e2e-tls: server pyext ## Run the full E2E suite over TLS (on-demand transport shakeout)
	cd crates/gnitz-py && GNITZ_TRANSPORT=tls GNITZ_WORKERS=$(WORKERS) \
		uv run pytest tests/ -v $(if $(K),-k '$(K)')

e2e-release: WORKERS = 4
e2e-release: release-server pyext ## Run the E2E suite against the release server
	cd crates/gnitz-py && GNITZ_SERVER_BIN=../../gnitz-server-release GNITZ_WORKERS=$(WORKERS) \
		uv run pytest tests/ -v $(if $(K),-k '$(K)')

# The debug-logging loop. It exists as a target so the documented way to chase a
# failure still rebuilds first: run `uv run pytest` by hand and you test whatever
# binary and extension happened to be installed last.
e2e-debug: WORKERS = 4
e2e-debug: server pyext ## Run the E2E suite with debug logging (W=4; use K= to narrow)
	cd crates/gnitz-py && GNITZ_LOG_LEVEL=debug GNITZ_WORKERS=$(WORKERS) \
		uv run pytest tests/ -x -v $(if $(K),-k '$(K)')

# Release codegen with the layout verifiers live — the only build where the
# kernels that ship and the checks that would catch them over-claiming both
# exist. An over-claimed batch aborts the server here instead of silently
# folding weights against the wrong element.
e2e-checked: WORKERS = 4
e2e-checked: checked-server pyext ## Run the E2E suite against the layout-verifying release server
	cd crates/gnitz-py && GNITZ_SERVER_BIN=../../gnitz-server-checked GNITZ_WORKERS=$(WORKERS) \
		uv run pytest tests/ -v $(if $(K),-k '$(K)')

release-test: e2e-release ## Validate the release build end-to-end

# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------

clean: ## Remove built binaries + per-run scratch data (keeps post-mortem logs)
	@echo "Removing server binaries and per-run scratch data..."
	@rm -f gnitz-server gnitz-server-release gnitz-server-checked gnitz-server-profiling
	rm -f crates/gnitz-py/python/gnitz/_native*.so
	@rm -rf tmp/pytest-of-* tmp/bench_*
	@# Cargo hardlinks each workspace library into the profile root from `deps/`
	@# and never removes one whose crate has left the workspace: `libgnitz_capi.*`
	@# outlived `crates/gnitz-capi/` by 307 MB. The links cost nothing to recreate
	@# — the next build re-links from `deps/` without recompiling — so deleting
	@# every one is both complete and free, and needs no member list to go stale.
	@rm -f crates/target/*/lib*.so crates/target/*/lib*.rlib \
	       crates/target/*/lib*.a crates/target/*/lib*.d

distclean: clean ## clean + cargo target cache + post-mortem logs
	cd crates && cargo clean
	@rm -f tmp/*.log

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

bench-features: release-server pyext-release ## Full features tier (per-feature maintenance cost), 4 workers
	cd crates/gnitz-py && uv run python ../../benchmarks/run.py --full --workers=4 -k features

bench-txn: release-server pyext-release ## Full transaction/HTAP/serving tiers, 4 workers
	cd crates/gnitz-py && uv run python ../../benchmarks/run.py --full --workers=4 -k "txn or htap or serving"

bench-sweep: release-server pyext-release ## Sweep workers×clients over {1,2,4}
	cd crates/gnitz-py && uv run python ../../benchmarks/run.py --full --workers=1,2,4 --clients=1,2,4

bench-perf: WORKERS = 4
bench-perf: FULL    = 1
bench-perf: PERF    = 1
bench-perf: bench ## Full + perf record + perf stat

# Builds for THIS machine only — the binary may SIGILL anywhere else, and its
# numbers are not comparable with a default-built run. For answering "what does
# this box leave on the table", not for committing a decision.
#
# Exported rather than passed per-recipe: a target-specific variable reaches the
# prerequisites too, so `release-server` and `pyext-release` pick it up without
# either of them having to know about RUSTFLAGS. Setting it in the environment
# is also what overrides crates/.cargo/config.toml.
bench-native: export RUSTFLAGS = -C target-cpu=native
bench-native: WORKERS = 4
bench-native: FULL    = 1
bench-native: bench ## Full benchmark built for the host CPU (non-portable binary)

bench-perf-dwarf: WORKERS = 4
bench-perf-dwarf: FULL       = 1
bench-perf-dwarf: PERF_DWARF = 1
bench-perf-dwarf: bench ## Full + perf with DWARF call graphs

profiling-server: ## Build frame-pointer release server -> ./gnitz-server-profiling (accurate perf call graphs)
	cd crates && RUSTFLAGS="-C target-cpu=$(TARGET_CPU) -C force-frame-pointers=yes" CARGO_TARGET_DIR=target/profiling \
		cargo build --release -p gnitz-server
	cp crates/target/profiling/release/gnitz-server gnitz-server-profiling

bench-profile: profiling-server pyext-release ## Profile incremental view maintenance under perf (frame pointers, W=4)
	cd crates/gnitz-py && GNITZ_SERVER_BIN=$(abspath gnitz-server-profiling) \
		uv run python ../../benchmarks/run.py --full --workers=4 --perf -k view_maintenance
	cd crates/gnitz-py && uv run python ../../benchmarks/report.py

profiling-server-dwarf: ## Build release server with DWARF unwind tables + line info -> ./gnitz-server-profiling-dwarf
	cd crates && RUSTFLAGS="-C target-cpu=$(TARGET_CPU) -C force-unwind-tables=yes -C debuginfo=1" CARGO_TARGET_DIR=target/profiling-dwarf \
		cargo build --release -p gnitz-server
	cp crates/target/profiling-dwarf/release/gnitz-server gnitz-server-profiling-dwarf

# The flagship profiling command: sweep over the {1,4}×{1,4} worker×client
# corner combinations under perf DWARF call-graph recording + perf stat. Uses the
# DWARF-capable server build (.eh_frame unwind tables), and writes a per-combo
# perf.data + flamegraph plus a combined report.
bench-sweep-dwarf: profiling-server-dwarf pyext-release ## Full sweep workers×clients {1,4} under perf DWARF profiling + report
	cd crates/gnitz-py && GNITZ_SERVER_BIN=$(abspath gnitz-server-profiling-dwarf) \
		uv run python ../../benchmarks/run.py --full --workers=1,4 --clients=1,4 --perf-dwarf --perf-stat
	cd crates/gnitz-py && uv run python ../../benchmarks/report.py --all
