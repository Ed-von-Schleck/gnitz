# ---------------------------------------------------------------------------
# GnitzDB Makefile.  Run `make help` for the target list.
# ---------------------------------------------------------------------------

SHELL         := bash
.SHELLFLAGS   := -eu -o pipefail -c          # fail on errors *inside* pipelines too
.DEFAULT_GOAL := help

# Knobs — override on the command line, e.g.
#   make bench SCENARIO=join,fanout REGIME=compacted
#   make test T=some_test_name
#   make e2e WORKERS=1 K='joins and not slow'
WORKERS    ?= 1                              # e2e and the benchmarks override this to 4 (see below)
ROWS       ?=                                # bench: rows a scenario loads
SCENARIO   ?=                                # bench: comma-separated scenario names
FAMILY     ?=                                # bench: comma-separated scenario families
REGIME     ?=                                # bench: comma-separated regimes (l0, compacted, checkpointed), or every
RAM_TIER   ?=                                # bench: GNITZ_RAM_TIER_BYTES of the server
PROFILE    ?=                                # bench-profile: comma-separated layers (default: all of them)
LABEL      ?=                                # bench: a label in the results directory's name
A          ?=                                # bench-compare: the results directory before (default: the second latest)
B          ?=                                # bench-compare: the results directory after (default: the latest)
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
        bench-rust bench bench-full bench-disk bench-profile bench-compare bench-report bench-native \
        profiling-server pyext-profiling

all: test

help: ## Show this help
	@grep -hE '^[a-zA-Z][a-zA-Z0-9_-]*:.*?##' $(MAKEFILE_LIST) \
		| sort | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Tests & quality gates
# ---------------------------------------------------------------------------

test: server ## Run all Rust workspace tests incl. gnitz-sql/gnitz-core integration (gnitz-py excluded — pyo3 extension can't link a test harness)
	cd crates && cargo test --workspace --exclude gnitz-py --features gnitz-sql/integration --features gnitz-core/integration --features gnitz-tokio/integration $(T)

rust-engine-test: ## Run only the gnitz-foundation + gnitz-zset + gnitz-store + gnitz-server tests (faster inner loop)
	cd crates && cargo test -p gnitz-foundation -p gnitz-zset -p gnitz-store -p gnitz $(T)

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
	cd crates && cargo build -p gnitz
	cp crates/target/debug/gnitz-server gnitz-server

release-server: ## Build the release server binary -> ./gnitz-server-release
	cd crates && cargo build --release -p gnitz
	cp crates/target/release/gnitz-server gnitz-server-release

checked-server: ## Build the release server WITH the Z-set layout verifiers -> ./gnitz-server-checked
	cd crates && cargo build --profile release-checked -p gnitz
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

# `GNITZ_RELEASE=1` travels with the binary, not beside it: this is the one
# build with `debug_assertions` off, so every `GNITZ_INJECT_*` seam is folded
# away and the tests that drive one must skip. Without it those tests inject
# into a server that cannot honour the request and then wait for an abort that
# never comes.
e2e-release: WORKERS = 4
e2e-release: release-server pyext ## Run the E2E suite against the release server
	cd crates/gnitz-py && GNITZ_SERVER_BIN=../../gnitz-server-release GNITZ_RELEASE=1 \
		GNITZ_WORKERS=$(WORKERS) uv run pytest tests/ -v $(if $(K),-k '$(K)')

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
# Benchmarks — the in-process Rust microbenchmarks and the whole-program suite
# ---------------------------------------------------------------------------

# The in-process microbenchmarks `make bench` cannot isolate. A green run says
# they still execute, not that the number any of them prints is meaningful.
#
# `--tests` excludes doctests: `--ignored` makes an ```ignore fenced example
# compile, and those are illustrative fragments that do not.
#
# The `_bench` filter keeps out the fault-seam tests, which a release build
# ignores because the seam folds away, and which `--ignored` alone would run.
bench-rust: release-server ## Run every Rust microbenchmark in release (T= runs one)
	mkdir -p tmp
	cd crates && GNITZ_SERVER_BIN=$(abspath gnitz-server-release) TMPDIR=$(abspath tmp) \
		cargo test --release --workspace --exclude gnitz-py --tests \
		--features gnitz-sql/integration --features gnitz-core/integration --features gnitz-tokio/integration \
		$(or $(T),_bench) \
		-- --ignored --nocapture --test-threads=1

# One scenario is one fresh server taken through phases; a run writes a
# directory under benchmarks/results/ and prints every phase as one line. The
# numbers of record are counts — instructions, syscalls, syncs, bytes — so two
# runs of the same code agree to a fraction of a percent on any machine.
# Held in a variable: a comma in a function's argument would split it.
DISK_FAMILIES := shape,mutation,policy,join
BENCH = cd crates/gnitz-py && uv run python ../../benchmarks/bench.py
BENCH_RUN = $(BENCH) run --workers=$(WORKERS) \
		$(if $(ROWS),--rows=$(ROWS)) $(if $(SCENARIO),--scenario=$(SCENARIO)) $(if $(FAMILY),--family=$(FAMILY)) \
		$(if $(RAM_TIER),--env=GNITZ_RAM_TIER_BYTES=$(RAM_TIER)) $(if $(LABEL),--label=$(LABEL))

bench: WORKERS = 4
bench: release-server pyext-release ## Whole-program benchmark: every scenario, 4 workers (knobs: SCENARIO, FAMILY, REGIME, ROWS, WORKERS, LABEL)
	$(BENCH_RUN) $(if $(REGIME),--regime=$(REGIME))

bench-full: WORKERS = 1,4
bench-full: release-server pyext-release ## Every scenario under each of its storage regimes, at 1 and at 4 workers
	$(BENCH_RUN) --regime=$(or $(REGIME),every)

# Bytes, not time: the same scenarios, read for what the data directory holds
# once a checkpoint has put every store on disk.
bench-disk: WORKERS = 4
bench-disk: release-server pyext-release ## Disk footprint by store and region, of the scenarios read for their bytes, under each regime
	$(BENCH_RUN) --regime=$(or $(REGIME),every) $(if $(SCENARIO)$(FAMILY),,--family=$(DISK_FAMILIES))
	$(BENCH) report --disk

bench-compare: ## What moved between two results directories (A=<before> B=<after>; the two latest by default)
	$(BENCH) compare $(if $(A)$(B),$(abspath $(A)) $(abspath $(B)))

bench-report: ## Print the latest results (SCENARIO= prints those phase by phase, with their profile)
	$(BENCH) report $(if $(SCENARIO),--scenario=$(SCENARIO))

# Builds for THIS machine only — the binary may SIGILL anywhere else, and its
# numbers are not comparable with a default-built run. For answering "what does
# this box leave on the table", not for committing a decision.
#
# Exported rather than passed per-recipe: a target-specific variable reaches the
# prerequisites too, so `release-server` and `pyext-release` pick it up without
# either of them having to know about RUSTFLAGS. Setting it in the environment
# is also what overrides crates/.cargo/config.toml.
bench-native: export RUSTFLAGS = -C target-cpu=native
bench-native: bench ## The benchmark built for the host CPU (non-portable binary)

# A profile reads its stacks off frame pointers and names an inlined function
# from debug info, so both builds carry both. They go to a target directory of
# their own: changed RUSTFLAGS would otherwise recompile the release build too.
PROFILING = CARGO_PROFILE_RELEASE_DEBUG=1 RUSTFLAGS="-C target-cpu=$(TARGET_CPU) -C force-frame-pointers=yes"

profiling-server: ## Build the release server with frame pointers and debug info -> ./gnitz-server-profiling
	cd crates && $(PROFILING) CARGO_TARGET_DIR=target/profiling cargo build --release -p gnitz
	cp crates/target/profiling/release/gnitz-server gnitz-server-profiling

pyext-profiling: ## Build & install the Python extension with frame pointers and debug info
	cd crates/gnitz-py && $(PROFILING) CARGO_TARGET_DIR=$(abspath crates/target/profiling) uv run maturin develop --release

# Each layer has a run of its own beside the unprofiled one, since a layer
# distorts what the others would see. More rows than `make bench` loads, so a
# phase lasts long enough to be sampled.
bench-profile: WORKERS = 4
bench-profile: ROWS := $(or $(ROWS),1000000)
bench-profile: profiling-server pyext-profiling ## Profile scenarios: flamegraphs on and off CPU, syscalls, ring ops, syncs, mallocs (knobs: SCENARIO, PROFILE, ROWS)
	$(BENCH_RUN) $(if $(REGIME),--regime=$(REGIME)) --server=$(abspath gnitz-server-profiling) \
		--profile=$(or $(PROFILE),all)
