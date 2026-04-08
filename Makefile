ALL_DATA_DIRS := storage_test_data \
                 catalog_test_data zstore_test_data \
                 uring_test_data

LOG_DIR := .test_logs

.PHONY: all test clean rust-engine-test server release-server pyext e2e e2e-release release-test bench bench-full bench-sweep bench-perf bench-perf-dwarf

# Benchmark knobs — override on the command line, e.g. make bench WORKERS=4 PERF=1
WORKERS ?= 1
CLIENTS ?= 1
FULL    ?=
PERF    ?=
PERF_DWARF ?=

all: test

# ---------------------------------------------------------------------------
# Test pipeline — Rust engine tests
# ---------------------------------------------------------------------------

test: rust-engine-test

# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------

clean:
	@echo "Cleaning all test data directories and logs..."
	@rm -rf $(ALL_DATA_DIRS)
	@rm -rf $(LOG_DIR)

rust-engine-test:
	cd crates && cargo test -p gnitz-engine

server:
	cd crates && cargo build -p gnitz-engine --bin gnitz-server
	cp crates/target/debug/gnitz-server gnitz-server

release-server:
	cd crates && cargo build --release -p gnitz-engine --bin gnitz-server
	cp crates/target/release/gnitz-server gnitz-server-release

pyext:
	cd crates/gnitz-py && uv run maturin develop 2>&1 | tail -1

e2e: server pyext
	cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/ -m "not slow" -v

e2e-release: release-server pyext
	cd crates/gnitz-py && GNITZ_SERVER_BIN=../../gnitz-server-release GNITZ_WORKERS=4 uv run pytest tests/ -m "not slow" -v

release-test:
	$(MAKE) test

# ---------------------------------------------------------------------------
# Benchmarks — SQL-level performance suite
# ---------------------------------------------------------------------------

bench: release-server
	cd crates/gnitz-py && uv run python ../../benchmarks/run.py \
		$(if $(FULL),--full) \
		--workers=$(WORKERS) --clients=$(CLIENTS) \
		$(if $(PERF),--perf --perf-stat) \
		$(if $(PERF_DWARF),--perf-dwarf)

bench-full: WORKERS = 4
bench-full: FULL    = 1
bench-full: bench

bench-sweep: release-server
	cd crates/gnitz-py && uv run python ../../benchmarks/run.py --full --workers=1,2,4 --clients=1,2,4

bench-perf: WORKERS = 4
bench-perf: FULL    = 1
bench-perf: PERF    = 1
bench-perf: bench

bench-perf-dwarf: WORKERS = 4
bench-perf-dwarf: FULL    = 1
bench-perf-dwarf: PERF_DWARF = 1
bench-perf-dwarf: bench
