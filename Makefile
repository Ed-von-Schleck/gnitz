ALL_DATA_DIRS := storage_test_data \
                 catalog_test_data zstore_test_data \
                 uring_test_data

LOG_DIR := .test_logs

.PHONY: all test clean rust-engine-test server release-server e2e e2e-release release-test

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

e2e: server
	cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/ -m "not slow" -v

e2e-release: release-server
	cd crates/gnitz-py && GNITZ_SERVER_BIN=../../gnitz-server-release GNITZ_WORKERS=4 uv run pytest tests/ -m "not slow" -v

release-test:
	$(MAKE) test
