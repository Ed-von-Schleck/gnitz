# Define environment and variables
PYPY_PATH ?= ../pypy
export PYTHONPATH := .:${PYPY_PATH}

RPYTHON          := pypy2 ${PYPY_PATH}/rpython/bin/rpython
RPYFLAGS_DEV     := --opt=1 --gc=incminimark --lldebug
RPYFLAGS_RELEASE := --opt=jit --gc=incminimark --lto
RPYFLAGS_NOJIT   := --opt=2   --gc=incminimark --lto
RPYFLAGS         ?= $(RPYFLAGS_DEV)
RELEASE_CFLAGS   ?= -march=native

ALL_DATA_DIRS := storage_test_data \
                 catalog_test_data zstore_test_data \
                 uring_test_data

LOG_DIR := .test_logs

.PHONY: all test clean rust-transport-debug rust-transport-release rust-engine-debug rust-engine-release rust-engine-test server pytest pytest-only e2e e2e-release release-server release-server-nojit release-test

all: test

# ---------------------------------------------------------------------------
# Test pipeline — Rust engine tests only (RPython tests fully converted)
# ---------------------------------------------------------------------------

test: rust-engine-test

# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------

clean:
	@echo "Cleaning all test data directories and logs..."
	@rm -rf $(ALL_DATA_DIRS)
	@rm -rf $(LOG_DIR)

rust-transport-debug:
	cd crates && cargo build -p gnitz-transport

rust-transport-release:
	cd crates && cargo build --release -p gnitz-transport

rust-engine-debug:
	cd crates && cargo build -p gnitz-engine

rust-engine-release:
	cd crates && cargo build --release -p gnitz-engine

rust-engine-test:
	cd crates && cargo test -p gnitz-engine

server: rust-transport-debug rust-engine-debug
	GNITZ_TRANSPORT_LIB=$(PWD)/crates/target/debug \
	GNITZ_ENGINE_LIB=$(PWD)/crates/target/debug \
	$(RPYTHON) $(RPYFLAGS) --output=gnitz-server-c gnitz/server/main.py

pytest: server
	cd py_client && uv run pytest tests/ -v
	cd py_client && GNITZ_WORKERS=4 uv run pytest tests/ -v

pytest-only:
	cd py_client && uv run pytest tests/ -v
	cd py_client && GNITZ_WORKERS=4 uv run pytest tests/ -v

e2e: gnitz-server-c
	cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/ -m "not slow" -v

e2e-release: gnitz-server-release-c
	cd crates/gnitz-py && GNITZ_SERVER_BIN=../../gnitz-server-release-c GNITZ_WORKERS=4 uv run pytest tests/ -m "not slow" -v

release-server: rust-transport-release rust-engine-release
	GNITZ_TRANSPORT_LIB=$(PWD)/crates/target/release \
	GNITZ_ENGINE_LIB=$(PWD)/crates/target/release \
	CFLAGS="$(RELEASE_CFLAGS)" $(RPYTHON) $(RPYFLAGS_RELEASE) --output=gnitz-server-release-c gnitz/server/main.py

release-server-nojit: rust-transport-release rust-engine-release
	GNITZ_TRANSPORT_LIB=$(PWD)/crates/target/release \
	GNITZ_ENGINE_LIB=$(PWD)/crates/target/release \
	CFLAGS="$(RELEASE_CFLAGS)" $(RPYTHON) $(RPYFLAGS_NOJIT) --output=gnitz-server-nojit-c gnitz/server/main.py

release-test:
	CFLAGS="$(RELEASE_CFLAGS)" $(MAKE) test
