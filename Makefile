# Define environment and variables
PYPY_PATH ?= ../pypy
export PYTHONPATH := .:${PYPY_PATH}

RPYTHON          := pypy2 ${PYPY_PATH}/rpython/bin/rpython
RPYFLAGS_DEV     := --opt=1 --gc=incminimark --lldebug
RPYFLAGS_RELEASE := --opt=jit --gc=incminimark --lto
RPYFLAGS_NOJIT   := --opt=2   --gc=incminimark --lto
RPYFLAGS         ?= $(RPYFLAGS_DEV)
RELEASE_CFLAGS   ?= -march=native

TEST_FILES := \
	rpython_tests/core_comprehensive_test.py \
	rpython_tests/storage_comprehensive_test.py \
	rpython_tests/dbsp_comprehensive_test.py \
	rpython_tests/catalog_comprehensive_test.py \
	rpython_tests/vm_comprehensive_test.py \
	rpython_tests/compile_graph_test.py \
	rpython_tests/server_test.py \
	rpython_tests/multicore_test.py \
	rpython_tests/eventfd_test.py \
	rpython_tests/ipc_transport_test.py \
	rpython_tests/uring_test.py

# Derived names
TEST_BINS := $(notdir $(TEST_FILES:.py=-c))
RUN_TARGETS := $(addprefix run-,$(TEST_BINS))

# Per-test data directories (only tests that create one)
DATA_DIR_storage_comprehensive_test-c := storage_test_data
DATA_DIR_dbsp_comprehensive_test-c := dbsp_test_data
DATA_DIR_catalog_comprehensive_test-c := catalog_test_data
DATA_DIR_vm_comprehensive_test-c := vm_test_data
DATA_DIR_compile_graph_test-c := compile_graph_test_data
DATA_DIR_server_test-c := server_test_data
DATA_DIR_multicore_test-c := multicore_test_data
DATA_DIR_eventfd_test-c := eventfd_test_data
DATA_DIR_ipc_transport_test-c := ipc_transport_test_data
DATA_DIR_uring_test-c := uring_test_data

ALL_DATA_DIRS := storage_test_data dbsp_test_data \
                 catalog_test_data zstore_test_data vm_test_data \
                 compile_graph_test_data server_test_data \
                 multicore_test_data eventfd_test_data \
                 ipc_transport_test_data uring_test_data

LOG_DIR := .test_logs

.PHONY: all test clean rust-transport server pytest pytest-only e2e e2e-release prove release-server release-server-nojit release-test $(RUN_TARGETS)

all: test

# ---------------------------------------------------------------------------
# Sequential test pipeline
#
# Each test's compile+run output is captured to .test_logs/<name>.log.
# On success: prints one PASS line.
# On failure: prints one FAIL line. Full log is shown in the summary.
#
# Tests compile sequentially to avoid races in the RPython build system.
# RPython's platform.compile() writes temp/object files during compilation;
# parallel invocations sharing the same gcc_cache can race on those files.
# gcc_cache uses atomic rename for its own writes, but the underlying
# platform.compile() step is not safe for concurrent calls on a cold cache.
# ---------------------------------------------------------------------------

test:
	@mkdir -p $(LOG_DIR)
	@$(MAKE) --no-print-directory -k $(RUN_TARGETS); \
	 EXIT=$$?; \
	 echo ""; \
	 echo "========================================"; \
	 echo "  TEST SUMMARY"; \
	 echo "========================================"; \
	 FAILED=0; \
	 for log in $(LOG_DIR)/*.log; do \
	   name=$$(basename $$log .log); \
	   if [ -f "$(LOG_DIR)/$$name.pass" ]; then \
	     echo "  PASS  $$name"; \
	   else \
	     echo "  FAIL  $$name"; \
	     FAILED=$$((FAILED + 1)); \
	   fi; \
	 done; \
	 echo "========================================"; \
	 if [ $$FAILED -gt 0 ]; then \
	   echo ""; \
	   echo "$$FAILED test(s) FAILED. Logs below:"; \
	   echo ""; \
	   for log in $(LOG_DIR)/*.log; do \
	     name=$$(basename $$log .log); \
	     if [ ! -f "$(LOG_DIR)/$$name.pass" ]; then \
	       echo ""; \
	       echo "vvvvvvvv FULL LOG: $$name vvvvvvvv"; \
	       cat $$log; \
	       echo "^^^^^^^^ END LOG: $$name ^^^^^^^^"; \
	       echo ""; \
	     fi; \
	   done; \
	   exit 1; \
	 else \
	   echo "  ALL TESTS PASSED"; \
	   rm -rf $(LOG_DIR); \
	 fi

# Compile: produce binary from source (output captured by run target)
$(TEST_BINS): %-c: rpython_tests/%.py
	@$(RPYTHON) $(RPYFLAGS) $< 2>&1

# Run: compile + execute, capture ALL output to log file.
# Pre-clean: remove any stale binary, data dir, and previous log/pass for this test.
# On success: create .pass marker, clean up binary + data.
# On failure: leave binary + data for inspection.
$(RUN_TARGETS): run-%-c: rpython_tests/%.py
	@mkdir -p $(LOG_DIR)
	@rm -f $*-c $(LOG_DIR)/$*-c.log $(LOG_DIR)/$*-c.pass
	@rm -rf $(DATA_DIR_$*-c)
	@( \
	  echo "=== Compiling: $< ===" && \
	  $(RPYTHON) $(RPYFLAGS) $< && \
	  echo "=== Running: $*-c ===" && \
	  ./$*-c && \
	  echo "=== PASSED: $*-c ===" \
	) > $(LOG_DIR)/$*-c.log 2>&1 && \
	( \
	  touch $(LOG_DIR)/$*-c.pass && \
	  echo "  PASS  $*-c" && \
	  rm -f $*-c && \
	  rm -rf $(DATA_DIR_$*-c) \
	) || \
	( \
	  echo "  FAIL  $*-c (see $(LOG_DIR)/$*-c.log)" \
	)

# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------

clean:
	@echo "Cleaning all test binaries, data directories, and logs..."
	@rm -f $(TEST_BINS)
	@rm -rf $(ALL_DATA_DIRS)
	@rm -rf $(LOG_DIR)

rust-transport:
	cd rust_client && cargo build --release -p gnitz-transport

server: rust-transport
	GNITZ_TRANSPORT_LIB=$(PWD)/rust_client/target/release \
	$(RPYTHON) $(RPYFLAGS) --output=gnitz-server-c gnitz/server/main.py

pytest: server
	cd py_client && uv run pytest tests/ -v
	cd py_client && GNITZ_WORKERS=4 uv run pytest tests/ -v

pytest-only:
	cd py_client && uv run pytest tests/ -v
	cd py_client && GNITZ_WORKERS=4 uv run pytest tests/ -v

e2e: gnitz-server-c
	cd rust_client/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/ -m "not slow" -v

e2e-release: gnitz-server-release-c
	cd rust_client/gnitz-py && GNITZ_SERVER_BIN=../../gnitz-server-release-c GNITZ_WORKERS=4 uv run pytest tests/ -m "not slow" -v

prove:
	$(MAKE) -C proofs prove

release-server: rust-transport
	GNITZ_TRANSPORT_LIB=$(PWD)/rust_client/target/release \
	CFLAGS="$(RELEASE_CFLAGS)" $(RPYTHON) $(RPYFLAGS_RELEASE) --output=gnitz-server-release-c gnitz/server/main.py

release-server-nojit: rust-transport
	GNITZ_TRANSPORT_LIB=$(PWD)/rust_client/target/release \
	CFLAGS="$(RELEASE_CFLAGS)" $(RPYTHON) $(RPYFLAGS_NOJIT) --output=gnitz-server-nojit-c gnitz/server/main.py

release-test:
	CFLAGS="$(RELEASE_CFLAGS)" $(MAKE) test RPYFLAGS="$(RPYFLAGS_RELEASE)"
