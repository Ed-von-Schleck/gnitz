# Define environment and variables
PYPY_PATH ?= ../pypy
export PYTHONPATH := .:${PYPY_PATH}

RPYTHON  := pypy2 ${PYPY_PATH}/rpython/bin/rpython
RPYFLAGS := --opt=1 --gc=incminimark --lldebug
JOBS     ?= $(shell nproc 2>/dev/null || echo 4)

TEST_FILES := \
	rpython_tests/core_comprehensive_test.py \
	rpython_tests/storage_comprehensive_test.py \
	rpython_tests/dbsp_comprehensive_test.py \
	rpython_tests/zstore_comprehensive_test.py \
	rpython_tests/catalog_comprehensive_test.py \
	rpython_tests/catalog_additional_test.py \
	rpython_tests/vm_comprehensive_test.py \
	rpython_tests/ipc_comprehensive_test.py \
	rpython_tests/expr_test.py \
	rpython_tests/compile_graph_test.py \
	rpython_tests/server_test.py \
	rpython_tests/partitioned_table_test.py \
	rpython_tests/master_worker_test.py \
	rpython_tests/exchange_test.py

# Derived names
TEST_BINS := $(notdir $(TEST_FILES:.py=-c))
RUN_TARGETS := $(addprefix run-,$(TEST_BINS))

# Per-test data directories (only tests that create one)
DATA_DIR_storage_comprehensive_test-c := storage_test_data
DATA_DIR_dbsp_comprehensive_test-c := dbsp_test_data
DATA_DIR_zstore_comprehensive_test-c := zstore_test_data
DATA_DIR_catalog_comprehensive_test-c := catalog_test_data
DATA_DIR_catalog_additional_test-c := catalog_additional_test_data
DATA_DIR_vm_comprehensive_test-c := vm_test_data
DATA_DIR_compile_graph_test-c := compile_graph_test_data
DATA_DIR_server_test-c := server_test_data
DATA_DIR_partitioned_table_test-c := partitioned_table_test_data
DATA_DIR_master_worker_test-c := master_worker_test_data
DATA_DIR_exchange_test-c := exchange_test_data

ALL_DATA_DIRS := storage_test_data dbsp_test_data zstore_test_data \
                 catalog_test_data catalog_additional_test_data vm_test_data \
                 compile_graph_test_data server_test_data partitioned_table_test_data \
                 master_worker_test_data exchange_test_data

LOG_DIR := .test_logs

.PHONY: all test clean server pytest pytest-only e2e prove $(RUN_TARGETS)

all: test

# ---------------------------------------------------------------------------
# Parallel test pipeline
#
# Each test's compile+run output is captured to .test_logs/<name>.log.
# On success: prints one PASS line.
# On failure: prints one FAIL line. Full log is shown in the summary.
#
# Cache warming: first test compiles serially to populate gcc_cache.
# ---------------------------------------------------------------------------

FIRST_TEST := core_comprehensive_test
REST_RUN_TARGETS := $(filter-out run-$(FIRST_TEST)-c,$(RUN_TARGETS))

test:
	@$(MAKE) --no-print-directory clean
	@mkdir -p $(LOG_DIR)
	@$(MAKE) --no-print-directory run-$(FIRST_TEST)-c
	@$(MAKE) --no-print-directory -j$(JOBS) -k $(REST_RUN_TARGETS); \
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
# On success: create .pass marker, clean up binary + data.
# On failure: leave binary + data for inspection.
$(RUN_TARGETS): run-%-c: rpython_tests/%.py
	@mkdir -p $(LOG_DIR)
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

server:
	$(RPYTHON) $(RPYFLAGS) gnitz/server/main.py
	@mv main-c gnitz-server-c

pytest: server
	cd py_client && uv run pytest tests/ -v
	cd py_client && GNITZ_WORKERS=4 uv run pytest tests/ -v

pytest-only:
	cd py_client && uv run pytest tests/ -v
	cd py_client && GNITZ_WORKERS=4 uv run pytest tests/ -v

e2e: gnitz-server-c
	cd rust_client/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/ -v --tb=short

prove:
	$(MAKE) -C proofs prove
