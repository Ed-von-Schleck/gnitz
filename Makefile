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
	rpython_tests/server_test.py

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

ALL_DATA_DIRS := storage_test_data dbsp_test_data zstore_test_data \
                 catalog_test_data catalog_additional_test_data vm_test_data \
                 compile_graph_test_data server_test_data

.PHONY: all test clean server pytest pytest-only $(RUN_TARGETS)

all: test

# ---------------------------------------------------------------------------
# Parallel test pipeline
#
# `make test` compiles and runs all tests in parallel (up to JOBS).
# Each run target depends on its binary, so Make pipelines compilation
# and execution: as soon as a binary is ready, its test starts running
# while other binaries are still compiling.
#
# Fail-fast: Make's default -j behaviour stops starting new targets on
# first failure and waits for in-flight targets to finish.
#
# Cleanup: only successful tests clean up their binary and data dir.
# Failed tests leave artifacts behind for inspection.
# ---------------------------------------------------------------------------

test:
	@$(MAKE) --no-print-directory clean
	@$(MAKE) --no-print-directory -j$(JOBS) $(RUN_TARGETS)
	@echo ""
	@echo "========================================"
	@echo "  ALL TESTS PASSED"
	@echo "========================================"

# Compile: produce binary from source
$(TEST_BINS): %-c: rpython_tests/%.py
	@echo "======================================== Compiling: $< ========================================"
	@$(RPYTHON) $(RPYFLAGS) $<

# Run: execute binary, clean up own artifacts on success only
$(RUN_TARGETS): run-%-c: %-c
	@echo "======================================== Running: $* ========================================"
	@./$<
	@echo "  PASSED: $<"
	@rm -f $<
	@rm -rf $(DATA_DIR_$<)

# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------

clean:
	@echo "Cleaning all test binaries and data directories..."
	@rm -f $(TEST_BINS)
	@rm -rf $(ALL_DATA_DIRS)

server:
	$(RPYTHON) $(RPYFLAGS) gnitz/server/main.py
	@mv main-c gnitz-server-c

pytest: server
	cd py_client && uv run pytest tests/ -v

pytest-only:
	cd py_client && uv run pytest tests/ -v
