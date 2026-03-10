# Define environment and variables
PYPY_PATH ?= ../pypy
export PYTHONPATH := .:${PYPY_PATH}

RPYTHON  := pypy2 ${PYPY_PATH}/rpython/bin/rpython
RPYFLAGS := --opt=1 --gc=incminimark --lldebug

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

# Derived names for cleanup
TEST_BINS := $(notdir $(TEST_FILES:.py=-c))
TEST_DATA_DIRS := storage_test_data dbsp_test_data zstore_test_data \
                  catalog_test_data catalog_additional_test_data vm_test_data \
                  compile_graph_test_data server_test_data

# Disable parallel execution to prevent shared directory collisions
.NOTPARALLEL:
.PHONY: all test clean $(TEST_FILES)

all: test

test: $(TEST_FILES)
	@echo ""
	@echo "========================================"
	@echo "  ALL TESTS PASSED"
	@echo "========================================"

$(TEST_FILES):
	@echo "========================================"
	@echo "  Preparing clean environment for $@"
	@echo "========================================"
	@rm -f $(notdir $(@:.py=-c))
	@rm -rf $(TEST_DATA_DIRS)

	@echo "========================================"
	@echo "  Compiling: $@"
	@echo "========================================"
	$(RPYTHON) $(RPYFLAGS) $@

	@echo "========================================"
	@echo "  Running: $(notdir $(@:.py=-c))"
	@echo "========================================"
	./$(notdir $(@:.py=-c))

	@echo "========================================"
	@echo "  Success! Cleaning up test artifacts..."
	@echo "========================================"
	@rm -f $(notdir $(@:.py=-c))
	@rm -rf $(TEST_DATA_DIRS)

clean:
	@echo "Cleaning all test binaries and data directories..."
	@rm -f $(TEST_BINS)
	@rm -rf $(TEST_DATA_DIRS)
