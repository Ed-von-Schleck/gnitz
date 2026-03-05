#!/bin/bash
set -euo pipefail

# 1. Setup Environment
export PYPY_PATH=../pypy
export PYTHONPATH=.:$PYPY_PATH

TESTS=(
    rpython_tests/core_comprehensive_test.py
    rpython_tests/storage_comprehensive_test.py
    rpython_tests/dbsp_comprehensive_test.py
    rpython_tests/zstore_comprehensive_test.py
    rpython_tests/catalog_comprehensive_test.py
    rpython_tests/catalog_additional_test.py
    rpython_tests/vm_comprehensive_test.py
    rpython_tests/ipc_comprehensive_test.py
)

BINARIES=()
TEST_DATA_DIRS=(
    storage_test_data
    dbsp_test_data
    zstore_test_data
    catalog_test_data
    catalog_additional_test_data
    vm_test_data
)

cleanup() {
    for bin in "${BINARIES[@]}"; do
        rm -f "$bin"
    done
    for dir in "${TEST_DATA_DIRS[@]}"; do
        rm -rf "$dir"
    done
}
trap cleanup EXIT

for test in "${TESTS[@]}"; do
    name=$(basename "$test" .py)
    bin="./${name}-c"
    BINARIES+=("$bin")

    echo "========================================"
    echo "  Compiling: $test"
    echo "========================================"
    pypy2 ../pypy/rpython/bin/rpython --opt=1 --gc=incminimark --lldebug "$test"

    echo "========================================"
    echo "  Running: $bin"
    echo "========================================"
    "$bin"
done

echo ""
echo "========================================"
echo "  ALL TESTS PASSED"
echo "========================================"
