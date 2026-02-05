#!/bin/bash

# 1. Setup Environment
export PYPY_PATH=../pypy
export PYTHONPATH=.:$PYPY_PATH

# 2. Run Unit Tests (Logic)
echo "Running Unit Tests..."
pypy2 -m unittest discover tests

# 3. Run RPython Translation Check (Types)
echo "Running RPython Translation Check..."
pypy2 $PYPY_PATH/rpython/bin/rpython --opt=0 --no-compile translate_test.py
