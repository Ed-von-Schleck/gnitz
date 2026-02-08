#!/bin/bash

# 1. Setup Environment
export PYPY_PATH=../pypy
export PYTHONPATH=.:$PYPY_PATH

pypy2 debug_reproduce_failure.py
