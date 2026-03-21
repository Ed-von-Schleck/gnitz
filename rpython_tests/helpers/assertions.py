# rpython_tests/helpers/assertions.py
#
# Shared test assertion helpers for RPython test binaries.

import os

from rpython.rlib.rarithmetic import (
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)


def fail(msg):
    os.write(2, "CRITICAL FAILURE: " + msg + "\n")
    raise Exception(msg)


def assert_true(condition, msg):
    if not condition:
        fail(msg)


def assert_false(condition, msg):
    if condition:
        fail(msg)


def assert_equal_i(expected, actual, msg):
    if expected != actual:
        fail(msg + " (Expected " + str(expected) + ", got " + str(actual) + ")")


def assert_equal_i64(expected, actual, msg):
    if expected != actual:
        fail(msg + " (Expected " + str(expected) + ", got " + str(actual) + ")")


def assert_equal_u64(expected, actual, msg):
    if expected != actual:
        fail(msg + " (Expected " + str(expected) + ", got " + str(actual) + ")")


def assert_equal_s(expected, actual, msg):
    if expected != actual:
        fail(msg + " (Expected '" + expected + "', got '" + actual + "')")


def _u64_to_hex(val):
    chars = "0123456789abcdef"
    res = ["0"] * 16
    temp = val
    for i in range(15, -1, -1):
        res[i] = chars[intmask(temp & r_uint64(0xF))]
        temp >>= 4
    return "".join(res)


def assert_equal_u128(expected, actual, msg):
    if expected != actual:
        hi_e = r_uint64(expected >> 64)
        lo_e = r_uint64(expected)
        hi_a = r_uint64(actual >> 64)
        lo_a = r_uint64(actual)
        os.write(2, "   Expected: " + _u64_to_hex(hi_e) + _u64_to_hex(lo_e) + "\n")
        os.write(2, "   Actual:   " + _u64_to_hex(hi_a) + _u64_to_hex(lo_a) + "\n")
        fail(msg + " (u128 mismatch)")


def assert_equal_f(expected, actual, msg):
    if expected != actual:
        fail(msg + " (Expected " + str(expected) + ", got " + str(actual) + ")")
