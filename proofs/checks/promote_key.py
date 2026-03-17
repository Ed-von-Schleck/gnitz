"""Z3 formal proofs for promote_to_index_key injectivity.

Proves four injectivity properties of gnitz/core/keys.py promote_to_index_key():
  P1. U8  zero-extension (8->128) is injective
  P2. U64 zero-extension (64->128) is injective
  P3. I64 bit-reinterpret (64->128) is injective (same as U64 path)
  P4. I32 sign-extend + reinterpret (32->64->128) is injective

Each branch promotes a column value to r_uint128 for index keys. Correctness
requires injectivity: distinct input values must produce distinct U128 keys.

4 Z3 queries + ~20 cross-checks.  Runs under PyPy2.
Exit code 0 on success, 1 on any failure.
"""
import subprocess
import sys


# -- Helpers ------------------------------------------------------------------

def run_z3(smt_text):
    """Pipe SMT-LIB2 text to z3, return stdout."""
    p = subprocess.Popen(
        ["z3", "-smt2", "-in"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    stdout, stderr = p.communicate(smt_text)
    if p.returncode != 0:
        raise RuntimeError("Z3 error (rc=%d): %s" % (p.returncode, stderr.strip()))
    return stdout.strip()


def report(msg):
    print(msg)
    sys.stdout.flush()


def prove(label, smt_text):
    """Run a query expecting unsat. Returns True on success."""
    result = run_z3(smt_text)
    if result == "unsat":
        report("  PASS  %s" % label)
        return True
    else:
        report("  FAIL  %s: expected unsat, got %s" % (label, result))
        return False


def parse_z3_value(z3_out):
    """Parse a Z3 (simplify ...) result into a Python int."""
    if z3_out.startswith("#x"):
        return int(z3_out[2:], 16)
    elif z3_out.startswith("#b"):
        return int(z3_out[2:], 2)
    elif z3_out.startswith("(_ bv"):
        return int(z3_out.split()[1][2:])
    else:
        return None


def fmt128(n):
    return "0x%032x" % (n & ((1 << 128) - 1))


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: promote_to_index_key injectivity")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-check: validate Z3 encoding against RPython -----------------------

report("  ... cross-checking promote_to_index_key against RPython")

print("  ... importing rpython.rlib.rarithmetic + rffi")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128, r_uint64, r_int64  # noqa: E402
from rpython.rtyper.lltypesystem import rffi  # noqa: E402
print("  ... imports done")
sys.stdout.flush()

MASK64 = (1 << 64) - 1
MASK128 = (1 << 128) - 1

# U8 cross-check: zero-extend 8->128
report("  ... cross-checking U8 zero-extension")
u8_vectors = [0, 1, 127, 128, 255]
for val in u8_vectors:
    rpython_val = int(r_uint128(r_uint64(val))) & MASK128
    smt_query = "(simplify ((_ zero_extend 120) (_ bv%d 8)))" % val
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check U8(%d): unexpected Z3 output: %s" % (val, z3_out))
        ok = False
    elif z3_val == rpython_val:
        report("  PASS  cross-check U8(%d) -> %s" % (val, fmt128(rpython_val)))
    else:
        report("  FAIL  cross-check U8(%d): RPython=%s Z3=%s" % (val, fmt128(rpython_val), fmt128(z3_val)))
        ok = False

# U64 cross-check: zero-extend 64->128
report("  ... cross-checking U64 zero-extension")
u64_vectors = [0, 1, (1 << 63) - 1, 1 << 63, (1 << 64) - 1]
for val in u64_vectors:
    rpython_val = int(r_uint128(r_uint64(val))) & MASK128
    smt_query = "(simplify ((_ zero_extend 64) #x%016x))" % (val & MASK64)
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check U64(%d): unexpected Z3 output: %s" % (val, z3_out))
        ok = False
    elif z3_val == rpython_val:
        report("  PASS  cross-check U64(0x%016x) -> %s" % (val & MASK64, fmt128(rpython_val)))
    else:
        report("  FAIL  cross-check U64(0x%016x): RPython=%s Z3=%s" % (val & MASK64, fmt128(rpython_val), fmt128(z3_val)))
        ok = False

# I64 cross-check: bit-reinterpret signed -> unsigned 64 -> zero-extend to 128
# In RPython: r_uint128(accessor.get_int(col_idx)) where get_int returns r_uint64
# The signed value is already stored as unsigned bit pattern in the column
report("  ... cross-checking I64 bit-reinterpret")
i64_bit_vectors = [0, 1, (1 << 64) - 1, 1 << 63, (1 << 63) - 1]
for val in i64_bit_vectors:
    # RPython path: get_int returns r_uint64 (unsigned bit pattern), then r_uint128()
    rpython_val = int(r_uint128(r_uint64(val))) & MASK128
    smt_query = "(simplify ((_ zero_extend 64) #x%016x))" % (val & MASK64)
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check I64(0x%016x): unexpected Z3 output: %s" % (val & MASK64, z3_out))
        ok = False
    elif z3_val == rpython_val:
        report("  PASS  cross-check I64(0x%016x) -> %s" % (val & MASK64, fmt128(rpython_val)))
    else:
        report("  FAIL  cross-check I64(0x%016x): RPython=%s Z3=%s" % (val & MASK64, fmt128(rpython_val), fmt128(z3_val)))
        ok = False

# I32 cross-check: sign-extend 32->64, then cast to unsigned 64, then zero-extend to 128
# RPython: r_uint128(rffi.cast(rffi.ULONGLONG, signed_64))
# where signed_64 = get_int_signed() returns r_int64 (sign-extended from I32)
report("  ... cross-checking I32 sign-extend + reinterpret")
i32_vectors = [0, 1, (1 << 32) - 1, 1 << 31, (1 << 31) - 1]
for val_u32 in i32_vectors:
    # Interpret as signed 32-bit
    if val_u32 >= (1 << 31):
        signed_32 = val_u32 - (1 << 32)
    else:
        signed_32 = val_u32
    # Sign-extend to 64 bits (what get_int_signed returns)
    signed_64 = r_int64(signed_32)
    # Cast to unsigned 64 (rffi.cast(ULONGLONG, signed_64))
    unsigned_64 = int(rffi.cast(rffi.ULONGLONG, signed_64)) & MASK64
    # Zero-extend to 128
    rpython_val = int(r_uint128(r_uint64(unsigned_64))) & MASK128

    # Z3: sign_extend 32->64, then zero_extend 64->128
    smt_query = "(simplify ((_ zero_extend 64) ((_ sign_extend 32) (_ bv%d 32))))" % val_u32
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check I32(0x%08x): unexpected Z3 output: %s" % (val_u32, z3_out))
        ok = False
    elif z3_val == rpython_val:
        report("  PASS  cross-check I32(0x%08x) [=%d] -> %s" % (val_u32, signed_32, fmt128(rpython_val)))
    else:
        report("  FAIL  cross-check I32(0x%08x): RPython=%s Z3=%s" % (val_u32, fmt128(rpython_val), fmt128(z3_val)))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: U8 zero-extension injective -----------------------------------------
#
# Two distinct 8-bit values a, b with zero_extend_120(a) == zero_extend_120(b)
# and a != b -> unsat.

report("  ... proving P1: U8 zero-extension injective")
ok &= prove("P1: U8 zero-extension (8->128) is injective", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 8))
(declare-const b (_ BitVec 8))
; a != b
(assert (not (= a b)))
; zero-extend both to 128 bits, assert they're equal
(assert (= ((_ zero_extend 120) a) ((_ zero_extend 120) b)))
(check-sat)
""")

# -- P2: U64 zero-extension injective ----------------------------------------
#
# Two distinct 64-bit values with same 128-bit zero-extension -> unsat.

report("  ... proving P2: U64 zero-extension injective")
ok &= prove("P2: U64 zero-extension (64->128) is injective", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 64))
(declare-const b (_ BitVec 64))
; a != b
(assert (not (= a b)))
; zero-extend both to 128 bits, assert they're equal
(assert (= ((_ zero_extend 64) a) ((_ zero_extend 64) b)))
(check-sat)
""")

# -- P3: I64 bit-reinterpret injective ----------------------------------------
#
# Same as P2: I64 path uses get_int() which returns r_uint64 (unsigned bit
# pattern), then r_uint128() zero-extends. Bit-reinterpret is identity on bits.

report("  ... proving P3: I64 bit-reinterpret injective")
ok &= prove("P3: I64 bit-reinterpret (64->128) is injective", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 64))
(declare-const b (_ BitVec 64))
; a != b
(assert (not (= a b)))
; zero-extend both to 128 bits (same as U64 path, bit-reinterpret is identity)
(assert (= ((_ zero_extend 64) a) ((_ zero_extend 64) b)))
(check-sat)
""")

# -- P4: I32 sign-extend + reinterpret injective -----------------------------
#
# Two distinct 32-bit signed values a, b with
# zero_extend_64(sign_extend_32(a)) == zero_extend_64(sign_extend_32(b))
# and a != b -> unsat.

report("  ... proving P4: I32 sign-extend + reinterpret injective")
ok &= prove("P4: I32 sign-extend + reinterpret (32->64->128) is injective", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 32))
(declare-const b (_ BitVec 32))
; a != b
(assert (not (= a b)))
; sign-extend 32->64, then zero-extend 64->128, assert they're equal
(assert (= ((_ zero_extend 64) ((_ sign_extend 32) a))
           ((_ zero_extend 64) ((_ sign_extend 32) b))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: promote_to_index_key is injective")
    print("    P1: U8  zero-extension (8->128) is injective")
    print("    P2: U64 zero-extension (64->128) is injective")
    print("    P3: I64 bit-reinterpret (64->128) is injective")
    print("    P4: I32 sign-extend + reinterpret (32->64->128) is injective")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
