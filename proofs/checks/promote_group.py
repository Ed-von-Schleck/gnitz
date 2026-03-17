"""Z3 formal proofs for promote_group_col_to_u64 injectivity.

Proves three injectivity properties of gnitz/dbsp/ops/group_index.py
promote_group_col_to_u64():
  P1. U8  zero-extension (8->64) is injective
  P2. I64 bit-reinterpret (64->64) is injective (identity on bits)
  P3. I32 sign-extend + reinterpret (32->64) is injective

Each branch promotes a column value to r_uint64 for group-index keys.
Parallel to promote_to_index_key (proven for 128-bit target) but targeting
64-bit.

3 Z3 queries + ~15 cross-checks.  Runs under PyPy2.
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


def fmt64(n):
    return "0x%016x" % (n & 0xFFFFFFFFFFFFFFFF)


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: promote_group_col_to_u64 injectivity")
print("=" * 56)
sys.stdout.flush()

ok = True

MASK64 = (1 << 64) - 1

# -- Cross-check: validate Z3 encoding against RPython -----------------------

report("  ... cross-checking promote_group_col_to_u64 against RPython")

print("  ... importing rpython.rlib.rarithmetic + rffi")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_uint64, r_int64  # noqa: E402
from rpython.rtyper.lltypesystem import rffi  # noqa: E402
print("  ... imports done")
sys.stdout.flush()

# U8 cross-check: r_uint64(accessor.get_int(col_idx)) — zero-extend 8->64
report("  ... cross-checking U8 zero-extension")
u8_vectors = [0, 1, 127, 128, 255]
for val in u8_vectors:
    rpython_val = int(r_uint64(val)) & MASK64
    smt_query = "(simplify ((_ zero_extend 56) (_ bv%d 8)))" % val
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check U8(%d): unexpected Z3 output: %s" % (val, z3_out))
        ok = False
    elif z3_val == rpython_val:
        report("  PASS  cross-check U8(%d) -> %s" % (val, fmt64(rpython_val)))
    else:
        report("  FAIL  cross-check U8(%d): RPython=%s Z3=%s" % (val, fmt64(rpython_val), fmt64(z3_val)))
        ok = False

# I64 cross-check: r_uint64(accessor.get_int(col_idx)) — bit-reinterpret 64->64
report("  ... cross-checking I64 bit-reinterpret")
i64_vectors = [0, 1, (1 << 64) - 1, 1 << 63, (1 << 63) - 1]
for val in i64_vectors:
    rpython_val = int(r_uint64(val)) & MASK64
    smt_query = "(simplify (_ bv%d 64))" % (val & MASK64)
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check I64(0x%016x): unexpected Z3 output: %s" % (val & MASK64, z3_out))
        ok = False
    elif z3_val == rpython_val:
        report("  PASS  cross-check I64(0x%016x) -> %s" % (val & MASK64, fmt64(rpython_val)))
    else:
        report("  FAIL  cross-check I64(0x%016x): RPython=%s Z3=%s" % (val & MASK64, fmt64(rpython_val), fmt64(z3_val)))
        ok = False

# I32 cross-check: r_uint64(rffi.cast(rffi.ULONGLONG, signed_64))
# where signed_64 = get_int_signed() (sign-extended from I32)
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

    # Z3: sign_extend 32->64, then reinterpret as unsigned (identity on bits)
    smt_query = "(simplify ((_ zero_extend 32) ((_ sign_extend 32) (_ bv%d 32))))" % val_u32
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check I32(0x%08x): unexpected Z3 output: %s" % (val_u32, z3_out))
        ok = False
    elif z3_val == unsigned_64:
        report("  PASS  cross-check I32(0x%08x) [=%d] -> %s" % (val_u32, signed_32, fmt64(unsigned_64)))
    else:
        report("  FAIL  cross-check I32(0x%08x): RPython=%s Z3=%s" % (val_u32, fmt64(unsigned_64), fmt64(z3_val)))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: U8 zero-extension (8->64) injective ---------------------------------
#
# Two distinct 8-bit values a, b with zero_extend_56(a) == zero_extend_56(b)
# and a != b -> unsat.

report("  ... proving P1: U8 zero-extension injective")
ok &= prove("P1: U8 zero-extension (8->64) is injective", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 8))
(declare-const b (_ BitVec 8))
; a != b
(assert (not (= a b)))
; zero-extend both to 64 bits, assert they're equal
(assert (= ((_ zero_extend 56) a) ((_ zero_extend 56) b)))
(check-sat)
""")

# -- P2: I64 bit-reinterpret (64->64) injective ------------------------------
#
# Identity on bits. Two distinct 64-bit values cannot be equal.

report("  ... proving P2: I64 bit-reinterpret injective")
ok &= prove("P2: I64 bit-reinterpret (64->64) is injective", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 64))
(declare-const b (_ BitVec 64))
; a != b
(assert (not (= a b)))
; bit-reinterpret is identity: assert they're equal
(assert (= a b))
(check-sat)
""")

# -- P3: I32 sign-extend + reinterpret (32->64) injective --------------------
#
# Two distinct 32-bit values a, b with
# zero_extend_32(sign_extend_32(a)) == zero_extend_32(sign_extend_32(b))
# and a != b -> unsat.
#
# SMT encoding mirrors RPython: sign_extend I32 to I64, then reinterpret
# as unsigned 64 (zero_extend of the sign_extend captures the full 64-bit
# unsigned result).

report("  ... proving P3: I32 sign-extend + reinterpret injective")
ok &= prove("P3: I32 sign-extend + reinterpret (32->64) is injective", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 32))
(declare-const b (_ BitVec 32))
; a != b
(assert (not (= a b)))
; sign-extend 32->64, then treat as unsigned 64-bit (zero_extend wraps to wider BV)
(assert (= ((_ zero_extend 32) ((_ sign_extend 32) a))
           ((_ zero_extend 32) ((_ sign_extend 32) b))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: promote_group_col_to_u64 is injective")
    print("    P1: U8  zero-extension (8->64) is injective")
    print("    P2: I64 bit-reinterpret (64->64) is injective")
    print("    P3: I32 sign-extend + reinterpret (32->64) is injective")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
