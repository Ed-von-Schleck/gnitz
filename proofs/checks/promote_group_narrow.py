"""Z3 formal proofs for promote_group_col_to_u64 narrow-int branches.

Proves four properties of gnitz/dbsp/ops/group_index.py lines 24-40
(promote_group_col_to_u64), covering the four branches NOT proved in
promote_group.py:
  P1. U16 zero-extension (16->64) is injective
  P2. U32 zero-extension (32->64) is injective
  P3. I8  sign-extend + reinterpret (8->64) is injective
  P4. I16 sign-extend + reinterpret (16->64) is injective

promote_group.py covers U8 (zero-extend 8->64), I64 (identity), and
I32 (sign-extend 32->64).  This proof covers the remaining narrow-int
branches that share the same two code paths:
  - U16/U32 share the U8/U64 path: r_uint64(accessor.get_int(col_idx))
  - I8/I16   share the I32  path:  r_uint64(rffi.cast(ULONGLONG, get_int_signed()))

4 Z3 queries + 12 cross-checks.  Runs under PyPy2.
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
    return "0x%016x" % (n & MASK64)


# -- Constants ----------------------------------------------------------------

MASK64 = (1 << 64) - 1


# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: promote_group_col_to_u64 narrow-int branches")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-check: validate Z3 encoding against RPython -----------------------

report("  ... importing rpython.rlib.rarithmetic + rffi")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_uint64, r_int64  # noqa: E402
from rpython.rtyper.lltypesystem import rffi            # noqa: E402
report("  ... imports done")

# U16: r_uint64(accessor.get_int(col_idx)) — zero-extend 16->64
report("  ... cross-checking U16 zero-extension")
u16_vectors = [0, 1, 255, 32767, 65535]
for val in u16_vectors:
    rpython_val = int(r_uint64(val)) & MASK64
    smt_query = "(simplify ((_ zero_extend 48) (_ bv%d 16)))" % val
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check U16(%d): unexpected Z3 output: %s" % (val, z3_out))
        ok = False
    elif z3_val == rpython_val:
        report("  PASS  cross-check U16(%d) -> %s" % (val, fmt64(rpython_val)))
    else:
        report("  FAIL  cross-check U16(%d): RPython=%s Z3=%s" % (
            val, fmt64(rpython_val), fmt64(z3_val)))
        ok = False

# U32: r_uint64(accessor.get_int(col_idx)) — zero-extend 32->64
report("  ... cross-checking U32 zero-extension")
u32_vectors = [0, 1, 2147483647, 2147483648, 4294967295]
for val in u32_vectors:
    rpython_val = int(r_uint64(val)) & MASK64
    smt_query = "(simplify ((_ zero_extend 32) (_ bv%d 32)))" % val
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check U32(%d): unexpected Z3 output: %s" % (val, z3_out))
        ok = False
    elif z3_val == rpython_val:
        report("  PASS  cross-check U32(%d) -> %s" % (val, fmt64(rpython_val)))
    else:
        report("  FAIL  cross-check U32(%d): RPython=%s Z3=%s" % (
            val, fmt64(rpython_val), fmt64(z3_val)))
        ok = False

# I8: r_uint64(rffi.cast(rffi.ULONGLONG, signed_64))
# where signed_64 = get_int_signed() sign-extended from the 8-bit column value
# Net effect: sign_extend 8->64, then reinterpret as unsigned (identity on bits)
report("  ... cross-checking I8 sign-extend + reinterpret")
i8_vectors_u = [0, 1, 127, 128, 255]  # stored as unsigned 8-bit patterns
for val_u8 in i8_vectors_u:
    # Interpret stored bits as signed 8-bit
    signed_8 = val_u8 - 256 if val_u8 >= 128 else val_u8
    # get_int_signed() sign-extends to r_int64
    signed_64 = r_int64(signed_8)
    # rffi.cast(ULONGLONG, signed_64) reinterprets bits as unsigned 64
    unsigned_64 = int(rffi.cast(rffi.ULONGLONG, signed_64)) & MASK64
    # Z3: sign_extend 8->64 (56 additional bits)
    smt_query = "(simplify ((_ sign_extend 56) (_ bv%d 8)))" % val_u8
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check I8(0x%02x): unexpected Z3 output: %s" % (val_u8, z3_out))
        ok = False
    elif z3_val == unsigned_64:
        report("  PASS  cross-check I8(0x%02x) [=%d] -> %s" % (
            val_u8, signed_8, fmt64(unsigned_64)))
    else:
        report("  FAIL  cross-check I8(0x%02x): RPython=%s Z3=%s" % (
            val_u8, fmt64(unsigned_64), fmt64(z3_val)))
        ok = False

# I16: r_uint64(rffi.cast(rffi.ULONGLONG, signed_64))
# where signed_64 = get_int_signed() sign-extended from the 16-bit column value
report("  ... cross-checking I16 sign-extend + reinterpret")
i16_vectors_u = [0, 1, 32767, 32768, 65535]  # stored as unsigned 16-bit patterns
for val_u16 in i16_vectors_u:
    # Interpret stored bits as signed 16-bit
    signed_16 = val_u16 - 65536 if val_u16 >= 32768 else val_u16
    # get_int_signed() sign-extends to r_int64
    signed_64 = r_int64(signed_16)
    # rffi.cast(ULONGLONG, signed_64) reinterprets bits as unsigned 64
    unsigned_64 = int(rffi.cast(rffi.ULONGLONG, signed_64)) & MASK64
    # Z3: sign_extend 16->64 (48 additional bits)
    smt_query = "(simplify ((_ sign_extend 48) (_ bv%d 16)))" % val_u16
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check I16(0x%04x): unexpected Z3 output: %s" % (val_u16, z3_out))
        ok = False
    elif z3_val == unsigned_64:
        report("  PASS  cross-check I16(0x%04x) [=%d] -> %s" % (
            val_u16, signed_16, fmt64(unsigned_64)))
    else:
        report("  FAIL  cross-check I16(0x%04x): RPython=%s Z3=%s" % (
            val_u16, fmt64(unsigned_64), fmt64(z3_val)))
        ok = False

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P1: U16 zero-extension (16->64) is injective ----------------------------

report("  ... proving P1: U16 zero-extension (16->64) is injective")
ok &= prove("P1: U16 zero-extension (16->64) is injective", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 16)) (declare-const b (_ BitVec 16))
(assert (not (= a b)))
(assert (= ((_ zero_extend 48) a) ((_ zero_extend 48) b)))
(check-sat)
""")

# -- P2: U32 zero-extension (32->64) is injective ----------------------------

report("  ... proving P2: U32 zero-extension (32->64) is injective")
ok &= prove("P2: U32 zero-extension (32->64) is injective", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 32)) (declare-const b (_ BitVec 32))
(assert (not (= a b)))
(assert (= ((_ zero_extend 32) a) ((_ zero_extend 32) b)))
(check-sat)
""")

# -- P3: I8 sign-extend + reinterpret (8->64) is injective -------------------
#
# sign_extend 8->64 maps each distinct 8-bit pattern to a distinct 64-bit
# pattern.  No two distinct source values can produce the same sign-extended
# result, because sign_extend is a bijection from 8-bit onto its image.

report("  ... proving P3: I8 sign-extend + reinterpret (8->64) is injective")
ok &= prove("P3: I8 sign-extend + reinterpret (8->64) is injective", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 8)) (declare-const b (_ BitVec 8))
(assert (not (= a b)))
(assert (= ((_ sign_extend 56) a) ((_ sign_extend 56) b)))
(check-sat)
""")

# -- P4: I16 sign-extend + reinterpret (16->64) is injective -----------------

report("  ... proving P4: I16 sign-extend + reinterpret (16->64) is injective")
ok &= prove("P4: I16 sign-extend + reinterpret (16->64) is injective", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 16)) (declare-const b (_ BitVec 16))
(assert (not (= a b)))
(assert (= ((_ sign_extend 48) a) ((_ sign_extend 48) b)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: promote_group_col_to_u64 narrow-int branches")
    print("    P1: U16 zero-extension (16->64) is injective")
    print("    P2: U32 zero-extension (32->64) is injective")
    print("    P3: I8  sign-extend + reinterpret (8->64) is injective")
    print("    P4: I16 sign-extend + reinterpret (16->64) is injective")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
