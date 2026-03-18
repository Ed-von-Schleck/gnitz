"""Z3 formal proofs for _promote_col_to_u128 injectivity.

Proves four properties of gnitz/dbsp/ops/linear.py lines 31-57
(_promote_col_to_u128), covering all non-U128, non-string paths:
  P1. zero_extend 64->128 is injective (covers U64/I64/float paths)
  P2. zero_extend 8->128 is injective (narrow-int representative)
  P3. Upper 64 bits of promoted value are always 0
  P4. Lower 64 bits of promoted value equal the input

All non-U128, non-string paths reduce to:
  r_uint128(r_uint64(v)) = zero_extend 64->128
Narrow int paths (U8/U16/U32/I8/I16/I32) first zero/sign-extend to 64 bits
via get_int(), then the same 64->128 step follows.  P2 proves the property
holds for any sub-64-bit source width by the 8-bit representative case.

Not duplicating: promote_group.py proves 64-bit target; this proof covers
the 128-bit target used by op_map reindexing.

4 Z3 queries + 5 cross-checks.  Runs under PyPy2.
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
    return "0x%032x" % (n & MASK128)


# -- Constants ----------------------------------------------------------------

MASK64 = (1 << 64) - 1
MASK128 = (1 << 128) - 1


# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: _promote_col_to_u128 injectivity")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-check: validate Z3 encoding against RPython -----------------------

report("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_uint64                      # noqa: E402
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128  # noqa: E402
report("  ... imports done")

report("  ... cross-checking zero_extend 64->128 against RPython")


def rpy_promote_u64(v):
    """r_uint128(r_uint64(v)) -- the common zero_extend 64->128 step."""
    return int(r_uint128(r_uint64(v))) & MASK128


# 5 test vectors: 0, 1, high-bit set, all-ones 64, typical pattern
u64_vectors = [0, 1, 1 << 63, MASK64, 0xDEADBEEFCAFEBABE & MASK64]

promoted_vals = []
for val in u64_vectors:
    rpy_val = rpy_promote_u64(val)
    smt_query = "(simplify ((_ zero_extend 64) (_ bv%d 64)))" % (val & MASK64)
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check promote(0x%016x): unexpected Z3 output: %s" % (
            val, z3_out))
        ok = False
    elif (int(rpy_val) & MASK128) == (int(z3_val) & MASK128):
        upper = (rpy_val >> 64) & MASK64
        lower = rpy_val & MASK64
        if upper == 0 and lower == (val & MASK64):
            report("  PASS  cross-check promote(0x%016x) -> %s [upper=0, lower=input]" % (
                val, fmt128(rpy_val)))
        else:
            report("  FAIL  cross-check promote(0x%016x): upper=0x%016x lower=0x%016x" % (
                val, upper, lower))
            ok = False
    else:
        report("  FAIL  cross-check promote(0x%016x): RPython=%s Z3=%s" % (
            val, fmt128(rpy_val), fmt128(z3_val)))
        ok = False
    promoted_vals.append(rpy_val)

# Pairwise distinctness check
n = len(promoted_vals)
all_distinct = True
for i in range(n):
    for j in range(i + 1, n):
        if (int(promoted_vals[i]) & MASK128) == (int(promoted_vals[j]) & MASK128):
            report("  FAIL  cross-check: promoted[%d] == promoted[%d] (collision!)" % (i, j))
            ok = False
            all_distinct = False
if all_distinct:
    report("  PASS  cross-check: all 5 promoted values are distinct")

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P1: zero_extend 64->128 is injective -------------------------------------
#
# Covers U64/I64/float paths: r_uint128(r_uint64(v)).
# Two distinct 64-bit inputs cannot map to the same 128-bit value.

report("  ... proving P1: zero_extend 64->128 is injective")
ok &= prove("P1: zero_extend 64->128 is injective (U64/I64/float paths)", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 64))
(declare-const b (_ BitVec 64))
(assert (not (= a b)))
(assert (= ((_ zero_extend 64) a) ((_ zero_extend 64) b)))
(check-sat)
""")

# -- P2: zero_extend 8->128 is injective (narrow-int representative) ----------
#
# Narrow int paths (U8/U16/U32/I8/I16/I32) pass through get_int() and then
# r_uint128(r_uint64(v)).  Since zero/sign-extension into 64 bits preserves
# the original value uniquely, and zero_extend 64->128 is injective (P1), the
# full path is injective.  As a representative, prove the tightest width: 8->128.
# The property is width-independent (zero_extend N->M is always injective).

report("  ... proving P2: zero_extend 8->128 is injective (narrow-int representative)")
ok &= prove("P2: zero_extend 8->128 is injective (narrow-int representative)", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 8))
(declare-const b (_ BitVec 8))
(assert (not (= a b)))
(assert (= ((_ zero_extend 120) a) ((_ zero_extend 120) b)))
(check-sat)
""")

# -- P3: upper 64 bits of promoted value are always 0 ------------------------
#
# r_uint128(r_uint64(v)) zero-extends v from 64 to 128 bits.
# The upper 64 bits are therefore always 0, regardless of v.

report("  ... proving P3: upper 64 bits of zero_extend 64->128 are always 0")
ok &= prove("P3: upper 64 bits of promoted value are always 0", """\
(set-logic QF_BV)
(declare-const v (_ BitVec 64))
(define-fun promoted () (_ BitVec 128) ((_ zero_extend 64) v))
(assert (not (= ((_ extract 127 64) promoted) (_ bv0 64))))
(check-sat)
""")

# -- P4: lower 64 bits of promoted value equal the input ----------------------

report("  ... proving P4: lower 64 bits of zero_extend 64->128 equal the input")
ok &= prove("P4: lower 64 bits of promoted value equal the input", """\
(set-logic QF_BV)
(declare-const v (_ BitVec 64))
(define-fun promoted () (_ BitVec 128) ((_ zero_extend 64) v))
(assert (not (= ((_ extract 63 0) promoted) v)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: _promote_col_to_u128 injectivity")
    print("    P1: zero_extend 64->128 is injective (U64/I64/float paths)")
    print("    P2: zero_extend 8->128 is injective (narrow-int representative)")
    print("    P3: upper 64 bits of promoted value are always 0")
    print("    P4: lower 64 bits of promoted value equal the input")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
