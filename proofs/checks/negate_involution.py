"""Z3 formal proofs for op_negate weight involution.

op_negate (gnitz/dbsp/ops/linear.py:109-116) negates every row weight
via ArenaZSetBatch.append_batch_negated, which writes -w for each weight.
This is 2's-complement negation on i64 weights (intmask(-w)).

Properties:
  P1. Double negation identity: -(-w) == w for all i64 w (64-bit BV, UNSAT)
  P2. Positive weights negate to negative: w > 0 -> -w < 0 (64-bit BV, UNSAT)
  P3. Negative non-min weights negate to positive:
      w < 0 AND w != MIN_I64 -> -w > 0 (64-bit BV, UNSAT)
  P4. MIN_I64 is the unique non-zero 2's-complement negation fixed point:
      exists w != 0 with -w == w (SAT), and that w must be MIN_I64 (UNSAT)

Note: uses bvsub(0, x) for negation to avoid Z3 4.15.8 bvneg reduction bug.

Source:
  - gnitz/dbsp/ops/linear.py:109-116  (op_negate via append_batch_negated)

5 Z3 queries + ~7 cross-checks.  Runs under PyPy2.
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


def check_sat(label, smt_text):
    """Run a query expecting sat. Returns True on success."""
    result = run_z3(smt_text)
    if result == "sat":
        report("  PASS  %s" % label)
        return True
    else:
        report("  FAIL  %s: expected sat, got %s" % (label, result))
        return False


# -- Constants ----------------------------------------------------------------

MASK64 = (1 << 64) - 1
MAX_I64 = (1 << 63) - 1
MIN_I64 = -(1 << 63)
MIN_I64_U = 1 << 63   # MIN_I64 as unsigned 64-bit = 0x8000000000000000


# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: op_negate weight involution")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-checks: validate negation via RPython ------------------------------

report("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_int64, intmask  # noqa: E402
report("  ... imports done")
sys.stdout.flush()


def neg64_py(w):
    """2's-complement weight negation matching append_batch_negated."""
    return r_int64(intmask(-r_int64(w)))


def to_bv64(n):
    """Convert signed Python int to unsigned 64-bit for SMT literals."""
    return n & MASK64


cross_check_vectors = [0, 1, -1, MAX_I64, MIN_I64, 42, -42]

report("  ... cross-checking double negation identity")
for w in cross_check_vectors:
    neg_w   = neg64_py(w)
    dbl_neg = neg64_py(int(neg_w))
    py_ok = (int(dbl_neg) & MASK64) == to_bv64(w)

    # Z3 simplify: bvsub(0, bvsub(0, w))
    smt_q = "(simplify (bvsub (_ bv0 64) (bvsub (_ bv0 64) (_ bv%d 64))))" % to_bv64(w)
    z3_out = run_z3(smt_q).strip()
    z3_val = None
    if z3_out.startswith("#x"):
        z3_val = int(z3_out[2:], 16)
    elif z3_out.startswith("(_ bv"):
        z3_val = int(z3_out.split()[1][2:])

    if py_ok and z3_val is not None and z3_val == to_bv64(w):
        report("  PASS  cross-check neg(neg(%d)) == %d" % (w, w))
    else:
        report("  FAIL  cross-check neg(neg(%d)): py_ok=%s z3=%s" % (w, py_ok, z3_out))
        ok = False

# MIN_I64 fixed-point cross-check
neg_min = neg64_py(MIN_I64)
if (int(neg_min) & MASK64) == MIN_I64_U:
    report("  PASS  cross-check: neg(MIN_I64) == MIN_I64 (fixed point)")
else:
    report("  FAIL  cross-check: neg(MIN_I64) != MIN_I64: got 0x%016x" % (int(neg_min) & MASK64))
    ok = False

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)


# neg64 SMT definition: use bvsub(0, x) to avoid Z3 4.15.8 bvneg reduction bug
NEG64_DEF = """\
(define-fun neg64 ((x (_ BitVec 64))) (_ BitVec 64)
  (bvsub (_ bv0 64) x))
"""

# -- P1: Double negation identity: -(-w) == w --------------------------------
#
# 2's-complement negation is an involution: neg(neg(w)) = w for ALL w,
# including MIN_I64 where neg(MIN_I64) = MIN_I64 and neg(MIN_I64) = MIN_I64.
# This means applying op_negate twice is equivalent to the identity operator.

report("  ... proving P1: double negation identity neg(neg(w)) == w")
ok &= prove("P1: neg64(neg64(w)) == w for all 64-bit w", """\
(set-logic QF_BV)
(declare-const w (_ BitVec 64))
""" + NEG64_DEF + """\
; Negate: double negation does not return w
(assert (not (= (neg64 (neg64 w)) w)))
(check-sat)
""")

# -- P2: Positive weight negates to negative ----------------------------------
#
# For w > 0 (signed, i.e., 1 <= w <= MAX_I64 = 2^63-1):
# -w = 2^64 - w lies in [2^64 - MAX_I64, 2^64 - 1] = [MIN_I64+1, -1],
# which is the negative range.  So the DBSP weight sign flips as expected.

report("  ... proving P2: w > 0 implies neg64(w) < 0 (signed)")
ok &= prove("P2: bvsgt w 0 => bvslt (neg64 w) 0", """\
(set-logic QF_BV)
(declare-const w (_ BitVec 64))
""" + NEG64_DEF + """\
(assert (bvsgt w (_ bv0 64)))
; Negate: -w >= 0
(assert (not (bvslt (neg64 w) (_ bv0 64))))
(check-sat)
""")

# -- P3: Negative non-MIN_I64 weight negates to positive ----------------------
#
# For w < 0 and w != MIN_I64 (i.e., MIN_I64 < w < 0, range [-2^63+1, -1]):
# -w lies in [1, 2^63-1] which is the positive range.
# MIN_I64 must be excluded: neg(MIN_I64) = MIN_I64 < 0 (not positive).

report("  ... proving P3: w < 0 AND w != MIN_I64 implies neg64(w) > 0")
ok &= prove("P3: bvslt w 0 AND w != MIN_I64 => bvsgt (neg64 w) 0", """\
(set-logic QF_BV)
(declare-const w (_ BitVec 64))
""" + NEG64_DEF + """\
(assert (bvslt w (_ bv0 64)))
(assert (not (= w #x8000000000000000)))
; Negate: -w <= 0
(assert (not (bvsgt (neg64 w) (_ bv0 64))))
(check-sat)
""")

# -- P4: MIN_I64 is the unique non-zero negation fixed point ------------------
#
# The only non-zero w with neg(w) == w is w = MIN_I64 = 0x8000000000000000,
# because -(-2^63) = 2^64 - 2^63 = 2^63 = MIN_I64 (wraps back).
# This is the single problematic weight for any code assuming neg flips sign.
#
# P4a: SAT — there exists a non-zero w with neg(w) == w.
# P4b: UNSAT — no such w other than 0 and MIN_I64 satisfies neg(w) == w.

report("  ... proving P4a: MIN_I64 is a non-zero negation fixed point (SAT)")
ok &= check_sat("P4a: exists w != 0 with neg64(w) == w", """\
(set-logic QF_BV)
(declare-const w (_ BitVec 64))
""" + NEG64_DEF + """\
(assert (not (= w (_ bv0 64))))
(assert (= (neg64 w) w))
(check-sat)
""")

report("  ... proving P4b: MIN_I64 is the ONLY non-zero negation fixed point (UNSAT)")
ok &= prove("P4b: neg64(w)==w AND w!=0 AND w!=MIN_I64 is UNSAT", """\
(set-logic QF_BV)
(declare-const w (_ BitVec 64))
""" + NEG64_DEF + """\
(assert (not (= w (_ bv0 64))))
(assert (not (= w #x8000000000000000)))
(assert (= (neg64 w) w))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: op_negate weight involution")
    print("    P1: double negation identity (-(-w) == w for all w)")
    print("    P2: positive weights negate to negative")
    print("    P3: negative non-MIN_I64 weights negate to positive")
    print("    P4: MIN_I64 is the unique non-zero negation fixed point")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
