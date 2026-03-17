"""Z3 formal proofs for merge sort range partition invariants.

Proves five properties of gnitz/dbsp/ops/reduce.py:250-253:

  mid = (lo + hi) >> 1   # floor((lo+hi)/2)
  _mergesort_by_cols(..., lo, mid, ...)   # recurse on [lo, mid)
  _mergesort_by_cols(..., mid, hi, ...)   # recurse on [mid, hi)

RPython '>>' on non-negative int is logical right shift = bvlshr.
Uses 16-bit BV with precondition lo, hi <= 32767 so lo+hi <= 65534 < 2^16
(no 16-bit unsigned overflow in the addition).

  P1. lo <= mid when lo <= hi (16-bit BV, UNSAT)
  P2. mid < hi when lo < hi (16-bit BV, UNSAT)
  P3. Both halves strictly smaller than full range when hi-lo > 1 (16-bit BV, UNSAT)
  P4. Partition sizes sum to full range: (mid-lo)+(hi-mid) == hi-lo (16-bit BV, UNSAT)
  P5. Left half <= right half (floor-division property) (16-bit BV, UNSAT)

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


def parse_z3_bv(z3_out):
    """Parse a (simplify ...) result into an unsigned Python int."""
    s = z3_out.strip()
    if s.startswith("#x"):
        return int(s[2:], 16)
    elif s.startswith("(_ bv"):
        parts = s.split()
        return int(parts[1][2:])
    return None


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: Merge sort range partition invariants")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-checks: Python midpoint computation --------------------------------

report("  ... cross-checking merge sort partition")

# (lo, hi) pairs for cross-checks
test_pairs = [
    (0,   0),
    (0,   1),
    (0,   2),
    (0,   3),
    (0,   10),
    (5,   15),
    (100, 200),
]

for lo, hi in test_pairs:
    mid = (lo + hi) >> 1
    sum_ok = ((mid - lo) + (hi - mid) == hi - lo)
    lo_le_mid = (lo <= mid)
    # P2 and P3 require lo < hi / hi - lo > 1
    mid_lt_hi = (mid < hi) if lo < hi else True
    both_smaller = (mid - lo < hi - lo and hi - mid < hi - lo) if hi - lo > 1 else True
    left_le_right = ((mid - lo) <= (hi - mid))
    if lo_le_mid and mid_lt_hi and sum_ok and both_smaller and left_le_right:
        report("  PASS  cross-check (%d, %d): mid=%d" % (lo, hi, mid))
    else:
        report("  FAIL  cross-check (%d, %d): mid=%d lo_le_mid=%s mid_lt_hi=%s sum=%s smaller=%s lr=%s" % (
            lo, hi, mid, lo_le_mid, mid_lt_hi, sum_ok, both_smaller, left_le_right))
        ok = False

# Z3 simplify cross-check: (3 + 10) >> 1 = 6
z3_out = run_z3("(simplify (bvlshr (bvadd (_ bv3 16) (_ bv10 16)) (_ bv1 16)))").strip()
got = parse_z3_bv(z3_out)
if got == 6:
    report("  PASS  cross-check Z3 simplify (3+10)>>1 == 6")
else:
    report("  FAIL  cross-check Z3 simplify (3+10)>>1: expected 6, got %s" % z3_out)
    ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# Shared preamble: bounds constraints used in all queries
BOUNDS = """\
(assert (bvule lo (_ bv32767 16)))
(assert (bvule hi (_ bv32767 16)))
"""

# -- P1: lo <= mid when lo <= hi (16-bit BV, UNSAT) --------------------------
#
# With lo, hi <= 32767, no unsigned overflow in lo+hi.
# mid = bvlshr (lo+hi) 1 (unsigned shift = floor division by 2).

report("  ... proving P1: lo <= mid when lo <= hi")

ok &= prove("P1: lo <= mid", """\
(set-logic QF_BV)
(declare-const lo (_ BitVec 16))
(declare-const hi (_ BitVec 16))
""" + BOUNDS + """\
(assert (bvule lo hi))
(define-fun mid () (_ BitVec 16) (bvlshr (bvadd lo hi) (_ bv1 16)))
; Negate: lo > mid
(assert (not (bvule lo mid)))
(check-sat)
""")

# -- P2: mid < hi when lo < hi (16-bit BV, UNSAT) ----------------------------

report("  ... proving P2: mid < hi when lo < hi")

ok &= prove("P2: mid < hi", """\
(set-logic QF_BV)
(declare-const lo (_ BitVec 16))
(declare-const hi (_ BitVec 16))
""" + BOUNDS + """\
(assert (bvult lo hi))
(define-fun mid () (_ BitVec 16) (bvlshr (bvadd lo hi) (_ bv1 16)))
; Negate: mid >= hi
(assert (not (bvult mid hi)))
(check-sat)
""")

# -- P3: Both halves strictly smaller when hi-lo > 1 (16-bit BV, UNSAT) ------
#
# Ensures recursion terminates: both sub-ranges are strictly smaller
# than the input range.  Also requires lo <= hi to avoid unsigned
# wraparound in (hi - lo) > 1 guard.

report("  ... proving P3: both halves strictly smaller when hi-lo > 1")

ok &= prove("P3: both sub-ranges strictly smaller", """\
(set-logic QF_BV)
(declare-const lo (_ BitVec 16))
(declare-const hi (_ BitVec 16))
""" + BOUNDS + """\
(assert (bvule lo hi))
(assert (bvugt (bvsub hi lo) (_ bv1 16)))
(define-fun mid ()      (_ BitVec 16) (bvlshr (bvadd lo hi) (_ bv1 16)))
(define-fun left_sz ()  (_ BitVec 16) (bvsub mid lo))
(define-fun right_sz () (_ BitVec 16) (bvsub hi mid))
(define-fun total_sz () (_ BitVec 16) (bvsub hi lo))
; Negate: at least one half is not strictly smaller than total
(assert (not (and (bvult left_sz total_sz) (bvult right_sz total_sz))))
(check-sat)
""")

# -- P4: Partition sizes sum to full range (16-bit BV, UNSAT) -----------------
#
# (mid - lo) + (hi - mid) == hi - lo for any lo <= hi.

report("  ... proving P4: partition sizes sum to full range")

ok &= prove("P4: (mid-lo) + (hi-mid) == hi-lo", """\
(set-logic QF_BV)
(declare-const lo (_ BitVec 16))
(declare-const hi (_ BitVec 16))
""" + BOUNDS + """\
(assert (bvule lo hi))
(define-fun mid () (_ BitVec 16) (bvlshr (bvadd lo hi) (_ bv1 16)))
; Negate: sizes do not sum correctly
(assert (not (= (bvadd (bvsub mid lo) (bvsub hi mid)) (bvsub hi lo))))
(check-sat)
""")

# -- P5: Left half <= right half (floor-division property) (16-bit BV, UNSAT)
#
# mid = floor((lo+hi)/2) implies mid - lo = floor((hi-lo)/2)
# and hi - mid = ceil((hi-lo)/2) >= floor((hi-lo)/2) = mid - lo.

report("  ... proving P5: left half <= right half (floor property)")

ok &= prove("P5: mid-lo <= hi-mid", """\
(set-logic QF_BV)
(declare-const lo (_ BitVec 16))
(declare-const hi (_ BitVec 16))
""" + BOUNDS + """\
(assert (bvule lo hi))
(define-fun mid () (_ BitVec 16) (bvlshr (bvadd lo hi) (_ bv1 16)))
; Negate: left half > right half
(assert (not (bvule (bvsub mid lo) (bvsub hi mid))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: Merge sort range partition invariants")
    print("    P1: lo <= mid when lo <= hi")
    print("    P2: mid < hi when lo < hi")
    print("    P3: both sub-ranges strictly smaller when hi-lo > 1")
    print("    P4: partition sizes sum to full range")
    print("    P5: left half <= right half (floor-division property)")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
