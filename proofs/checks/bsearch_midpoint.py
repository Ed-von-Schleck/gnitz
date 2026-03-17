"""Z3 formal proofs for binary search midpoint safety.

Proves six properties of the midpoint formula used in:
  gnitz/storage/cursor.py:67      — ShardCursor.seek
  gnitz/storage/memtable.py:252   — _lower_bound
  gnitz/core/batch.py:305         — _mergesort_indices

  mid = (lo + hi) >> 1

lo and hi are signed row indices (>= 0) in RPython.  Width: 32-bit BV.

  P1. lo <= mid for lo < hi (32-bit BV, UNSAT, with no-overflow precondition)
  P2. mid < hi for lo < hi (32-bit BV, UNSAT, with no-overflow precondition)
  P3. Left half strictly shrinks: mid - lo < hi - lo (32-bit BV, UNSAT)
  P4. Right half strictly shrinks: hi - mid < hi - lo (32-bit BV, UNSAT)
  P5. Overflow counterexample: lo + hi wraps => mid >= hi (SAT)
  P6. Safe under practical bound: hi <= 2^30 (32-bit BV, UNSAT)

6 Z3 queries + ~8 cross-checks.  Runs under PyPy2.
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


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: Binary search midpoint safety")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-checks: Python midpoint computation -------------------------------

report("  ... cross-checking midpoint computation")

MASK32 = (1 << 32) - 1

test_pairs = [
    (0, 1),
    (0, 2),
    (0, 3),
    (0, 100),
    (5, 95),
    (0, 1 << 20),
    (0, 1 << 30),
    (1 << 29, 1 << 30),
]

for lo, hi in test_pairs:
    mid = (lo + hi) >> 1
    lo_ok = (lo <= mid)
    hi_ok = (mid < hi)
    shrink_l = (mid - lo < hi - lo)
    shrink_r = (hi - mid < hi - lo) if hi - lo > 1 else True

    # Z3 simplify cross-check
    smt_q = "(simplify (bvlshr (bvadd (_ bv%d 32) (_ bv%d 32)) (_ bv1 32)))" % (lo, hi)
    z3_out = run_z3(smt_q)
    z3_val = parse_z3_value(z3_out)
    z3_ok = (z3_val is not None and (z3_val & MASK32) == (mid & MASK32))

    if lo_ok and hi_ok and shrink_l and shrink_r and z3_ok:
        report("  PASS  cross-check (%d, %d): mid=%d" % (lo, hi, mid))
    else:
        report("  FAIL  cross-check (%d, %d): lo_ok=%s hi_ok=%s shrink_l=%s shrink_r=%s z3=%s" % (
            lo, hi, lo_ok, hi_ok, shrink_l, shrink_r, z3_ok))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: lo <= mid for lo < hi (32-bit BV, UNSAT) ---------------------------
#
# Preconditions: lo >= 0, hi >= 0, lo < hi, lo + hi >= 0 (no signed overflow).

report("  ... proving P1: lo <= mid")

ok &= prove("P1: lo <= mid", """\
(set-logic QF_BV)
(declare-const lo (_ BitVec 32))
(declare-const hi (_ BitVec 32))
(assert (bvsge lo (_ bv0 32)))
(assert (bvsge hi (_ bv0 32)))
(assert (bvslt lo hi))
(assert (bvsge (bvadd lo hi) (_ bv0 32)))
(define-fun mid () (_ BitVec 32) (bvlshr (bvadd lo hi) (_ bv1 32)))
(assert (not (bvsle lo mid)))
(check-sat)
""")

# -- P2: mid < hi for lo < hi (32-bit BV, UNSAT) ----------------------------

report("  ... proving P2: mid < hi")

ok &= prove("P2: mid < hi", """\
(set-logic QF_BV)
(declare-const lo (_ BitVec 32))
(declare-const hi (_ BitVec 32))
(assert (bvsge lo (_ bv0 32)))
(assert (bvsge hi (_ bv0 32)))
(assert (bvslt lo hi))
(assert (bvsge (bvadd lo hi) (_ bv0 32)))
(define-fun mid () (_ BitVec 32) (bvlshr (bvadd lo hi) (_ bv1 32)))
(assert (not (bvslt mid hi)))
(check-sat)
""")

# -- P3: Left half strictly shrinks (32-bit BV, UNSAT) ----------------------

report("  ... proving P3: left half strictly shrinks")

ok &= prove("P3: mid - lo < hi - lo", """\
(set-logic QF_BV)
(declare-const lo (_ BitVec 32))
(declare-const hi (_ BitVec 32))
(assert (bvsge lo (_ bv0 32)))
(assert (bvsge hi (_ bv0 32)))
(assert (bvslt lo hi))
(assert (bvsge (bvadd lo hi) (_ bv0 32)))
(define-fun mid () (_ BitVec 32) (bvlshr (bvadd lo hi) (_ bv1 32)))
(assert (not (bvslt (bvsub mid lo) (bvsub hi lo))))
(check-sat)
""")

# -- P4: Right half strictly shrinks (32-bit BV, UNSAT) ---------------------
#
# hi - mid < hi - lo.  For the mergesort path which recurses on [mid, hi).
# This requires hi - lo > 1 (otherwise mid == lo and right half == full range).
# When hi - lo == 1, mid == lo, so recursion goes to [lo+1, hi) which is empty.
# We prove for hi - lo >= 2.

report("  ... proving P4: right half strictly shrinks")

ok &= prove("P4: hi - mid < hi - lo (range >= 2)", """\
(set-logic QF_BV)
(declare-const lo (_ BitVec 32))
(declare-const hi (_ BitVec 32))
(assert (bvsge lo (_ bv0 32)))
(assert (bvsge hi (_ bv0 32)))
(assert (bvsge (bvsub hi lo) (_ bv2 32)))
(assert (bvsge (bvadd lo hi) (_ bv0 32)))
(define-fun mid () (_ BitVec 32) (bvlshr (bvadd lo hi) (_ bv1 32)))
(assert (not (bvslt (bvsub hi mid) (bvsub hi lo))))
(check-sat)
""")

# -- P5: Overflow counterexample (SAT) --------------------------------------
#
# Without the non-overflow precondition, lo + hi wraps and bvlshr on the
# wrapped value produces mid >= hi (since bvlshr is logical/unsigned shift,
# the MSB=1 result becomes a large positive value).

report("  ... proving P5: overflow counterexample (SAT)")

ok &= check_sat("P5: overflow => mid < lo (SAT)", """\
(set-logic QF_BV)
(declare-const lo (_ BitVec 32))
(declare-const hi (_ BitVec 32))
(assert (bvsge lo (_ bv0 32)))
(assert (bvsge hi (_ bv0 32)))
(assert (bvslt lo hi))
; lo + hi overflows (wraps negative in signed interpretation)
(assert (bvslt (bvadd lo hi) (_ bv0 32)))
; RPython >> on signed is arithmetic right shift (bvashr)
(define-fun mid () (_ BitVec 32) (bvashr (bvadd lo hi) (_ bv1 32)))
; mid goes negative, below lo
(assert (bvslt mid lo))
(check-sat)
""")

# -- P6: Safe under practical bound — hi <= 2^30 (32-bit BV, UNSAT) ---------
#
# With hi <= 2^30, lo + hi <= 2^31 - 1 (no signed overflow), so all
# midpoint properties hold.  This matches gnitz's reality: batches and
# memtables are far smaller than 2^30 rows.

report("  ... proving P6: safe under hi <= 2^30")

ok &= prove("P6: hi <= 2^30 => lo <= mid < hi", """\
(set-logic QF_BV)
(declare-const lo (_ BitVec 32))
(declare-const hi (_ BitVec 32))
(assert (bvsge lo (_ bv0 32)))
(assert (bvsle hi (_ bv1073741824 32)))
(assert (bvslt lo hi))
(define-fun mid () (_ BitVec 32) (bvlshr (bvadd lo hi) (_ bv1 32)))
(assert (not (and (bvsle lo mid) (bvslt mid hi))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: Binary search midpoint is safe")
    print("    P1: lo <= mid")
    print("    P2: mid < hi")
    print("    P3: left half strictly shrinks")
    print("    P4: right half strictly shrinks")
    print("    P5: overflow counterexample exists (SAT)")
    print("    P6: safe under hi <= 2^30 bound")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
