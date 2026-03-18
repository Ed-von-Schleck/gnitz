"""Z3 formal proofs for _op_union_merge sortedness preservation.

_op_union_merge (gnitz/dbsp/ops/linear.py:119-143) implements a standard
merge walk over two sorted input batches and marks its output sorted(True).

The three cases of the merge step are each proved to preserve non-decreasing
order: the next output key is always >= the current output key.

  All PKs are 128-bit unsigned; proofs use 16-bit unsigned BV — the
  monotonicity property is width-independent.

  P1. Case a < b (advance a-pointer): min(a_next, b) >= a  (16-bit BV, UNSAT)
  P2. Case a == b (advance both):     min(a_next, b_next) >= a  (16-bit BV, UNSAT)
  P3. Case b < a (advance b-pointer): min(a, b_next) >= b  (16-bit BV, UNSAT)

3 Z3 queries + ~9 Python cross-checks.  Runs under PyPy2.
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


# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: _op_union_merge sortedness preservation")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-checks: Python simulation of _op_union_merge ----------------------

report("  ... cross-checking merge walk output against simulated _op_union_merge")


def simulate_union_merge(a_seq, b_seq):
    """Direct Python simulation of _op_union_merge logic from linear.py."""
    out = []
    i = 0
    j = 0
    n_a = len(a_seq)
    n_b = len(b_seq)
    while i < n_a and j < n_b:
        pk_a = a_seq[i]
        pk_b = b_seq[j]
        if pk_a < pk_b:
            out.append(pk_a)
            i += 1
        elif pk_b < pk_a:
            out.append(pk_b)
            j += 1
        else:
            out.append(pk_a)
            out.append(pk_b)
            i += 1
            j += 1
    while i < n_a:
        out.append(a_seq[i])
        i += 1
    while j < n_b:
        out.append(b_seq[j])
        j += 1
    return out


# Test cases: (a_seq, b_seq) — both inputs must be sorted (non-decreasing).
test_cases = [
    ([1, 3, 5],         [2, 4, 6]),
    ([0, 0, 1],         [0, 1, 1]),      # duplicates: non-strict sort
    ([1, 2, 3],         [4, 5, 6]),      # no interleaving
    ([10],              [1, 20]),
    ([5, 5, 10],        [5, 8, 12]),
    ([],                [1, 2, 3]),      # empty left
    ([1, 2, 3],         []),             # empty right
    ([0xFFFF, 0xFFFF],  [0xFFFF]),       # all equal, max 16-bit
    ([0, 5, 10, 15],    [3, 7, 11, 20]),
]

for a_seq, b_seq in test_cases:
    out = simulate_union_merge(a_seq, b_seq)
    is_sorted = all(out[k] <= out[k + 1] for k in range(len(out) - 1))
    if is_sorted:
        report("  PASS  cross-check: merge(%s, %s) -> %s (sorted)" % (a_seq, b_seq, out))
    else:
        report("  FAIL  cross-check: merge(%s, %s) -> %s (NOT sorted)" % (a_seq, b_seq, out))
        ok = False

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)


# -- P1: Case a < b: next output = min(a_next, b) >= a ----------------------
#
# We output a (because a < b) and advance the a-pointer.  The b-pointer stays.
# Next merge step considers a_next (with a_next >= a since A is sorted) and b.
# Since b > a and a_next >= a, both candidates for the next output are >= a.
# The merge picks the smaller of (a_next, b), which is still >= a.

report("  ... proving P1: case a < b: min(a_next, b) >= a")
ok &= prove("P1: bvult a b AND bvuge a_next a => bvuge min(a_next, b) a", """\
(set-logic QF_BV)
(declare-const a      (_ BitVec 16))
(declare-const b      (_ BitVec 16))
(declare-const a_next (_ BitVec 16))
; Chose a this step (a < b); A is non-decreasing
(assert (bvult a b))
(assert (bvuge a_next a))
(define-fun next_out () (_ BitVec 16)
  (ite (bvult a_next b) a_next b))
; Negate: next_out < a
(assert (not (bvuge next_out a)))
(check-sat)
""")

# -- P2: Case a == b: next output = min(a_next, b_next) >= a -----------------
#
# We output both a and b (same key, in that order), then advance both pointers.
# a_next >= a (A non-decreasing) and b_next >= b == a (B non-decreasing).
# Both candidates for the next output are >= a, so min(a_next, b_next) >= a.
# This verifies that the pair (b, min(a_next, b_next)) in the output sequence
# satisfies b <= next, since b == a <= min(a_next, b_next).

report("  ... proving P2: case a == b: min(a_next, b_next) >= a")
ok &= prove("P2: bveq a b AND bvuge a_next a AND bvuge b_next b => bvuge min(a_next,b_next) a", """\
(set-logic QF_BV)
(declare-const a      (_ BitVec 16))
(declare-const b      (_ BitVec 16))
(declare-const a_next (_ BitVec 16))
(declare-const b_next (_ BitVec 16))
; Tie case (a == b); both inputs non-decreasing
(assert (= a b))
(assert (bvuge a_next a))
(assert (bvuge b_next b))
(define-fun next_out () (_ BitVec 16)
  (ite (bvult a_next b_next) a_next b_next))
; Negate: next_out < a
(assert (not (bvuge next_out a)))
(check-sat)
""")

# -- P3: Case b < a: next output = min(a, b_next) >= b ----------------------
#
# Symmetric to P1: we output b (because b < a) and advance the b-pointer.
# The a-pointer stays.  Next step considers a and b_next (with b_next >= b).
# Since a > b and b_next >= b, both candidates are >= b.

report("  ... proving P3: case b < a: min(a, b_next) >= b")
ok &= prove("P3: bvult b a AND bvuge b_next b => bvuge min(a, b_next) b", """\
(set-logic QF_BV)
(declare-const a      (_ BitVec 16))
(declare-const b      (_ BitVec 16))
(declare-const b_next (_ BitVec 16))
; Chose b this step (b < a); B is non-decreasing
(assert (bvult b a))
(assert (bvuge b_next b))
(define-fun next_out () (_ BitVec 16)
  (ite (bvult a b_next) a b_next))
; Negate: next_out < b
(assert (not (bvuge next_out b)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: _op_union_merge sortedness preservation")
    print("    P1: case a < b: next output >= a (advance a)")
    print("    P2: case a == b: next output >= a (advance both)")
    print("    P3: case b < a: next output >= b (advance b)")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
