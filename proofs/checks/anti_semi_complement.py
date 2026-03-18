"""Z3 formal proofs for anti-join / semi-join complement partition.

op_anti_join_* and op_semi_join_* (gnitz/dbsp/ops/anti_join.py:30-149) are
complementary operators: for each left-delta row, exactly one emits it.
The decision is whether the row's key has a positive-weight match in the trace.

Membership predicate: weight > r_int64(0)  (gnitz/dbsp/ops/anti_join.py:48,116)

For integer-valued weights this is equivalent to weight >= 1.
There is no 64-bit signed integer strictly between 0 and 1.

  P1a. Integer gap: no signed 64-bit integer lies strictly in (0, 1)
       (64-bit BV, UNSAT) — proves w > 0 implies w >= 1
  P1b. Threshold converse: w >= 1 implies w > 0
       (64-bit BV, UNSAT) — completes the biconditional w > 0 <=> w >= 1
  P2.  Anti and semi emit decisions are mutually exclusive: the row
       cannot simultaneously satisfy "in_trace" and "not in_trace"
       (64-bit BV, UNSAT)
  P3.  Weight trichotomy: every i64 weight is negative, zero, or positive —
       the 3-way membership decision is exhaustive (64-bit BV, UNSAT)
  P4.  ADAPTIVE_SWAP_THRESHOLD = 1 is the identity multiplier:
       delta > 1 * trace iff delta > trace (16-bit BV, UNSAT)

4 Z3 queries + ~8 Python cross-checks.  Runs under PyPy2.
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


# -- Constants ----------------------------------------------------------------

# From gnitz/dbsp/ops/anti_join.py:8
ADAPTIVE_SWAP_THRESHOLD = 1

MAX_I64 = (1 << 63) - 1
MIN_I64 = -(1 << 63)

# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: anti-join / semi-join complement partition")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-checks -------------------------------------------------------------

report("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_int64  # noqa: E402
report("  ... imports done")
sys.stdout.flush()


def in_trace(w):
    """Trace membership predicate from anti_join.py: weight > 0."""
    return w > r_int64(0)


# Complement partition: for every weight, exactly one of anti/semi fires.
report("  ... cross-checking complement partition")
threshold_vectors = [
    (r_int64(MIN_I64), False),
    (r_int64(-5),      False),
    (r_int64(-1),      False),
    (r_int64(0),       False),
    (r_int64(1),       True),
    (r_int64(5),       True),
    (r_int64(MAX_I64), True),
]
for w, expected_in_trace in threshold_vectors:
    it   = in_trace(w)
    anti = not it
    semi = it
    partition_ok = (anti != semi)
    threshold_ok = (it == expected_in_trace)
    if partition_ok and threshold_ok:
        report("  PASS  cross-check w=%d: in_trace=%s anti=%s semi=%s" % (
            int(w), it, anti, semi))
    else:
        report("  FAIL  cross-check w=%d: in_trace=%s(exp=%s) partition_ok=%s" % (
            int(w), it, expected_in_trace, partition_ok))
        ok = False

# Threshold equivalence: w > 0 <=> w >= 1
report("  ... cross-checking threshold equivalence w > 0 <=> w >= 1")
equiv_vectors = [
    (r_int64(MIN_I64), False),
    (r_int64(-1),      False),
    (r_int64(0),       False),
    (r_int64(1),       True),
    (r_int64(2),       True),
    (r_int64(MAX_I64), True),
]
for w, expected in equiv_vectors:
    gt = (w > r_int64(0))
    ge = (w >= r_int64(1))
    if gt == expected and ge == expected and gt == ge:
        report("  PASS  cross-check w=%d: w>0=%s == w>=1=%s" % (int(w), gt, ge))
    else:
        report("  FAIL  cross-check w=%d: w>0=%s w>=1=%s expected=%s" % (
            int(w), gt, ge, expected))
        ok = False

# ADAPTIVE_SWAP_THRESHOLD cross-check
report("  ... cross-checking ADAPTIVE_SWAP_THRESHOLD=1 identity")
swap_vectors = [(10, 5), (3, 3), (2, 4), (1, 0), (0, 1), (256, 255)]
for delta, trace in swap_vectors:
    threshold_result = delta > ADAPTIVE_SWAP_THRESHOLD * trace
    direct_result    = delta > trace
    if threshold_result == direct_result:
        report("  PASS  cross-check swap: %d > 1*%d (%s) == %d > %d (%s)" % (
            delta, trace, threshold_result, delta, trace, direct_result))
    else:
        report("  FAIL  cross-check swap: delta=%d trace=%d: 1*t=%s direct=%s" % (
            delta, trace, threshold_result, direct_result))
        ok = False

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P1a: Integer gap: no signed 64-bit integer lies strictly in (0, 1) ------
#
# The membership check `weight > r_int64(0)` is the DBSP predicate for
# "this key is present in the trace set".  For integer-valued weights,
# no weight can satisfy 0 < w < 1 (integers are discrete).
# Therefore: w > 0 implies w >= 1, with no intermediate cases.

report("  ... proving P1a: no signed 64-bit integer in open interval (0, 1)")
ok &= prove("P1a: UNSAT of bvsgt w 0 AND bvslt w 1", """\
(set-logic QF_BV)
(declare-const w (_ BitVec 64))
; w > 0 (signed)
(assert (bvsgt w (_ bv0 64)))
; w < 1 (signed): impossible for integers — no integer lies strictly between 0 and 1
(assert (bvslt w (_ bv1 64)))
(check-sat)
""")

# -- P1b: Threshold converse: w >= 1 implies w > 0 ---------------------------
#
# Completes the biconditional: w > 0 <=> w >= 1.
# Direction: if w >= 1 (signed) then w > 0 (signed).

report("  ... proving P1b: w >= 1 implies w > 0 (converse direction)")
ok &= prove("P1b: UNSAT of bvsge w 1 AND bvsle w 0", """\
(set-logic QF_BV)
(declare-const w (_ BitVec 64))
; w >= 1 (signed)
(assert (bvsge w (_ bv1 64)))
; w <= 0 (signed): contradicts w >= 1 > 0
(assert (bvsle w (_ bv0 64)))
(check-sat)
""")

# -- P2: Anti and semi emit decisions are mutually exclusive ------------------
#
# anti_emit = NOT in_trace = (w <= 0)
# semi_emit = in_trace     = (w > 0)
# These two conditions partition all weights; they cannot both hold.
# Proves: UNSAT of (w > 0 AND w <= 0).

report("  ... proving P2: anti and semi emit decisions are mutually exclusive")
ok &= prove("P2: UNSAT of semi_emit AND anti_emit (w>0 AND w<=0)", """\
(set-logic QF_BV)
(declare-const w (_ BitVec 64))
; semi_emit: weight > 0
(assert (bvsgt w (_ bv0 64)))
; anti_emit: weight <= 0 — contradiction with semi_emit
(assert (bvsle w (_ bv0 64)))
(check-sat)
""")

# -- P3: Weight trichotomy: negative, zero, or positive (exhaustive) ----------
#
# The three-way membership decision in anti/semi-join is complete:
# every i64 weight falls into exactly one of {< 0}, {== 0}, {> 0}.
# Proves that no weight escapes the three cases.

report("  ... proving P3: weight trichotomy is exhaustive")
ok &= prove("P3: every weight is negative, zero, or positive", """\
(set-logic QF_BV)
(declare-const w (_ BitVec 64))
; Negate: w does not fall in any of the three cases
(assert (not (or
  (bvslt w (_ bv0 64))
  (= w (_ bv0 64))
  (bvsgt w (_ bv0 64)))))
(check-sat)
""")

# -- P4: ADAPTIVE_SWAP_THRESHOLD = 1 is the identity multiplier ---------------
#
# anti_join and semi_join swap to the trace-driven walk when:
#   delta_len > ADAPTIVE_SWAP_THRESHOLD * trace_len
# With ADAPTIVE_SWAP_THRESHOLD = 1 this becomes delta_len > trace_len.
# Multiplying by 1 is the identity: d > 1*t iff d > t.
# Proves: UNSAT of (d > 1*t) XOR (d > t).
# Uses 16-bit BV (bvmul with one concrete operand is fast at any width).

report("  ... proving P4: ADAPTIVE_SWAP_THRESHOLD=1 is identity (d > 1*t iff d > t)")
ok &= prove("P4: d > 1*t <=> d > t (bvmul by 1, 16-bit)", """\
(set-logic QF_BV)
(declare-const d (_ BitVec 16))
(declare-const t (_ BitVec 16))
; Negate: (d > 1*t) XOR (d > t)
(assert (xor
  (bvugt d (bvmul (_ bv1 16) t))
  (bvugt d t)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: anti-join / semi-join complement partition")
    print("    P1a: no signed integer strictly between 0 and 1 (integer gap)")
    print("    P1b: w >= 1 implies w > 0 (threshold equivalence converse)")
    print("    P2: anti and semi emit decisions are mutually exclusive")
    print("    P3: weight trichotomy is exhaustive")
    print("    P4: ADAPTIVE_SWAP_THRESHOLD=1 is identity multiplier")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
