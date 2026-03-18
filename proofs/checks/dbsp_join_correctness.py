"""Z3 formal proofs for DBSP incremental join correctness.

Gnitz implements the DBSP bilinear join decomposition (McSherry, 2013):

  delta(A join B) = J(dA, dB) + J(dA, Bbar) + J(dB, Abar)

where Abar = integral(A) is the trace (accumulated history), and dA is the
delta (current increment).  Each term maps to a gnitz operator:

  J(dA, dB)   -- op_join_delta_delta  (gnitz/dbsp/ops/join.py:264-322)
  J(dA, Bbar) -- op_join_delta_trace  (gnitz/dbsp/ops/join.py:129-201)
  J(dB, Abar) -- op_join_delta_trace  (gnitz/dbsp/ops/join.py:129-201) swapped

For a matching row pair (same join key), the weight computation is:

  w(J(dA, dB)  ) = da * db      (join.py:309: w_delta * w_trace)
  w(J(dA, Bbar)) = da * wb      (join.py:164: w_delta * w_trace)
  w(J(dB, Abar)) = db * wa      (same operator, sides swapped)

Incremental update ΔJ = da*db + da*wb + db*wa.

The full join weight after the update: wa*wb + ΔJ = (wa+da)*(wb+db).

Properties proved:

  P1. Bilinear decomposition (correctness of the three-term formula):
      (wa+da)*(wb+db) - wa*wb = da*wb + wa*db + da*db   (32-bit BV, UNSAT)
      -- the three DT+DD operators produce the EXACT join delta with no error

  P2. DD term is necessary (two operators are insufficient):
      EXISTS wa,wb,da,db: da*wb + wa*db != (wa+da)*(wb+db) - wa*wb   (32-bit BV, SAT)
      -- a two-operator DT-only circuit is INCORRECT for simultaneous updates

  P3. Zero-weight output is possible from non-zero inputs (weight overflow):
      EXISTS da != 0, wb != 0: da * wb = 0   (32-bit BV, SAT)
      -- the join.py:165 zero-weight guard is NOT merely an optimisation;
         modular overflow can produce spurious weight-0 rows that must be dropped
         for Z-Set correctness (a weight-0 row means absence from the set)

  P4. Negate homomorphism (join commutes with weight negation):
      forall da, wb: (-da) * wb = -(da * wb)   (32-bit BV, UNSAT)
      -- negate(J(A, B)) = J(negate(A), B) at the weight level;
         required for the Z-Set group structure used in DBSP negate/union

  P5. Bilinear symmetry (sum of two DT terms is order-independent):
      forall wa,wb,da,db: da*wb + wa*db = db*wa + wb*da   (32-bit BV, UNSAT)
      -- J(dA, Bbar) + J(dB, Abar) = J(dB, Abar) + J(dA, Bbar);
         the two DT operators can be fired in either order; their sum is identical

  P6. Initial-state reduction (empty traces collapse to DD-only):
      wa = 0, wb = 0 => da*wb + wa*db + da*db = da*db   (32-bit BV, UNSAT)
      -- on the very first tick (Abar = Bbar = 0), only op_join_delta_delta
         contributes; both DT terms are zero and can be skipped safely

6 Z3 queries (5 UNSAT + 1 SAT) + 12 Python cross-checks.  Runs under PyPy2.
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


# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: DBSP incremental join correctness")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-checks: validate bilinear identity against RPython -----------------

report("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_int64, intmask  # noqa: E402
report("  ... imports done")
sys.stdout.flush()


def mul64(a, b):
    """Wrapping i64 multiplication, matching join.py:164 intmask(w_delta * w_trace)."""
    return intmask(r_int64(a) * r_int64(b))


def add64(a, b):
    """Wrapping i64 addition."""
    return intmask(r_int64(a) + r_int64(b))


def sub64(a, b):
    """Wrapping i64 subtraction."""
    return intmask(r_int64(a) - r_int64(b))


# bilinear_lhs(wa, wb, da, db) = (wa+da)*(wb+db) - wa*wb
def bilinear_lhs(wa, wb, da, db):
    return sub64(mul64(add64(wa, da), add64(wb, db)), mul64(wa, wb))


# bilinear_rhs(wa, wb, da, db) = da*wb + wa*db + da*db   (the three operator terms)
def bilinear_rhs(wa, wb, da, db):
    return add64(mul64(da, wb), add64(mul64(wa, db), mul64(da, db)))


report("  ... cross-checking bilinear decomposition identity")

# Cross-check vectors: (wa, wb, da, db)
bilinear_vectors = [
    (0,       0,       0,        0),         # all zero
    (1,       1,       1,        1),         # all one: (2*2 - 1*1) = 3, (1+1+1)=3
    (100,     200,     10,       20),        # typical values
    (-5,      3,       2,       -7),         # sign mixing
    (MAX_I64, 1,       1,        0),         # near max, zero db
    (0,       0,       MAX_I64,  MIN_I64),   # empty trace, large deltas
    (1 << 62, 2,       2,        2),         # overflow case
    (MIN_I64, MIN_I64, 1,        1),         # min values in trace
]

for wa, wb, da, db in bilinear_vectors:
    lhs = bilinear_lhs(wa, wb, da, db) & MASK64
    rhs = bilinear_rhs(wa, wb, da, db) & MASK64
    if lhs == rhs:
        report("  PASS  cross-check bilinear wa=%d wb=%d da=%d db=%d: lhs==rhs==%d" % (
            wa, wb, da, db, lhs))
    else:
        report("  FAIL  cross-check bilinear wa=%d wb=%d da=%d db=%d: lhs=0x%016x rhs=0x%016x" % (
            wa, wb, da, db, lhs, rhs))
        ok = False

report("  ... cross-checking negate homomorphism neg(da)*wb == neg(da*wb)")

# Cross-check vectors: (da, wb)
neg_hom_vectors = [
    (1,       -1),
    (-5,      3),
    (MAX_I64, -1),
    (MIN_I64, 1),    # neg(MIN_I64) = MIN_I64 (fixed point), MIN_I64*1 = MIN_I64
    (42,      100),
    (-1,      -1),
]

for da, wb in neg_hom_vectors:
    neg_da_times_wb = mul64(intmask(-r_int64(da)), wb) & MASK64
    neg_of_product  = intmask(-r_int64(mul64(da, wb))) & MASK64
    if neg_da_times_wb == neg_of_product:
        report("  PASS  cross-check neg_hom da=%d wb=%d: both=0x%016x" % (
            da, wb, neg_da_times_wb))
    else:
        report("  FAIL  cross-check neg_hom da=%d wb=%d: neg(da)*wb=0x%016x neg(da*wb)=0x%016x" % (
            da, wb, neg_da_times_wb, neg_of_product))
        ok = False

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P1: DBSP bilinear decomposition ------------------------------------------
#
# The incremental join delta decomposes EXACTLY as:
#   (wa + da)*(wb + db) - wa*wb  =  da*wb + wa*db + da*db
#
# LHS: new join weight minus old join weight = the delta we need to produce.
# RHS: sum of weights from the three gnitz join operators:
#   da*wb  -> op_join_delta_trace(dA, Bbar)    join.py:164 w_delta*w_trace
#   wa*db  -> op_join_delta_trace(dB, Abar)    join.py:164 w_delta*w_trace (swapped)
#   da*db  -> op_join_delta_delta(dA, dB)      join.py:309 w_delta*w_trace
#
# Proved at 32-bit BV: property is width-independent (holds in any Z/2^n ring).
# This is the DBSP Fundamental Theorem at the weight level.

report("  ... proving P1: DBSP bilinear decomposition is exact")
ok &= prove("P1: (wa+da)*(wb+db) - wa*wb = da*wb + wa*db + da*db", """\
(set-logic QF_BV)
(declare-const wa (_ BitVec 32))
(declare-const wb (_ BitVec 32))
(declare-const da (_ BitVec 32))
(declare-const db (_ BitVec 32))
; Negate: LHS != RHS
(assert (not (=
  (bvsub (bvmul (bvadd wa da) (bvadd wb db)) (bvmul wa wb))
  (bvadd (bvmul da wb) (bvadd (bvmul wa db) (bvmul da db))))))
(check-sat)
""")

# -- P2: DD term is necessary (two-term circuit is incomplete) ----------------
#
# A circuit with only two DT operators (dropping op_join_delta_delta) produces
# an incorrect result when both sides update simultaneously.
# This proves that all three gnitz join operators are REQUIRED — no redundancy.
#
# Counterexample model: wa=0, wb=0, da=1, db=0x55555557 (non-zero, non-trivial)
# The missed term da*db is non-zero, so the two-term sum is wrong.

report("  ... proving P2: DD term is necessary (SAT counterexample)")
ok &= check_sat("P2: EXISTS wa,wb,da,db where two-term formula != full delta", """\
(set-logic QF_BV)
(declare-const wa (_ BitVec 32))
(declare-const wb (_ BitVec 32))
(declare-const da (_ BitVec 32))
(declare-const db (_ BitVec 32))
; Two-term (DT-only) formula differs from full three-term delta
(assert (not (=
  (bvadd (bvmul da wb) (bvmul wa db))
  (bvsub (bvmul (bvadd wa da) (bvadd wb db)) (bvmul wa wb)))))
(check-sat)
""")

# -- P3: Zero-weight output is possible from non-zero inputs ------------------
#
# Modular weight overflow can produce w_out = da*wb = 0 even when da != 0
# and wb != 0.  This means the join.py:165 check
#   if w_out != r_int64(0): ...
# is NOT merely an optimisation — it is a correctness requirement.
#
# A weight-0 row means "absent from the Z-Set"; emitting it would corrupt the
# output.  Dropping it is required.  The SAT witness confirms this is reachable.
#
# Example at 32-bit: da = 0xac623e86, wb = 0x80000000 -> da*wb = 0 mod 2^32.

report("  ... proving P3: overflow can produce zero output from nonzero inputs (SAT)")
ok &= check_sat("P3: EXISTS da!=0, wb!=0: da*wb = 0 (overflow zero-weight)", """\
(set-logic QF_BV)
(declare-const da (_ BitVec 32))
(declare-const wb (_ BitVec 32))
(assert (not (= da (_ bv0 32))))
(assert (not (= wb (_ bv0 32))))
(assert (= (bvmul da wb) (_ bv0 32)))
(check-sat)
""")

# -- P4: Negate homomorphism --------------------------------------------------
#
# The join weight computation w_delta * w_trace is homomorphic with respect to
# weight negation: negating either input negates the output.
#
#   neg(da) * wb  =  neg(da * wb)
#
# This proves J(negate(A), B) = negate(J(A, B)) at the weight level, which is
# required for the Z-Set group structure.  In particular, the DBSP circuit can
# freely move negate() past join() — the two operators commute.
#
# Uses bvmul(x, 0xFF...FF) for negation to avoid Z3 4.15.8 bvneg/bvsub bug.

report("  ... proving P4: negate homomorphism neg(da)*wb = neg(da*wb)")
ok &= prove("P4: bvmul(neg(da), wb) = neg(bvmul(da, wb))", """\
(set-logic QF_BV)
(declare-const da (_ BitVec 32))
(declare-const wb (_ BitVec 32))
; neg(x) = x * 0xFFFFFFFF  (= -1 mod 2^32; avoids Z3 4.15.8 bvneg bug)
(define-fun neg32 ((x (_ BitVec 32))) (_ BitVec 32)
  (bvmul x #xffffffff))
; Negate: neg(da)*wb != neg(da*wb)
(assert (not (= (bvmul (neg32 da) wb) (neg32 (bvmul da wb)))))
(check-sat)
""")

# -- P5: Bilinear symmetry of the two DT terms --------------------------------
#
# The sum of the two DT contributions is order-independent:
#
#   da*wb + wa*db  =  db*wa + wb*da
#
# This proves that it makes no algebraic difference which of dA or dB is placed
# on the "delta" side versus the "trace" side of op_join_delta_trace.  The two
# DT operators contribute the same total regardless of which one runs first,
# or which direction the delta-trace assignment is made.

report("  ... proving P5: sum of DT terms is symmetric (order-independent)")
ok &= prove("P5: da*wb + wa*db = db*wa + wb*da", """\
(set-logic QF_BV)
(declare-const wa (_ BitVec 32))
(declare-const wb (_ BitVec 32))
(declare-const da (_ BitVec 32))
(declare-const db (_ BitVec 32))
; Negate: sums differ
(assert (not (=
  (bvadd (bvmul da wb) (bvmul wa db))
  (bvadd (bvmul db wa) (bvmul wb da)))))
(check-sat)
""")

# -- P6: Initial-state reduction (empty traces collapse to DD-only) -----------
#
# On the very first tick, both integral traces are empty: wa = 0, wb = 0.
# The three-term formula reduces to just the DD term:
#
#   da*0 + 0*db + da*db  =  da*db
#
# Both DT terms contribute zero.  This proves that the first tick only needs
# op_join_delta_delta — no DT operators need to fire.
#
# In gnitz the circuit always includes all three operators, but this theorem
# confirms that the DT operators are idle on the first tick and produce no
# spurious output.

report("  ... proving P6: empty traces reduce to DD-only")
ok &= prove("P6: wa=0, wb=0 => da*wb + wa*db + da*db = da*db", """\
(set-logic QF_BV)
(declare-const da (_ BitVec 32))
(declare-const db (_ BitVec 32))
; wa = wb = 0
; Negate: three-term formula with zero traces != DD term alone
(assert (not (=
  (bvadd (bvmul da (_ bv0 32))
  (bvadd (bvmul (_ bv0 32) db)
         (bvmul da db)))
  (bvmul da db))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: DBSP incremental join correctness")
    print("    P1: bilinear decomposition is exact")
    print("         (wa+da)*(wb+db) - wa*wb = da*wb + wa*db + da*db")
    print("    P2: DD term is necessary (two-term circuit is incomplete)")
    print("    P3: zero-weight output possible from overflow (guard required)")
    print("    P4: negate homomorphism neg(da)*wb = neg(da*wb)")
    print("    P5: sum of DT terms is symmetric (order-independent)")
    print("    P6: empty traces reduce to DD-only (first tick)")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
