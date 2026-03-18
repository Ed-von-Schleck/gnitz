"""Z3 formal proofs for ReduceAccessor slot index coverage.

Source: gnitz/dbsp/ops/reduce.py:45-71 (ReduceAccessor.__init__)

ReduceAccessor builds a mapping_to_input array of size num_out.  Two paths:

  Natural PK (single U64/U128 group col):
    num_out = 1 + A
    Writes: slot 0 (group col as PK), slots 1..A (agg sentinels)

  Synthetic PK (all other cases):
    num_out = 1 + G + A   (G = len(group_indices), A = num_aggs)
    Writes: slot 0 (-2), slots 1..G (group indices), slots 1+G..G+A (agg)
    agg_base = 1 + G

  P1. Natural path: max write index A < num_out = 1+A (8-bit BV, UNSAT)
  P2. Synthetic path: max write index G+A < num_out = 1+G+A (8-bit BV, UNSAT)
  P3. agg_base = 1+G is strictly above all group slot indices [1..G] (8-bit BV, UNSAT)
  P4. Group slots {1..G} and agg slots {1+G..G+A} are disjoint (8-bit BV, UNSAT)

4 Z3 queries + 6 cross-checks.  Runs under PyPy2.
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

print("=" * 56)
print("  Z3 PROOF: ReduceAccessor slot index coverage")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-checks -------------------------------------------------------------

report("  ... cross-checking ReduceAccessor slot coverage on concrete (G, A) pairs")

test_cases = [(1, 0), (1, 1), (1, 3), (2, 2), (3, 1), (1, 50)]

for (G, A) in test_cases:
    # Synthetic PK path
    num_out = 1 + G + A
    mapping = [None] * num_out
    mapping[0] = -2
    for i in range(G):
        mapping[i + 1] = i      # group col index (simulated)
    agg_base = 1 + G
    for i in range(A):
        mapping[agg_base + i] = -1 if i == 0 else -(i + 2)

    if None in mapping:
        report("  FAIL  cross-check (G=%d, A=%d) synthetic: slot %d not written" % (
            G, A, mapping.index(None)))
        ok = False
    else:
        report("  PASS  cross-check (G=%d, A=%d) synthetic: all %d slots written" % (
            G, A, num_out))

    # Natural PK path (G=1 only)
    if G == 1:
        num_out_nat = 1 + A
        mapping_nat = [None] * num_out_nat
        mapping_nat[0] = 0    # group col index
        for i in range(A):
            mapping_nat[1 + i] = -1 if i == 0 else -(i + 2)

        if None in mapping_nat:
            report("  FAIL  cross-check (G=%d, A=%d) natural: slot %d not written" % (
                G, A, mapping_nat.index(None)))
            ok = False
        else:
            report("  PASS  cross-check (G=%d, A=%d) natural:  all %d slots written" % (
                G, A, num_out_nat))

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: Natural path max write index A < num_out = 1+A (8-bit BV, UNSAT) ----
#
# Slots written: 0, 1, ..., A.  Max index = A.  num_out = 1+A.
# Negate: A >= 1+A  (only possible on overflow; restricted to A <= 50)

report("  ... proving P1: natural path max write index A < num_out (1+A)")

ok &= prove("P1: natural path slot A < num_out (1+A) for A in [0,50]", """\
(set-logic QF_BV)
(declare-const A (_ BitVec 8))
(assert (bvule A (_ bv50 8)))
; Negate: A >= 1+A  (overflow condition — impossible in this range)
(assert (not (bvult A (bvadd A (_ bv1 8)))))
(check-sat)
""")

# -- P2: Synthetic path max write index G+A < num_out = 1+G+A (UNSAT) --------
#
# Slots written: 0, 1..G, 1+G..G+A.  Max index = G+A.  num_out = 1+G+A.
# Negate: G+A >= 1+G+A  (impossible in range G in [1,10], A in [0,50])

report("  ... proving P2: synthetic path max write index G+A < num_out (1+G+A)")

ok &= prove("P2: synthetic path slot G+A < num_out (1+G+A) for G in [1,10], A in [0,50]", """\
(set-logic QF_BV)
(declare-const G (_ BitVec 8))
(declare-const A (_ BitVec 8))
(assert (bvuge G (_ bv1 8)))
(assert (bvule G (_ bv10 8)))
(assert (bvule A (_ bv50 8)))
(define-fun max_idx () (_ BitVec 8) (bvadd G A))
(define-fun num_out () (_ BitVec 8) (bvadd (bvadd G A) (_ bv1 8)))
; Negate: max_idx >= num_out
(assert (not (bvult max_idx num_out)))
(check-sat)
""")

# -- P3: agg_base = 1+G is strictly above all group slot indices [1..G] -------
#
# Group slots are [1..G].  agg_base = 1+G.  Any group slot i satisfies i <= G < 1+G.

report("  ... proving P3: agg_base (1+G) strictly above all group slot indices [1..G]")

ok &= prove("P3: agg_base (1+G) > i for all group slot indices i in [1,G]", """\
(set-logic QF_BV)
(declare-const G (_ BitVec 8))
(declare-const i (_ BitVec 8))
(assert (bvuge G (_ bv1 8)))
(assert (bvule G (_ bv10 8)))
; i is a group slot index: i in [1, G]
(assert (bvuge i (_ bv1 8)))
(assert (bvule i G))
(define-fun agg_base () (_ BitVec 8) (bvadd G (_ bv1 8)))
; Negate: agg_base <= i  (agg base overlaps with group slots)
(assert (not (bvugt agg_base i)))
(check-sat)
""")

# -- P4: Group slots {1..G} and agg slots {1+G..G+A} are disjoint ------------
#
# A group slot i in [1,G] and an agg slot j in [1+G, G+A] cannot be equal.
# When A=0 the agg range is empty (j constraints unsatisfiable) — vacuously unsat.

report("  ... proving P4: group slots {1..G} and agg slots {1+G..G+A} are disjoint")

ok &= prove("P4: group and agg slot ranges are disjoint", """\
(set-logic QF_BV)
(declare-const G (_ BitVec 8))
(declare-const A (_ BitVec 8))
(declare-const i (_ BitVec 8))
(declare-const j (_ BitVec 8))
(assert (bvuge G (_ bv1 8)))
(assert (bvule G (_ bv10 8)))
(assert (bvule A (_ bv50 8)))
; i = group slot: [1, G]
(assert (bvuge i (_ bv1 8)))
(assert (bvule i G))
; j = agg slot: [1+G, G+A]
(assert (bvuge j (bvadd G (_ bv1 8))))
(assert (bvule j (bvadd G A)))
; Negate: i >= j  (slots overlap)
(assert (not (bvult i j)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: ReduceAccessor slot index coverage")
    print("    P1: natural path max index A < num_out (1+A)")
    print("    P2: synthetic path max index G+A < num_out (1+G+A)")
    print("    P3: agg_base (1+G) is strictly above all group slot indices")
    print("    P4: group slots {1..G} and agg slots {1+G..G+A} are disjoint")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
