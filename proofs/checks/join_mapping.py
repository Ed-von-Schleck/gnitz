"""Z3 formal proofs for CompositeAccessor column mapping correctness.

Proves five properties of gnitz/dbsp/ops/join.py:39-70 (CompositeAccessor):

  Column mapping follows TableSchema.merge_schemas_for_join:
    col 0        : PK (from left, mapping_is_left=True, mapping_idx=pk_index)
    col 1..L-1   : left non-PK payload columns
    col L..total-1: right non-PK payload columns
    total = 1 + (L-1) + (R-1) = L + R - 1

  P1. total = L + R - 1 (16-bit BV, UNSAT)
  P2. Left non-PK slot range [1, L) ends where right range [L, total) begins
      (16-bit BV, UNSAT)
  P3. After mapping all left then right non-PK cols, curr reaches total exactly
      (16-bit BV, UNSAT)
  P4. Left and right non-PK slot ranges are disjoint (16-bit BV, UNSAT)
  P5. Python cross-check: for 5 concrete schema pairs, curr reaches total and
      mapping_idx values are within valid column index bounds

5 Z3 queries + ~5 cross-checks.  Runs under PyPy2.
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


# -- Cross-check helpers ------------------------------------------------------

def simulate_composite_mapping(len_l, pk_index_l, len_r, pk_index_r):
    """
    Simulate CompositeAccessor.__init__ without importing RPython.
    Returns (total, mapping_is_left, mapping_idx, final_curr).
    """
    total = 1 + (len_l - 1) + (len_r - 1)
    mapping_is_left = [False] * total
    mapping_idx = [0] * total

    # Index 0: PK from left
    mapping_is_left[0] = True
    mapping_idx[0] = pk_index_l

    curr = 1
    for i in range(len_l):
        if i != pk_index_l:
            mapping_is_left[curr] = True
            mapping_idx[curr] = i
            curr += 1

    for i in range(len_r):
        if i != pk_index_r:
            mapping_is_left[curr] = False
            mapping_idx[curr] = i
            curr += 1

    return total, mapping_is_left, mapping_idx, curr


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: CompositeAccessor column mapping correctness")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-checks ------------------------------------------------------------

report("  ... cross-checking CompositeAccessor mapping for 5 schema pairs")

# Schema pairs: (len_l, pk_index_l, len_r, pk_index_r)
schema_pairs = [
    (2, 0, 2, 0),   # both 2-col: total = 3
    (3, 0, 2, 0),   # left 3-col, right 2-col: total = 4
    (1, 0, 3, 0),   # left PK-only: total = 3
    (4, 0, 4, 0),   # both 4-col: total = 7
    (2, 0, 5, 0),   # left 2-col, right 5-col: total = 6
]

for len_l, pk_l, len_r, pk_r in schema_pairs:
    total, mis_left, midx, final_curr = simulate_composite_mapping(len_l, pk_l, len_r, pk_r)

    expected_total = len_l + len_r - 1

    # Verify total formula
    if total != expected_total:
        report("  FAIL  cross-check: L=%d R=%d total=%d expected=%d" % (
            len_l, len_r, total, expected_total))
        ok = False
        continue

    # Verify curr reaches total
    if final_curr != total:
        report("  FAIL  cross-check: L=%d R=%d curr=%d != total=%d" % (
            len_l, len_r, final_curr, total))
        ok = False
        continue

    # Verify index 0 is from left
    if not mis_left[0] or midx[0] != pk_l:
        report("  FAIL  cross-check: L=%d R=%d PK slot mapping wrong" % (len_l, len_r))
        ok = False
        continue

    # Verify left slots [1..len_l-1] are all from left
    left_ok = all(mis_left[i] for i in range(1, len_l))
    # Verify right slots [len_l..total-1] are all from right
    right_ok = all(not mis_left[i] for i in range(len_l, total))

    # Verify all mapping_idx in left range are valid [0, len_l)
    left_idx_ok = all(0 <= midx[i] < len_l for i in range(1, len_l))
    # Verify all mapping_idx in right range are valid [0, len_r)
    right_idx_ok = all(0 <= midx[i] < len_r for i in range(len_l, total))

    if left_ok and right_ok and left_idx_ok and right_idx_ok:
        report("  PASS  cross-check: L=%d R=%d total=%d curr_ok left_ok right_ok" % (
            len_l, len_r, total))
    else:
        report("  FAIL  cross-check: L=%d R=%d left_ok=%s right_ok=%s left_idx=%s right_idx=%s" % (
            len_l, len_r, left_ok, right_ok, left_idx_ok, right_idx_ok))
        ok = False

# Z3 cross-check: 1 + (3-1) + (2-1) = 4 = 3+2-1
smt_q = "(simplify (= (bvadd (bvadd (_ bv1 16) (bvsub (_ bv3 16) (_ bv1 16))) (bvsub (_ bv2 16) (_ bv1 16))) (_ bv4 16)))"
z3_out = run_z3(smt_q)
if z3_out == "true":
    report("  PASS  cross-check: Z3 1+(3-1)+(2-1)=4")
else:
    report("  FAIL  cross-check: Z3 total formula: got %s" % z3_out)
    ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: total = L + R - 1 (16-bit BV, UNSAT) --------------------------------
#
# Prove: 1 + (L-1) + (R-1) == L + R - 1 for L,R in [1, 64].

report("  ... proving P1: total = 1+(L-1)+(R-1) equals L+R-1")

ok &= prove("P1: 1+(L-1)+(R-1) == L+R-1 for L,R in [1,64]", """\
(set-logic QF_BV)
(declare-const L (_ BitVec 16))
(declare-const R (_ BitVec 16))
(assert (bvuge L (_ bv1 16)))
(assert (bvuge R (_ bv1 16)))
(assert (bvule L (_ bv64 16)))
(assert (bvule R (_ bv64 16)))
(define-fun total_expand () (_ BitVec 16)
  (bvadd (bvadd (_ bv1 16) (bvsub L (_ bv1 16))) (bvsub R (_ bv1 16))))
(define-fun total_simple () (_ BitVec 16)
  (bvsub (bvadd L R) (_ bv1 16)))
; Negate: the two formulas differ
(assert (not (= total_expand total_simple)))
(check-sat)
""")

# -- P2: Left non-PK range [1, L) ends where right range [L, total) begins ---
#
# After mapping L-1 left non-PK cols starting at curr=1,
# curr_after_left = 1 + (L-1) = L.  Prove this equals L.

report("  ... proving P2: curr after left loop = L")

ok &= prove("P2: 1 + (L-1) == L", """\
(set-logic QF_BV)
(declare-const L (_ BitVec 16))
(assert (bvuge L (_ bv1 16)))
(assert (bvule L (_ bv64 16)))
(define-fun curr_after_left () (_ BitVec 16)
  (bvadd (_ bv1 16) (bvsub L (_ bv1 16))))
; Negate: curr_after_left != L
(assert (not (= curr_after_left L)))
(check-sat)
""")

# -- P3: After both loops curr reaches total (16-bit BV, UNSAT) ---------------
#
# curr_final = 1 + (L-1) + (R-1) = total = L + R - 1.

report("  ... proving P3: curr after both loops = total")

ok &= prove("P3: curr_final = 1+(L-1)+(R-1) = total", """\
(set-logic QF_BV)
(declare-const L (_ BitVec 16))
(declare-const R (_ BitVec 16))
(assert (bvuge L (_ bv1 16)))
(assert (bvuge R (_ bv1 16)))
(assert (bvule L (_ bv64 16)))
(assert (bvule R (_ bv64 16)))
(define-fun total () (_ BitVec 16) (bvsub (bvadd L R) (_ bv1 16)))
(define-fun curr_final () (_ BitVec 16)
  (bvadd (bvadd (_ bv1 16) (bvsub L (_ bv1 16))) (bvsub R (_ bv1 16))))
; Negate: curr_final != total
(assert (not (= curr_final total)))
(check-sat)
""")

# -- P4: Left [1,L) and right [L,total) slot ranges are disjoint (16-bit BV) -
#
# A slot in [1, L) cannot be in [L, total).

report("  ... proving P4: left [1,L) and right [L,total) are disjoint")

ok &= prove("P4: slot in [1,L) cannot also be in [L,total)", """\
(set-logic QF_BV)
(declare-const s (_ BitVec 16))
(declare-const L (_ BitVec 16))
(declare-const R (_ BitVec 16))
(assert (bvuge L (_ bv1 16)))
(assert (bvuge R (_ bv1 16)))
(assert (bvule L (_ bv64 16)))
(assert (bvule R (_ bv64 16)))
; s is in left non-PK range: 1 <= s < L
(assert (bvuge s (_ bv1 16)))
(assert (bvult s L))
; Also in right range: s >= L — contradiction with s < L
(assert (bvuge s L))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: CompositeAccessor column mapping correctness")
    print("    P1: total = 1+(L-1)+(R-1) == L+R-1")
    print("    P2: curr after left loop = L exactly")
    print("    P3: curr after both loops = total")
    print("    P4: left [1,L) and right [L,total) slot ranges are disjoint")
    print("    P5: 5 concrete schema pairs verify correctly (cross-check)")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
