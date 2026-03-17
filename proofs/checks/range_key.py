"""Z3 formal proofs for U128 range key scanning correctness.

Proves five properties of gnitz/catalog/program_cache.py:31-32:

  start_key = r_uint128(r_uint64(view_id)) << 64
  end_key   = r_uint128(r_uint64(view_id + 1)) << 64

Used in 6+ call sites to scan system tables by view_id prefix.
The view_id occupies the high 64 bits of a U128 key.

  P1. Range contains all matching keys (128-bit BV, UNSAT)
  P2. Ranges for different view_ids are disjoint (128-bit BV, UNSAT)
  P3. end > start for view_id < MAX (128-bit BV, UNSAT)
  P4. Overflow counterexample: view_id == MAX => end == 0 (SAT)
  P5. Range is exact — no extra keys included (128-bit BV, UNSAT)

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


def check_sat(label, smt_text):
    """Run a query expecting sat. Returns True on success."""
    result = run_z3(smt_text)
    if result == "sat":
        report("  PASS  %s" % label)
        return True
    else:
        report("  FAIL  %s: expected sat, got %s" % (label, result))
        return False


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: U128 range key scanning")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-checks: Python range key computation -----------------------------

report("  ... cross-checking range key computation")

print("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_uint64  # noqa: E402
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128  # noqa: E402
print("  ... imports done")
sys.stdout.flush()

MASK128 = (1 << 128) - 1

test_view_ids = [0, 1, 100, (1 << 32) - 1, (1 << 63) - 1]

for vid in test_view_ids:
    start = int(r_uint128(r_uint64(vid)) << 64) & MASK128
    end = int(r_uint128(r_uint64(vid + 1)) << 64) & MASK128

    # start < end
    range_ok = (start < end)

    # A key with matching view_id is in range
    sample_key = start | 0x123456789ABCDEF0
    in_range = (start <= sample_key < end)

    # A key with view_id+1 is NOT in range
    next_start = end
    not_in_range = not (start <= next_start < end)

    # High 64 bits of start == view_id
    hi_bits = (start >> 64) & ((1 << 64) - 1)
    hi_ok = (hi_bits == vid)

    if range_ok and in_range and not_in_range and hi_ok:
        report("  PASS  cross-check view_id=%d: start=0x%032x end=0x%032x" % (
            vid, start, end))
    else:
        report("  FAIL  cross-check view_id=%d: range=%s in=%s notin=%s hi=%s" % (
            vid, range_ok, in_range, not_in_range, hi_ok))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: Range contains all matching keys (128-bit BV, UNSAT) ---------------
#
# If extract[127:64](k) == view_id, then start <= k < end.

report("  ... proving P1: range contains all matching keys")

ok &= prove("P1: matching key in [start, end)", """\
(set-logic QF_BV)
(declare-const view_id (_ BitVec 64))
(declare-const k (_ BitVec 128))
(assert (bvult view_id #xffffffffffffffff))
(define-fun start_key () (_ BitVec 128)
  (bvshl ((_ zero_extend 64) view_id) (_ bv64 128)))
(define-fun end_key () (_ BitVec 128)
  (bvshl ((_ zero_extend 64) (bvadd view_id (_ bv1 64))) (_ bv64 128)))
(assert (= ((_ extract 127 64) k) view_id))
(assert (not (and (bvuge k start_key) (bvult k end_key))))
(check-sat)
""")

# -- P2: Ranges for different view_ids are disjoint (128-bit BV, UNSAT) -----

report("  ... proving P2: disjoint ranges")

ok &= prove("P2: different view_ids => disjoint ranges", """\
(set-logic QF_BV)
(declare-const v1 (_ BitVec 64))
(declare-const v2 (_ BitVec 64))
(declare-const k (_ BitVec 128))
(assert (not (= v1 v2)))
(assert (bvult v1 #xffffffffffffffff))
(assert (bvult v2 #xffffffffffffffff))
(define-fun s1 () (_ BitVec 128)
  (bvshl ((_ zero_extend 64) v1) (_ bv64 128)))
(define-fun e1 () (_ BitVec 128)
  (bvshl ((_ zero_extend 64) (bvadd v1 (_ bv1 64))) (_ bv64 128)))
(define-fun s2 () (_ BitVec 128)
  (bvshl ((_ zero_extend 64) v2) (_ bv64 128)))
(define-fun e2 () (_ BitVec 128)
  (bvshl ((_ zero_extend 64) (bvadd v2 (_ bv1 64))) (_ bv64 128)))
(assert (and (bvuge k s1) (bvult k e1)))
(assert (and (bvuge k s2) (bvult k e2)))
(check-sat)
""")

# -- P3: end > start for view_id < MAX (128-bit BV, UNSAT) ------------------

report("  ... proving P3: end > start")

ok &= prove("P3: end_key > start_key", """\
(set-logic QF_BV)
(declare-const view_id (_ BitVec 64))
(assert (bvult view_id #xffffffffffffffff))
(define-fun start_key () (_ BitVec 128)
  (bvshl ((_ zero_extend 64) view_id) (_ bv64 128)))
(define-fun end_key () (_ BitVec 128)
  (bvshl ((_ zero_extend 64) (bvadd view_id (_ bv1 64))) (_ bv64 128)))
(assert (not (bvugt end_key start_key)))
(check-sat)
""")

# -- P4: Overflow counterexample (SAT) --------------------------------------
#
# view_id == MAX => view_id + 1 wraps to 0 => end_key == 0.

report("  ... proving P4: overflow counterexample (SAT)")

ok &= check_sat("P4: view_id==MAX => end==0 (SAT)", """\
(set-logic QF_BV)
; Concrete: view_id = 0xFFFFFFFFFFFFFFFF, view_id + 1 wraps to 0
(assert (=
  (bvshl ((_ zero_extend 64) (bvadd #xffffffffffffffff (_ bv1 64))) (_ bv64 128))
  (_ bv0 128)))
(check-sat)
""")

# -- P5: Range is exact — no extra keys (128-bit BV, UNSAT) -----------------
#
# If k in [start, end), then extract[127:64](k) == view_id.

report("  ... proving P5: range is exact")

ok &= prove("P5: k in range => high bits == view_id", """\
(set-logic QF_BV)
(declare-const view_id (_ BitVec 64))
(declare-const k (_ BitVec 128))
(assert (bvult view_id #xffffffffffffffff))
(define-fun start_key () (_ BitVec 128)
  (bvshl ((_ zero_extend 64) view_id) (_ bv64 128)))
(define-fun end_key () (_ BitVec 128)
  (bvshl ((_ zero_extend 64) (bvadd view_id (_ bv1 64))) (_ bv64 128)))
(assert (bvuge k start_key))
(assert (bvult k end_key))
(assert (not (= ((_ extract 127 64) k) view_id)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: U128 range key scanning is correct")
    print("    P1: range contains all matching keys")
    print("    P2: different view_ids have disjoint ranges")
    print("    P3: end > start for view_id < MAX")
    print("    P4: overflow counterexample (view_id == MAX)")
    print("    P5: range is exact (no extra keys)")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
