"""Z3 formal proofs for shard region bounds check safety.

Proves four properties of gnitz/storage/shard_table.py:165-176:

  u_off = r_uint64(u64_ptr[0])
  u_sz  = r_uint64(u64_ptr[1])
  u_limit = r_uint64(self.size)
  if u_off > u_limit or u_sz > u_limit or (u_off + u_sz) > u_limit:
      raise CorruptShardError(...)

self.size comes from rffi.cast(lltype.Signed, st.st_size) where
fsize >= HEADER_SIZE (64), so u_limit is in [64, 2^63-1] as unsigned.

Properties:
  P1. No u64 overflow: under signed-file-size invariant (u_limit < 2^63),
      if guards 1+2 pass then u_off + u_sz cannot wrap around in u64.
  P2. Soundness: guards pass => mathematical sum <= u_limit (the 64-bit
      check is not fooled by overflow).
  P3. Completeness: region in bounds => guards pass (no false positives).
  P4. Counterexample: without u_limit < 2^63, overflow CAN fool the check.

4 Z3 queries + 8 cross-checks.  Runs under PyPy2.
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


def fmt64(n):
    return "0x%016x" % (n & MASK64)


# -- Constants ----------------------------------------------------------------

MASK64 = (1 << 64) - 1

# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: shard region bounds check safety")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-check: validate guard logic against RPython r_uint64 ---------------

report("  ... cross-checking guard logic against RPython r_uint64")

print("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_uint64  # noqa: E402
print("  ... imports done")
sys.stdout.flush()


def guard_passes(u_off, u_sz, u_limit):
    """Pure RPython reimplementation of the shard bounds check."""
    return not (u_off > u_limit or u_sz > u_limit or (u_off + u_sz) > u_limit)


cross_check_vectors = [
    # (u_off, u_sz, u_limit, expected)
    (0, 0, 64, True),                                # trivial
    (60, 4, 64, True),                               # exact boundary
    (60, 5, 64, False),                               # one past
    (65, 0, 64, False),                               # guard 1: off > limit
    (0, 65, 64, False),                               # guard 2: sz > limit
    ((1 << 62), (1 << 62) - 1, (1 << 63) - 1, True), # sum = 2^63-1 = limit
    ((1 << 63) - 1, (1 << 63) - 1, (1 << 63) - 1, False),  # sum wraps
    ((1 << 63), (1 << 63), (1 << 63), True),          # P4 counterexample: overflow!
]

for u_off_v, u_sz_v, u_limit_v, expected in cross_check_vectors:
    u_off_r = r_uint64(u_off_v)
    u_sz_r = r_uint64(u_sz_v)
    u_limit_r = r_uint64(u_limit_v)
    py_result = guard_passes(u_off_r, u_sz_r, u_limit_r)

    # Z3 evaluation with concrete values
    smt_query = """\
(simplify (let ((u_off (_ bv%d 64))
                (u_sz (_ bv%d 64))
                (u_limit (_ bv%d 64)))
  (not (or (bvugt u_off u_limit)
           (bvugt u_sz u_limit)
           (bvugt (bvadd u_off u_sz) u_limit)))))""" % (
        u_off_v & MASK64, u_sz_v & MASK64, u_limit_v & MASK64)
    z3_out = run_z3(smt_query)
    z3_result = z3_out == "true"

    if py_result == expected and z3_result == expected:
        report("  PASS  cross-check (%s, %s, %s) -> %s" % (
            fmt64(u_off_v), fmt64(u_sz_v), fmt64(u_limit_v), expected))
    else:
        report("  FAIL  cross-check (%s, %s, %s): expected=%s Python=%s Z3=%s" % (
            fmt64(u_off_v), fmt64(u_sz_v), fmt64(u_limit_v), expected, py_result, z3_result))
        ok = False

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P1: No u64 overflow under signed-file-size invariant --------------------
#
# If u_limit < 2^63 and u_off <= u_limit and u_sz <= u_limit,
# then u_off + u_sz cannot wrap around (sum < operand is impossible).
# Key insight: each operand < 2^63, so their sum < 2^64.

report("  ... proving P1: no u64 overflow under signed-file-size invariant")
ok &= prove("P1: no u64 wraparound when u_limit < 2^63", """\
(set-logic QF_BV)
(declare-const u_off (_ BitVec 64))
(declare-const u_sz (_ BitVec 64))
(declare-const u_limit (_ BitVec 64))
; Precondition: u_limit < 2^63 (signed file size)
(assert (bvult u_limit #x8000000000000000))
; Guards 1 and 2 pass
(assert (not (bvugt u_off u_limit)))
(assert (not (bvugt u_sz u_limit)))
; Negate: overflow occurred (wraparound => sum < operand)
(assert (bvult (bvadd u_off u_sz) u_off))
(check-sat)
""")

# -- P2: Soundness — guards pass => mathematical sum <= u_limit ---------------
#
# If all three guards pass and u_limit < 2^63, then the TRUE (65-bit)
# sum u_off + u_sz <= u_limit.  This proves the 64-bit check isn't fooled.

report("  ... proving P2: soundness (guards pass => math sum <= u_limit)")
ok &= prove("P2: guards pass => math sum <= u_limit", """\
(set-logic QF_BV)
(declare-const u_off (_ BitVec 64))
(declare-const u_sz (_ BitVec 64))
(declare-const u_limit (_ BitVec 64))
(assert (bvult u_limit #x8000000000000000))
(assert (not (bvugt u_off u_limit)))
(assert (not (bvugt u_sz u_limit)))
(assert (not (bvugt (bvadd u_off u_sz) u_limit)))
; Negate: mathematical sum exceeds u_limit
(define-fun math_sum () (_ BitVec 65)
  (bvadd ((_ zero_extend 1) u_off) ((_ zero_extend 1) u_sz)))
(assert (bvugt math_sum ((_ zero_extend 1) u_limit)))
(check-sat)
""")

# -- P3: Completeness — region in bounds => guards pass -----------------------
#
# If the mathematical sum <= u_limit and u_limit < 2^63, then all three
# guard conditions evaluate to false (no exception raised).

report("  ... proving P3: completeness (region in bounds => guards pass)")
ok &= prove("P3: math sum <= u_limit => all guards pass", """\
(set-logic QF_BV)
(declare-const u_off (_ BitVec 64))
(declare-const u_sz (_ BitVec 64))
(declare-const u_limit (_ BitVec 64))
(assert (bvult u_limit #x8000000000000000))
(define-fun math_sum () (_ BitVec 65)
  (bvadd ((_ zero_extend 1) u_off) ((_ zero_extend 1) u_sz)))
(assert (not (bvugt math_sum ((_ zero_extend 1) u_limit))))
; Negate: at least one guard fires
(assert (or
  (bvugt u_off u_limit)
  (bvugt u_sz u_limit)
  (bvugt (bvadd u_off u_sz) u_limit)))
(check-sat)
""")

# -- P4: Counterexample — without constraint, overflow fools the check --------
#
# Without u_limit < 2^63, the guards can pass even when the true sum
# exceeds u_limit.  E.g. u_off = u_sz = u_limit = 2^63: sum wraps to 0,
# which is <= 2^63, so all guards pass but the region is out of bounds.

report("  ... proving P4: without invariant, overflow can fool the check (SAT)")
ok &= check_sat("P4: counterexample exists (guards pass but math sum > u_limit)", """\
(set-logic QF_BV)
(declare-const u_off (_ BitVec 64))
(declare-const u_sz (_ BitVec 64))
(declare-const u_limit (_ BitVec 64))
; NO u_limit < 2^63 constraint
; All three guards pass
(assert (not (bvugt u_off u_limit)))
(assert (not (bvugt u_sz u_limit)))
(assert (not (bvugt (bvadd u_off u_sz) u_limit)))
; But mathematical sum exceeds u_limit
(define-fun math_sum () (_ BitVec 65)
  (bvadd ((_ zero_extend 1) u_off) ((_ zero_extend 1) u_sz)))
(assert (bvugt math_sum ((_ zero_extend 1) u_limit)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: shard region bounds check is correct")
    print("    P1: no u64 wraparound under signed-file-size invariant")
    print("    P2: soundness (guards pass => math sum <= u_limit)")
    print("    P3: completeness (region in bounds => guards pass)")
    print("    P4: counterexample (invariant is necessary)")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
