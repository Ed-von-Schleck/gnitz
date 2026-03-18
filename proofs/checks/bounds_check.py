"""Z3 formal proofs for MappedBuffer bounds check safety.

Proves three properties of gnitz/storage/buffer.py:37-39 _check_bounds():
  P1. Soundness: check_passes -> offset + length <= size
  P2. Completeness: offset >= 0 /\ length >= 0 /\ offset + length <= size -> check_passes
  P3. Equivalence: under non-negative preconditions,
      (offset > size - length) <-> (offset + length > size)

Source function:
  def _check_bounds(self, offset, length):
      if offset < 0 or length < 0 or offset > self.size - length:
          raise errors.BoundsError(offset, length, self.size)

4 Z3 queries (3 UNSAT + 1 SAT) + 8 cross-checks.  Runs under PyPy2.
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


def witness(label, smt_text):
    """Run a query expecting sat (counterexample exists). Returns True on success."""
    result = run_z3(smt_text)
    if result == "sat":
        report("  PASS  %s" % label)
        return True
    else:
        report("  FAIL  %s: expected sat, got %s" % (label, result))
        return False


# -- SMT preamble (shared across P1-P3) --------------------------------------
# 32-bit signed BVs. Use bvslt/bvsgt/bvsle/bvsge for signed comparisons.
# Constrain size >= 0 and size < 2^30 to prevent signed overflow in size-length.

BOUNDS_PREAMBLE = """\
(set-logic QF_BV)
(declare-const offset (_ BitVec 32))
(declare-const length (_ BitVec 32))
(declare-const size (_ BitVec 32))
; size >= 0 (signed)
(assert (not (bvslt size (_ bv0 32))))
; size < 2^30 (prevent overflow in size - length)
(assert (bvslt size (_ bv1073741824 32)))
; check_passes = NOT(offset < 0 OR length < 0 OR offset > size - length)
(define-fun check_passes () Bool
  (and (not (bvslt offset (_ bv0 32)))
       (not (bvslt length (_ bv0 32)))
       (not (bvsgt offset (bvsub size length)))))
"""


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: MappedBuffer bounds check safety")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-check: validate check logic against pure Python --------------------
# Can't import _check_bounds due to FFI, so reimplement in pure Python.

report("  ... cross-checking bounds check against pure Python")

def check_passes_py(offset, length, size):
    """Pure Python reimplementation of _check_bounds pass condition."""
    return not (offset < 0 or length < 0 or offset > size - length)


cross_check_vectors = [
    # (offset, length, size, expected)
    (0, 0, 0, True),
    (0, 10, 10, True),
    (0, 11, 10, False),
    (5, 5, 10, True),
    (5, 6, 10, False),
    (-1, 5, 10, False),
    (0, -1, 10, False),
    (0, 0, 100, True),
]

for offset, length, size, expected in cross_check_vectors:
    py_result = check_passes_py(offset, length, size)

    # Z3 computation with concrete values
    # Convert to unsigned 32-bit two's complement for SMT
    def to_bv32(n):
        return n & 0xFFFFFFFF

    smt_query = """\
(simplify (let ((offset (_ bv%d 32))
                (length (_ bv%d 32))
                (size (_ bv%d 32)))
  (and (not (bvslt offset (_ bv0 32)))
       (not (bvslt length (_ bv0 32)))
       (not (bvsgt offset (bvsub size length))))))""" % (
        to_bv32(offset), to_bv32(length), to_bv32(size))
    z3_out = run_z3(smt_query)

    z3_result = z3_out == "true"

    if py_result == expected and z3_result == expected:
        report("  PASS  cross-check (%d, %d, %d) -> %s" % (offset, length, size, expected))
    else:
        report("  FAIL  cross-check (%d, %d, %d): expected=%s Python=%s Z3=%s" % (
            offset, length, size, expected, py_result, z3_result))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: Soundness ------------------------------------------------------------
#
# check_passes -> offset + length <= size
# i.e. if the check passes, the access is within bounds.

report("  ... proving P1: soundness")
ok &= prove("P1: check_passes -> offset + length <= size", BOUNDS_PREAMBLE + """\
; Negate: check passes BUT offset + length > size
(assert check_passes)
(assert (bvsgt (bvadd offset length) size))
(check-sat)
""")

# -- P2: Completeness ---------------------------------------------------------
#
# offset >= 0 /\ length >= 0 /\ offset + length <= size -> check_passes
# i.e. every valid access passes the check.
# Guard: offset <= size and length <= size (realistic: can't read past buffer).
# This ensures offset + length <= 2*size < 2^31, preventing signed BV overflow.

report("  ... proving P2: completeness")
ok &= prove("P2: valid access -> check_passes", BOUNDS_PREAMBLE + """\
; Negate: offset >= 0 AND length >= 0 AND offset + length <= size BUT NOT check_passes
(assert (not (bvslt offset (_ bv0 32))))
(assert (not (bvslt length (_ bv0 32))))
; Realistic bounds: offset and length each <= size (prevents bvadd overflow)
(assert (bvsle offset size))
(assert (bvsle length size))
(assert (bvsle (bvadd offset length) size))
(assert (not check_passes))
(check-sat)
""")

# -- P3: Equivalence ----------------------------------------------------------
#
# Under non-negative preconditions:
# (offset > size - length) <-> (offset + length > size)
# This shows the subtraction form used in the code is equivalent to the
# more intuitive addition form.

report("  ... proving P3: equivalence of subtraction and addition forms")
ok &= prove("P3: (offset > size - length) <-> (offset + length > size)", BOUNDS_PREAMBLE + """\
; Non-negative preconditions
(assert (not (bvslt offset (_ bv0 32))))
(assert (not (bvslt length (_ bv0 32))))
; Realistic bounds: both <= size (prevents signed overflow in bvadd)
(assert (bvsle offset size))
(assert (bvsle length size))
; Negate: the two forms disagree
(assert (not (= (bvsgt offset (bvsub size length))
                (bvsgt (bvadd offset length) size))))
(check-sat)
""")

# -- P4: SAT witness — overflow breaks addition form without local guards -----
#
# P3 requires offset <= size AND length <= size.  Without these guards,
# bvadd(offset, length) can overflow signed 32-bit, making the addition form
# give the wrong answer while the subtraction form (used by the code) is correct.
#
# Witness: offset=0x70000000, length=0x10000000, size=0x20000000
#   subtraction: 0x70000000 > 0x20000000 - 0x10000000 = 0x10000000  -> TRUE (OOB)
#   addition:    0x70000000 + 0x10000000 = 0x80000000 as signed = MIN_INT32
#                MIN_INT32 > 0x20000000 = FALSE (wrong)

report("  ... P4 SAT: overflow breaks addition form without local guards")
ok &= witness("P4: SAT witness — guards in P3 are load-bearing (overflow breaks add form)", BOUNDS_PREAMBLE + """\
; Non-negative inputs; size < 2^30 maintained (preamble guards)
(assert (not (bvslt offset (_ bv0 32))))
(assert (not (bvslt length (_ bv0 32))))
; No local guards: offset and length may exceed size
; Subtraction form says OOB
(assert (bvsgt offset (bvsub size length)))
; Addition form (with overflow) says in-bounds
(assert (not (bvsgt (bvadd offset length) size)))
(check-sat)   ; SAT: e.g. offset=0x70000000, length=0x10000000, size=0x20000000
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: MappedBuffer bounds check is correct")
    print("    P1: soundness (check passes -> access in bounds)")
    print("    P2: completeness (valid access -> check passes)")
    print("    P3: subtraction form == addition form")
    print("    P4: SAT — overflow breaks add form; P3 guards are load-bearing")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
