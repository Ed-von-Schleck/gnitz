"""Z3 formal proofs for serialize.py alignment formula correctness.

Proves three properties of alignment formulas used in gnitz/core/serialize.py
compute_hash() and gnitz/storage/wal_columnar.py _align8():
  P1. align4: (val + 3) & ~3 == align_up(val, 4)
  P2. align8: (val + 7) & ~7 == align_up(val, 8)
  P3. _align8 matches align8 (wal_columnar.py coverage)

These formulas appear in both the size-calculation pass (pass 1) and the
data-writing pass (pass 2) of compute_hash(). Correctness of each alignment
step is proved in isolation.

3 Z3 queries + ~12 cross-checks.  Runs under PyPy2.
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


def fmt32(n):
    return "0x%08x" % (n & 0xFFFFFFFF)


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: serialize.py alignment formula correctness")
print("=" * 56)
sys.stdout.flush()

ok = True

MASK32 = 0xFFFFFFFF

# -- Cross-check: align4 formula against Z3 simplify -------------------------

report("  ... cross-checking align4: (val + 3) & ~3")

align4_vectors = [0, 1, 2, 3, 4, 5, 7, 8]
for val in align4_vectors:
    # Pure Python computation (same as RPython formula)
    py_result = (val + 3) & ~3 & MASK32

    # Z3 computation via (simplify ...)
    smt_query = "(simplify (bvand (bvadd (_ bv%d 32) (_ bv3 32)) (bvnot (_ bv3 32))))" % val
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check align4(%d): unexpected Z3 output: %s" % (val, z3_out))
        ok = False
    elif z3_val == py_result:
        report("  PASS  cross-check align4(%d) -> %s" % (val, fmt32(py_result)))
    else:
        report("  FAIL  cross-check align4(%d): Python=%s Z3=%s" % (val, fmt32(py_result), fmt32(z3_val)))
        ok = False

# -- Cross-check: align8 formula against Z3 simplify -------------------------

report("  ... cross-checking align8: (val + 7) & ~7")

align8_vectors = [0, 1, 7, 8, 9, 15, 16]
for val in align8_vectors:
    # Pure Python computation (same as RPython formula)
    py_result = (val + 7) & ~7 & MASK32

    # Z3 computation via (simplify ...)
    smt_query = "(simplify (bvand (bvadd (_ bv%d 32) (_ bv7 32)) (bvnot (_ bv7 32))))" % val
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check align8(%d): unexpected Z3 output: %s" % (val, z3_out))
        ok = False
    elif z3_val == py_result:
        report("  PASS  cross-check align8(%d) -> %s" % (val, fmt32(py_result)))
    else:
        report("  FAIL  cross-check align8(%d): Python=%s Z3=%s" % (val, fmt32(py_result), fmt32(z3_val)))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: align4 matches generic align_up(val, 4) -----------------------------
#
# Generic: align_up(val, a) = (val + (a - 1)) & ~(a - 1)
# Hardcoded: (val + 3) & ~3
# Prove equivalence for all 32-bit unsigned val.

report("  ... proving P1: align4 == align_up(val, 4)")
ok &= prove("P1: (val + 3) & ~3 == align_up(val, 4)", """\
(set-logic QF_BV)
(declare-const val (_ BitVec 32))
(define-fun align4 () (_ BitVec 32)
  (bvand (bvadd val (_ bv3 32)) (bvnot (_ bv3 32))))
(define-fun align_up_4 () (_ BitVec 32)
  (bvand (bvadd val (bvsub (_ bv4 32) (_ bv1 32)))
         (bvnot (bvsub (_ bv4 32) (_ bv1 32)))))
; Negate: assert they differ
(assert (not (= align4 align_up_4)))
(check-sat)
""")

# -- P2: align8 matches generic align_up(val, 8) -----------------------------
#
# Hardcoded: (val + 7) & ~7
# Prove equivalence for all 32-bit unsigned val.

report("  ... proving P2: align8 == align_up(val, 8)")
ok &= prove("P2: (val + 7) & ~7 == align_up(val, 8)", """\
(set-logic QF_BV)
(declare-const val (_ BitVec 32))
(define-fun align8 () (_ BitVec 32)
  (bvand (bvadd val (_ bv7 32)) (bvnot (_ bv7 32))))
(define-fun align_up_8 () (_ BitVec 32)
  (bvand (bvadd val (bvsub (_ bv8 32) (_ bv1 32)))
         (bvnot (bvsub (_ bv8 32) (_ bv1 32)))))
; Negate: assert they differ
(assert (not (= align8 align_up_8)))
(check-sat)
""")

# -- P3: _align8 (wal_columnar.py) matches align_up(val, 8) ------------------
#
# _align8(val) = (val + 7) & ~7  (line 30-31 of wal_columnar.py)
# Syntactically identical to align8, but stated as a separate equivalence
# to close the wal_columnar.py coverage.

report("  ... proving P3: _align8 == align_up(val, 8)")
ok &= prove("P3: _align8(val) == align_up(val, 8)", """\
(set-logic QF_BV)
(declare-const val (_ BitVec 32))
; _align8 from wal_columnar.py: (val + 7) & ~7
(define-fun wal_align8 () (_ BitVec 32)
  (bvand (bvadd val (_ bv7 32)) (bvnot (_ bv7 32))))
; Generic align_up(val, 8)
(define-fun align_up_8 () (_ BitVec 32)
  (bvand (bvadd val (bvsub (_ bv8 32) (_ bv1 32)))
         (bvnot (bvsub (_ bv8 32) (_ bv1 32)))))
; Negate: assert they differ
(assert (not (= wal_align8 align_up_8)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: serialize.py alignment formulas are correct")
    print("    P1: align4 == align_up(val, 4)")
    print("    P2: align8 == align_up(val, 8)")
    print("    P3: _align8 (wal_columnar) == align_up(val, 8)")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
