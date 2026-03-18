"""Z3 formal proofs for align_up formula correctness.

Proves four properties of the alignment formula used throughout gnitz:
  P1. Aligned output: result is always a multiple of align
  P2. Never decreases (no overflow): align_up(val, align) >= val
  P3. Tightest value: align_up(val, align) - val < align
  P4. align_64 matches generic: hardcoded align=64 matches the generic formula

Source functions (all use the same formula: (val + align - 1) & ~(align - 1)):
  - gnitz/core/types.py:53-54      _align(offset, alignment)
  - gnitz/storage/buffer.py:115    Buffer._align(offset, alignment)
  - gnitz/storage/buffer.py:23-25  align_64(val) hardcoded to 64

5 Z3 queries (4 UNSAT + 1 SAT) + ~15 cross-checks.  Runs under PyPy2.
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


# -- SMT preamble (shared across P1-P3) --------------------------------------
# 32-bit unsigned BVs. Formula is width-independent.

ALIGN_PREAMBLE = """\
(set-logic QF_BV)
(declare-const val (_ BitVec 32))
(declare-const align (_ BitVec 32))
; align is a power of 2: align != 0 && (align & (align-1)) == 0
(assert (not (= align (_ bv0 32))))
(assert (= (bvand align (bvsub align (_ bv1 32))) (_ bv0 32)))
; align_up formula
(define-fun align_up () (_ BitVec 32)
  (bvand (bvadd val (bvsub align (_ bv1 32)))
         (bvnot (bvsub align (_ bv1 32)))))
"""


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: align_up formula correctness")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-check: validate Z3 encoding against RPython -----------------------

report("  ... cross-checking align_up against RPython")

print("  ... importing gnitz.core.types")
sys.stdout.flush()
from gnitz.core.types import _align as align_up  # noqa: E402
from gnitz.storage.buffer import align_64         # noqa: E402
print("  ... imports done")
sys.stdout.flush()

# align_up cross-check vectors: (val, align)
align_vectors = [
    (0, 1), (0, 8), (1, 8), (7, 8), (8, 8), (9, 8),
    (0, 64), (63, 64), (64, 64), (65, 64),
]

for val, alignment in align_vectors:
    # RPython computation
    rpython_val = int(align_up(val, alignment)) & 0xFFFFFFFF

    # Z3 computation
    smt_query = "(simplify (bvand (bvadd (_ bv%d 32) (bvsub (_ bv%d 32) (_ bv1 32))) (bvnot (bvsub (_ bv%d 32) (_ bv1 32)))))" % (
        val, alignment, alignment)
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)

    if z3_val is None:
        report("  FAIL  cross-check align_up(%d, %d): unexpected Z3 output: %s" % (val, alignment, z3_out))
        ok = False
        continue

    if z3_val == rpython_val:
        report("  PASS  cross-check align_up(%d, %d) -> %d" % (val, alignment, rpython_val))
    else:
        report("  FAIL  cross-check align_up(%d, %d): RPython=%d Z3=%d" % (val, alignment, rpython_val, z3_val))
        ok = False

# align_64 cross-check vectors
align64_vectors = [0, 1, 63, 64, 65]

for val in align64_vectors:
    rpython_val = align_64(val) & 0xFFFFFFFF

    smt_query = "(simplify (bvand (bvadd (_ bv%d 32) (_ bv63 32)) (bvnot (_ bv63 32))))" % val
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)

    if z3_val is None:
        report("  FAIL  cross-check align_64(%d): unexpected Z3 output: %s" % (val, z3_out))
        ok = False
        continue

    if z3_val == rpython_val:
        report("  PASS  cross-check align_64(%d) -> %d" % (val, rpython_val))
    else:
        report("  FAIL  cross-check align_64(%d): RPython=%d Z3=%d" % (val, rpython_val, z3_val))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: Aligned output -------------------------------------------------------
#
# align_up(val, align) & (align-1) == 0
# i.e. the result is always a multiple of align.

report("  ... proving P1: aligned output")
ok &= prove("P1: align_up(val, align) is a multiple of align", ALIGN_PREAMBLE + """\
; Negate: assert align_up & (align-1) != 0
(assert (not (= (bvand align_up (bvsub align (_ bv1 32))) (_ bv0 32))))
(check-sat)
""")

# -- P2: Never decreases (no overflow) ----------------------------------------
#
# Guard: val <= MAX - align + 1 (room for addition without wrapping).
# Then align_up(val, align) >= val.

report("  ... proving P2: never decreases (no overflow)")
ok &= prove("P2: align_up(val, align) >= val (no overflow)", ALIGN_PREAMBLE + """\
; Guard: val + align - 1 does not overflow (unsigned)
; i.e. val <= 0xFFFFFFFF - align + 1
(assert (bvule val (bvadd (bvnot align) (_ bv1 32))))
; Negate: assert align_up < val
(assert (bvult align_up val))
(check-sat)
""")

# -- P3: Tightest value (no overflow) -----------------------------------------
#
# Same guard. align_up(val, align) - val < align.
# i.e. we round up by less than one alignment unit.

report("  ... proving P3: tightest value")
ok &= prove("P3: align_up(val, align) - val < align", ALIGN_PREAMBLE + """\
; Guard: val + align - 1 does not overflow (unsigned)
(assert (bvule val (bvadd (bvnot align) (_ bv1 32))))
; Negate: assert align_up - val >= align
(assert (not (bvult (bvsub align_up val) align)))
(check-sat)
""")

# -- P4: align_64 matches generic formula ------------------------------------
#
# (val + 63) & ~63 == align_up(val, 64)
# Pure formula equality, no guard needed.

report("  ... proving P4: align_64 matches generic")
ok &= prove("P4: (val+63) & ~63 == align_up(val, 64)", """\
(set-logic QF_BV)
(declare-const val (_ BitVec 32))
; align_up(val, 64) = (val + 63) & ~63
(define-fun generic () (_ BitVec 32)
  (bvand (bvadd val (bvsub (_ bv64 32) (_ bv1 32)))
         (bvnot (bvsub (_ bv64 32) (_ bv1 32)))))
; hardcoded align_64 formula
(define-fun hardcoded () (_ BitVec 32)
  (bvand (bvadd val (_ bv63 32))
         (bvnot (_ bv63 32))))
; Negate: assert they differ
(assert (not (= generic hardcoded)))
(check-sat)
""")

# -- P5: SAT witness — overflow guard is essential for P2/P3 -----------------
#
# P2 and P3 include the guard: val <= 0xFFFFFFFF - align + 1.
# Without it, val + align - 1 wraps unsigned, producing align_up < val.
#
# Witness: val=0xFFFFFFFF, align=2
#   val + align - 1 = 0xFFFFFFFF + 1 = 0x00000000 (unsigned wrap)
#   align_up = 0x00000000 & ~0x1 = 0x00000000 < 0xFFFFFFFF = val

report("  ... P5 SAT: overflow guard is essential (wrap makes align_up < val)")
ok &= witness("P5: SAT witness — overflow guard in P2/P3 is load-bearing", ALIGN_PREAMBLE + """\
; No overflow guard: val may be large enough to wrap
; align_up wraps below val
(assert (bvult align_up val))
(check-sat)   ; SAT: e.g. val=0xFFFFFFFF, align=2 gives align_up=0
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: align_up formula is correct")
    print("    P1: output is always aligned (multiple of align)")
    print("    P2: never decreases val (no overflow)")
    print("    P3: rounds up by less than one alignment unit")
    print("    P4: align_64 matches generic align_up(val, 64)")
    print("    P5: SAT — wrap makes align_up < val; P2/P3 guard is load-bearing")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
