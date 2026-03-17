"""Z3 formal proofs for aggregation spec packing roundtrip.

Proves five properties of gnitz/core/opcodes.py:62,
rpython_tests/helpers/circuit_builder.py:143,
gnitz/catalog/program_cache.py:547-549:

Pack:   packed = (func_id << 32) | col_idx
Unpack: func_id = packed >> 32; col_idx = packed & 0xFFFFFFFF

  P1. Pack/unpack roundtrip for func_id (64-bit BV, UNSAT)
  P2. Pack/unpack roundtrip for col_idx (64-bit BV, UNSAT)
  P3. Bit fields are disjoint (64-bit BV, UNSAT)
  P4. Packing is injective (64-bit BV, UNSAT)
  P5. RPython >> on positive packed value is logical shift (64-bit BV, UNSAT)

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


# -- Constants ----------------------------------------------------------------

MASK32 = (1 << 32) - 1
MASK64 = (1 << 64) - 1

# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: Aggregation spec packing roundtrip")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-checks ------------------------------------------------------------

report("  ... cross-checking agg spec packing")

test_vectors = [
    (0, 0),
    (1, 0),
    (0, 1),
    (1, 2),
    (255, 511),
    (0xFFFFFFFF, 0xFFFFFFFF),
]

for func_id, col_idx in test_vectors:
    # Pack
    packed = ((func_id << 32) | col_idx) & MASK64

    # Unpack
    extracted_func = (packed >> 32) & MASK64
    extracted_col = packed & MASK32

    # Roundtrip check
    func_ok = (extracted_func == func_id)
    col_ok = (extracted_col == col_idx)

    if func_ok and col_ok:
        # Z3 simplify cross-check
        smt_q = "(simplify (bvlshr (bvor (bvshl (_ bv%d 64) (_ bv32 64)) (_ bv%d 64)) (_ bv32 64)))" % (
            func_id, col_idx)
        z3_out = run_z3(smt_q)
        z3_val = parse_z3_value(z3_out)
        if z3_val is not None and z3_val == func_id:
            report("  PASS  cross-check: (%d, %d) -> packed=0x%016x" % (
                func_id, col_idx, packed))
        else:
            report("  FAIL  cross-check: (%d, %d) Z3 func_id=%s expected %d" % (
                func_id, col_idx, z3_out, func_id))
            ok = False
    else:
        report("  FAIL  cross-check: (%d, %d) func=%s col=%s" % (
            func_id, col_idx, func_ok, col_ok))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: Pack/unpack roundtrip for func_id (64-bit BV, UNSAT) ----------------

report("  ... proving P1: pack/unpack roundtrip for func_id")

ok &= prove("P1: (packed >> 32) == func_id", """\
(set-logic QF_BV)
(declare-const func_id (_ BitVec 64))
(declare-const col_idx (_ BitVec 64))
(assert (bvult func_id (_ bv4294967296 64)))
(assert (bvult col_idx (_ bv4294967296 64)))
(define-fun packed () (_ BitVec 64)
  (bvor (bvshl func_id (_ bv32 64)) col_idx))
(define-fun extracted_func () (_ BitVec 64) (bvlshr packed (_ bv32 64)))
; Negate: extracted != func_id
(assert (not (= extracted_func func_id)))
(check-sat)
""")

# -- P2: Pack/unpack roundtrip for col_idx (64-bit BV, UNSAT) ----------------

report("  ... proving P2: pack/unpack roundtrip for col_idx")

ok &= prove("P2: (packed & 0xFFFFFFFF) == col_idx", """\
(set-logic QF_BV)
(declare-const func_id (_ BitVec 64))
(declare-const col_idx (_ BitVec 64))
(assert (bvult func_id (_ bv4294967296 64)))
(assert (bvult col_idx (_ bv4294967296 64)))
(define-fun packed () (_ BitVec 64)
  (bvor (bvshl func_id (_ bv32 64)) col_idx))
(define-fun extracted_col () (_ BitVec 64)
  (bvand packed #x00000000ffffffff))
; Negate: extracted != col_idx
(assert (not (= extracted_col col_idx)))
(check-sat)
""")

# -- P3: Bit fields are disjoint (64-bit BV, UNSAT) --------------------------
#
# (func_id << 32) & 0xFFFFFFFF == 0 — high field doesn't leak into low.

report("  ... proving P3: bit fields are disjoint")

ok &= prove("P3: (func_id << 32) & 0xFFFFFFFF == 0", """\
(set-logic QF_BV)
(declare-const func_id (_ BitVec 64))
(assert (bvult func_id (_ bv4294967296 64)))
; Negate: low 32 bits of shifted func_id are non-zero
(assert (not (= (bvand (bvshl func_id (_ bv32 64)) #x00000000ffffffff) (_ bv0 64))))
(check-sat)
""")

# -- P4: Packing is injective (64-bit BV, UNSAT) -----------------------------
#
# Different (func_id, col_idx) pairs produce different packed values.

report("  ... proving P4: packing is injective")

ok &= prove("P4: (f1,c1) != (f2,c2) implies pack(f1,c1) != pack(f2,c2)", """\
(set-logic QF_BV)
(declare-const f1 (_ BitVec 64))
(declare-const c1 (_ BitVec 64))
(declare-const f2 (_ BitVec 64))
(declare-const c2 (_ BitVec 64))
(assert (bvult f1 (_ bv4294967296 64)))
(assert (bvult c1 (_ bv4294967296 64)))
(assert (bvult f2 (_ bv4294967296 64)))
(assert (bvult c2 (_ bv4294967296 64)))
; Inputs differ
(assert (not (and (= f1 f2) (= c1 c2))))
(define-fun p1 () (_ BitVec 64) (bvor (bvshl f1 (_ bv32 64)) c1))
(define-fun p2 () (_ BitVec 64) (bvor (bvshl f2 (_ bv32 64)) c2))
; Assert: packed values are equal (collision)
(assert (= p1 p2))
(check-sat)
""")

# -- P5: RPython >> on positive packed is logical shift (64-bit BV, UNSAT) ----
#
# In RPython, packed >> 32 where packed >= 0 (positive signed int)
# is equivalent to unsigned logical right shift.

report("  ... proving P5: arithmetic right shift = logical for positive values")

ok &= prove("P5: bvashr(packed, 32) == bvlshr(packed, 32) when packed >= 0", """\
(set-logic QF_BV)
(declare-const packed (_ BitVec 64))
; packed is non-negative (sign bit = 0)
(assert (bvsge packed (_ bv0 64)))
; Negate: arithmetic shift != logical shift
(assert (not (= (bvashr packed (_ bv32 64)) (bvlshr packed (_ bv32 64)))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: Aggregation spec packing roundtrip")
    print("    P1: pack/unpack roundtrip for func_id")
    print("    P2: pack/unpack roundtrip for col_idx")
    print("    P3: bit fields are disjoint")
    print("    P4: packing is injective")
    print("    P5: arithmetic >> == logical >> for positive packed values")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
