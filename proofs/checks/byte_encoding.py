"""Z3 formal proofs for U16/U8/U32 byte-level encoding roundtrip.

Proves five properties of gnitz/core/batch.py:633-654:

  U16 (little-endian byte write):
    dest[0] = chr(v16 & 0xFF)
    dest[1] = chr((v16 >> 8) & 0xFF)

  U8:
    dest[0] = chr(intmask(val) & 0xFF)

  U32:
    rffi.cast(rffi.UINTP, dest)[0] = rffi.cast(rffi.UINT, val & 0xFFFFFFFF)

  P1. U16 roundtrip: (hi << 8) | lo == v (16-bit BV, UNSAT)
  P2. U16 byte lanes are disjoint (16-bit BV, UNSAT)
  P3. U8 extraction range: v & 0xFF in [0, 255] (16-bit BV, UNSAT)
  P4. U32 mask preserves low 32 bits (64-bit BV, UNSAT)
  P5. U32 mask idempotence (64-bit BV, UNSAT)

5 Z3 queries + ~7 cross-checks.  Runs under PyPy2.
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


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: U16/U8/U32 byte-level encoding")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-checks: U16 encode/decode ----------------------------------------

report("  ... cross-checking U16 encode/decode")

u16_vectors = [0, 1, 127, 128, 255, 256, 0xFFFF]
for v in u16_vectors:
    lo = v & 0xFF
    hi = (v >> 8) & 0xFF
    reconstructed = (hi << 8) | lo
    py_ok = (reconstructed == v)

    # Z3 simplify cross-check
    smt_q = "(simplify (bvor (bvshl (bvand (bvlshr (_ bv%d 16) (_ bv8 16)) (_ bv255 16)) (_ bv8 16)) (bvand (_ bv%d 16) (_ bv255 16))))" % (v, v)
    z3_out = run_z3(smt_q)
    z3_val = parse_z3_value(z3_out)
    z3_ok = (z3_val == v)

    if py_ok and z3_ok:
        report("  PASS  cross-check U16(0x%04x): lo=0x%02x hi=0x%02x" % (v, lo, hi))
    else:
        report("  FAIL  cross-check U16(0x%04x): py=%s z3=%s" % (v, py_ok, z3_ok))
        ok = False

# -- Cross-checks: U32 mask -------------------------------------------------

report("  ... cross-checking U32 mask")

u32_vectors = [0, 0xDEADBEEF, 0xFFFFFFFF]
for v in u32_vectors:
    masked = v & 0xFFFFFFFF
    py_ok = (masked == v)  # all inputs <= 32 bits

    smt_q = "(simplify (bvand (_ bv%d 64) #x00000000ffffffff))" % v
    z3_out = run_z3(smt_q)
    z3_val = parse_z3_value(z3_out)
    z3_ok = (z3_val == masked)

    if py_ok and z3_ok:
        report("  PASS  cross-check U32(0x%08x)" % v)
    else:
        report("  FAIL  cross-check U32(0x%08x): py=%s z3=%s" % (v, py_ok, z3_ok))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: U16 roundtrip (16-bit BV, UNSAT) -----------------------------------

report("  ... proving P1: U16 roundtrip")
ok &= prove("P1: U16 (hi<<8)|lo == v", """\
(set-logic QF_BV)
(declare-const v (_ BitVec 16))
(define-fun lo () (_ BitVec 16) (bvand v (_ bv255 16)))
(define-fun hi () (_ BitVec 16) (bvand (bvlshr v (_ bv8 16)) (_ bv255 16)))
(define-fun reconstructed () (_ BitVec 16)
  (bvor (bvshl hi (_ bv8 16)) lo))
(assert (not (= reconstructed v)))
(check-sat)
""")

# -- P2: U16 byte lanes are disjoint (16-bit BV, UNSAT) ---------------------

report("  ... proving P2: U16 byte lanes disjoint")
ok &= prove("P2: lo & hi_shifted == 0", """\
(set-logic QF_BV)
(declare-const v (_ BitVec 16))
(define-fun lo () (_ BitVec 16) (bvand v (_ bv255 16)))
(define-fun hi_shifted () (_ BitVec 16) (bvand v #xff00))
(assert (not (= (bvand lo hi_shifted) (_ bv0 16))))
(check-sat)
""")

# -- P3: U8 extraction range (16-bit BV, UNSAT) -----------------------------

report("  ... proving P3: U8 extraction in [0, 255]")
ok &= prove("P3: v & 0xFF <= 255", """\
(set-logic QF_BV)
(declare-const v (_ BitVec 16))
(assert (bvugt (bvand v (_ bv255 16)) (_ bv255 16)))
(check-sat)
""")

# -- P4: U32 mask preserves low 32 bits (64-bit BV, UNSAT) ------------------

report("  ... proving P4: U32 mask == extract[31:0]")
ok &= prove("P4: v & 0xFFFFFFFF == zext(extract(v))", """\
(set-logic QF_BV)
(declare-const v (_ BitVec 64))
(assert (not (= (bvand v #x00000000ffffffff)
               ((_ zero_extend 32) ((_ extract 31 0) v)))))
(check-sat)
""")

# -- P5: U32 mask idempotence (64-bit BV, UNSAT) ----------------------------

report("  ... proving P5: U32 mask idempotence")
ok &= prove("P5: (v & mask) & mask == v & mask", """\
(set-logic QF_BV)
(declare-const v (_ BitVec 64))
(define-fun masked () (_ BitVec 64) (bvand v #x00000000ffffffff))
(assert (not (= (bvand masked #x00000000ffffffff) masked)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: U16/U8/U32 byte-level encoding is correct")
    print("    P1: U16 roundtrip")
    print("    P2: U16 byte lanes disjoint")
    print("    P3: U8 extraction range [0, 255]")
    print("    P4: U32 mask preserves low 32 bits")
    print("    P5: U32 mask idempotence")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
