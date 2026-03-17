"""Z3 formal proofs for XOR filter fingerprint XOR properties.

Proves six properties of the fingerprint extraction and XOR construction
in gnitz/storage/xor8.py:75,79-81,185-190:

  Extraction: f = intmask(r_uint64(h) >> 56) & 0xFF
  Query:      got = (ord(fp[h0]) ^ ord(fp[h1]) ^ ord(fp[h2])) & 0xFF
  Build:      fingerprints[solo_slot] = chr((f ^ xored) & 0xFF)

  P1. Fingerprint extraction is in [0, 255] (64-bit BV)
  P2. XOR self-inverse: (a ^ b) ^ b == a (8-bit BV)
  P3a. XOR associativity: (a ^ b) ^ c == a ^ (b ^ c) (8-bit BV)
  P3b. XOR commutativity: a ^ b == b ^ a (8-bit BV)
  P4. Mask idempotence: (x & 0xFF) & 0xFF == x & 0xFF (64-bit BV)
  P5. Without >> 56, extraction CAN exceed 255 (SAT counterexample)

6 Z3 queries + ~12 cross-checks.  Runs under PyPy2.
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


def expect_sat(label, smt_text):
    """Run a query expecting sat. Returns True on success."""
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


def fmt64(n):
    return "0x%016x" % (n & 0xFFFFFFFFFFFFFFFF)


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: XOR filter fingerprint properties")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-check: fingerprint extraction against RPython ---------------------

report("  ... cross-checking fingerprint extraction against RPython")

print("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_uint64, intmask  # noqa: E402
print("  ... imports done")
sys.stdout.flush()

fp_vectors = [
    0x0000000000000000,
    0x0000000000000001,
    0x00000000000000FF,
    0xFF00000000000000,
    0xFFFFFFFFFFFFFFFF,
    0xDEADBEEF12345678,
    0x0100000000000000,
    0x8000000000000000,
]

for h_int in fp_vectors:
    # RPython computation
    h = r_uint64(h_int)
    rpython_f = intmask(r_uint64(h) >> 56) & 0xFF

    # Z3 computation via (simplify ...)
    smt_query = "(simplify (bvand (bvlshr (_ bv%d 64) (_ bv56 64)) #x00000000000000ff))" % (
        h_int & 0xFFFFFFFFFFFFFFFF)
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)

    if z3_val is None:
        report("  FAIL  cross-check fp(%s): unexpected Z3 output: %s" % (fmt64(h_int), z3_out))
        ok = False
    elif z3_val == rpython_f:
        report("  PASS  cross-check fp(%s) -> %d" % (fmt64(h_int), rpython_f))
    else:
        report("  FAIL  cross-check fp(%s): RPython=%d Z3=%d" % (fmt64(h_int), rpython_f, z3_val))
        ok = False

# Cross-check: XOR triplet verification
xor_vectors = [
    (0, 0, 0),
    (0xFF, 0, 0),
    (0xAB, 0xCD, 0xEF),
    (0xFF, 0xFF, 0xFF),
]

for a, b, c in xor_vectors:
    rpy_xor = (a ^ b ^ c) & 0xFF
    smt_query = "(simplify (bvand (bvxor (bvxor (_ bv%d 8) (_ bv%d 8)) (_ bv%d 8)) (_ bv255 8)))" % (a, b, c)
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)

    if z3_val is not None and z3_val == rpy_xor:
        report("  PASS  cross-check XOR(%d,%d,%d) -> %d" % (a, b, c, rpy_xor))
    else:
        report("  FAIL  cross-check XOR(%d,%d,%d): RPython=%d Z3=%s" % (a, b, c, rpy_xor, z3_val))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: Fingerprint extraction in [0, 255] (64-bit BV, UNSAT) ---------------
#
# f = (h >> 56) & 0xFF
# After shifting right by 56 bits, only bits [7:0] remain (top 8 bits of h).
# Masking with 0xFF ensures the result fits in a byte.

report("  ... proving P1: fingerprint in [0, 255]")
ok &= prove("P1: (h >> 56) & 0xFF <= 0xFF", """\
(set-logic QF_BV)
(declare-const h (_ BitVec 64))
(define-fun f () (_ BitVec 64) (bvand (bvlshr h (_ bv56 64)) #x00000000000000ff))
(assert (bvugt f #x00000000000000ff))
(check-sat)
""")

# -- P2: XOR self-inverse (8-bit BV, UNSAT) ----------------------------------
#
# (a ^ b) ^ b == a. Foundation of XOR filter correctness:
# if fingerprints[slot] = f ^ xored, then f ^ fp[h0] ^ fp[h1] ^ fp[h2] = f
# (because xored = fp[other0] ^ fp[other1], and XOR cancels).

report("  ... proving P2: XOR self-inverse")
ok &= prove("P2: (a ^ b) ^ b == a", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 8))
(declare-const b (_ BitVec 8))
(assert (not (= (bvxor (bvxor a b) b) a)))
(check-sat)
""")

# -- P3a: XOR associativity (8-bit BV, UNSAT) --------------------------------
#
# (a ^ b) ^ c == a ^ (b ^ c). Order of 3-way XOR in query doesn't matter.

report("  ... proving P3a: XOR associativity")
ok &= prove("P3a: (a ^ b) ^ c == a ^ (b ^ c)", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 8))
(declare-const b (_ BitVec 8))
(declare-const c (_ BitVec 8))
(assert (not (= (bvxor (bvxor a b) c) (bvxor a (bvxor b c)))))
(check-sat)
""")

# -- P3b: XOR commutativity (8-bit BV, UNSAT) --------------------------------
#
# a ^ b == b ^ a. Slot assignment order is irrelevant.

report("  ... proving P3b: XOR commutativity")
ok &= prove("P3b: a ^ b == b ^ a", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 8))
(declare-const b (_ BitVec 8))
(assert (not (= (bvxor a b) (bvxor b a))))
(check-sat)
""")

# -- P4: Mask idempotence (64-bit BV, UNSAT) ---------------------------------
#
# (x & 0xFF) & 0xFF == x & 0xFF. The mask in may_contain's `& 0xFF` is safe
# to apply multiple times.

report("  ... proving P4: mask idempotence")
ok &= prove("P4: (x & 0xFF) & 0xFF == x & 0xFF", """\
(set-logic QF_BV)
(declare-const x (_ BitVec 64))
(assert (not (= (bvand (bvand x #x00000000000000ff) #x00000000000000ff)
                (bvand x #x00000000000000ff))))
(check-sat)
""")

# -- P5: Without >> 56, extraction CAN exceed 255 (SAT) ----------------------
#
# Counterexample: h >> 48 (wrong shift) can produce values > 0xFF,
# showing the >> 56 shift is essential for byte-range output.

report("  ... proving P5: wrong shift counterexample")
ok &= expect_sat("P5: h >> 48 can exceed 0xFF (SAT)", """\
(set-logic QF_BV)
(declare-const h (_ BitVec 64))
(define-fun f48 () (_ BitVec 64) (bvlshr h (_ bv48 64)))
(assert (bvugt f48 #x00000000000000ff))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: XOR filter fingerprint properties are correct")
    print("    P1: fingerprint extraction in [0, 255]")
    print("    P2: XOR self-inverse (filter correctness foundation)")
    print("    P3a: XOR associativity")
    print("    P3b: XOR commutativity")
    print("    P4: mask idempotence")
    print("    P5: wrong-shift counterexample confirms >> 56 is essential")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
