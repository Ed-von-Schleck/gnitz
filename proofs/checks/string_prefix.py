"""Z3 formal proofs for compute_prefix byte-packing correctness.

Proves two property groups of gnitz/core/strings.py compute_prefix():
  P1a-d. Byte roundtrip: extracting byte i from the prefix recovers the original byte
  P2.    Injectivity: different 4-byte tuples produce different prefixes

Together these guarantee that the prefix comparison in compare_structures() is a
sound fast-path: equal prefixes guarantee the first 4 bytes match, and each byte
is faithfully recoverable.

5 Z3 queries total + 8 cross-checks.  Runs under PyPy2.
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


# -- SMT preamble (shared across P1/P2) --------------------------------------
# 32-bit BVs: prefix fits in 32 bits, all ops are bvor/bvshl/bvlshr/bvand.

PREFIX_PREAMBLE = """\
(set-logic QF_BV)
(declare-const b0 (_ BitVec 32))
(declare-const b1 (_ BitVec 32))
(declare-const b2 (_ BitVec 32))
(declare-const b3 (_ BitVec 32))
; byte range: each < 256
(assert (bvult b0 (_ bv256 32)))
(assert (bvult b1 (_ bv256 32)))
(assert (bvult b2 (_ bv256 32)))
(assert (bvult b3 (_ bv256 32)))
; prefix = b0 | (b1 << 8) | (b2 << 16) | (b3 << 24)
(define-fun prefix () (_ BitVec 32)
  (bvor (bvor b0 (bvshl b1 (_ bv8 32)))
        (bvor (bvshl b2 (_ bv16 32)) (bvshl b3 (_ bv24 32)))))
"""


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: compute_prefix byte-packing correctness")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-check: validate Z3 encoding against RPython -----------------------

report("  ... cross-checking compute_prefix against RPython")

print("  ... importing gnitz.core.strings")
sys.stdout.flush()
from gnitz.core.strings import compute_prefix  # noqa: E402
print("  ... imports done")
sys.stdout.flush()

cross_check_vectors = [
    ("empty",          ""),
    ("single byte",    "a"),
    ("two bytes",      "ab"),
    ("three bytes",    "abc"),
    ("four bytes",     "abcd"),
    ("five bytes",     "abcde"),
    ("high bytes",     "\xff\x00\x80\x7f"),
    ("all zeros",      "\x00\x00\x00\x00"),
]

for label, s in cross_check_vectors:
    # RPython computation
    rpython_val = compute_prefix(s)
    # Normalize to unsigned 32-bit for comparison
    rpython_u32 = rpython_val & 0xFFFFFFFF

    # Z3 computation: build prefix from actual byte values
    byte_vals = [ord(s[i]) if i < len(s) else 0 for i in range(4)]
    smt_query = "(simplify (bvor (bvor (_ bv%d 32) (bvshl (_ bv%d 32) (_ bv8 32))) (bvor (bvshl (_ bv%d 32) (_ bv16 32)) (bvshl (_ bv%d 32) (_ bv24 32)))))" % (
        byte_vals[0], byte_vals[1], byte_vals[2], byte_vals[3])
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)

    if z3_val is None:
        report("  FAIL  cross-check %s: unexpected Z3 output: %s" % (label, z3_out))
        ok = False
        continue

    if z3_val == rpython_u32:
        report("  PASS  cross-check %-12s %-8r -> 0x%08x" % (label, s, rpython_u32))
    else:
        report("  FAIL  cross-check %s: RPython=0x%08x Z3=0x%08x" % (label, rpython_u32, z3_val))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1a-d: Byte extraction roundtrip (4 queries) ----------------------------
#
# Extracting byte i from the prefix recovers the original byte:
#   (prefix >> (i*8)) & 0xFF == bi
#
# This is what compare_structures relies on when it falls through from the
# prefix check to byte-by-byte comparison.

report("  ... proving P1a-d: byte extraction roundtrip")

for i, byte_name in enumerate(["b0", "b1", "b2", "b3"]):
    shift = i * 8
    ok &= prove("P1%s: (prefix >> %d) & 0xFF == %s" % (chr(ord('a') + i), shift, byte_name),
                PREFIX_PREAMBLE + """\
; Negate: assert extracted byte != original byte
(assert (not (= (bvand (bvlshr prefix (_ bv%d 32)) (_ bv255 32)) %s)))
(check-sat)
""" % (shift, byte_name))

# -- P2: Injectivity (1 query) -----------------------------------------------
#
# Two different 4-byte tuples produce different prefixes.
# Negate: assert prefix(a0..a3) == prefix(b0..b3) and (a0,a1,a2,a3) != (b0,b1,b2,b3)
# -> expect unsat.
#
# This ensures the prefix comparison in compare_structures is a sound fast-path:
# equal prefixes guarantee the first 4 bytes match.

report("  ... proving P2: injectivity")

ok &= prove("P2: different 4-byte tuples -> different prefixes", """\
(set-logic QF_BV)
(declare-const a0 (_ BitVec 32))
(declare-const a1 (_ BitVec 32))
(declare-const a2 (_ BitVec 32))
(declare-const a3 (_ BitVec 32))
(declare-const b0 (_ BitVec 32))
(declare-const b1 (_ BitVec 32))
(declare-const b2 (_ BitVec 32))
(declare-const b3 (_ BitVec 32))
; byte ranges
(assert (bvult a0 (_ bv256 32)))
(assert (bvult a1 (_ bv256 32)))
(assert (bvult a2 (_ bv256 32)))
(assert (bvult a3 (_ bv256 32)))
(assert (bvult b0 (_ bv256 32)))
(assert (bvult b1 (_ bv256 32)))
(assert (bvult b2 (_ bv256 32)))
(assert (bvult b3 (_ bv256 32)))
; prefix definitions
(define-fun prefix_a () (_ BitVec 32)
  (bvor (bvor a0 (bvshl a1 (_ bv8 32)))
        (bvor (bvshl a2 (_ bv16 32)) (bvshl a3 (_ bv24 32)))))
(define-fun prefix_b () (_ BitVec 32)
  (bvor (bvor b0 (bvshl b1 (_ bv8 32)))
        (bvor (bvshl b2 (_ bv16 32)) (bvshl b3 (_ bv24 32)))))
; Assert prefixes are equal
(assert (= prefix_a prefix_b))
; Assert tuples differ (at least one byte differs)
(assert (or (not (= a0 b0)) (not (= a1 b1)) (not (= a2 b2)) (not (= a3 b3))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: compute_prefix byte-packing is correct")
    print("    P1a-d: each byte recoverable from prefix (roundtrip)")
    print("    P2:    different byte tuples -> different prefixes (injective)")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
