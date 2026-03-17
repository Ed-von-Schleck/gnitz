"""Z3 formal proofs for Bloom filter memory safety.

Proves five groups of properties of gnitz/storage/bloom.py:
  P1. h2 is always odd (probe stride is never zero)
  P2. pos < num_bits (urem bound)
  P3. byte_idx < num_bytes (array access in-bounds)
  P4. bit_mask is a valid single-bit byte value (no overflow, non-zero)
  P5. All 7 probes are distinct (full coverage, no wasted work)

Together these guarantee that every array access in add() and may_contain() is
safe: the byte index is in-bounds and the bit mask correctly sets/tests exactly
one bit within that byte.

11 Z3 queries total + cross-check.  Runs under PyPy2.
Exit code 0 on success, 1 on any failure.
"""
import subprocess
import sys


# ── Helpers ──────────────────────────────────────────────────────────

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


def fmt64(n):
    return "0x%016x" % (n & 0xFFFFFFFFFFFFFFFF)


# ── Main ─────────────────────────────────────────────────────────────

print("=" * 56)
print("  Z3 PROOF: Bloom filter memory safety")
print("=" * 56)
sys.stdout.flush()

ok = True

# ── Cross-check: validate Z3 encoding against RPython ────────────────
# For several concrete h_val values, verify that the Z3 expression for h2
# matches the RPython computation:  h2 = (h_val >> 32) | r_uint64(1)

report("  ... cross-checking h2 computation against RPython")

print("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_uint64  # noqa: E402
print("  ... imports done")
sys.stdout.flush()

cross_check_vectors = [
    0x0000000000000000,
    0x0000000000000001,
    0x00000000FFFFFFFF,
    0xDEADBEEFCAFEBABE,
    0xFFFFFFFFFFFFFFFF,
    0x8000000000000000,
    0x0000000100000000,
    0x123456789ABCDEF0,
]

for h_val_int in cross_check_vectors:
    # RPython computation
    h_val = r_uint64(h_val_int)
    rpython_h2 = int((h_val >> 32) | r_uint64(1)) & 0xFFFFFFFFFFFFFFFF

    # Z3 computation via (simplify ...)
    smt_query = "(simplify (bvor (bvlshr (_ bv%d 64) (_ bv32 64)) (_ bv1 64)))" % (
        h_val_int & 0xFFFFFFFFFFFFFFFF)
    z3_out = run_z3(smt_query)

    if z3_out.startswith("#x"):
        z3_val = int(z3_out[2:], 16)
    elif z3_out.startswith("#b"):
        z3_val = int(z3_out[2:], 2)
    elif z3_out.startswith("(_ bv"):
        z3_val = int(z3_out.split()[1][2:])
    else:
        report("  FAIL  cross-check h2(%s): unexpected Z3 output: %s" % (fmt64(h_val_int), z3_out))
        ok = False
        continue

    if z3_val == rpython_h2:
        report("  PASS  cross-check h2(%s) -> %s" % (fmt64(h_val_int), fmt64(rpython_h2)))
    else:
        report("  FAIL  cross-check h2(%s): RPython=%s Z3=%s" % (
            fmt64(h_val_int), fmt64(rpython_h2), fmt64(z3_val)))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# ── P1: h2 is always odd (64-bit BV) ────────────────────────────────
#
# h2 = (h_val >> 32) | 1
# Prove: the LSB of h2 is always 1, i.e., h2 is odd.
# This guarantees the probe stride is never zero.

report("  ... proving P1: h2 is always odd")
ok &= prove("P1: h2 LSB is always 1", """\
(set-logic QF_BV)
(declare-const h_val (_ BitVec 64))
(define-fun h2 () (_ BitVec 64)
  (bvor (bvlshr h_val (_ bv32 64)) (_ bv1 64)))
; Negate: assert LSB is NOT 1
(assert (not (= ((_ extract 0 0) h2) #b1)))
(check-sat)
""")

# ── P2: pos < num_bits (16-bit BV) ──────────────────────────────────
#
# pos = (h1 + i * h2) % num_bits  (all unsigned 64-bit, result < 2^31)
# Prove: bvurem(x, n) < n for n > 0.
# bvurem is a BV axiom: width-independent, so 16-bit BVs suffice and
# avoid Z3's expensive division bit-blasting at larger widths.

report("  ... proving P2: pos < num_bits")
ok &= prove("P2: bvurem(x, num_bits) < num_bits", """\
(set-logic QF_BV)
(declare-const x (_ BitVec 16))
(declare-const num_bits (_ BitVec 16))
; num_bits > 0
(assert (bvugt num_bits (_ bv0 16)))
; Negate: assert bvurem(x, num_bits) >= num_bits
(assert (not (bvult (bvurem x num_bits) num_bits)))
(check-sat)
""")

# ── P3: byte_idx < num_bytes (32-bit BV) ────────────────────────────
#
# Given pos < num_bits and num_bits = num_bytes * 8:
# byte_idx = pos >> 3
# Prove: pos >> 3 < num_bytes.
#
# 32-bit BVs suffice: pos < 2^31, num_bytes < 2^28.

report("  ... proving P3: byte_idx < num_bytes")
ok &= prove("P3: (pos >> 3) < num_bytes", """\
(set-logic QF_BV)
(declare-const pos (_ BitVec 32))
(declare-const num_bytes (_ BitVec 32))
; num_bytes >= 8 (minimum bloom filter)
(assert (bvuge num_bytes (_ bv8 32)))
; pos < num_bytes * 8
(assert (bvult pos (bvmul num_bytes (_ bv8 32))))
; Negate: assert pos >> 3 >= num_bytes
(assert (not (bvult (bvlshr pos (_ bv3 32)) num_bytes)))
(check-sat)
""")

# ── P4: bit_mask is a valid single-bit byte value (16-bit BV) ───────
#
# bit_mask = 1 << (pos & 7)
# Prove: (a) bit_mask > 0 (non-zero — the OR actually sets a bit)
#        (b) bit_mask < 256 (fits in a byte — no overflow)
#
# 16-bit BVs: pos & 7 fits in 3 bits, 1 << 7 = 128 fits in 8 bits.

report("  ... proving P4a: bit_mask > 0")
ok &= prove("P4a: (1 << (pos & 7)) > 0", """\
(set-logic QF_BV)
(declare-const pos (_ BitVec 16))
(define-fun bit_mask () (_ BitVec 16)
  (bvshl (_ bv1 16) (bvand pos (_ bv7 16))))
; Negate: assert bit_mask == 0
(assert (= bit_mask (_ bv0 16)))
(check-sat)
""")

report("  ... proving P4b: bit_mask < 256")
ok &= prove("P4b: (1 << (pos & 7)) < 256", """\
(set-logic QF_BV)
(declare-const pos (_ BitVec 16))
(define-fun bit_mask () (_ BitVec 16)
  (bvshl (_ bv1 16) (bvand pos (_ bv7 16))))
; Negate: assert bit_mask >= 256
(assert (not (bvult bit_mask (_ bv256 16))))
(check-sat)
""")

# ── P5: All 7 probes are distinct (8-bit BV, 6 queries) ────────────
#
# Core lemma: For odd h2 and d in {1,...,6}: (d * h2) & 7 != 0
#
# Since h2 & 1 = 1, the low 3 bits of d * h2 are determined by d * (h2 & 7).
# With h2 & 7 in {1, 3, 5, 7} and d in {1,...,6}, none of the 24 products
# are 0 mod 8.
#
# 6 queries, one per d value.  Each uses 8-bit BVs.
#
# Transitivity argument (not a Z3 query):
#   num_bits = 8 * num_bytes, so 8 | num_bits.
#   If 8 does not divide d*h2 (proven above), then num_bits does not divide
#   d*h2 either (since num_bits | d*h2 would require 8 | d*h2 by transitivity
#   of divisibility).  Therefore probes at positions i and i+d never collide
#   mod num_bits, so all 7 probes (i = 0..6, max difference d = 6) are distinct.

report("  ... proving P5: all 7 probes are distinct (6 queries)")

for d in range(1, 7):
    ok &= prove("P5.%d: (d=%d) * h2 mod 8 != 0 for odd h2" % (d, d), """\
(set-logic QF_BV)
(declare-const h2 (_ BitVec 8))
; h2 is odd (LSB = 1)
(assert (= ((_ extract 0 0) h2) #b1))
; Negate: assert (d * h2) & 7 == 0
(assert (= (bvand (bvmul (_ bv%d 8) h2) (_ bv7 8)) (_ bv0 8)))
(check-sat)
""" % d)

# ── Summary ──────────────────────────────────────────────────────────

print("=" * 56)
if ok:
    print("  PROVED: Bloom filter array accesses are always safe")
    print("    P1: h2 stride is always odd (never zero)")
    print("    P2: probe position < num_bits (urem bound)")
    print("    P3: byte index < num_bytes (array in-bounds)")
    print("    P4: bit mask in {1,2,4,8,16,32,64,128} (valid byte)")
    print("    P5: all 7 probes are distinct (no wasted work)")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
