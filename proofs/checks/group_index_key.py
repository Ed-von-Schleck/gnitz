"""Z3 formal proofs for reduce group index key composition roundtrip.

Proves five properties of gnitz/dbsp/ops/group_index.py:78-94
(ReduceGroupIndex):

  The secondary index key is:
    composite = (gc_u64 << 64) | source_pk_lo
  stored as a U128 primary key.  This gives rows in the same group
  contiguous keys, enabling O(log N + k) range scans.

  P1. extract[127:64](composite) == gc_u64 (128-bit BV, UNSAT)
  P2. extract[63:0](composite) == source_pk_lo (128-bit BV, UNSAT)
  P3. High and low bit-fields are disjoint (128-bit BV, UNSAT)
  P4. Packing is injective: different (gc1, pk1) yields different key
      (128-bit BV, UNSAT)
  P5. Keys with different gc values have different high 64 bits, so
      groups are contiguous in sort order (128-bit BV, UNSAT)

5 Z3 queries + ~6 cross-checks.  Runs under PyPy2.
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


# -- Constants ----------------------------------------------------------------

MASK64  = (1 << 64) - 1
MASK128 = (1 << 128) - 1


def pack_key(gc_u64, pk_lo):
    """Python simulation of (gc_u64 << 64) | source_pk_lo."""
    return ((gc_u64 & MASK64) << 64) | (pk_lo & MASK64)


def extract_hi(composite):
    return (composite >> 64) & MASK64


def extract_lo(composite):
    return composite & MASK64


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: Reduce group index key composition roundtrip")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-checks ------------------------------------------------------------

report("  ... cross-checking group index key packing")

# Test vectors: (gc_u64, pk_lo)
vectors = [
    (0, 0),
    (1, 0),
    (0, 1),
    (42, 0xDEADBEEF),
    ((1 << 63) - 1, (1 << 63) - 1),
    (0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF),
]

for gc, pk in vectors:
    composite = pack_key(gc, pk)
    hi = extract_hi(composite)
    lo = extract_lo(composite)
    if hi == (gc & MASK64) and lo == (pk & MASK64):
        report("  PASS  cross-check: gc=0x%016x pk=0x%016x key=0x%032x [roundtrip OK]" % (
            gc & MASK64, pk & MASK64, composite & MASK128))
    else:
        report("  FAIL  cross-check: gc=0x%016x pk=0x%016x hi=0x%016x lo=0x%016x" % (
            gc & MASK64, pk & MASK64, hi, lo))
        ok = False

# Verify injectivity on a sample of vectors
keys = [pack_key(gc, pk) for gc, pk in vectors]
if len(set(keys)) == len(keys):
    report("  PASS  cross-check: all %d test keys are distinct" % len(keys))
else:
    report("  FAIL  cross-check: test key collision detected")
    ok = False

# Verify group contiguity: same gc → same high bits
gc_a = 5
keys_gc_a = [pack_key(gc_a, pk) for pk in [0, 1, 100, 0xFFFFFFFF]]
all_same_hi = all(extract_hi(k) == gc_a for k in keys_gc_a)
if all_same_hi:
    report("  PASS  cross-check: same gc=%d gives same high 64 bits" % gc_a)
else:
    report("  FAIL  cross-check: same gc gives different high bits")
    ok = False

# Z3 simplify: extract[127:64] of (1 << 64) == 1
smt_q = "(simplify ((_ extract 127 64) (bvshl ((_ zero_extend 64) (_ bv1 64)) (_ bv64 128))))"
z3_out = run_z3(smt_q)
expected = 1
if z3_out.startswith("#x"):
    z3_val = int(z3_out[2:], 16)
    if z3_val == expected:
        report("  PASS  cross-check: Z3 extract[127:64](1 << 64) = 1")
    else:
        report("  FAIL  cross-check: Z3 extract result %d != 1" % z3_val)
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: extract[127:64](composite) == gc_u64 (128-bit BV, UNSAT) -------------
#
# composite = (zero_extend_64(gc) << 64) | zero_extend_64(pk_lo)
# The high 64 bits are exactly gc_u64.

report("  ... proving P1: extract[127:64](composite) == gc_u64")

ok &= prove("P1: extract[127:64]((gc<<64)|pk_lo) == gc", """\
(set-logic QF_BV)
(declare-const gc (_ BitVec 64))
(declare-const pk_lo (_ BitVec 64))
(define-fun composite () (_ BitVec 128)
  (bvor (bvshl ((_ zero_extend 64) gc) (_ bv64 128))
        ((_ zero_extend 64) pk_lo)))
; Negate: high 64 bits != gc
(assert (not (= ((_ extract 127 64) composite) gc)))
(check-sat)
""")

# -- P2: extract[63:0](composite) == source_pk_lo (128-bit BV, UNSAT) ---------
#
# The low 64 bits are exactly pk_lo.

report("  ... proving P2: extract[63:0](composite) == pk_lo")

ok &= prove("P2: extract[63:0]((gc<<64)|pk_lo) == pk_lo", """\
(set-logic QF_BV)
(declare-const gc (_ BitVec 64))
(declare-const pk_lo (_ BitVec 64))
(define-fun composite () (_ BitVec 128)
  (bvor (bvshl ((_ zero_extend 64) gc) (_ bv64 128))
        ((_ zero_extend 64) pk_lo)))
; Negate: low 64 bits != pk_lo
(assert (not (= ((_ extract 63 0) composite) pk_lo)))
(check-sat)
""")

# -- P3: High and low bit-fields are disjoint (128-bit BV, UNSAT) -------------
#
# (gc_part << 64) and zero_extend(pk_lo) share no bits.

report("  ... proving P3: high and low bit-fields are disjoint")

ok &= prove("P3: (gc<<64) AND zero_extend(pk_lo) == 0", """\
(set-logic QF_BV)
(declare-const gc (_ BitVec 64))
(declare-const pk_lo (_ BitVec 64))
(define-fun high_part () (_ BitVec 128)
  (bvshl ((_ zero_extend 64) gc) (_ bv64 128)))
(define-fun low_part () (_ BitVec 128)
  ((_ zero_extend 64) pk_lo))
; Negate: they share at least one bit
(assert (not (= (bvand high_part low_part) (_ bv0 128))))
(check-sat)
""")

# -- P4: Packing is injective (128-bit BV, UNSAT) ----------------------------
#
# Different (gc1, pk1) pairs produce different composite keys.

report("  ... proving P4: packing (gc, pk_lo) -> key is injective")

ok &= prove("P4: (gc1,pk1) != (gc2,pk2) implies key1 != key2", """\
(set-logic QF_BV)
(declare-const gc1 (_ BitVec 64))
(declare-const pk1 (_ BitVec 64))
(declare-const gc2 (_ BitVec 64))
(declare-const pk2 (_ BitVec 64))
; Inputs differ in at least one field
(assert (not (and (= gc1 gc2) (= pk1 pk2))))
(define-fun key1 () (_ BitVec 128)
  (bvor (bvshl ((_ zero_extend 64) gc1) (_ bv64 128))
        ((_ zero_extend 64) pk1)))
(define-fun key2 () (_ BitVec 128)
  (bvor (bvshl ((_ zero_extend 64) gc2) (_ bv64 128))
        ((_ zero_extend 64) pk2)))
; Negate: different inputs produce equal keys
(assert (= key1 key2))
(check-sat)
""")

# -- P5: Different gc values → different high 64 bits (128-bit BV, UNSAT) ----
#
# Proves that keys from different groups sort into different contiguous
# ranges, enabling range scans: gc1 != gc2 → extract[127:64](key1) != extract[127:64](key2).

report("  ... proving P5: different gc values give different high 64 bits")

ok &= prove("P5: gc1 != gc2 implies extract[127:64](key1) != extract[127:64](key2)", """\
(set-logic QF_BV)
(declare-const gc1 (_ BitVec 64))
(declare-const gc2 (_ BitVec 64))
(declare-const pk1 (_ BitVec 64))
(declare-const pk2 (_ BitVec 64))
; Group columns differ
(assert (not (= gc1 gc2)))
(define-fun key1 () (_ BitVec 128)
  (bvor (bvshl ((_ zero_extend 64) gc1) (_ bv64 128))
        ((_ zero_extend 64) pk1)))
(define-fun key2 () (_ BitVec 128)
  (bvor (bvshl ((_ zero_extend 64) gc2) (_ bv64 128))
        ((_ zero_extend 64) pk2)))
; Negate: high 64 bits are equal despite different gc values
(assert (= ((_ extract 127 64) key1) ((_ extract 127 64) key2)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: Reduce group index key composition roundtrip")
    print("    P1: extract[127:64](composite) == gc_u64")
    print("    P2: extract[63:0](composite) == source_pk_lo")
    print("    P3: high and low bit-fields are disjoint")
    print("    P4: packing is injective")
    print("    P5: different gc values give different high 64 bits")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
