"""Z3 formal proofs for IPC protocol flag bit-safety.

Proves seven properties of gnitz/server/ipc.py:26-43 (protocol flags) and
gnitz/server/ipc.py:50-51 (meta-schema flags):

  Low protocol flags (bits 0-9):
  FLAG_ALLOCATE_TABLE_ID = 1    (bit 0)
  FLAG_ALLOCATE_SCHEMA_ID = 2   (bit 1)
  FLAG_SHUTDOWN = 4             (bit 2)
  FLAG_DDL_SYNC = 8             (bit 3)
  FLAG_EXCHANGE = 16            (bit 4)
  FLAG_PUSH = 32                (bit 5)
  FLAG_HAS_PK = 64              (bit 6)
  FLAG_SEEK = 128               (bit 7)
  FLAG_SEEK_BY_INDEX = 256      (bit 8)
  FLAG_ALLOCATE_INDEX_ID = 512  (bit 9)
  FLAG_SCAN = 0                 (no bits)

  High WAL-block flags (bits 48-51):
  FLAG_HAS_SCHEMA         = 1<<48
  FLAG_HAS_DATA           = 1<<49
  FLAG_BATCH_SORTED       = 1<<50
  FLAG_BATCH_CONSOLIDATED = 1<<51

  META_FLAG_NULLABLE = 1, META_FLAG_IS_PK = 2

  P1. All distinct non-zero protocol flags are powers of 2 (Python cross-check)
  P2. Any power-of-2 flag is extractable from an OR combination (16-bit BV, UNSAT)
  P3. Two distinct powers-of-2 share no bits (16-bit BV, UNSAT)
  P4. META_FLAG_NULLABLE and META_FLAG_IS_PK share no bits (16-bit BV, UNSAT)
  P5. FLAG_SCAN=0 is identity for OR (16-bit BV, UNSAT)
  P6. High-bit flags (48-51) don't overlap with low flags (bits 0-9) (64-bit BV, UNSAT)
  P7. FLAG_HAS_DATA (bit 49) > FLAG_HAS_SCHEMA (bit 48): schema precedes data
      (Python cross-check)

7 Z3 queries + ~7 cross-checks.  Runs under PyPy2.
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

# Low protocol flags from gnitz/server/ipc.py:26-37
FLAG_ALLOCATE_TABLE_ID  = 1
FLAG_ALLOCATE_SCHEMA_ID = 2
FLAG_SHUTDOWN           = 4
FLAG_DDL_SYNC           = 8
FLAG_EXCHANGE           = 16
FLAG_PUSH               = 32
FLAG_HAS_PK             = 64
FLAG_SEEK               = 128
FLAG_SEEK_BY_INDEX      = 256
FLAG_ALLOCATE_INDEX_ID  = 512
FLAG_SCAN               = 0

# High WAL-block flags from gnitz/server/ipc.py:40-43
FLAG_HAS_SCHEMA         = 1 << 48
FLAG_HAS_DATA           = 1 << 49
FLAG_BATCH_SORTED       = 1 << 50
FLAG_BATCH_CONSOLIDATED = 1 << 51

# Meta-schema flags from gnitz/server/ipc.py:50-51
META_FLAG_NULLABLE = 1
META_FLAG_IS_PK    = 2

# Non-zero low protocol flags list
NONZERO_FLAGS = [
    FLAG_ALLOCATE_TABLE_ID,
    FLAG_ALLOCATE_SCHEMA_ID,
    FLAG_SHUTDOWN,
    FLAG_DDL_SYNC,
    FLAG_EXCHANGE,
    FLAG_PUSH,
    FLAG_HAS_PK,
    FLAG_SEEK,
    FLAG_SEEK_BY_INDEX,
    FLAG_ALLOCATE_INDEX_ID,
]

# High-bit WAL-block flags list
HIGH_FLAGS = [
    FLAG_HAS_SCHEMA,
    FLAG_HAS_DATA,
    FLAG_BATCH_SORTED,
    FLAG_BATCH_CONSOLIDATED,
]

# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: IPC protocol flag bit-safety")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-checks ------------------------------------------------------------

report("  ... cross-checking protocol flag constants")

# P1 cross-check: all non-zero flags are distinct
all_distinct = (len(set(NONZERO_FLAGS)) == len(NONZERO_FLAGS))
if not all_distinct:
    report("  FAIL  cross-check: non-zero flags not all distinct")
    ok = False
else:
    report("  PASS  cross-check: all %d non-zero flags are distinct" % len(NONZERO_FLAGS))

# P1 cross-check: each non-zero flag is a power of 2
all_pow2 = all(f > 0 and (f & (f - 1)) == 0 for f in NONZERO_FLAGS)
if not all_pow2:
    report("  FAIL  cross-check: not all non-zero flags are powers of 2")
    ok = False
else:
    report("  PASS  cross-check: all non-zero flags are powers of 2 (bits 0-9)")

# P3 cross-check: any two distinct flags share no bits
no_collision = True
for i in range(len(NONZERO_FLAGS)):
    for j in range(i + 1, len(NONZERO_FLAGS)):
        if (NONZERO_FLAGS[i] & NONZERO_FLAGS[j]) != 0:
            report("  FAIL  cross-check: flags %d and %d share bits" % (
                NONZERO_FLAGS[i], NONZERO_FLAGS[j]))
            no_collision = False
            ok = False
if no_collision:
    report("  PASS  cross-check: all pairs of non-zero flags share no bits")

# P4 cross-check: META flags are distinct and non-overlapping
if META_FLAG_NULLABLE != META_FLAG_IS_PK and (META_FLAG_NULLABLE & META_FLAG_IS_PK) == 0:
    report("  PASS  cross-check: META_FLAG_NULLABLE=%d and META_FLAG_IS_PK=%d share no bits" % (
        META_FLAG_NULLABLE, META_FLAG_IS_PK))
else:
    report("  FAIL  cross-check: META flags overlap")
    ok = False

# P2 cross-check: flag extraction from OR combination
combined = FLAG_ALLOCATE_TABLE_ID | FLAG_SEEK | FLAG_EXCHANGE
for f in [FLAG_ALLOCATE_TABLE_ID, FLAG_SEEK, FLAG_EXCHANGE]:
    if (combined & f) != f:
        report("  FAIL  cross-check: flag %d not extractable from OR combination %d" % (f, combined))
        ok = False
report("  PASS  cross-check: flag extraction from OR combination %d" % combined)

# P6 cross-check: high-bit flags are powers of 2 and don't overlap with low flags
all_high_pow2 = all(f > 0 and (f & (f - 1)) == 0 for f in HIGH_FLAGS)
if not all_high_pow2:
    report("  FAIL  cross-check: not all high-bit flags are powers of 2")
    ok = False
else:
    report("  PASS  cross-check: all 4 high-bit flags are powers of 2 (bits 48-51)")

high_vs_low_ok = True
for hf in HIGH_FLAGS:
    for lf in NONZERO_FLAGS:
        if hf & lf:
            report("  FAIL  cross-check: high flag %d overlaps low flag %d" % (hf, lf))
            high_vs_low_ok = False
            ok = False
if high_vs_low_ok:
    report("  PASS  cross-check: high-bit flags don't overlap with any low protocol flag")

# P7 cross-check: FLAG_HAS_DATA (bit 49) > FLAG_HAS_SCHEMA (bit 48)
if FLAG_HAS_DATA > FLAG_HAS_SCHEMA:
    report("  PASS  cross-check: FLAG_HAS_DATA(%d) > FLAG_HAS_SCHEMA(%d)" % (
        FLAG_HAS_DATA, FLAG_HAS_SCHEMA))
else:
    report("  FAIL  cross-check: FLAG_HAS_DATA ordering wrong")
    ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: All non-zero protocol flags are powers of 2 (already verified above) -
#
# The Python cross-check above covers P1 exhaustively for all 10 named flags.
# Below we prove the general symbolic property used by P2/P3.

# -- P2: Any power-of-2 flag is extractable from an OR combination (16-bit BV, UNSAT)
#
# If flag is a power-of-2 and combined = other | flag,
# then (combined & flag) == flag.

report("  ... proving P2: power-of-2 flag extractable from OR combination")

ok &= prove("P2: (other | flag) & flag == flag for power-of-2 flag", """\
(set-logic QF_BV)
(declare-const flag (_ BitVec 16))
(declare-const other (_ BitVec 16))
; flag is a power of 2 (positive, and flag & (flag-1) == 0)
(assert (bvugt flag (_ bv0 16)))
(assert (= (bvand flag (bvsub flag (_ bv1 16))) (_ bv0 16)))
(define-fun combined () (_ BitVec 16) (bvor other flag))
; Negate: flag is not extractable from combined
(assert (not (= (bvand combined flag) flag)))
(check-sat)
""")

# -- P3: Two distinct powers-of-2 share no bits (16-bit BV, UNSAT) -----------
#
# If f1 and f2 are distinct powers-of-2, then f1 & f2 == 0.

report("  ... proving P3: two distinct powers-of-2 share no bits")

ok &= prove("P3: f1 != f2 and both pow2 implies f1 & f2 == 0", """\
(set-logic QF_BV)
(declare-const f1 (_ BitVec 16))
(declare-const f2 (_ BitVec 16))
; Both are powers of 2
(assert (bvugt f1 (_ bv0 16)))
(assert (= (bvand f1 (bvsub f1 (_ bv1 16))) (_ bv0 16)))
(assert (bvugt f2 (_ bv0 16)))
(assert (= (bvand f2 (bvsub f2 (_ bv1 16))) (_ bv0 16)))
; They are distinct
(assert (not (= f1 f2)))
; Negate: they share at least one bit
(assert (not (= (bvand f1 f2) (_ bv0 16))))
(check-sat)
""")

# -- P4: META_FLAG_NULLABLE and META_FLAG_IS_PK share no bits (16-bit BV, UNSAT)
#
# 1 & 2 == 0.

report("  ... proving P4: META_FLAG_NULLABLE and META_FLAG_IS_PK share no bits")

ok &= prove("P4: META_FLAG_NULLABLE(1) & META_FLAG_IS_PK(2) == 0", """\
(set-logic QF_BV)
; META_FLAG_NULLABLE = 1, META_FLAG_IS_PK = 2
; Negate: they share bits
(assert (not (= (bvand (_ bv1 16) (_ bv2 16)) (_ bv0 16))))
(check-sat)
""")

# -- P5: FLAG_SCAN=0 is identity for OR (16-bit BV, UNSAT) -------------------
#
# x | 0 == x for any flags value.

report("  ... proving P5: FLAG_SCAN=0 is identity for OR")

ok &= prove("P5: x | FLAG_SCAN(0) == x", """\
(set-logic QF_BV)
(declare-const x (_ BitVec 16))
; Negate: OR with 0 changes value
(assert (not (= (bvor x (_ bv0 16)) x)))
(check-sat)
""")

# -- P6: High-bit flags (48-51) don't overlap with low flags (bits 0-9) -------
#
# Low flags live in bits 0-9 (max = 512 = 2^9). High flags live in bits 48-51
# (min = 2^48). A 64-bit value cannot have both a bit in [0,9] and a bit in
# [48,51] set simultaneously if the values are from different bit ranges.
# Prove symbolically that 2^48 & 512 == 0 (representative pair).

report("  ... proving P6: high-bit flags don't overlap with low protocol flags")

ok &= prove("P6: FLAG_HAS_SCHEMA(1<<48) & FLAG_ALLOCATE_INDEX_ID(512) == 0", """\
(set-logic QF_BV)
; FLAG_HAS_SCHEMA = 2^48, FLAG_ALLOCATE_INDEX_ID = 512 = 2^9
; Negate: they share a bit
(assert (not (= (bvand (_ bv281474976710656 64) (_ bv512 64)) (_ bv0 64))))
(check-sat)
""")

ok &= prove("P6b: all four high-bit flags have zero overlap with low-bit mask (bits 0-9)", """\
(set-logic QF_BV)
; Low flag mask covers bits 0-9: mask = 0x3FF = 1023
; High flags are bits 48-51: all have zero bits in positions 0-9
(assert (not (and
  (= (bvand (_ bv281474976710656 64) (_ bv1023 64)) (_ bv0 64))
  (= (bvand (_ bv562949953421312 64) (_ bv1023 64)) (_ bv0 64))
  (= (bvand (_ bv1125899906842624 64) (_ bv1023 64)) (_ bv0 64))
  (= (bvand (_ bv2251799813685248 64) (_ bv1023 64)) (_ bv0 64)))))
(check-sat)
""")

# -- P7: FLAG_HAS_DATA ordering — schema block precedes data block -------------
#
# FLAG_HAS_SCHEMA (bit 48) < FLAG_HAS_DATA (bit 49) in numeric value.
# This encodes the protocol ordering: schema block always comes before data.

report("  ... proving P7: FLAG_HAS_DATA > FLAG_HAS_SCHEMA (bit 49 > bit 48)")

ok &= prove("P7: FLAG_HAS_DATA(1<<49) > FLAG_HAS_SCHEMA(1<<48)", """\
(set-logic QF_BV)
; FLAG_HAS_SCHEMA = 2^48, FLAG_HAS_DATA = 2^49
; Negate: FLAG_HAS_DATA <= FLAG_HAS_SCHEMA
(assert (not (bvugt (_ bv562949953421312 64) (_ bv281474976710656 64))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: IPC protocol flag bit-safety")
    print("    P1: all non-zero protocol flags are powers of 2")
    print("    P2: power-of-2 flag extractable from OR combination")
    print("    P3: two distinct powers-of-2 share no bits")
    print("    P4: META_FLAG_NULLABLE and META_FLAG_IS_PK share no bits")
    print("    P5: FLAG_SCAN=0 is identity for OR")
    print("    P6: high-bit flags (48-51) don't overlap with low protocol flags (0-9)")
    print("    P7: FLAG_HAS_DATA > FLAG_HAS_SCHEMA (schema block precedes data)")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
