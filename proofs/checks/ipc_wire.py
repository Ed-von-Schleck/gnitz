"""Z3 formal proofs for IPC wire format structural integrity.

Part A: Header field packing (gnitz/server/ipc.py:18-74)
  P1. Header non-overlap: no two fields share a byte.
  P2. Header complete coverage: every byte in [0, 96) belongs to a field.

Part B: ZSet section alignment chain (gnitz/server/ipc.py:286-335)
  P3. ZSet structural offsets are 64-byte aligned.
  P4. ZSet structural sections are non-overlapping.

4 Z3 queries + 6 cross-checks.  Runs under PyPy2.
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

MASK64 = (1 << 64) - 1

# Header field layout: (offset, size) for each of 13 fields.
# From gnitz/server/ipc.py:18-31, 70-71
HEADER_FIELDS = [
    (0, 8),    # Magic
    (8, 4),    # Status
    (12, 4),   # Error Str Len
    (16, 8),   # Target ID
    (24, 8),   # Client ID
    (32, 8),   # Schema Row Count
    (40, 8),   # Schema Blob Size
    (48, 8),   # Data Row Count
    (56, 8),   # Data Blob Size
    (64, 8),   # p4 (data_pk_index)
    (72, 8),   # Flags
    (80, 8),   # p5 (seek_pk_lo)
    (88, 8),   # p6 (seek_pk_hi)
]

HEADER_SIZE = 96

# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: IPC wire format structural integrity")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-check Part A: header field layout ----------------------------------

report("  ... cross-checking header field layout")

print("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_uint64  # noqa: E402
print("  ... imports done")
sys.stdout.flush()

# Verify non-overlap in Python
byte_owner = [-1] * HEADER_SIZE
for fi, (off, sz) in enumerate(HEADER_FIELDS):
    for b in range(off, off + sz):
        if b >= HEADER_SIZE:
            report("  FAIL  header field %d: byte %d out of range [0, %d)" % (fi, b, HEADER_SIZE))
            ok = False
        elif byte_owner[b] != -1:
            report("  FAIL  header field %d overlaps field %d at byte %d" % (fi, byte_owner[b], b))
            ok = False
        else:
            byte_owner[b] = fi

# Verify complete coverage
uncovered = [b for b in range(HEADER_SIZE) if byte_owner[b] == -1]
if uncovered:
    report("  FAIL  header bytes not covered: %s" % uncovered)
    ok = False
else:
    report("  PASS  cross-check: header fields non-overlapping and cover [0, 96)")

# Verify total size
total_field_bytes = sum(sz for _, sz in HEADER_FIELDS)
if total_field_bytes != HEADER_SIZE:
    report("  FAIL  total field bytes %d != HEADER_SIZE %d" % (total_field_bytes, HEADER_SIZE))
    ok = False
else:
    report("  PASS  cross-check: total field bytes == 96")

# -- Cross-check Part B: ZSet alignment chain ---------------------------------

report("  ... cross-checking ZSet alignment chain")


def align_up_py(val, align):
    """Pure Python align_up matching gnitz/server/ipc.py:96-99."""
    u_val = val & MASK64
    u_align = align & MASK64
    return (u_val + u_align - 1) & ~(u_align - 1)


test_counts = [1, 8, 9, 100, 1 << 20]

for count in test_counts:
    struct_sz = count * 8
    pk_lo_off = 0
    pk_hi_off = align_up_py(pk_lo_off + struct_sz, 64)
    weight_off = align_up_py(pk_hi_off + struct_sz, 64)
    null_off = align_up_py(weight_off + struct_sz, 64)

    aligned = (pk_hi_off % 64 == 0 and weight_off % 64 == 0 and null_off % 64 == 0)
    monotonic = (struct_sz <= pk_hi_off and
                 pk_hi_off + struct_sz <= weight_off and
                 weight_off + struct_sz <= null_off)

    # Z3 cross-check
    smt_query = """\
(simplify (let ((count (_ bv%d 64)))
  (let ((struct_sz (bvmul count (_ bv8 64))))
    (let ((pk_hi_off (bvand (bvadd struct_sz (_ bv63 64)) (bvnot (_ bv63 64)))))
      (let ((weight_off (bvand (bvadd (bvadd pk_hi_off struct_sz) (_ bv63 64)) (bvnot (_ bv63 64)))))
        (let ((null_off (bvand (bvadd (bvadd weight_off struct_sz) (_ bv63 64)) (bvnot (_ bv63 64)))))
          (and
            (= (bvand pk_hi_off (_ bv63 64)) (_ bv0 64))
            (= (bvand weight_off (_ bv63 64)) (_ bv0 64))
            (= (bvand null_off (_ bv63 64)) (_ bv0 64))
            (bvule struct_sz pk_hi_off)
            (bvule (bvadd pk_hi_off struct_sz) weight_off)
            (bvule (bvadd weight_off struct_sz) null_off))))))))""" % count
    z3_out = run_z3(smt_query)
    z3_ok = z3_out == "true"

    if aligned and monotonic and z3_ok:
        report("  PASS  cross-check count=%d: pk_hi=%d weight=%d null=%d" % (
            count, pk_hi_off, weight_off, null_off))
    else:
        report("  FAIL  cross-check count=%d: aligned=%s monotonic=%s z3=%s" % (
            count, aligned, monotonic, z3_ok))
        ok = False

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P1: Header non-overlap --------------------------------------------------
#
# For each pair of fields (i, j), one ends before the other starts.
# Encode as: for a symbolic byte index b, it belongs to at most one field.
# Use 16-bit BV (sufficient for byte range [0, 96)).

report("  ... proving P1: header non-overlap")

# Build assertion: for every pair (i, j), ranges don't overlap
pair_asserts = []
for i in range(len(HEADER_FIELDS)):
    oi, si = HEADER_FIELDS[i]
    ei = oi + si
    for j in range(i + 1, len(HEADER_FIELDS)):
        oj, sj = HEADER_FIELDS[j]
        ej = oj + sj
        # end_i <= start_j OR end_j <= start_i
        pair_asserts.append(
            "(or (bvule (_ bv%d 16) (_ bv%d 16)) (bvule (_ bv%d 16) (_ bv%d 16)))" % (
                ei, oj, ej, oi))

ok &= prove("P1: header non-overlap", """\
(set-logic QF_BV)
; Negate: at least one pair overlaps
(assert (not (and
  %s)))
(check-sat)
""" % "\n  ".join(pair_asserts))

# -- P2: Header complete coverage ---------------------------------------------
#
# Every byte in [0, 96) belongs to at least one field.
# Use a symbolic byte index b in [0, 96), assert it's NOT in any field range.

report("  ... proving P2: header complete coverage")

# Build: b >= start_i AND b < end_i  (membership in field i)
membership_clauses = []
for off, sz in HEADER_FIELDS:
    end = off + sz
    membership_clauses.append(
        "(and (not (bvult b (_ bv%d 16))) (bvult b (_ bv%d 16)))" % (off, end))

ok &= prove("P2: header complete coverage", """\
(set-logic QF_BV)
(declare-const b (_ BitVec 16))
; b is in [0, 96)
(assert (not (bvult b (_ bv0 16))))
(assert (bvult b (_ bv96 16))
)
; Negate: b does not belong to any field
(assert (not (or
  %s)))
(check-sat)
""" % "\n  ".join(membership_clauses))

# -- P3: ZSet structural offsets are 64-byte aligned --------------------------
#
# For symbolic count in (0, 2^28], each structural offset has low 6 bits = 0.
# 2^28 bound prevents u64 overflow in count * 8 (2^28 * 8 = 2^31).

report("  ... proving P3: ZSet structural offsets are 64-byte aligned")
ok &= prove("P3: pk_hi_off, weight_off, null_off are 64-aligned", """\
(set-logic QF_BV)
(declare-const count (_ BitVec 64))
(assert (bvugt count (_ bv0 64)))
(assert (bvule count (_ bv268435456 64)))
(define-fun struct_sz () (_ BitVec 64) (bvmul count (_ bv8 64)))
(define-fun a64 ((x (_ BitVec 64))) (_ BitVec 64)
  (bvand (bvadd x (_ bv63 64)) (bvnot (_ bv63 64))))
(define-fun pk_hi_off () (_ BitVec 64) (a64 struct_sz))
(define-fun weight_off () (_ BitVec 64) (a64 (bvadd pk_hi_off struct_sz)))
(define-fun null_off () (_ BitVec 64) (a64 (bvadd weight_off struct_sz)))
; Negate: at least one offset is not 64-aligned
(assert (not (and
  (= (bvand pk_hi_off (_ bv63 64)) (_ bv0 64))
  (= (bvand weight_off (_ bv63 64)) (_ bv0 64))
  (= (bvand null_off (_ bv63 64)) (_ bv0 64)))))
(check-sat)
""")

# -- P4: ZSet structural sections are non-overlapping -------------------------
#
# Each section occupies [off, off + struct_sz). Sections must not overlap:
# pk_lo ends before pk_hi starts, pk_hi ends before weight starts, etc.

report("  ... proving P4: ZSet structural sections are non-overlapping")
ok &= prove("P4: structural sections [off, off+struct_sz) don't overlap", """\
(set-logic QF_BV)
(declare-const count (_ BitVec 64))
(assert (bvugt count (_ bv0 64)))
(assert (bvule count (_ bv268435456 64)))
(define-fun struct_sz () (_ BitVec 64) (bvmul count (_ bv8 64)))
(define-fun a64 ((x (_ BitVec 64))) (_ BitVec 64)
  (bvand (bvadd x (_ bv63 64)) (bvnot (_ bv63 64))))
(define-fun pk_hi_off () (_ BitVec 64) (a64 struct_sz))
(define-fun weight_off () (_ BitVec 64) (a64 (bvadd pk_hi_off struct_sz)))
(define-fun null_off () (_ BitVec 64) (a64 (bvadd weight_off struct_sz)))
; Negate: some section overlaps another
; pk_lo=[0, struct_sz), pk_hi=[pk_hi_off, pk_hi_off+struct_sz), etc.
(assert (not (and
  (bvule struct_sz pk_hi_off)
  (bvule (bvadd pk_hi_off struct_sz) weight_off)
  (bvule (bvadd weight_off struct_sz) null_off))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: IPC wire format structural integrity")
    print("    P1: header non-overlap")
    print("    P2: header complete coverage [0, 96)")
    print("    P3: ZSet structural offsets are 64-aligned")
    print("    P4: ZSet structural sections are non-overlapping")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
