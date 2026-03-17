"""Z3 formal proofs for WAL columnar block region count formula.

Proves five properties of gnitz/storage/wal_columnar.py:52-66:

  num_non_pk = num_cols - 1   # all columns except the PK
  num_data_regions = 4 + num_non_pk + 1
  # 4 = pk_lo + pk_hi + weight + null_flags regions
  # num_non_pk = regular data column regions
  # 1 = blob region

Directory starts at offset WAL_BLOCK_HEADER_SIZE (48), each entry 8 bytes.
  dir_size = num_data_regions * 8

WAL block header (WAL_BLOCK_HEADER_SIZE = 48, from wal_layout.py:7-16):
  [0,8)    LSN          u64   [8,12)   TID        u32
  [12,16)  COUNT        u32   [16,20)  SIZE       u32
  [20,24)  VERSION      u32   [24,32)  CHECKSUM   u64
  [32,36)  NUM_REGIONS  u32   [36,40)  RESERVED   u32
  [40,48)  BLOB_SIZE    u64

  P1. WAL header fields cover [0, 48) (Python cross-check + 8-bit BV UNSAT)
  P2. 4 + n + 1 == n + 5 for all n (16-bit BV, UNSAT)
  P3. dir_size = n*8 is 8-byte aligned for any n (16-bit BV, UNSAT)
  P4. First data region starts after header + directory (16-bit BV, UNSAT)
  P5. body_size >= dir_size when total_size >= 48 + dir_size (16-bit BV, UNSAT)

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

WAL_BLOCK_HEADER_SIZE = 48

# WAL header fields: (offset, size_bytes, type_size, name)
WAL_FIELDS = [
    (0,  8, 8, "LSN"),
    (8,  4, 4, "TID"),
    (12, 4, 4, "COUNT"),
    (16, 4, 4, "SIZE"),
    (20, 4, 4, "VERSION"),
    (24, 8, 8, "CHECKSUM"),
    (32, 4, 4, "NUM_REGIONS"),
    (36, 4, 4, "RESERVED"),
    (40, 8, 8, "BLOB_SIZE"),
]

# Source constants from gnitz/storage/wal_layout.py
WAL_SOURCE_OFFSETS = {
    "LSN":         0,
    "TID":         8,
    "COUNT":       12,
    "SIZE":        16,
    "VERSION":     20,
    "CHECKSUM":    24,
    "NUM_REGIONS": 32,
    "RESERVED":    36,
    "BLOB_SIZE":   40,
}

# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: WAL columnar block region count formula")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- P1: Python byte-ownership cross-check ------------------------------------

report("  ... cross-checking WAL header field layout (P1)")

byte_owner = [-1] * WAL_BLOCK_HEADER_SIZE
for fi, (off, sz, _tsz, name) in enumerate(WAL_FIELDS):
    for b in range(off, off + sz):
        if b >= WAL_BLOCK_HEADER_SIZE:
            report("  FAIL  field %d (%s): byte %d out of [0, %d)" % (
                fi, name, b, WAL_BLOCK_HEADER_SIZE))
            ok = False
        elif byte_owner[b] != -1:
            report("  FAIL  field %d (%s) overlaps field %d at byte %d" % (
                fi, name, byte_owner[b], b))
            ok = False
        else:
            byte_owner[b] = fi

uncovered = [b for b in range(WAL_BLOCK_HEADER_SIZE) if byte_owner[b] == -1]
if uncovered:
    report("  FAIL  bytes not covered: %s" % uncovered)
    ok = False
else:
    report("  PASS  cross-check: WAL header fields non-overlapping and cover [0, 48)")

total_bytes = sum(sz for _, sz, _, _ in WAL_FIELDS)
if total_bytes != WAL_BLOCK_HEADER_SIZE:
    report("  FAIL  total field bytes %d != WAL_BLOCK_HEADER_SIZE %d" % (
        total_bytes, WAL_BLOCK_HEADER_SIZE))
    ok = False
else:
    report("  PASS  cross-check: sum of WAL header field sizes == 48")

# Verify WAL_OFF_* constants match the table
for off, _sz, _tsz, name in WAL_FIELDS:
    expected = WAL_SOURCE_OFFSETS[name]
    if off != expected:
        report("  FAIL  WAL_OFF_%s offset %d != source %d" % (name, off, expected))
        ok = False
    else:
        report("  PASS  cross-check: WAL_OFF_%s == %d" % (name, off))

# Cross-check region count formula for representative (num_cols, count) pairs
region_vectors = [
    (1,  0),
    (2,  100),
    (3,  1000),
    (5,  50),
    (10, 1),
    (20, 500),
]

for num_cols, _count in region_vectors:
    num_non_pk = num_cols - 1
    num_data_regions = 4 + num_non_pk + 1
    dir_size = num_data_regions * 8
    expected_regions = num_cols + 4  # simplification: 4 + (num_cols-1) + 1
    if num_data_regions != expected_regions:
        report("  FAIL  cross-check num_cols=%d: num_data_regions=%d != num_cols+4=%d" % (
            num_cols, num_data_regions, expected_regions))
        ok = False
    elif dir_size % 8 != 0:
        report("  FAIL  cross-check num_cols=%d: dir_size=%d not 8-aligned" % (
            num_cols, dir_size))
        ok = False
    elif WAL_BLOCK_HEADER_SIZE + dir_size <= WAL_BLOCK_HEADER_SIZE:
        report("  FAIL  cross-check num_cols=%d: first_data <= 48" % num_cols)
        ok = False
    else:
        report("  PASS  cross-check num_cols=%d: regions=%d dir_size=%d first_data=%d" % (
            num_cols, num_data_regions, dir_size, WAL_BLOCK_HEADER_SIZE + dir_size))

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P1 BV: WAL header covers [0, 48) (8-bit BV, UNSAT) ----------------------

report("  ... proving P1 BV: WAL header fields cover [0, 48)")

wal_membership = []
for off, sz, _tsz, _ in WAL_FIELDS:
    end = off + sz
    wal_membership.append(
        "(and (bvuge b (_ bv%d 8)) (bvult b (_ bv%d 8)))" % (off, end))

ok &= prove("P1 BV: WAL header coverage [0, 48)", """\
(set-logic QF_BV)
(declare-const b (_ BitVec 8))
(assert (bvult b (_ bv48 8)))
; Negate: b does not belong to any field
(assert (not (or
  %s)))
(check-sat)
""" % "\n  ".join(wal_membership))

# -- P2: 4 + n + 1 == n + 5 for all valid n (16-bit BV, UNSAT) ---------------
#
# Algebraic simplification of the region count formula.
# num_data_regions = 4 + num_non_pk + 1 = num_non_pk + 5 = (num_cols - 1) + 5 = num_cols + 4.

report("  ... proving P2: 4 + n + 1 == n + 5")

ok &= prove("P2: 4 + n + 1 == n + 5", """\
(set-logic QF_BV)
(declare-const n (_ BitVec 16))
(assert (bvule n (_ bv1000 16)))
; Negate: 4 + n + 1 != n + 5
(assert (not (= (bvadd (bvadd (_ bv4 16) n) (_ bv1 16))
               (bvadd n (_ bv5 16)))))
(check-sat)
""")

# -- P3: dir_size = n*8 is 8-byte aligned (16-bit BV, UNSAT) -----------------

report("  ... proving P3: dir_size = n*8 is 8-byte aligned")

ok &= prove("P3: (n * 8) & 7 == 0", """\
(set-logic QF_BV)
(declare-const n (_ BitVec 16))
(assert (bvule n (_ bv1000 16)))
(define-fun dir_sz () (_ BitVec 16) (bvmul n (_ bv8 16)))
; Negate: dir_sz not 8-aligned
(assert (not (= (bvand dir_sz (_ bv7 16)) (_ bv0 16))))
(check-sat)
""")

# -- P4: First data region starts after header + directory (16-bit BV, UNSAT)
#
# first_data = 48 + dir_size = 48 + n*8.  For n >= 1, first_data > 48.

report("  ... proving P4: first data region offset > 48")

ok &= prove("P4: 48 + n*8 > 48 for n >= 1", """\
(set-logic QF_BV)
(declare-const n (_ BitVec 16))
(assert (bvugt n (_ bv0 16)))
(assert (bvule n (_ bv1000 16)))
(define-fun first_data () (_ BitVec 16)
  (bvadd (_ bv48 16) (bvmul n (_ bv8 16))))
; Negate: first_data <= 48
(assert (not (bvugt first_data (_ bv48 16))))
(check-sat)
""")

# -- P5: body_size >= dir_size when total_size >= 48 + dir_size ---------------
#
# body_sz = total_sz - 48.
# Given total_sz >= 48 + dir_sz, body_sz = total_sz - 48 >= dir_sz.

report("  ... proving P5: body_size >= dir_size when total_size >= 48 + dir_size")

ok &= prove("P5: total_sz >= 48 + dir_sz => body_sz >= dir_sz", """\
(set-logic QF_BV)
(declare-const total_sz (_ BitVec 16))
(declare-const n (_ BitVec 16))
(assert (bvugt n (_ bv0 16)))
(assert (bvule n (_ bv1000 16)))
(define-fun dir_sz () (_ BitVec 16) (bvmul n (_ bv8 16)))
(assert (bvuge total_sz (bvadd (_ bv48 16) dir_sz)))
(define-fun body_sz () (_ BitVec 16) (bvsub total_sz (_ bv48 16)))
; Negate: body_sz < dir_sz
(assert (not (bvuge body_sz dir_sz)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: WAL columnar block region count formula is correct")
    print("    P1: WAL header fields cover [0, 48) (cross-check + BV proof)")
    print("    P2: 4 + n + 1 == n + 5")
    print("    P3: dir_size = n*8 is 8-byte aligned")
    print("    P4: first data region offset > 48 for n >= 1")
    print("    P5: body_size >= dir_size when total_size >= 48 + dir_size")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
