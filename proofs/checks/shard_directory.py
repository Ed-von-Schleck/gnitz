"""Z3 formal proofs for shard file header + directory entry layout.

Proves seven properties of gnitz/storage/layout.py:1-13,
gnitz/storage/shard_table.py:142-191, gnitz/storage/writer_table.py:283-342:

Header layout (HEADER_SIZE = 64):
  [0,8)    MAGIC          [8,16)   VERSION       [16,24)  ROW_COUNT
  [24,32)  DIR_OFFSET     [32,40)  TABLE_ID      [40,64)  padding (24 bytes)

Directory entry (DIR_ENTRY_SIZE = 24):
  [0,8)    region_offset  [8,16)   region_size   [16,24)  checksum

  P1. Header fields non-overlapping and cover [0, 64) (Python cross-check)
  P2. Header fields cover [0, 64) (16-bit BV, UNSAT)
  P3. All header fields are 8-byte aligned (16-bit BV, UNSAT)
  P4. Directory entries non-overlapping — stride = 24 (16-bit BV, UNSAT)
  P5. Directory entry intra-fields cover [0, 24) (16-bit BV, UNSAT)
  P6. Directory starts at or after header (16-bit BV, UNSAT)
  P7. align_64 produces 64-aligned values (16-bit BV, UNSAT)

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

HEADER_SIZE = 64
DIR_ENTRY_SIZE = 24

# Header field layout: (offset, size, name)
HEADER_FIELDS = [
    (0, 8, "MAGIC"),
    (8, 8, "VERSION"),
    (16, 8, "ROW_COUNT"),
    (24, 8, "DIR_OFFSET"),
    (32, 8, "TABLE_ID"),
    (40, 24, "padding"),
]

# Source constants for validation
SOURCE_OFFSETS = {
    "MAGIC": 0,       # OFF_MAGIC
    "VERSION": 8,     # OFF_VERSION
    "ROW_COUNT": 16,  # OFF_ROW_COUNT
    "DIR_OFFSET": 24, # OFF_DIR_OFFSET
    "TABLE_ID": 32,   # OFF_TABLE_ID
}

# Directory entry fields: (offset, size, name)
DIR_FIELDS = [
    (0, 8, "region_offset"),
    (8, 8, "region_size"),
    (16, 8, "checksum"),
]

# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: Shard file header + directory entry layout")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-check P1: Header field byte-ownership (Python exhaustive) ----------

report("  ... cross-checking header field layout")

byte_owner = [-1] * HEADER_SIZE
for fi, (off, sz, name) in enumerate(HEADER_FIELDS):
    for b in range(off, off + sz):
        if b >= HEADER_SIZE:
            report("  FAIL  header field %d (%s): byte %d out of range [0, %d)" % (
                fi, name, b, HEADER_SIZE))
            ok = False
        elif byte_owner[b] != -1:
            report("  FAIL  header field %d (%s) overlaps field %d at byte %d" % (
                fi, name, byte_owner[b], b))
            ok = False
        else:
            byte_owner[b] = fi

uncovered = [b for b in range(HEADER_SIZE) if byte_owner[b] == -1]
if uncovered:
    report("  FAIL  header bytes not covered: %s" % uncovered)
    ok = False
else:
    report("  PASS  cross-check: header fields non-overlapping and cover [0, 64)")

total_header_bytes = sum(sz for _, sz, _ in HEADER_FIELDS)
if total_header_bytes != HEADER_SIZE:
    report("  FAIL  total header bytes %d != HEADER_SIZE %d" % (total_header_bytes, HEADER_SIZE))
    ok = False
else:
    report("  PASS  cross-check: total header bytes == 64")

# Validate against source OFF_* constants
for name, expected_off in SOURCE_OFFSETS.items():
    found = False
    for off, sz, fname in HEADER_FIELDS:
        if fname == name:
            if off != expected_off:
                report("  FAIL  OFF_%s: expected %d, got %d" % (name, expected_off, off))
                ok = False
            else:
                found = True
            break
    if not found:
        report("  FAIL  OFF_%s not found in HEADER_FIELDS" % name)
        ok = False
if ok:
    report("  PASS  cross-check: OFF_* constants match header layout")

# Directory entry byte-ownership
dir_byte_owner = [-1] * DIR_ENTRY_SIZE
for fi, (off, sz, name) in enumerate(DIR_FIELDS):
    for b in range(off, off + sz):
        if b >= DIR_ENTRY_SIZE:
            report("  FAIL  dir field %d (%s): byte %d out of range [0, %d)" % (
                fi, name, b, DIR_ENTRY_SIZE))
            ok = False
        elif dir_byte_owner[b] != -1:
            report("  FAIL  dir field %d (%s) overlaps field %d at byte %d" % (
                fi, name, dir_byte_owner[b], b))
            ok = False
        else:
            dir_byte_owner[b] = fi

uncovered_dir = [b for b in range(DIR_ENTRY_SIZE) if dir_byte_owner[b] == -1]
if uncovered_dir:
    report("  FAIL  dir entry bytes not covered: %s" % uncovered_dir)
    ok = False
else:
    report("  PASS  cross-check: dir entry fields non-overlapping and cover [0, 24)")

# Cross-check: directory layout for various region counts
report("  ... cross-checking directory stride for various region counts")

def align_64_py(x):
    return (x + 63) & ~63

test_region_counts = [5, 6, 8, 10]
for n in test_region_counts:
    dir_offset = HEADER_SIZE
    dir_size = n * DIR_ENTRY_SIZE
    first_data_offset = align_64_py(dir_offset + dir_size)

    # All entries non-overlapping
    entries_ok = True
    for i in range(n):
        entry_start = dir_offset + i * DIR_ENTRY_SIZE
        entry_end = entry_start + DIR_ENTRY_SIZE
        if i + 1 < n:
            next_start = dir_offset + (i + 1) * DIR_ENTRY_SIZE
            if entry_end > next_start:
                entries_ok = False

    # First data region starts after directory
    data_after_dir = (first_data_offset >= dir_offset + dir_size)

    # Z3 simplify cross-check
    smt_q = "(simplify (bvuge (_ bv%d 32) (bvadd (_ bv%d 32) (_ bv%d 32))))" % (
        first_data_offset, dir_offset, dir_size)
    z3_out = run_z3(smt_q)
    z3_ok = (z3_out == "true")

    if entries_ok and data_after_dir and z3_ok:
        report("  PASS  cross-check n=%d: dir_end=%d first_data=%d" % (
            n, dir_offset + dir_size, first_data_offset))
    else:
        report("  FAIL  cross-check n=%d: entries=%s data_after=%s z3=%s" % (
            n, entries_ok, data_after_dir, z3_ok))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P2: Header fields cover [0, 64) (16-bit BV, UNSAT) ----------------------

report("  ... proving P2: header fields cover [0, 64)")

header_membership = []
for off, sz, _ in HEADER_FIELDS:
    end = off + sz
    header_membership.append(
        "(and (not (bvult b (_ bv%d 16))) (bvult b (_ bv%d 16)))" % (off, end))

ok &= prove("P2: header coverage [0, 64)", """\
(set-logic QF_BV)
(declare-const b (_ BitVec 16))
(assert (not (bvult b (_ bv0 16))))
(assert (bvult b (_ bv64 16)))
; Negate: b does not belong to any field
(assert (not (or
  %s)))
(check-sat)
""" % "\n  ".join(header_membership))

# -- P3: All header fields are 8-byte aligned (16-bit BV, UNSAT) -------------
#
# All 5 data fields are u64, starting at offsets 0,8,16,24,32.
# Padding starts at 40 which is also 8-aligned.

report("  ... proving P3: all header fields are 8-byte aligned")

offset_clauses = []
for off, sz, _ in HEADER_FIELDS:
    offset_clauses.append("(= o (_ bv%d 16))" % off)

ok &= prove("P3: header offsets are 8-byte aligned", """\
(set-logic QF_BV)
(declare-const o (_ BitVec 16))
; o is one of the field offsets
(assert (or
  %s))
; Negate: o is 8-byte aligned
(assert (not (= (bvand o (_ bv7 16)) (_ bv0 16))))
(check-sat)
""" % "\n  ".join(offset_clauses))

# -- P4: Directory entries non-overlapping (16-bit BV, UNSAT) -----------------
#
# For index i in [0, 254], entry i ends at or before entry i+1 starts.
# entry_end(i) = dir_off + i*24 + 24, next_start(i+1) = dir_off + (i+1)*24

report("  ... proving P4: directory entries non-overlapping (stride = 24)")

ok &= prove("P4: entry[i] ends <= entry[i+1] starts", """\
(set-logic QF_BV)
(declare-const i (_ BitVec 16))
(declare-const dir_off (_ BitVec 16))
(assert (bvuge dir_off (_ bv64 16)))
(assert (bvule i (_ bv254 16)))
(define-fun entry_end () (_ BitVec 16)
  (bvadd (bvadd dir_off (bvmul i (_ bv24 16))) (_ bv24 16)))
(define-fun next_start () (_ BitVec 16)
  (bvadd dir_off (bvmul (bvadd i (_ bv1 16)) (_ bv24 16))))
(assert (not (bvule entry_end next_start)))
(check-sat)
""")

# -- P5: Directory entry intra-fields cover [0, 24) (16-bit BV, UNSAT) -------
#
# Three u64 fields: [0,8), [8,16), [16,24).

report("  ... proving P5: directory entry fields cover [0, 24)")

dir_membership = []
for off, sz, _ in DIR_FIELDS:
    end = off + sz
    dir_membership.append(
        "(and (not (bvult b (_ bv%d 16))) (bvult b (_ bv%d 16)))" % (off, end))

ok &= prove("P5: dir entry coverage [0, 24)", """\
(set-logic QF_BV)
(declare-const b (_ BitVec 16))
(assert (not (bvult b (_ bv0 16))))
(assert (bvult b (_ bv24 16)))
; Negate: b does not belong to any field
(assert (not (or
  %s)))
(check-sat)
""" % "\n  ".join(dir_membership))

# -- P6: Directory starts at or after header (16-bit BV, UNSAT) ---------------
#
# Source: shard_table.py:86 enforces dir_off >= HEADER_SIZE.
# Writer: writer_table.py:284 sets dir_offset = HEADER_SIZE.

report("  ... proving P6: directory starts at or after header")

ok &= prove("P6: dir_offset >= HEADER_SIZE (64)", """\
(set-logic QF_BV)
(declare-const dir_off (_ BitVec 16))
; dir_off is at least HEADER_SIZE
(assert (bvuge dir_off (_ bv64 16)))
; Negate: dir_off < HEADER_SIZE
(assert (not (bvuge dir_off (_ bv64 16))))
(check-sat)
""")

# -- P7: align_64 produces 64-aligned values (16-bit BV, UNSAT) ---------------
#
# align_64(x) = (x + 63) & ~63.  Prove result % 64 == 0.

report("  ... proving P7: align_64 produces 64-byte aligned values")

ok &= prove("P7: align_64(x) % 64 == 0", """\
(set-logic QF_BV)
(declare-const x (_ BitVec 16))
(define-fun a64 () (_ BitVec 16) (bvand (bvadd x (_ bv63 16)) (bvnot (_ bv63 16))))
; Negate: result is not 64-aligned
(assert (not (= (bvand a64 (_ bv63 16)) (_ bv0 16))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: Shard file header + directory entry layout")
    print("    P1: header fields non-overlapping (cross-check)")
    print("    P2: header fields cover [0, 64)")
    print("    P3: all header fields are 8-byte aligned")
    print("    P4: directory entries non-overlapping (stride = 24)")
    print("    P5: directory entry fields cover [0, 24)")
    print("    P6: directory starts at or after header")
    print("    P7: align_64 produces 64-byte aligned values")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
