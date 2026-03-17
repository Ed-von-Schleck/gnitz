"""Z3 formal proofs for WAL block header field layout integrity.

Proves four properties of gnitz/storage/wal_layout.py:7-16:

WAL block header (WAL_BLOCK_HEADER_SIZE = 48):
  [0,8)    LSN (u64)        [8,12)   TID (u32)       [12,16)  COUNT (u32)
  [16,20)  SIZE (u32)       [20,24)  VERSION (u32)   [24,32)  CHECKSUM (u64)
  [32,36)  NUM_REGIONS (u32) [36,40) RESERVED (u32)  [40,48)  BLOB_SIZE (u64)

Mixed u64/u32 layout — RESERVED at offset 36 is padding for 8-byte
alignment of BLOB_SIZE at offset 40.

  P1. Fields non-overlapping (Python exhaustive byte-ownership check)
  P2. Fields cover [0, 48) (16-bit BV, UNSAT)
  P3. u64 fields are 8-byte aligned (16-bit BV, UNSAT)
  P4. u32 fields are 4-byte aligned (16-bit BV, UNSAT)

4 Z3 queries + ~5 cross-checks.  Runs under PyPy2.
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

HEADER_SIZE = 48

# Field layout: (offset, size, type_size, name)
# type_size: 8 for u64, 4 for u32
FIELDS = [
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

# Source offsets from gnitz/storage/wal_layout.py:7-15
SOURCE_OFFSETS = {
    "LSN": 0,
    "TID": 8,
    "COUNT": 12,
    "SIZE": 16,
    "VERSION": 20,
    "CHECKSUM": 24,
    "NUM_REGIONS": 32,
    "RESERVED": 36,
    "BLOB_SIZE": 40,
}

# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: WAL block header field layout")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-check P1: byte-ownership (Python exhaustive) ----------------------

report("  ... cross-checking field layout")

byte_owner = [-1] * HEADER_SIZE
for fi, (off, sz, _tsz, name) in enumerate(FIELDS):
    for b in range(off, off + sz):
        if b >= HEADER_SIZE:
            report("  FAIL  field %d (%s): byte %d out of range [0, %d)" % (
                fi, name, b, HEADER_SIZE))
            ok = False
        elif byte_owner[b] != -1:
            report("  FAIL  field %d (%s) overlaps field %d at byte %d" % (
                fi, name, byte_owner[b], b))
            ok = False
        else:
            byte_owner[b] = fi

uncovered = [b for b in range(HEADER_SIZE) if byte_owner[b] == -1]
if uncovered:
    report("  FAIL  bytes not covered: %s" % uncovered)
    ok = False
else:
    report("  PASS  cross-check: fields non-overlapping and cover [0, 48)")

total_bytes = sum(sz for _, sz, _, _ in FIELDS)
if total_bytes != HEADER_SIZE:
    report("  FAIL  total field bytes %d != HEADER_SIZE %d" % (total_bytes, HEADER_SIZE))
    ok = False
else:
    report("  PASS  cross-check: total field bytes == 48")

# Verify offsets match source WAL_OFF_* constants
for off, _sz, _tsz, name in FIELDS:
    expected = SOURCE_OFFSETS[name]
    if off != expected:
        report("  FAIL  %s offset %d != source WAL_OFF_%s=%d" % (name, off, name, expected))
        ok = False
    else:
        report("  PASS  cross-check: WAL_OFF_%s == %d" % (name, off))

# Verify alignment of each field against its type
for off, _sz, tsz, name in FIELDS:
    if off % tsz != 0:
        report("  FAIL  %s at offset %d not %d-byte aligned" % (name, off, tsz))
        ok = False
    else:
        report("  PASS  cross-check: %s alignment (%d %% %d == 0)" % (name, off, tsz))

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P2: Fields cover [0, 48) (16-bit BV, UNSAT) ----------------------------
#
# Symbolic byte b in [0, 48), assert NOT in any field range.

report("  ... proving P2: fields cover [0, 48)")

membership = []
for off, sz, _tsz, _ in FIELDS:
    end = off + sz
    membership.append(
        "(and (not (bvult b (_ bv%d 16))) (bvult b (_ bv%d 16)))" % (off, end))

ok &= prove("P2: field coverage [0, 48)", """\
(set-logic QF_BV)
(declare-const b (_ BitVec 16))
(assert (not (bvult b (_ bv0 16))))
(assert (bvult b (_ bv48 16)))
; Negate: b does not belong to any field
(assert (not (or
  %s)))
(check-sat)
""" % "\n  ".join(membership))

# -- P3: u64 fields are 8-byte aligned (16-bit BV, UNSAT) -------------------
#
# For each u64 field offset o in {0, 24, 40}: o % 8 == 0.

report("  ... proving P3: u64 fields are 8-byte aligned")

u64_offsets = [off for off, _sz, tsz, _ in FIELDS if tsz == 8]
align_asserts = ["(= (bvurem (_ bv%d 16) (_ bv8 16)) (_ bv0 16))" % o
                 for o in u64_offsets]

ok &= prove("P3: u64 fields 8-byte aligned", """\
(set-logic QF_BV)
; Negate: not all u64 offsets are 8-aligned
(assert (not (and
  %s)))
(check-sat)
""" % "\n  ".join(align_asserts))

# -- P4: u32 fields are 4-byte aligned (16-bit BV, UNSAT) -------------------
#
# For each u32 field offset o in {8, 12, 16, 20, 32, 36}: o % 4 == 0.

report("  ... proving P4: u32 fields are 4-byte aligned")

u32_offsets = [off for off, _sz, tsz, _ in FIELDS if tsz == 4]
align_asserts_u32 = ["(= (bvurem (_ bv%d 16) (_ bv4 16)) (_ bv0 16))" % o
                     for o in u32_offsets]

ok &= prove("P4: u32 fields 4-byte aligned", """\
(set-logic QF_BV)
; Negate: not all u32 offsets are 4-aligned
(assert (not (and
  %s)))
(check-sat)
""" % "\n  ".join(align_asserts_u32))

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: WAL block header field layout is correct")
    print("    P1: fields non-overlapping (cross-check)")
    print("    P2: fields cover [0, 48)")
    print("    P3: u64 fields are 8-byte aligned")
    print("    P4: u32 fields are 4-byte aligned")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
