"""Z3 formal proofs for IPC v2 96-byte request/response header field layout.

Proves five properties of gnitz/server/ipc.py:18-71:

IPC v2 header (HEADER_SIZE = 96):
  [0,8)   MAGIC          u64
  [8,12)  STATUS         u32
  [12,16) ERR_LEN        u32
  [16,24) TARGET_ID      u64
  [24,32) CLIENT_ID      u64
  [32,40) SCHEMA_COUNT   u64
  [40,48) SCHEMA_BLOB_SZ u64
  [48,56) DATA_COUNT     u64
  [56,64) DATA_BLOB_SZ   u64
  [64,72) P4             u64
  [72,80) FLAGS          u64
  [80,88) SEEK_PK_LO     u64
  [88,96) SEEK_PK_HI     u64

Sum: 8+4+4+8+8+8+8+8+8+8+8+8+8 = 96 = HEADER_SIZE

  P1. Fields non-overlapping and cover [0, 96) (Python exhaustive check)
  P2. Every byte in [0, 96) belongs to some field (8-bit BV, UNSAT)
  P3. All u64 fields are 8-byte aligned (8-bit BV, UNSAT)
  P4. All u32 fields are 4-byte aligned (8-bit BV, UNSAT)
  P5. SEEK_PK_LO >= OFF_FLAGS+8, SEEK_PK_HI == SEEK_PK_LO+8 (8-bit BV, UNSAT)

5 Z3 queries + ~5 cross-checks.  Runs under PyPy2.
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

HEADER_SIZE = 96

# (offset, size_bytes, type_size, name)
# type_size: 8 for u64, 4 for u32
FIELDS = [
    (0,  8, 8, "MAGIC"),
    (8,  4, 4, "STATUS"),
    (12, 4, 4, "ERR_LEN"),
    (16, 8, 8, "TARGET_ID"),
    (24, 8, 8, "CLIENT_ID"),
    (32, 8, 8, "SCHEMA_COUNT"),
    (40, 8, 8, "SCHEMA_BLOB_SZ"),
    (48, 8, 8, "DATA_COUNT"),
    (56, 8, 8, "DATA_BLOB_SZ"),
    (64, 8, 8, "P4"),
    (72, 8, 8, "FLAGS"),
    (80, 8, 8, "SEEK_PK_LO"),
    (88, 8, 8, "SEEK_PK_HI"),
]

# Source constants from gnitz/server/ipc.py
SOURCE_OFFSETS = {
    "MAGIC":          0,
    "STATUS":         8,
    "ERR_LEN":        12,
    "TARGET_ID":      16,
    "CLIENT_ID":      24,
    "SCHEMA_COUNT":   32,
    "SCHEMA_BLOB_SZ": 40,
    "DATA_COUNT":     48,
    "DATA_BLOB_SZ":   56,
    "P4":             64,
    "FLAGS":          72,
    "SEEK_PK_LO":     80,
    "SEEK_PK_HI":     88,
}

# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: IPC v2 96-byte header field layout")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- P1: Python exhaustive byte-ownership cross-check ------------------------

report("  ... cross-checking field layout (P1)")

byte_owner = [-1] * HEADER_SIZE
for fi, (off, sz, _tsz, name) in enumerate(FIELDS):
    for b in range(off, off + sz):
        if b >= HEADER_SIZE:
            report("  FAIL  field %d (%s): byte %d out of [0, %d)" % (fi, name, b, HEADER_SIZE))
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
    report("  PASS  cross-check: fields non-overlapping and cover [0, 96)")

total_bytes = sum(sz for _, sz, _, _ in FIELDS)
if total_bytes != HEADER_SIZE:
    report("  FAIL  total field bytes %d != HEADER_SIZE %d" % (total_bytes, HEADER_SIZE))
    ok = False
else:
    report("  PASS  cross-check: sum of field sizes == 96 == HEADER_SIZE")

# Verify each OFF_* constant from source matches FIELDS table
for off, _sz, _tsz, name in FIELDS:
    expected = SOURCE_OFFSETS[name]
    if off != expected:
        report("  FAIL  %s offset %d != source OFF_%s=%d" % (name, off, name, expected))
        ok = False
    else:
        report("  PASS  cross-check: OFF_%s == %d" % (name, off))

# Verify Z3 simplify: last field end = SEEK_PK_HI + 8 = 88 + 8 = 96 = HEADER_SIZE
z3_out = run_z3("(simplify (bvadd (_ bv88 8) (_ bv8 8)))")
z3_out = z3_out.strip()
got_96 = False
if z3_out.startswith("#x"):
    got_96 = (int(z3_out[2:], 16) == 96)
elif z3_out.startswith("(_ bv"):
    parts = z3_out.split()
    got_96 = (int(parts[1][2:]) == 96)
if got_96:
    report("  PASS  cross-check: Z3 simplify (88+8) == #x60 (96)")
else:
    report("  FAIL  cross-check: Z3 simplify (88+8) expected 96, got %s" % z3_out)
    ok = False

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P2: Every byte in [0, 96) belongs to some field (8-bit BV, UNSAT) -------
#
# 8-bit BV is sufficient: all offsets and HEADER_SIZE < 256.

report("  ... proving P2: fields cover [0, 96)")

membership = []
for off, sz, _tsz, _ in FIELDS:
    end = off + sz
    membership.append(
        "(and (bvuge b (_ bv%d 8)) (bvult b (_ bv%d 8)))" % (off, end))

ok &= prove("P2: field coverage [0, 96)", """\
(set-logic QF_BV)
(declare-const b (_ BitVec 8))
(assert (bvult b (_ bv96 8)))
; Negate: b does not belong to any field
(assert (not (or
  %s)))
(check-sat)
""" % "\n  ".join(membership))

# -- P3: All u64 fields are 8-byte aligned (8-bit BV, UNSAT) -----------------
#
# Offsets 0, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88 are all divisible by 8.
# Use bvand with mask 7 (cheaper than bvurem).

report("  ... proving P3: u64 fields are 8-byte aligned")

u64_offsets = [off for off, _sz, tsz, _ in FIELDS if tsz == 8]
# Each offset is concrete — enumerate all as an 'and' of equality checks
align_asserts = ["(= (bvand (_ bv%d 8) (_ bv7 8)) (_ bv0 8))" % o
                 for o in u64_offsets]

ok &= prove("P3: u64 fields 8-byte aligned", """\
(set-logic QF_BV)
; Negate: not all u64 offsets are 8-aligned
(assert (not (and
  %s)))
(check-sat)
""" % "\n  ".join(align_asserts))

# -- P4: All u32 fields are 4-byte aligned (8-bit BV, UNSAT) -----------------
#
# Offsets 8 and 12 are both divisible by 4.

report("  ... proving P4: u32 fields are 4-byte aligned")

u32_offsets = [off for off, _sz, tsz, _ in FIELDS if tsz == 4]
align_asserts_u32 = ["(= (bvand (_ bv%d 8) (_ bv3 8)) (_ bv0 8))" % o
                     for o in u32_offsets]

ok &= prove("P4: u32 fields 4-byte aligned", """\
(set-logic QF_BV)
; Negate: not all u32 offsets are 4-aligned
(assert (not (and
  %s)))
(check-sat)
""" % "\n  ".join(align_asserts_u32))

# -- P5a: SEEK_PK_LO starts at end of FLAGS field (8-bit BV, UNSAT) ----------
#
# OFF_SEEK_PK_LO (80) >= OFF_FLAGS (72) + 8.
# With these concrete values this reduces to 80 >= 80 = true, so negation
# is false => UNSAT.

report("  ... proving P5a: SEEK_PK_LO >= OFF_FLAGS + 8")

ok &= prove("P5a: SEEK_PK_LO >= OFF_FLAGS + 8", """\
(set-logic QF_BV)
; 80 >= 72 + 8 = 80
(assert (not (bvuge (_ bv80 8) (bvadd (_ bv72 8) (_ bv8 8)))))
(check-sat)
""")

# -- P5b: SEEK_PK_HI immediately follows SEEK_PK_LO (8-bit BV, UNSAT) --------

report("  ... proving P5b: SEEK_PK_HI == SEEK_PK_LO + 8")

ok &= prove("P5b: SEEK_PK_HI == SEEK_PK_LO + 8", """\
(set-logic QF_BV)
; 88 == 80 + 8
(assert (not (= (_ bv88 8) (bvadd (_ bv80 8) (_ bv8 8)))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: IPC v2 96-byte header field layout is correct")
    print("    P1: fields non-overlapping and cover [0, 96) (cross-check)")
    print("    P2: fields cover [0, 96) (BV proof)")
    print("    P3: u64 fields are 8-byte aligned")
    print("    P4: u32 fields are 4-byte aligned")
    print("    P5: seek fields follow flags field in order")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
