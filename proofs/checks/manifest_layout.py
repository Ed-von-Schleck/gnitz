"""Z3 formal proofs for manifest entry/header field layout integrity.

Proves five properties of gnitz/storage/manifest.py:14-92:

Entry layout (ENTRY_SIZE = 184):
  [0,8)    table_id       [8,16)   pk_min_lo     [16,24)  pk_min_hi
  [24,32)  pk_max_lo      [32,40)  pk_max_hi     [40,48)  min_lsn
  [48,56)  max_lsn        [56,184) filename (128 bytes)

Header layout (HEADER_SIZE = 64):
  [0,8)    magic          [8,16)   version       [16,24)  entry_count
  [24,32)  global_max_lsn [32,64)  padding (32 bytes)

  P1. Entry fields non-overlapping (Python exhaustive byte-ownership check)
  P2. Entry fields cover [0, 184) (16-bit BV, UNSAT)
  P3. Header fields cover [0, 64) (16-bit BV, UNSAT)
  P4. U128 split/reconstruct roundtrip (128-bit BV, UNSAT)
  P5. Filename region safety: 56 + 128 == 184 (16-bit BV, UNSAT)

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


# -- Constants ----------------------------------------------------------------

ENTRY_SIZE = 184
HEADER_SIZE = 64

# Entry field layout: (offset, size, name)
ENTRY_FIELDS = [
    (0, 8, "table_id"),
    (8, 8, "pk_min_lo"),
    (16, 8, "pk_min_hi"),
    (24, 8, "pk_max_lo"),
    (32, 8, "pk_max_hi"),
    (40, 8, "min_lsn"),
    (48, 8, "max_lsn"),
    (56, 128, "filename"),
]

# Header field layout: (offset, size, name)
HEADER_FIELDS = [
    (0, 8, "magic"),
    (8, 8, "version"),
    (16, 8, "entry_count"),
    (24, 8, "global_max_lsn"),
    (32, 32, "padding"),
]

MASK64 = (1 << 64) - 1

# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: Manifest entry/header field layout")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-check P1: Entry field byte-ownership (Python exhaustive) -----------

report("  ... cross-checking entry field layout")

byte_owner = [-1] * ENTRY_SIZE
for fi, (off, sz, name) in enumerate(ENTRY_FIELDS):
    for b in range(off, off + sz):
        if b >= ENTRY_SIZE:
            report("  FAIL  entry field %d (%s): byte %d out of range [0, %d)" % (
                fi, name, b, ENTRY_SIZE))
            ok = False
        elif byte_owner[b] != -1:
            report("  FAIL  entry field %d (%s) overlaps field %d at byte %d" % (
                fi, name, byte_owner[b], b))
            ok = False
        else:
            byte_owner[b] = fi

uncovered_entry = [b for b in range(ENTRY_SIZE) if byte_owner[b] == -1]
if uncovered_entry:
    report("  FAIL  entry bytes not covered: %s" % uncovered_entry)
    ok = False
else:
    report("  PASS  cross-check: entry fields non-overlapping and cover [0, 184)")

total_entry_bytes = sum(sz for _, sz, _ in ENTRY_FIELDS)
if total_entry_bytes != ENTRY_SIZE:
    report("  FAIL  total entry bytes %d != ENTRY_SIZE %d" % (total_entry_bytes, ENTRY_SIZE))
    ok = False
else:
    report("  PASS  cross-check: total entry bytes == 184")

# Header byte-ownership
byte_owner_h = [-1] * HEADER_SIZE
for fi, (off, sz, name) in enumerate(HEADER_FIELDS):
    for b in range(off, off + sz):
        if b >= HEADER_SIZE:
            report("  FAIL  header field %d (%s): byte %d out of range [0, %d)" % (
                fi, name, b, HEADER_SIZE))
            ok = False
        elif byte_owner_h[b] != -1:
            report("  FAIL  header field %d (%s) overlaps field %d at byte %d" % (
                fi, name, byte_owner_h[b], b))
            ok = False
        else:
            byte_owner_h[b] = fi

uncovered_header = [b for b in range(HEADER_SIZE) if byte_owner_h[b] == -1]
if uncovered_header:
    report("  FAIL  header bytes not covered: %s" % uncovered_header)
    ok = False
else:
    report("  PASS  cross-check: header fields non-overlapping and cover [0, 64)")

total_header_bytes = sum(sz for _, sz, _ in HEADER_FIELDS)
if total_header_bytes != HEADER_SIZE:
    report("  FAIL  total header bytes %d != HEADER_SIZE %d" % (total_header_bytes, HEADER_SIZE))
    ok = False
else:
    report("  PASS  cross-check: total header bytes == 64")

# U128 roundtrip cross-check
print("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_uint64  # noqa: E402
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128  # noqa: E402
print("  ... imports done")
sys.stdout.flush()

u128_vectors = [
    0,
    1,
    (1 << 64) - 1,
    (1 << 128) - 1,
    0xDEADBEEFCAFEBABE0000000012345678,
]

for val in u128_vectors:
    v = r_uint128(val)
    lo = r_uint64(v)
    hi = r_uint64(v >> 64)
    reconstructed = (r_uint128(hi) << 64) | r_uint128(lo)
    rpy_ok = (int(reconstructed) == int(v))

    if rpy_ok:
        report("  PASS  cross-check U128 roundtrip(0x%032x)" % (int(v)))
    else:
        report("  FAIL  cross-check U128 roundtrip(0x%032x): got 0x%032x" % (
            int(v), int(reconstructed)))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P2: Entry fields cover [0, 184) (16-bit BV, UNSAT) ----------------------
#
# Symbolic byte b in [0, 184), assert NOT in any field range.

report("  ... proving P2: entry fields cover [0, 184)")

entry_membership = []
for off, sz, _ in ENTRY_FIELDS:
    end = off + sz
    entry_membership.append(
        "(and (not (bvult b (_ bv%d 16))) (bvult b (_ bv%d 16)))" % (off, end))

ok &= prove("P2: entry coverage [0, 184)", """\
(set-logic QF_BV)
(declare-const b (_ BitVec 16))
(assert (not (bvult b (_ bv0 16))))
(assert (bvult b (_ bv184 16)))
; Negate: b does not belong to any field
(assert (not (or
  %s)))
(check-sat)
""" % "\n  ".join(entry_membership))

# -- P3: Header fields cover [0, 64) (16-bit BV, UNSAT) ----------------------
#
# Same pattern for header: 4 data fields + padding region [32, 64).

report("  ... proving P3: header fields cover [0, 64)")

header_membership = []
for off, sz, _ in HEADER_FIELDS:
    end = off + sz
    header_membership.append(
        "(and (not (bvult b (_ bv%d 16))) (bvult b (_ bv%d 16)))" % (off, end))

ok &= prove("P3: header coverage [0, 64)", """\
(set-logic QF_BV)
(declare-const b (_ BitVec 16))
(assert (not (bvult b (_ bv0 16))))
(assert (bvult b (_ bv64 16)))
; Negate: b does not belong to any field
(assert (not (or
  %s)))
(check-sat)
""" % "\n  ".join(header_membership))

# -- P4: U128 split/reconstruct roundtrip (128-bit BV, UNSAT) ----------------
#
# val = (hi << 64) | lo where lo = extract[63:0](val), hi = extract[127:64](val)
# Used in _read_manifest_entry to reconstruct pk_min/pk_max from lo/hi halves.

report("  ... proving P4: U128 split/reconstruct roundtrip")
ok &= prove("P4: U128 roundtrip", """\
(set-logic QF_BV)
(declare-const val (_ BitVec 128))
(define-fun lo () (_ BitVec 64) ((_ extract 63 0) val))
(define-fun hi () (_ BitVec 64) ((_ extract 127 64) val))
(define-fun reconstructed () (_ BitVec 128)
  (bvor (bvshl ((_ zero_extend 64) hi) (_ bv64 128))
        ((_ zero_extend 64) lo)))
(assert (not (= reconstructed val)))
(check-sat)
""")

# -- P5: Filename region safety (16-bit BV, UNSAT) ---------------------------
#
# Filename starts at offset 56 and is 128 bytes: 56 + 128 == 184 == ENTRY_SIZE.
# No buffer overrun when writing/reading the filename field.

report("  ... proving P5: filename region safety")
ok &= prove("P5: 56 + 128 == 184 (ENTRY_SIZE)", """\
(set-logic QF_BV)
(declare-const entry_size (_ BitVec 16))
(assert (= entry_size (_ bv184 16)))
; Negate: filename end != entry_size
(assert (not (= (bvadd (_ bv56 16) (_ bv128 16)) entry_size)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: Manifest field layout is correct")
    print("    P1: entry fields non-overlapping (cross-check)")
    print("    P2: entry fields cover [0, 184)")
    print("    P3: header fields cover [0, 64)")
    print("    P4: U128 split/reconstruct roundtrip")
    print("    P5: filename region safety (56 + 128 == 184)")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
