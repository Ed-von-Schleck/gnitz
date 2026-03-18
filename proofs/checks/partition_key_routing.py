"""Z3 formal proofs for _partition_for_key bitmask output range.

Proves three properties of gnitz/storage/partitioned_table.py lines 60-63:
  P1. r_uint64(h) & r_uint64(0xFF) <= 255 for all 64-bit h
  P2. intmask(x) is non-negative when x <= 255 (bit 31 is 0)
  P3. Full end-to-end: intmask(h & 0xFF) in [0, 255] for all 64-bit h

Source formula:
    def _partition_for_key(pk_lo, pk_hi):
        h = xxh.hash_u128_inline(pk_lo, pk_hi)
        return intmask(r_uint64(h) & r_uint64(0xFF))

h is treated as an arbitrary 64-bit value (xxh output range).  The AND mask
guarantees the result fits in 8 bits; intmask reinterprets as signed 32-bit
but 255 < 2^31 so the sign bit is never set.

3 Z3 queries + 7 cross-checks.  Runs under PyPy2.
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

MASK64 = (1 << 64) - 1


# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: _partition_for_key bitmask output range [0, 255]")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-check: validate Z3 encoding against RPython -----------------------

report("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_uint64, intmask  # noqa: E402
report("  ... imports done")

report("  ... cross-checking _partition_for_key formula against RPython")


def rpy_partition_formula(h):
    """Apply intmask(r_uint64(h) & r_uint64(0xFF)) as in partitioned_table.py:63."""
    return intmask(r_uint64(h) & r_uint64(0xFF))


# 7 test vectors: 0, 1, 255, 256, mixed-byte, all-high-bits, arbitrary
h_vectors = [
    0,
    1,
    255,
    256,
    0xFF0F,
    0xFFFFFFFFFFFFFF00,
    0xABCDEF0123456789,
]

cross_ok = True
for h_val in h_vectors:
    py_result = rpy_partition_formula(h_val)
    # Validate via Z3 simplify
    smt = "(simplify ((_ extract 31 0) (bvand (_ bv%d 64) #x00000000000000ff)))" % (h_val & MASK64)
    z3_raw = run_z3(smt)
    z3_result = parse_z3_value(z3_raw)
    if z3_result is None:
        report("  FAIL  cross-check h=0x%x: could not parse Z3 output %r" % (h_val, z3_raw))
        ok = False
        cross_ok = False
        continue
    if not (0 <= py_result <= 255):
        report("  FAIL  cross-check h=0x%x: RPython result %d out of [0,255]" % (h_val, py_result))
        ok = False
        cross_ok = False
        continue
    if py_result != z3_result:
        report("  FAIL  cross-check h=0x%x: RPython=%d Z3=%d mismatch" % (h_val, py_result, z3_result))
        ok = False
        cross_ok = False
    else:
        report("  PASS  cross-check h=0x%016x -> %d" % (h_val & MASK64, py_result))

if not cross_ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch — aborting proof phase")
    print("=" * 60)
    sys.exit(1)

# -- P1: AND with 0xFF produces value <= 255 (64-bit BV) ---------------------
#
# r_uint64(h) & r_uint64(0xFF) can only clear bits relative to h; the mask
# 0xFF has no bits set above bit 7, so the result is always <= 255.

report("  ... proving P1: (h & 0xFF) <= 255 for all 64-bit h")
ok &= prove("P1: r_uint64(h) & 0xFF <= 255 for all 64-bit h", """\
(set-logic QF_BV)
(declare-const h (_ BitVec 64))
(assert (not (bvule (bvand h #x00000000000000ff) (_ bv255 64))))
(check-sat)
""")

# -- P2: intmask(x) is non-negative when x <= 255 ----------------------------
#
# intmask reinterprets a 64-bit unsigned as a signed 32-bit value by taking
# the low 32 bits.  When x <= 255, bit 31 of x is 0 (255 = 0xFF < 2^31), so
# the extracted 32-bit value is non-negative as a signed integer.

report("  ... proving P2: intmask(x) >= 0 when x <= 255")
ok &= prove("P2: intmask(x) >= 0 as signed-32 when x <= 255 (64-bit input)", """\
(set-logic QF_BV)
(declare-const x (_ BitVec 64))
(assert (bvule x (_ bv255 64)))
(assert (not (bvsge ((_ extract 31 0) x) (_ bv0 32))))
(check-sat)
""")

# -- P3: Full end-to-end: intmask(h & 0xFF) in [0, 255] ----------------------
#
# Combines P1 (mask clears high bits) and P2 (sign bit is 0 for small values)
# to prove the complete formula always returns a value in [0, 255].

report("  ... proving P3: intmask(h & 0xFF) in [0, 255] for all 64-bit h")
ok &= prove("P3: intmask(h & 0xFF) in [0, 255] for all 64-bit h", """\
(set-logic QF_BV)
(declare-const h (_ BitVec 64))
(define-fun masked () (_ BitVec 64) (bvand h #x00000000000000ff))
(define-fun result () (_ BitVec 32) ((_ extract 31 0) masked))
(assert (not (and (bvsge result (_ bv0 32)) (bvsle result (_ bv255 32)))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: _partition_for_key bitmask output range [0, 255]")
    print("    P1: r_uint64(h) & 0xFF <= 255 for all 64-bit h")
    print("    P2: intmask(x) >= 0 as signed-32 when x <= 255")
    print("    P3: intmask(h & 0xFF) in [0, 255] for all 64-bit h")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
