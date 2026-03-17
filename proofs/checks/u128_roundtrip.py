"""Z3 formal proofs for write/read u128 serialization roundtrip.

Proves three properties of the u128 serialization in gnitz:
  P1. Roundtrip: read(write(val)) == val
  P2. Lo extraction: extract[63:0](val) == lo half
  P3. Hi extraction: extract[127:64](val) == hi half

Source functions:
  - gnitz/storage/wal_layout.py:87-90  write_u128(buf, offset, val)
  - gnitz/storage/wal_layout.py:93-95  read_u128(buf, offset)
  - gnitz/core/batch.py:505-509        _read_col_u128 (same reconstruction)

Write path: lo = intmask(val) & MASK64, hi = intmask(val >> 64) & MASK64
Read path: (r_uint128(hi) << 64) | r_uint128(lo)

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


def fmt128(n):
    return "0x%032x" % (n & ((1 << 128) - 1))


# -- SMT preamble (shared across P1-P3) --------------------------------------
# 128-bit BVs. Only extract/shift/or -- essentially free.

U128_PREAMBLE = """\
(set-logic QF_BV)
(declare-const val (_ BitVec 128))
; write path: split into lo and hi 64-bit halves
(define-fun lo () (_ BitVec 64) ((_ extract 63 0) val))
(define-fun hi () (_ BitVec 64) ((_ extract 127 64) val))
; read path: reconstruct from halves
(define-fun reconstructed () (_ BitVec 128)
  (bvor (bvshl ((_ zero_extend 64) hi) (_ bv64 128))
        ((_ zero_extend 64) lo)))
"""


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: u128 write/read roundtrip")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-check: validate Z3 encoding against RPython -----------------------

report("  ... cross-checking u128 roundtrip against RPython")

print("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128, r_uint64, intmask  # noqa: E402
print("  ... imports done")
sys.stdout.flush()

MASK64 = (1 << 64) - 1
MASK128 = (1 << 128) - 1

cross_check_vectors = [
    0,
    1,
    (1 << 64) - 1,
    1 << 64,
    1 << 127,
    (1 << 128) - 1,
    0xDEADBEEFCAFEBABE123456789ABCDEF0,
]

for val_int in cross_check_vectors:
    val_int = val_int & MASK128

    # RPython write path: lo = intmask(val) & MASK64, hi = intmask(val >> 64) & MASK64
    val = r_uint128(val_int)
    lo = int(r_uint64(intmask(val))) & MASK64
    hi = int(r_uint64(intmask(val >> 64))) & MASK64

    # RPython read path: reconstruct
    rpython_reconstructed = int((r_uint128(hi) << 64) | r_uint128(lo)) & MASK128

    # Z3 computation: extract lo and hi, reconstruct
    smt_query = """\
(simplify (let ((lo ((_ extract 63 0) #x%032x))
                (hi ((_ extract 127 64) #x%032x)))
  (bvor (bvshl ((_ zero_extend 64) hi) (_ bv64 128))
        ((_ zero_extend 64) lo))))""" % (val_int, val_int)
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)

    if z3_val is None:
        report("  FAIL  cross-check u128(%s): unexpected Z3 output: %s" % (fmt128(val_int), z3_out))
        ok = False
        continue

    if z3_val == rpython_reconstructed == val_int:
        report("  PASS  cross-check u128(%s) roundtrip OK" % fmt128(val_int))
    else:
        report("  FAIL  cross-check u128(%s): RPython=%s Z3=%s" % (
            fmt128(val_int), fmt128(rpython_reconstructed), fmt128(z3_val)))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: Roundtrip -------------------------------------------------------------
#
# reconstructed == val for all 128-bit values.

report("  ... proving P1: roundtrip")
ok &= prove("P1: read(write(val)) == val", U128_PREAMBLE + """\
; Negate: assert reconstructed != val
(assert (not (= reconstructed val)))
(check-sat)
""")

# -- P2: Lo extraction --------------------------------------------------------
#
# extract[63:0](val) captures exactly the low 64 bits.
# Equivalent to: (val & MASK64) truncated to 64 bits == lo.

report("  ... proving P2: lo extraction")
ok &= prove("P2: extract[63:0](val & MASK64) == lo", U128_PREAMBLE + """\
; MASK64 as 128-bit: 0x0000000000000000FFFFFFFFFFFFFFFF
(define-fun mask64_128 () (_ BitVec 128)
  #x0000000000000000ffffffffffffffff)
; Negate: assert (extract[63:0](val & mask64)) != lo
(assert (not (= ((_ extract 63 0) (bvand val mask64_128)) lo)))
(check-sat)
""")

# -- P3: Hi extraction --------------------------------------------------------
#
# extract[63:0](val >> 64) == hi.

report("  ... proving P3: hi extraction")
ok &= prove("P3: extract[63:0](val >> 64) == hi", U128_PREAMBLE + """\
; Negate: assert extract[63:0](val >> 64) != hi
(assert (not (= ((_ extract 63 0) (bvlshr val (_ bv64 128))) hi)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: u128 serialization roundtrip is correct")
    print("    P1: read(write(val)) == val for all 128-bit values")
    print("    P2: lo extraction captures low 64 bits")
    print("    P3: hi extraction captures high 64 bits")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
