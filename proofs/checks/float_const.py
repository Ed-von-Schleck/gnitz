"""Z3 formal proofs for float constant encoding roundtrip & injectivity.

Proves four properties of the float encoding in gnitz/dbsp/expr.py:618-624
(encoder) and gnitz/dbsp/expr.py:142-144 (decoder):

  Encoder: bits = float2longlong(value); lo = intmask(bits); hi = intmask(bits >> 32)
  Decoder: regs[dst] = r_int64(intmask((a2 << 32) | (a1 & 0xFFFFFFFF)))

The split/join is identical to load_const_int. This script extends coverage
to float-specific concerns: the float2longlong/longlong2float bijection
(cross-check only -- Z3 can't model IEEE 754 natively), half-independence,
and encoding injectivity.

  P1. Split/join roundtrip (64-bit BV, UNSAT)
  P2a. Lo half independent of hi (64-bit BV, UNSAT)
  P2b. Hi half independent of lo (64-bit BV, UNSAT)
  P3. Encoding is injective (64-bit BV, UNSAT)

4 Z3 queries + ~10 cross-checks.  Runs under PyPy2.
Exit code 0 on success, 1 on any failure.
"""
import subprocess
import sys
import struct


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


def fmt64(n):
    return "0x%016x" % (n & 0xFFFFFFFFFFFFFFFF)


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: Float constant encoding roundtrip")
print("=" * 56)
sys.stdout.flush()

ok = True

MASK32 = 0xFFFFFFFF
MASK64 = (1 << 64) - 1

# -- Cross-check: float encode/decode against RPython ------------------------

report("  ... cross-checking float encode/decode against RPython")

print("  ... importing rpython.rlib")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_int64, intmask  # noqa: E402
from rpython.rlib.longlong2float import float2longlong, longlong2float  # noqa: E402
print("  ... imports done")
sys.stdout.flush()


def encode_float(value):
    """Replicate ExprBuilder.load_const_float encoder."""
    bits = float2longlong(value)
    lo = intmask(bits)
    hi = intmask(bits >> 32)
    return lo, hi


def decode_float(a1, a2):
    """Replicate eval_expr LOAD_CONST decoder + longlong2float."""
    bits = r_int64(intmask((a2 << 32) | (a1 & 0xFFFFFFFF)))
    return longlong2float(bits)


# Use struct to get NaN bit pattern portably
nan_bits = struct.unpack("q", struct.pack("d", float("nan")))[0]

cross_check_vectors = [
    0.0,
    1.0,
    -1.0,
    3.14159,
    1e308,
    -1e308,
    5e-324,
    float("inf"),
    float("-inf"),
]

for val in cross_check_vectors:
    lo, hi = encode_float(val)
    reconstructed = decode_float(lo, hi)

    # Float comparison (works for all except NaN)
    rpy_ok = (reconstructed == val)

    # Z3 cross-check: split/join on the bit pattern
    bits_u = int(float2longlong(val)) & MASK64
    smt_query = """\
(simplify
  (let ((lo ((_ extract 31 0) (_ bv%d 64)))
        (hi ((_ extract 63 32) (_ bv%d 64))))
    (bvor (bvshl ((_ sign_extend 32) hi) (_ bv32 64))
          ((_ zero_extend 32) lo))))""" % (bits_u, bits_u)
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)

    if z3_val is None:
        report("  FAIL  cross-check float(%s): unexpected Z3 output: %s" % (val, z3_out))
        ok = False
    elif rpy_ok and z3_val == bits_u:
        report("  PASS  cross-check float(%s) -> bits=%s lo=0x%08x hi=0x%08x" % (
            val, fmt64(bits_u), lo & MASK32, hi & MASK32))
    else:
        report("  FAIL  cross-check float(%s): RPython_ok=%s Z3=%s expected=%s" % (
            val, rpy_ok, fmt64(z3_val) if z3_val is not None else "?", fmt64(bits_u)))
        ok = False

# NaN: compare bit patterns (NaN != NaN as float)
lo, hi = encode_float(float("nan"))
bits_reconstructed = r_int64(intmask((hi << 32) | (lo & 0xFFFFFFFF)))
nan_rpy_ok = ((int(bits_reconstructed) & MASK64) == (nan_bits & MASK64))

if nan_rpy_ok:
    report("  PASS  cross-check float(NaN) -> bits=%s (bit-pattern match)" % fmt64(nan_bits & MASK64))
else:
    report("  FAIL  cross-check float(NaN): bit-pattern mismatch")
    ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: Split/join roundtrip (64-bit BV, UNSAT) ----------------------------
#
# Encoder: lo = extract[31:0](val), hi = extract[63:32](val)
# Decoder: (sign_extend_32(hi) << 32) | zero_extend_32(lo)
# Same as load_const P1, re-proved for the float encoding path.

report("  ... proving P1: float split/join roundtrip")
ok &= prove("P1: decode(encode(val)) == val", """\
(set-logic QF_BV)
(declare-const val (_ BitVec 64))
(define-fun lo () (_ BitVec 32) ((_ extract 31 0) val))
(define-fun hi () (_ BitVec 32) ((_ extract 63 32) val))
(define-fun reconstructed () (_ BitVec 64)
  (bvor (bvshl ((_ sign_extend 32) hi) (_ bv32 64))
        ((_ zero_extend 32) lo)))
(assert (not (= reconstructed val)))
(check-sat)
""")

# -- P2a: Lo half independent of hi (64-bit BV, UNSAT) ----------------------
#
# Changing hi bits does not affect extracted lo bits of the reconstruction.
# Guarantees that the lo operand carries its information without interference.

report("  ... proving P2a: lo half independent of hi")
ok &= prove("P2a: lo independent of hi", """\
(set-logic QF_BV)
(declare-const lo (_ BitVec 32))
(declare-const hi1 (_ BitVec 32))
(declare-const hi2 (_ BitVec 32))
(define-fun v1 () (_ BitVec 64) (concat hi1 lo))
(define-fun v2 () (_ BitVec 64) (concat hi2 lo))
(define-fun r1 () (_ BitVec 64)
  (bvor (bvshl ((_ sign_extend 32) ((_ extract 63 32) v1)) (_ bv32 64))
        ((_ zero_extend 32) ((_ extract 31 0) v1))))
(define-fun r2 () (_ BitVec 64)
  (bvor (bvshl ((_ sign_extend 32) ((_ extract 63 32) v2)) (_ bv32 64))
        ((_ zero_extend 32) ((_ extract 31 0) v2))))
(assert (not (= ((_ extract 31 0) r1) ((_ extract 31 0) r2))))
(check-sat)
""")

# -- P2b: Hi half independent of lo (64-bit BV, UNSAT) ----------------------
#
# Changing lo bits does not affect extracted hi bits of the reconstruction.

report("  ... proving P2b: hi half independent of lo")
ok &= prove("P2b: hi independent of lo", """\
(set-logic QF_BV)
(declare-const lo1 (_ BitVec 32))
(declare-const lo2 (_ BitVec 32))
(declare-const hi (_ BitVec 32))
(define-fun v1 () (_ BitVec 64) (concat hi lo1))
(define-fun v2 () (_ BitVec 64) (concat hi lo2))
(define-fun r1 () (_ BitVec 64)
  (bvor (bvshl ((_ sign_extend 32) ((_ extract 63 32) v1)) (_ bv32 64))
        ((_ zero_extend 32) ((_ extract 31 0) v1))))
(define-fun r2 () (_ BitVec 64)
  (bvor (bvshl ((_ sign_extend 32) ((_ extract 63 32) v2)) (_ bv32 64))
        ((_ zero_extend 32) ((_ extract 31 0) v2))))
(assert (not (= ((_ extract 63 32) r1) ((_ extract 63 32) r2))))
(check-sat)
""")

# -- P3: Encoding is injective (64-bit BV, UNSAT) ---------------------------
#
# Different 64-bit inputs produce different (lo, hi) pairs.
# Since lo = extract[31:0] and hi = extract[63:32], if both halves match,
# the full values must be equal.

report("  ... proving P3: encoding is injective")
ok &= prove("P3: v1 != v2 => (lo1, hi1) != (lo2, hi2)", """\
(set-logic QF_BV)
(declare-const v1 (_ BitVec 64))
(declare-const v2 (_ BitVec 64))
(assert (not (= v1 v2)))
(assert (= ((_ extract 31 0) v1) ((_ extract 31 0) v2)))
(assert (= ((_ extract 63 32) v1) ((_ extract 63 32) v2)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: Float constant encoding is correct")
    print("    P1: split/join roundtrip")
    print("    P2a: lo half independent of hi")
    print("    P2b: hi half independent of lo")
    print("    P3: encoding is injective")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
