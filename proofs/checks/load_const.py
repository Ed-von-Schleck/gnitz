"""Z3 formal proofs for LOAD_CONST i64 split/join roundtrip.

Proves two properties of the LOAD_CONST encoding in gnitz/dbsp/expr.py:
  P1. Roundtrip: decode(encode(val)) == val for all 64-bit values
  P2. Lo/Hi extraction covers all bits: concat(hi, lo) == val

Source:
  - gnitz/dbsp/expr.py:598-604 — ExprBuilder.load_const_int(value) (encoder)
  - gnitz/dbsp/expr.py:130-132 — eval_expr LOAD_CONST case (decoder)

Encoder splits i64 into two 32-bit halves via intmask.
Decoder reconstructs via (a2 << 32) | (a1 & 0xFFFFFFFF).

2 Z3 queries + ~8 cross-checks.  Runs under PyPy2.
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


def fmt64(n):
    return "0x%016x" % (n & 0xFFFFFFFFFFFFFFFF)


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: LOAD_CONST i64 split/join roundtrip")
print("=" * 56)
sys.stdout.flush()

ok = True

MASK32 = 0xFFFFFFFF
MASK64 = (1 << 64) - 1

# -- Cross-check: validate encode/decode against RPython ---------------------

report("  ... cross-checking LOAD_CONST encode/decode against RPython")

print("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_int64, intmask  # noqa: E402
print("  ... imports done")
sys.stdout.flush()

def encode(value):
    """Replicate ExprBuilder.load_const_int encoder."""
    v = r_int64(value)
    lo = intmask(v)
    hi = intmask(v >> 32)
    return lo, hi

def decode(a1, a2):
    """Replicate eval_expr LOAD_CONST decoder."""
    return r_int64(intmask((a2 << 32) | (a1 & 0xFFFFFFFF)))

cross_check_vectors = [
    0,
    1,
    -1,
    (1 << 31) - 1,    # 2^31 - 1 = INT32_MAX
    -(1 << 31),        # -2^31 = INT32_MIN
    (1 << 63) - 1,    # 2^63 - 1 = INT64_MAX
    -(1 << 63),        # -2^63 = INT64_MIN
    0xDEADBEEFCAFEBABE,
]

for val in cross_check_vectors:
    lo, hi = encode(val)
    reconstructed = decode(lo, hi)
    original = r_int64(val)

    # Check RPython roundtrip (compare as unsigned bit patterns to avoid
    # PyPy2 int-vs-long comparison issues with r_int64 values)
    rpy_ok = ((int(reconstructed) & MASK64) == (int(original) & MASK64))

    # Also check Z3 encoding: extract lo/hi, reconstruct
    val_u = int(r_int64(val)) & MASK64
    smt_query = """\
(simplify
  (let ((lo ((_ extract 31 0) (_ bv%d 64)))
        (hi ((_ extract 63 32) (_ bv%d 64))))
    (bvor (bvshl ((_ sign_extend 32) hi) (_ bv32 64))
          ((_ zero_extend 32) lo))))""" % (val_u, val_u)
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)

    if z3_val is None:
        report("  FAIL  cross-check LOAD_CONST(%s): unexpected Z3 output: %s" % (fmt64(val_u), z3_out))
        ok = False
    elif rpy_ok and z3_val == val_u:
        report("  PASS  cross-check LOAD_CONST(%s) -> lo=0x%08x hi=0x%08x" % (
            fmt64(val_u), lo & MASK32, hi & MASK32))
    else:
        report("  FAIL  cross-check LOAD_CONST(%s): RPython_ok=%s Z3=%s expected=%s" % (
            fmt64(val_u), rpy_ok, fmt64(z3_val) if z3_val is not None else "?", fmt64(val_u)))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: Roundtrip: decode(encode(val)) == val --------------------------------
#
# Encoder: lo = extract[31:0](val), hi = extract[63:32](val)
# Decoder: (sign_extend_32(hi) << 32) | zero_extend_32(lo)
#
# The sign_extend of hi fills bits [63:32] when shifted left by 32, which
# places hi's 32 bits at [63:32] and zeros at [31:0]. Upper sign-extended
# bits are shifted out of 64-bit range. OR with zero-extended lo fills [31:0].
# Result: concat(hi, lo) == val.

report("  ... proving P1: LOAD_CONST roundtrip")
ok &= prove("P1: decode(encode(val)) == val", """\
(set-logic QF_BV)
(declare-const val (_ BitVec 64))
; Encoder: split into lo32 and hi32
(define-fun lo () (_ BitVec 32) ((_ extract 31 0) val))
(define-fun hi () (_ BitVec 32) ((_ extract 63 32) val))
; Decoder: (sign_extend_32(hi) << 32) | zero_extend_32(lo)
; This models: r_int64(intmask((a2 << 32) | (a1 & 0xFFFFFFFF)))
; where a2 = intmask(v >> 32) is sign-extended to 64 bits before shift
(define-fun reconstructed () (_ BitVec 64)
  (bvor (bvshl ((_ sign_extend 32) hi) (_ bv32 64))
        ((_ zero_extend 32) lo)))
; Negate: assert reconstructed != val
(assert (not (= reconstructed val)))
(check-sat)
""")

# -- P2: Lo/Hi extraction covers all bits ------------------------------------
#
# concat(hi, lo) == val. Sanity check that extract operations cover all 64
# bits without overlap or gap.

report("  ... proving P2: concat(hi, lo) == val")
ok &= prove("P2: concat(extract[63:32], extract[31:0]) == val", """\
(set-logic QF_BV)
(declare-const val (_ BitVec 64))
(define-fun lo () (_ BitVec 32) ((_ extract 31 0) val))
(define-fun hi () (_ BitVec 32) ((_ extract 63 32) val))
; Negate: assert concat(hi, lo) != val
(assert (not (= (concat hi lo) val)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: LOAD_CONST i64 split/join is correct")
    print("    P1: decode(encode(val)) == val (roundtrip)")
    print("    P2: concat(hi, lo) == val (bit coverage)")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
