"""Z3 formal proofs for DBSP weight arithmetic.

Proves four properties of weight arithmetic used in join and accumulator ops:
  P1. Multiplication identity: w * 1 == w
  P2. Zero annihilation: w * 0 == 0
  P3. Distributivity: a * (b + c) == a*b + a*c (16-bit, width-independent)
  P4. Counterexample: overflow sign flip exists (SAT)

Source:
  - gnitz/dbsp/ops/join.py:151 — intmask(w_delta * w_trace)
  - gnitz/dbsp/functions.py:172,199,210 — accumulator arithmetic

Weight arithmetic uses wrapping i64 multiplication and addition.  These
properties document that the RPython code correctly implements DBSP algebra
under modular arithmetic (Z/2^64 ring).

4 Z3 queries + ~10 cross-checks.  Runs under PyPy2.
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


def check_sat(label, smt_text):
    """Run a query expecting sat. Returns True on success."""
    result = run_z3(smt_text)
    if result == "sat":
        report("  PASS  %s" % label)
        return True
    else:
        report("  FAIL  %s: expected sat, got %s" % (label, result))
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
    return "0x%016x" % (n & MASK64)


def to_bv64(n):
    """Convert a Python int (possibly negative) to unsigned 64-bit for SMT."""
    return n & MASK64


# -- Constants ----------------------------------------------------------------

MASK64 = (1 << 64) - 1
MAX_I64 = (1 << 63) - 1
MIN_I64 = -(1 << 63)


# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: DBSP weight arithmetic")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-check: validate Z3 encoding against RPython -----------------------

report("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_int64, intmask  # noqa: E402
report("  ... imports done")

def rpy_mul(a, b):
    """Replicate RPython weight multiplication: r_int64(intmask(a * b))."""
    return r_int64(intmask(r_int64(a) * r_int64(b)))

def to_signed64(unsigned):
    """Convert unsigned 64-bit to signed."""
    if unsigned >= (1 << 63):
        return unsigned - (1 << 64)
    return unsigned

# Cross-check vectors: (a, b, expected_signed_result)
cross_check_vectors = [
    (1, 1, 1),
    (MAX_I64, 1, MAX_I64),
    (0, MAX_I64, 0),
    (2, 3, 6),
    (-1, -1, 1),
    (-1, 1, -1),
    (MAX_I64, 2, -2),           # overflow!
    (MIN_I64, -1, MIN_I64),     # overflow! -MIN_I64 wraps
    (MIN_I64, 2, 0),            # overflow!
    (1 << 62, 3, -(1 << 62)),   # overflow: 3*2^62 = -2^62 mod 2^64
]

report("  ... cross-checking weight multiplication against RPython")

for a, b, expected in cross_check_vectors:
    rpy_result = rpy_mul(a, b)
    rpy_unsigned = int(rpy_result) & MASK64

    # Z3 evaluation via check-sat + get-value (avoids bvneg non-reduction)
    a_u = to_bv64(a)
    b_u = to_bv64(b)
    smt_query = """\
(set-logic QF_BV)
(define-fun result () (_ BitVec 64) (bvmul (_ bv%d 64) (_ bv%d 64)))
(check-sat)
(get-value (result))""" % (a_u, b_u)
    z3_out = run_z3(smt_query)

    # Parse: "sat\n((result #x...))" or "sat\n((result (bvneg #x...)))"
    z3_unsigned = None
    for line in z3_out.split("\n"):
        line = line.strip()
        if "#x" in line:
            idx = line.index("#x")
            hex_str = line[idx + 2:].rstrip(")")
            raw = int(hex_str, 16)
            if "bvneg" in line:
                z3_unsigned = (1 << 64) - raw
            else:
                z3_unsigned = raw
            break

    expected_unsigned = to_bv64(expected)

    if z3_unsigned is not None and rpy_unsigned == expected_unsigned and z3_unsigned == expected_unsigned:
        report("  PASS  cross-check %d * %d = %d" % (a, b, expected))
    else:
        report("  FAIL  cross-check %d * %d: expected=%s RPython=%s Z3=%s" % (
            a, b, fmt64(expected_unsigned), fmt64(rpy_unsigned),
            fmt64(z3_unsigned) if z3_unsigned is not None else "?"))
        ok = False

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P1: Multiplication identity: w * 1 == w ---------------------------------
#
# Proves weight x 1 preserves value (neutral element).

report("  ... proving P1: multiplication identity")
ok &= prove("P1: bvmul(w, 1) == w", """\
(set-logic QF_BV)
(declare-const w (_ BitVec 64))
(assert (not (= (bvmul w (_ bv1 64)) w)))
(check-sat)
""")

# -- P2: Zero annihilation: w * 0 == 0 ----------------------------------------
#
# Proves zero-weight rows produce zero output.

report("  ... proving P2: zero annihilation")
ok &= prove("P2: bvmul(w, 0) == 0", """\
(set-logic QF_BV)
(declare-const w (_ BitVec 64))
(assert (not (= (bvmul w (_ bv0 64)) (_ bv0 64))))
(check-sat)
""")

# -- P3: Distributivity: a * (b + c) == a*b + a*c ----------------------------
#
# Uses 16-bit because bvmul with both operands symbolic is expensive at 64-bit.
# The property is width-independent (modular arithmetic distributes at any width).
# Proves merge_accumulated formula correctness: acc + prev * weight equals
# step-by-step accumulation.

report("  ... proving P3: distributivity (16-bit)")
ok &= prove("P3: a * (b + c) == a*b + a*c (distributivity)", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 16))
(declare-const b (_ BitVec 16))
(declare-const c (_ BitVec 16))
(assert (not (= (bvmul a (bvadd b c))
                (bvadd (bvmul a b) (bvmul a c)))))
(check-sat)
""")

# -- P4: Counterexample — overflow sign flip (SAT) ---------------------------
#
# Exists a > 0, b > 0 (signed) such that a * b < 0 (signed).
# Documents that weight overflow is possible and well-defined (wrapping, not UB).

report("  ... proving P4: overflow sign flip (SAT)")
ok &= check_sat("P4: exists a>0, b>0 (signed) with a*b < 0 (signed)", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 64))
(declare-const b (_ BitVec 64))
; a > 0 and b > 0 (signed)
(assert (bvsgt a (_ bv0 64)))
(assert (bvsgt b (_ bv0 64)))
; a * b < 0 (signed)
(assert (bvslt (bvmul a b) (_ bv0 64)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: DBSP weight arithmetic")
    print("    P1: multiplication identity (w * 1 == w)")
    print("    P2: zero annihilation (w * 0 == 0)")
    print("    P3: distributivity (a*(b+c) == a*b + a*c)")
    print("    P4: overflow sign flip exists (counterexample)")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
