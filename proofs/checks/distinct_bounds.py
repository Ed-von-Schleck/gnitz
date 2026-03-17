"""Z3 formal proofs for distinct operator output bounds.

Proves four properties of gnitz/dbsp/ops/distinct.py:56-73:

  s_old = 0
  if w_old > 0: s_old = 1
  elif w_old < 0: s_old = -1
  w_new = r_int64(intmask(w_old + w_delta))
  s_new = 0
  if w_new > 0: s_new = 1
  elif w_new < 0: s_new = -1
  out_w = s_new - s_old

Properties:
  P1. sign(w) in {-1, 0, 1} for all i64 w.
  P2. out_w in {-2, -1, 0, 1, 2} for all i64 w_old, w_delta.
  P3. out_w = 0 iff sign(w_new) == sign(w_old) (no-op equivalence).
  P4. Overflow can cause max-magnitude output: exists w_old > 0, w_delta > 0
      such that out_w = -2 (wrapping addition flips sign).

4 Z3 queries + 12 cross-checks.  Runs under PyPy2.
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


# -- Constants ----------------------------------------------------------------

MASK64 = (1 << 64) - 1
MAX_I64 = (1 << 63) - 1
MIN_I64 = -(1 << 63)

# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: distinct operator output bounds")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-check: validate sign and distinct logic against RPython ------------

report("  ... cross-checking sign/distinct logic against RPython")

print("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_int64, intmask  # noqa: E402
print("  ... imports done")
sys.stdout.flush()


def sign64(w):
    if w > r_int64(0):
        return 1
    elif w < r_int64(0):
        return -1
    return 0


def distinct_out(w_old, w_delta):
    """Compute distinct output weight matching the source code."""
    s_old = sign64(w_old)
    w_new = r_int64(intmask(w_old + w_delta))
    s_new = sign64(w_new)
    return s_new - s_old


def to_bv64(n):
    """Convert a Python int (possibly negative) to unsigned 64-bit for SMT."""
    return n & MASK64


# All 9 (s_old, s_new) pairs from {-1, 0, 1}^2 via representative w values,
# plus 3 i64 edge cases.
cross_check_vectors = [
    # (w_old, w_delta, expected_out)
    # s_old=-1, s_new=-1: out=0
    (r_int64(-5), r_int64(-3), 0),
    # s_old=-1, s_new=0: out=1
    (r_int64(-5), r_int64(5), 1),
    # s_old=-1, s_new=1: out=2
    (r_int64(-5), r_int64(10), 2),
    # s_old=0, s_new=-1: out=-1
    (r_int64(0), r_int64(-1), -1),
    # s_old=0, s_new=0: out=0
    (r_int64(0), r_int64(0), 0),
    # s_old=0, s_new=1: out=1
    (r_int64(0), r_int64(1), 1),
    # s_old=1, s_new=-1: out=-2
    (r_int64(5), r_int64(-10), -2),
    # s_old=1, s_new=0: out=-1
    (r_int64(5), r_int64(-5), -1),
    # s_old=1, s_new=1: out=0
    (r_int64(5), r_int64(3), 0),
    # Edge: MAX_I64 + 1 wraps to MIN_I64, out = -1 - 1 = -2
    (r_int64(MAX_I64), r_int64(1), -2),
    # Edge: MIN_I64 + (-1) wraps to MAX_I64, out = 1 - (-1) = 2
    (r_int64(MIN_I64), r_int64(-1), 2),
    # Edge: 0 + 0 = 0, out = 0
    (r_int64(0), r_int64(0), 0),
]

for w_old, w_delta, expected in cross_check_vectors:
    py_result = distinct_out(w_old, w_delta)

    # Z3 evaluation: use solver + get-value for reliable concrete output.
    # (simplify) doesn't always reduce ite+bvneg to a flat literal.
    smt_query = """\
(set-logic QF_BV)
(declare-const result (_ BitVec 64))
(assert (= result
  (let ((w_old (_ bv%d 64))
        (w_delta (_ bv%d 64)))
    (let ((w_new (bvadd w_old w_delta)))
      (let ((s_old (ite (bvsgt w_old (_ bv0 64)) (_ bv1 64)
                     (ite (bvslt w_old (_ bv0 64)) #xffffffffffffffff (_ bv0 64))))
            (s_new (ite (bvsgt w_new (_ bv0 64)) (_ bv1 64)
                     (ite (bvslt w_new (_ bv0 64)) #xffffffffffffffff (_ bv0 64)))))
        (bvsub s_new s_old))))))
(check-sat)
(get-value (result))""" % (to_bv64(int(w_old)), to_bv64(int(w_delta)))
    z3_out = run_z3(smt_query)

    # Parse: output is "sat\n((result #x...))" or "sat\n((result (bvneg #x...)))"
    z3_unsigned = None
    for line in z3_out.split("\n"):
        line = line.strip()
        if "#x" in line:
            idx = line.index("#x")
            hex_str = line[idx + 2:].rstrip(")")
            raw = int(hex_str, 16)
            if "bvneg" in line:
                # (bvneg #xN) means (2^64 - N) as unsigned
                z3_unsigned = (1 << 64) - raw
            else:
                z3_unsigned = raw
            break

    if z3_unsigned is not None:
        # Convert unsigned to signed
        if z3_unsigned >= (1 << 63):
            z3_signed = z3_unsigned - (1 << 64)
        else:
            z3_signed = z3_unsigned
    else:
        z3_signed = None

    if py_result == expected and z3_signed == expected:
        report("  PASS  cross-check (w_old=%d, w_delta=%d) -> %d" % (
            int(w_old), int(w_delta), expected))
    else:
        report("  FAIL  cross-check (w_old=%d, w_delta=%d): expected=%d Python=%d Z3=%s" % (
            int(w_old), int(w_delta), expected, py_result, z3_signed))
        ok = False

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- SMT sign64 definition (shared) ------------------------------------------

SIGN64_DEF = """\
(define-fun sign64 ((x (_ BitVec 64))) (_ BitVec 64)
  (ite (bvsgt x (_ bv0 64)) (_ bv1 64)
    (ite (bvslt x (_ bv0 64)) #xffffffffffffffff
      (_ bv0 64))))
"""

# -- P1: sign(w) in {-1, 0, 1} -----------------------------------------------
#
# The three-way branch always produces one of three values.

report("  ... proving P1: sign(w) in {-1, 0, 1}")
ok &= prove("P1: sign(w) in {-1, 0, 1}", """\
(set-logic QF_BV)
(declare-const w (_ BitVec 64))
""" + SIGN64_DEF + """\
(define-fun s () (_ BitVec 64) (sign64 w))
; Negate: s not in {-1, 0, 1}
(assert (not (or
  (= s (_ bv1 64))
  (= s #xffffffffffffffff)
  (= s (_ bv0 64)))))
(check-sat)
""")

# -- P2: out_w in {-2, -1, 0, 1, 2} ------------------------------------------
#
# sign(w_new) - sign(w_old) is the difference of two {-1,0,1} values,
# so it must be in {-2, -1, 0, 1, 2}.

report("  ... proving P2: out_w in {-2, -1, 0, 1, 2}")
ok &= prove("P2: out_w in {-2, -1, 0, 1, 2}", """\
(set-logic QF_BV)
(declare-const w_old (_ BitVec 64))
(declare-const w_delta (_ BitVec 64))
""" + SIGN64_DEF + """\
(define-fun w_new () (_ BitVec 64) (bvadd w_old w_delta))
(define-fun s_old () (_ BitVec 64) (sign64 w_old))
(define-fun s_new () (_ BitVec 64) (sign64 w_new))
(define-fun out_w () (_ BitVec 64) (bvsub s_new s_old))
; Negate: out_w not in {-2, -1, 0, 1, 2}
; Use hex literals: -1 = 0xFFFFFFFFFFFFFFFF, -2 = 0xFFFFFFFFFFFFFFFE
(assert (not (or
  (= out_w (_ bv0 64))
  (= out_w (_ bv1 64))
  (= out_w #xffffffffffffffff)
  (= out_w (_ bv2 64))
  (= out_w #xfffffffffffffffe))))
(check-sat)
""")

# -- P3: out_w = 0 iff sign(w_new) == sign(w_old) ----------------------------
#
# Zero output means no change in set membership.

report("  ... proving P3: out_w = 0 iff sign(w_new) == sign(w_old)")
ok &= prove("P3: out_w == 0 <=> s_new == s_old", """\
(set-logic QF_BV)
(declare-const w_old (_ BitVec 64))
(declare-const w_delta (_ BitVec 64))
""" + SIGN64_DEF + """\
(define-fun w_new () (_ BitVec 64) (bvadd w_old w_delta))
(define-fun s_old () (_ BitVec 64) (sign64 w_old))
(define-fun s_new () (_ BitVec 64) (sign64 w_new))
(define-fun out_w () (_ BitVec 64) (bvsub s_new s_old))
; Negate: (out_w == 0) XOR (s_new == s_old)
(assert (xor (= out_w (_ bv0 64)) (= s_new s_old)))
(check-sat)
""")

# -- P4: Overflow can cause max-magnitude output (SAT) ------------------------
#
# There exist w_old > 0, w_delta > 0 such that out_w = -2 (wrapping
# turns positive weight negative, flipping from +1 to -1).

report("  ... proving P4: overflow can cause out_w = -2 (SAT)")
ok &= check_sat("P4: exists w_old>0, w_delta>0 with out_w = -2", """\
(set-logic QF_BV)
(declare-const w_old (_ BitVec 64))
(declare-const w_delta (_ BitVec 64))
""" + SIGN64_DEF + """\
(define-fun w_new () (_ BitVec 64) (bvadd w_old w_delta))
(define-fun s_old () (_ BitVec 64) (sign64 w_old))
(define-fun s_new () (_ BitVec 64) (sign64 w_new))
(define-fun out_w () (_ BitVec 64) (bvsub s_new s_old))
(assert (bvsgt w_old (_ bv0 64)))
(assert (bvsgt w_delta (_ bv0 64)))
(assert (= out_w #xfffffffffffffffe))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: distinct operator output bounds")
    print("    P1: sign(w) in {-1, 0, 1}")
    print("    P2: out_w in {-2, -1, 0, 1, 2}")
    print("    P3: out_w == 0 <=> sign unchanged")
    print("    P4: overflow can produce out_w = -2 (counterexample)")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
