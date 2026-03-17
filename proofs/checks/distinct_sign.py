"""Z3 formal proofs for DBSP distinct operator sign function correctness.

Proves five properties of gnitz/dbsp/ops/distinct.py:56-73:

  s_old = 0
  if w_old > r_int64(0): s_old = 1
  elif w_old < r_int64(0): s_old = -1
  w_new = r_int64(intmask(w_old + w_delta))
  s_new = 0
  if w_new > r_int64(0): s_new = 1
  elif w_new < r_int64(0): s_new = -1
  out_w = s_new - s_old

All arithmetic is 64-bit signed (r_int64). sign64 defined using
bvsgt/bvslt; -1 encoded as #xffffffffffffffff (no bvneg).

  P1. sign64(w) in {-1, 0, 1} for all 64-bit w (64-bit BV, UNSAT)
  P2. out_w in {-2,-1,0,1,2} for all (w_old, w_delta) (64-bit BV, UNSAT)
  P3. s_old == s_new => out_w == 0 (row suppressed) (64-bit BV, UNSAT)
  P4. out_w == 0 => s_old == s_new (converse of P3) (64-bit BV, UNSAT)
  P5. w_old>=0, w_new>=0 => out_w in {-1,0,1} (64-bit BV, UNSAT)

5 Z3 queries + ~10 cross-checks.  Runs under PyPy2.
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

MASK64 = (1 << 64) - 1


# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: DBSP distinct operator sign correctness")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-checks: Python sign/distinct computation --------------------------

report("  ... cross-checking sign/distinct logic")

from rpython.rlib.rarithmetic import r_int64, intmask  # noqa: E402


def sign64(w):
    """RPython sign function: returns -1, 0, or 1."""
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


# 7 cross-check vectors: (w_old, w_delta, exp_out, exp_s_old, exp_s_new)
cross_check_vectors = [
    (0,   0,   0,   0,  0),   # zero stays zero
    (0,   1,   1,   0,  1),   # 0 -> +1: emit +1
    (1,  -1,  -1,   1,  0),   # +1 -> 0: suppress +1 (emit -1)
    (2,  -1,   0,   1,  1),   # +2 -> +1: same sign, no emit
    (3,  -3,  -1,   1,  0),   # +3 -> 0: suppress +1 (emit -1)
    (-1,  1,   1,  -1,  0),   # -1 -> 0: suppress -1 (emit +1)
    (-1,  2,   2,  -1,  1),   # -1 -> +1: sign flip, emit +2
]

for w_old_i, w_delta_i, exp_out, exp_s_old, exp_s_new in cross_check_vectors:
    w_old = r_int64(w_old_i)
    w_delta = r_int64(w_delta_i)
    py_s_old = sign64(w_old)
    w_new = r_int64(intmask(w_old + w_delta))
    py_s_new = sign64(w_new)
    py_out = py_s_new - py_s_old
    if py_s_old == exp_s_old and py_s_new == exp_s_new and py_out == exp_out:
        report("  PASS  cross-check (w_old=%d, w_delta=%d): s_old=%d s_new=%d out=%d" % (
            w_old_i, w_delta_i, py_s_old, py_s_new, py_out))
    else:
        report("  FAIL  cross-check (w_old=%d, w_delta=%d): exp s_old=%d s_new=%d out=%d, got %d %d %d" % (
            w_old_i, w_delta_i, exp_s_old, exp_s_new, exp_out, py_s_old, py_s_new, py_out))
        ok = False

# Z3 simplify cross-checks for concrete sign values
def parse_z3_bv(z3_out):
    """Parse a (simplify ...) result that is a hex or bv literal into unsigned int."""
    s = z3_out.strip()
    if s.startswith("#x"):
        return int(s[2:], 16)
    elif s.startswith("(_ bv"):
        parts = s.split()
        return int(parts[1][2:])
    return None


# sign64(5) = 1, sign64(-3) = -1 (= 0xffffffffffffffff unsigned), sign64(0) = 0
simplify_cases = [
    (5,  1,          "sign64(5) == 1"),
    (-3, MASK64,     "sign64(-3) == -1"),
    (0,  0,          "sign64(0) == 0"),
]

for val, expected_unsigned, label in simplify_cases:
    bv_val = val & MASK64
    smt_q = ("(simplify (ite (bvsgt (_ bv%d 64) (_ bv0 64)) (_ bv1 64)"
             " (ite (bvslt (_ bv%d 64) (_ bv0 64)) #xffffffffffffffff (_ bv0 64))))") % (
                 bv_val, bv_val)
    z3_out = run_z3(smt_q)
    got = parse_z3_bv(z3_out)
    if got == expected_unsigned:
        signed_exp = val if abs(val) < 2 else (expected_unsigned if expected_unsigned < (1 << 63)
                                               else expected_unsigned - (1 << 64))
        report("  PASS  cross-check Z3 simplify %s" % label)
    else:
        report("  FAIL  cross-check Z3 simplify %s: expected %s, got %s" % (
            label, hex(expected_unsigned), z3_out))
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

# -- P1: sign64(w) in {-1, 0, 1} for any 64-bit w ---------------------------
#
# The three-way branch always yields one of three bit-patterns.

report("  ... proving P1: sign64(w) in {-1, 0, 1}")

ok &= prove("P1: sign64(w) in {-1, 0, 1}", """\
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
# s_old, s_new each in {-1,0,1} so their difference covers {-2..2}.

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
(assert (not (or
  (= out_w (_ bv0 64))
  (= out_w (_ bv1 64))
  (= out_w #xffffffffffffffff)
  (= out_w (_ bv2 64))
  (= out_w #xfffffffffffffffe))))
(check-sat)
""")

# -- P3: s_old == s_new => out_w == 0 (row suppressed) -----------------------
#
# If the sign does not change, the distinct output is zero (no membership
# change).

report("  ... proving P3: s_old == s_new => out_w == 0")

ok &= prove("P3: s_old == s_new => out_w == 0", """\
(set-logic QF_BV)
(declare-const w_old (_ BitVec 64))
(declare-const w_delta (_ BitVec 64))
""" + SIGN64_DEF + """\
(define-fun w_new () (_ BitVec 64) (bvadd w_old w_delta))
(define-fun s_old () (_ BitVec 64) (sign64 w_old))
(define-fun s_new () (_ BitVec 64) (sign64 w_new))
(define-fun out_w () (_ BitVec 64) (bvsub s_new s_old))
; Hypothesis: signs are equal
(assert (= s_old s_new))
; Negate conclusion: out_w != 0
(assert (not (= out_w (_ bv0 64))))
(check-sat)
""")

# -- P4: out_w == 0 => s_old == s_new (converse of P3) -----------------------
#
# Zero output implies no sign change: the two directions together give
# the biconditional (row suppressed iff sign unchanged).

report("  ... proving P4: out_w == 0 => s_old == s_new")

ok &= prove("P4: out_w == 0 => s_old == s_new", """\
(set-logic QF_BV)
(declare-const w_old (_ BitVec 64))
(declare-const w_delta (_ BitVec 64))
""" + SIGN64_DEF + """\
(define-fun w_new () (_ BitVec 64) (bvadd w_old w_delta))
(define-fun s_old () (_ BitVec 64) (sign64 w_old))
(define-fun s_new () (_ BitVec 64) (sign64 w_new))
(define-fun out_w () (_ BitVec 64) (bvsub s_new s_old))
; Hypothesis: output weight is zero
(assert (= out_w (_ bv0 64)))
; Negate conclusion: signs differ
(assert (not (= s_old s_new)))
(check-sat)
""")

# -- P5: Non-negative weights => out_w in {-1, 0, 1} -------------------------
#
# When w_old >= 0 and w_new >= 0, s_old and s_new are both in {0, 1}, so
# out_w = s_new - s_old in {-1, 0, 1}.  This is the base-table ingestion
# case: accumulated weights never go negative.

report("  ... proving P5: w_old>=0, w_new>=0 => out_w in {-1, 0, 1}")

ok &= prove("P5: non-negative weights => out_w in {-1, 0, 1}", """\
(set-logic QF_BV)
(declare-const w_old (_ BitVec 64))
(declare-const w_delta (_ BitVec 64))
""" + SIGN64_DEF + """\
(define-fun w_new () (_ BitVec 64) (bvadd w_old w_delta))
(define-fun s_old () (_ BitVec 64) (sign64 w_old))
(define-fun s_new () (_ BitVec 64) (sign64 w_new))
(define-fun out_w () (_ BitVec 64) (bvsub s_new s_old))
; Precondition: both weights non-negative (base-table ingestion case)
(assert (bvsge w_old (_ bv0 64)))
(assert (bvsge w_new (_ bv0 64)))
; Negate: out_w not in {-1, 0, 1}
(assert (not (or
  (= out_w #xffffffffffffffff)
  (= out_w (_ bv0 64))
  (= out_w (_ bv1 64)))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: DBSP distinct operator sign correctness")
    print("    P1: sign64(w) in {-1, 0, 1}")
    print("    P2: out_w in {-2, -1, 0, 1, 2}")
    print("    P3: s_old == s_new => out_w == 0")
    print("    P4: out_w == 0 => s_old == s_new")
    print("    P5: non-negative weights => out_w in {-1, 0, 1}")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
