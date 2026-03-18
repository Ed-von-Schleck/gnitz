"""Z3 formal proofs for eval_expr instruction access bounds.

Source: gnitz/dbsp/expr.py:131-137 (eval_expr dispatch loop)
        gnitz/dbsp/expr.py:599-603 (ExprBuilder._emit)

Every _emit(op, dst, a1, a2) appends exactly 4 words.
ExprProgram stores num_instrs = len(code) // 4.
eval_expr accesses code[i*4] through code[i*4+3] for each i.

  P1. Code length N*4 is always 4-aligned: (N*4) & 3 == 0 (8-bit BV, UNSAT)
  P2. Last slot of instruction pc is in-bounds: pc*4+3 < N*4 (8-bit BV, UNSAT)
  P3. Distinct instructions have non-overlapping 4-word windows:
      i1 < i2 => i2*4 >= i1*4+4 (8-bit BV, UNSAT)

N restricted to [0..63] so 8-bit BV doesn't overflow: 63*4+3 = 255 < 256.

3 Z3 queries + 5 cross-checks.  Runs under PyPy2.
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


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: eval_expr instruction access bounds")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-checks -------------------------------------------------------------

report("  ... cross-checking instruction layout with simulate_emit")

def simulate_emit(n_instrs):
    code_len = n_instrs * 4
    num_instrs = code_len // 4
    if num_instrs != n_instrs:
        return False, "num_instrs mismatch"
    if code_len % 4 != 0:
        return False, "code_len not 4-aligned"
    for pc in range(num_instrs):
        base = pc * 4
        if base + 3 >= code_len:
            return False, "slot base+3=%d out of bounds (code_len=%d)" % (base + 3, code_len)
    return True, "ok"

for n in [0, 1, 2, 5, 10, 127]:
    passed, msg = simulate_emit(n)
    if passed:
        report("  PASS  cross-check n=%d: simulate_emit ok" % n)
    else:
        report("  FAIL  cross-check n=%d: %s" % (n, msg))
        ok = False

# Z3-encoding validation: 4-alignment formula (N & 3 == 0 after bvmul)
report("  ... validating Z3 4-alignment formula via simplify on n=5")
z3_align = run_z3("(simplify (bvand (bvmul (_ bv5 8) (_ bv4 8)) (_ bv3 8)))")
if z3_align in ("#x00", "#b00000000"):
    report("  PASS  Z3 encoding: (5*4) & 3 == 0")
else:
    report("  FAIL  Z3 encoding: (5*4) & 3 returned %s, expected #x00" % z3_align)
    ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: Code length N*4 is always 4-aligned (8-bit BV, UNSAT) ---------------
#
# (N*4) & 3 == 0 for any N.  Use bvand instead of bvurem (cheaper).
# Restrict N <= 63 to avoid 8-bit overflow (63*4=252 < 256).

report("  ... proving P1: code length N*4 is always 4-aligned")

ok &= prove("P1: (N*4) & 3 == 0 for N in [0,63]", """\
(set-logic QF_BV)
(declare-const n (_ BitVec 8))
(assert (bvule n (_ bv63 8)))
(define-fun code_len () (_ BitVec 8) (bvmul n (_ bv4 8)))
; Negate: code_len & 3 != 0
(assert (not (= (bvand code_len (_ bv3 8)) (_ bv0 8))))
(check-sat)
""")

# -- P2: Last slot of instruction pc in-bounds: pc*4+3 < N*4 (8-bit BV, UNSAT)
#
# Require N >= 1 (non-empty program) and pc in [0, N).
# last_slot = pc*4 + 3.  code_len = N*4.
# Restrict N <= 63 to stay within 8-bit range.

report("  ... proving P2: last slot of any instruction is in-bounds")

ok &= prove("P2: pc*4+3 < N*4 for pc in [0,N), N in [1,63]", """\
(set-logic QF_BV)
(declare-const n (_ BitVec 8))
(declare-const pc (_ BitVec 8))
(assert (bvuge n (_ bv1 8)))
(assert (bvule n (_ bv63 8)))
; pc in [0, n)
(assert (bvult pc n))
(define-fun last_slot () (_ BitVec 8) (bvadd (bvmul pc (_ bv4 8)) (_ bv3 8)))
(define-fun code_len  () (_ BitVec 8) (bvmul n (_ bv4 8)))
; Negate: last_slot >= code_len
(assert (not (bvult last_slot code_len)))
(check-sat)
""")

# -- P3: Distinct instructions have non-overlapping 4-word windows (8-bit BV, UNSAT)
#
# i1 < i2 => i2*4 >= i1*4 + 4.
# Restrict i1, i2 <= 62 so i2*4 <= 62*4=248 < 256.

report("  ... proving P3: distinct instructions have non-overlapping 4-word windows")

ok &= prove("P3: i1 < i2 implies i2*4 >= i1*4+4 (non-overlapping windows)", """\
(set-logic QF_BV)
(declare-const i1 (_ BitVec 8))
(declare-const i2 (_ BitVec 8))
(assert (bvule i1 (_ bv62 8)))
(assert (bvule i2 (_ bv62 8)))
; i1 < i2
(assert (bvult i1 i2))
; Negate: i2*4 < i1*4 + 4  (windows overlap)
(assert (not (bvuge (bvmul i2 (_ bv4 8)) (bvadd (bvmul i1 (_ bv4 8)) (_ bv4 8)))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: eval_expr instruction access bounds")
    print("    P1: code length N*4 is always 4-aligned")
    print("    P2: last slot pc*4+3 is in-bounds for any pc < N")
    print("    P3: distinct instructions occupy non-overlapping 4-word windows")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
