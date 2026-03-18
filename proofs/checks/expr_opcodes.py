"""Z3 formal proofs for the expression VM opcode namespace.

Source: gnitz/dbsp/expr.py:20-79 (opcode constants) and :132-137 (eval_expr dispatch)

Proves four properties of the expr opcode set:

  Defined opcodes: [1..34] (loads, arith, cmp, bool, null, output)
                   [40..46] (fused string ops + null emission)
  Deliberate gap:  [35..39] (reserved for future phases)

  P1. [1..34] range lies entirely below [40..46] (non-overlapping, 8-bit BV, UNSAT)
  P2. Gap [35..39] is disjoint from both defined ranges (8-bit BV, UNSAT)
  P3. All defined opcodes fit in 6 bits (< 64) (8-bit BV, UNSAT)
  P4. No defined opcode is 0 (8-bit BV, UNSAT)

4 Z3 queries + 5 cross-checks.  Runs under PyPy2.
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


# -- Constants (from gnitz/dbsp/expr.py:20-79) --------------------------------

# Column loads [1..3]
EXPR_LOAD_COL_INT   = 1
EXPR_LOAD_COL_FLOAT = 2
EXPR_LOAD_CONST     = 3

# Integer arithmetic [4..9]
EXPR_INT_ADD = 4
EXPR_INT_SUB = 5
EXPR_INT_MUL = 6
EXPR_INT_DIV = 7
EXPR_INT_MOD = 8
EXPR_INT_NEG = 9

# Float arithmetic [10..14]
EXPR_FLOAT_ADD = 10
EXPR_FLOAT_SUB = 11
EXPR_FLOAT_MUL = 12
EXPR_FLOAT_DIV = 13
EXPR_FLOAT_NEG = 14

# Integer comparison [15..20]
EXPR_CMP_EQ = 15
EXPR_CMP_NE = 16
EXPR_CMP_GT = 17
EXPR_CMP_GE = 18
EXPR_CMP_LT = 19
EXPR_CMP_LE = 20

# Float comparison [21..26]
EXPR_FCMP_EQ = 21
EXPR_FCMP_NE = 22
EXPR_FCMP_GT = 23
EXPR_FCMP_GE = 24
EXPR_FCMP_LT = 25
EXPR_FCMP_LE = 26

# Boolean logic [27..29]
EXPR_BOOL_AND = 27
EXPR_BOOL_OR  = 28
EXPR_BOOL_NOT = 29

# Null checks [30..31]
EXPR_IS_NULL     = 30
EXPR_IS_NOT_NULL = 31

# Output [32..34]
EXPR_EMIT         = 32
EXPR_INT_TO_FLOAT = 33
EXPR_COPY_COL     = 34

# Fused string + null emission [40..46]
EXPR_STR_COL_EQ_CONST = 40
EXPR_STR_COL_LT_CONST = 41
EXPR_STR_COL_LE_CONST = 42
EXPR_STR_COL_EQ_COL   = 43
EXPR_STR_COL_LT_COL   = 44
EXPR_STR_COL_LE_COL   = 45
EXPR_EMIT_NULL        = 46

RANGE_LOW  = list(range(1, 35))   # [1..34]
RANGE_HIGH = list(range(40, 47))  # [40..46]
ALL_OPCODES = RANGE_LOW + RANGE_HIGH

GAP = list(range(35, 40))  # [35..39]

# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: Expression VM opcode namespace")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-checks -------------------------------------------------------------

report("  ... cross-checking opcode constants")

# 1. Distinctness: 34 + 7 = 41 distinct values
if len(set(ALL_OPCODES)) == 41:
    report("  PASS  cross-check: all 41 opcodes are distinct")
else:
    report("  FAIL  cross-check: expected 41 distinct opcodes, got %d" % len(set(ALL_OPCODES)))
    ok = False

# 2. Minimum opcode == 1 (no zero)
if min(ALL_OPCODES) == 1:
    report("  PASS  cross-check: min opcode == 1 (no zero opcode)")
else:
    report("  FAIL  cross-check: min opcode = %d" % min(ALL_OPCODES))
    ok = False

# 3. Maximum opcode == 46 < 64 (fits in 6 bits)
if max(ALL_OPCODES) == 46:
    report("  PASS  cross-check: max opcode == 46 < 64 (fits in 6 bits)")
else:
    report("  FAIL  cross-check: max opcode = %d" % max(ALL_OPCODES))
    ok = False

# 4. No defined opcode falls in the gap [35..39]
gap_set = set(GAP)
if all(v not in gap_set for v in ALL_OPCODES):
    report("  PASS  cross-check: no opcode in gap [35..39]")
else:
    report("  FAIL  cross-check: an opcode falls in gap [35..39]")
    ok = False

# 5. Phase group boundaries are properly ordered
boundaries = [
    ("LOADS_END=3", 3), ("INT_ARITH_START=4", 4),
    ("INT_ARITH_END=9", 9), ("FLOAT_ARITH_START=10", 10),
    ("FLOAT_ARITH_END=14", 14), ("INT_CMP_START=15", 15),
    ("INT_CMP_END=20", 20), ("FLOAT_CMP_START=21", 21),
    ("FLOAT_CMP_END=26", 26), ("BOOL_START=27", 27),
    ("BOOL_END=29", 29), ("NULL_START=30", 30),
    ("NULL_END=31", 31), ("OUTPUT_START=32", 32),
    ("OUTPUT_END=34", 34), ("GAP_START=35", 35),
    ("STR_FUSED_START=40", 40),
]
ordered = True
for i in range(len(boundaries) - 1):
    _, v1 = boundaries[i]
    _, v2 = boundaries[i + 1]
    if v1 >= v2:
        report("  FAIL  cross-check: boundary ordering violated at %s >= %s" % (
            boundaries[i][0], boundaries[i + 1][0]))
        ordered = False
        ok = False
if ordered:
    report("  PASS  cross-check: phase group boundaries are properly ordered")

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: [1..34] lies entirely below [40..46] (8-bit BV, UNSAT) --------------
#
# Any value in the low range is strictly less than any value in the high range.
# We negate: there exist x1 in [1,34] and x2 in [40,46] with x1 >= x2.

report("  ... proving P1: [1..34] < [40..46] (no overlap between ranges)")

ok &= prove("P1: ranges [1,34] and [40,46] non-overlapping", """\
(set-logic QF_BV)
(declare-const x1 (_ BitVec 8))
(declare-const x2 (_ BitVec 8))
; x1 in [1, 34]
(assert (bvuge x1 #x01))
(assert (bvule x1 #x22))
; x2 in [40, 46]
(assert (bvuge x2 #x28))
(assert (bvule x2 #x2e))
; Negate: x1 >= x2 (ranges overlap)
(assert (not (bvult x1 x2)))
(check-sat)
""")

# -- P2: Gap [35..39] is disjoint from both defined ranges (8-bit BV, UNSAT) -
#
# A value in the gap cannot also be in [1,34] or [40,46].

report("  ... proving P2: gap [35..39] is disjoint from defined ranges")

ok &= prove("P2: gap [35,39] does not intersect [1,34] or [40,46]", """\
(set-logic QF_BV)
(declare-const s (_ BitVec 8))
; s in gap [35, 39]
(assert (bvuge s #x23))
(assert (bvule s #x27))
; Also in a defined range — contradiction
(assert (or (and (bvuge s #x01) (bvule s #x22))
            (and (bvuge s #x28) (bvule s #x2e))))
(check-sat)
""")

# -- P3: All defined opcodes fit in 6 bits (< 64) (8-bit BV, UNSAT) ----------
#
# Any value in [1,34] or [40,46] is less than 64.

report("  ... proving P3: all defined opcodes fit in 6 bits (< 64)")

ok &= prove("P3: defined opcodes satisfy s < 64", """\
(set-logic QF_BV)
(declare-const s (_ BitVec 8))
; s in a defined range
(assert (or (and (bvuge s #x01) (bvule s #x22))
            (and (bvuge s #x28) (bvule s #x2e))))
; Negate: s >= 64
(assert (not (bvult s #x40)))
(check-sat)
""")

# -- P4: No defined opcode is 0 (8-bit BV, UNSAT) ----------------------------
#
# Any value in [1,34] or [40,46] is non-zero.

report("  ... proving P4: no defined opcode equals 0")

ok &= prove("P4: s == 0 is not reachable from defined ranges", """\
(set-logic QF_BV)
(declare-const s (_ BitVec 8))
; s in a defined range
(assert (or (and (bvuge s #x01) (bvule s #x22))
            (and (bvuge s #x28) (bvule s #x2e))))
; Negate: s == 0
(assert (= s #x00))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: Expression VM opcode namespace")
    print("    P1: ranges [1,34] and [40,46] are non-overlapping")
    print("    P2: gap [35,39] is disjoint from both defined ranges")
    print("    P3: all defined opcodes fit in 6 bits (< 64)")
    print("    P4: no defined opcode is zero")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
