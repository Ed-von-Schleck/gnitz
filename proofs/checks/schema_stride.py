"""Z3 formal proofs for memtable stride calculation correctness.

Proves six properties of gnitz/core/types.py:53-54 (_align formula) and
gnitz/core/types.py:115-127 (TableSchema.__init__ stride loop):

  _align(x, a) = (x + a - 1) & ~(a - 1)   [for power-of-2 a]

  Stride loop (non-PK columns only):
    current_offset = 0; max_alignment = 1
    for each non-PK col:
        current_offset = _align(current_offset, col.alignment) + col.size
        max_alignment = max(max_alignment, col.alignment)
    memtable_stride = _align(current_offset, max_alignment)

  P1. _align(x, a) % a == 0 for any power-of-2 a (16-bit BV, UNSAT)
      [novel: existing align_up.py only proves for a=64]
  P2. _align(x, a) >= x for any power-of-2 a (16-bit BV, UNSAT)
  P3. _align(x, a) - x < a for any power-of-2 a (16-bit BV, UNSAT)
  P4. max of two powers-of-2 is a power-of-2 (16-bit BV, UNSAT)
  P5. memtable_stride >= sum of non-PK column sizes (Python cross-check)
  P6. memtable_stride % max_alignment == 0 (Python cross-check)

4 Z3 queries + ~6 cross-checks.  Runs under PyPy2.
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


# -- Python helpers -----------------------------------------------------------

def py_align(x, a):
    return (x + a - 1) & ~(a - 1)


def compute_stride(col_defs, pk_index=0):
    """
    Simulate TableSchema.__init__ stride computation.
    col_defs: list of (size, alignment)
    Returns (memtable_stride, max_alignment, sum_sizes).
    """
    current_offset = 0
    max_alignment = 1
    sum_sizes = 0
    for i, (size, alignment) in enumerate(col_defs):
        if i == pk_index:
            continue
        current_offset = py_align(current_offset, alignment)
        current_offset += size
        sum_sizes += size
        if alignment > max_alignment:
            max_alignment = alignment
    stride = py_align(current_offset, max_alignment)
    return stride, max_alignment, sum_sizes


# -- Constants: all types (size, alignment) from types.py:17-28 --------------

# (size, alignment)
TYPE_U8     = (1, 1)
TYPE_I8     = (1, 1)
TYPE_U16    = (2, 2)
TYPE_I16    = (2, 2)
TYPE_U32    = (4, 4)
TYPE_I32    = (4, 4)
TYPE_F32    = (4, 4)
TYPE_U64    = (8, 8)
TYPE_I64    = (8, 8)
TYPE_F64    = (8, 8)
TYPE_STRING = (16, 8)
TYPE_U128   = (16, 16)

# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: Memtable stride calculation correctness")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-checks ------------------------------------------------------------

report("  ... cross-checking stride computation for sample schemas")

# Test schemas: each is a list of (size, alignment) with pk_index=0
# The first entry is the PK (U64 or U128), skipped by the stride loop.
test_schemas = [
    # (description, col_defs, pk_index)
    ("PK-only [U64]", [TYPE_U64], 0),
    ("[U64-PK, U8]", [TYPE_U64, TYPE_U8], 0),
    ("[U64-PK, U8, I64]", [TYPE_U64, TYPE_U8, TYPE_I64], 0),
    ("[U128-PK, U32, U8, I64]", [TYPE_U128, TYPE_U32, TYPE_U8, TYPE_I64], 0),
    ("[U64-PK, STRING]", [TYPE_U64, TYPE_STRING], 0),
    ("[U64-PK, U128]", [TYPE_U64, TYPE_U128], 0),
    ("[U64-PK, U8, U16, U32]", [TYPE_U64, TYPE_U8, TYPE_U16, TYPE_U32], 0),
]

all_pass = True
for desc, cols, pk_idx in test_schemas:
    stride, max_align, sum_sizes = compute_stride(cols, pk_idx)

    # P5: stride >= sum of non-PK col sizes
    p5_ok = (stride >= sum_sizes)
    # P6: stride % max_alignment == 0
    p6_ok = (stride % max_align == 0)

    if p5_ok and p6_ok:
        report("  PASS  cross-check: %s stride=%d max_align=%d sum_sizes=%d" % (
            desc, stride, max_align, sum_sizes))
    else:
        report("  FAIL  cross-check: %s stride=%d sum=%d align=%d p5=%s p6=%s" % (
            desc, stride, sum_sizes, max_align, p5_ok, p6_ok))
        ok = False
        all_pass = False

# Z3 cross-check: _align(5, 8) = (5+7)&~7 = 12&~7 = 8
smt_q = "(simplify (bvand (bvadd (_ bv5 16) (_ bv7 16)) (bvnot (_ bv7 16))))"
z3_out = run_z3(smt_q)
expected = py_align(5, 8)
if z3_out.startswith("#x"):
    z3_val = int(z3_out[2:], 16)
    if z3_val == expected:
        report("  PASS  cross-check: Z3 _align(5, 8) = %d" % expected)
    else:
        report("  FAIL  cross-check: Z3 _align(5, 8) = %d expected %d" % (z3_val, expected))
        ok = False

# Z3 cross-check: _align(0, 16) = 0
smt_q = "(simplify (bvand (bvadd (_ bv0 16) (_ bv15 16)) (bvnot (_ bv15 16))))"
z3_out = run_z3(smt_q)
expected = py_align(0, 16)
if z3_out.startswith("#x"):
    z3_val = int(z3_out[2:], 16)
    if z3_val == expected:
        report("  PASS  cross-check: Z3 _align(0, 16) = %d" % expected)
    else:
        report("  FAIL  cross-check: Z3 _align(0, 16) = %d expected %d" % (z3_val, expected))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: _align(x, a) % a == 0 for power-of-2 a (16-bit BV, UNSAT) -----------
#
# Novel: existing align_up.py only proves for ALIGNMENT=64.
# Here we prove symbolically for any power-of-2 alignment.
# Bound: x <= 32767 ensures x + (a-1) fits in 16 bits for any power-of-2 a.
# (max a = 32768 = 2^15; x + 32767 <= 32767 + 32767 = 65534 < 65536)

report("  ... proving P1: _align(x, a) is a-aligned for any power-of-2 a")

ok &= prove("P1: (x+a-1)&~(a-1) mod a == 0 for pow2 a, x <= 32767", """\
(set-logic QF_BV)
(declare-const x (_ BitVec 16))
(declare-const a (_ BitVec 16))
; a is a power of 2 (positive, a & (a-1) == 0)
(assert (bvugt a (_ bv0 16)))
(assert (= (bvand a (bvsub a (_ bv1 16))) (_ bv0 16)))
; x bounded to avoid 16-bit overflow in x + (a-1)
(assert (bvule x (_ bv32767 16)))
(define-fun aligned () (_ BitVec 16)
  (bvand (bvadd x (bvsub a (_ bv1 16))) (bvnot (bvsub a (_ bv1 16)))))
; Negate: aligned & (a-1) != 0 (not a-aligned)
(assert (not (= (bvand aligned (bvsub a (_ bv1 16))) (_ bv0 16))))
(check-sat)
""")

# -- P2: _align(x, a) >= x for power-of-2 a (16-bit BV, UNSAT) ---------------

report("  ... proving P2: _align(x, a) >= x for any power-of-2 a")

ok &= prove("P2: (x+a-1)&~(a-1) >= x for pow2 a, x <= 32767", """\
(set-logic QF_BV)
(declare-const x (_ BitVec 16))
(declare-const a (_ BitVec 16))
(assert (bvugt a (_ bv0 16)))
(assert (= (bvand a (bvsub a (_ bv1 16))) (_ bv0 16)))
(assert (bvule x (_ bv32767 16)))
(define-fun aligned () (_ BitVec 16)
  (bvand (bvadd x (bvsub a (_ bv1 16))) (bvnot (bvsub a (_ bv1 16)))))
; Negate: aligned < x
(assert (bvult aligned x))
(check-sat)
""")

# -- P3: _align(x, a) - x < a for power-of-2 a (16-bit BV, UNSAT) -----------
#
# Padding is less than alignment.

report("  ... proving P3: _align(x, a) - x < a (padding < alignment)")

ok &= prove("P3: padding < a for pow2 a, x <= 32767", """\
(set-logic QF_BV)
(declare-const x (_ BitVec 16))
(declare-const a (_ BitVec 16))
(assert (bvugt a (_ bv0 16)))
(assert (= (bvand a (bvsub a (_ bv1 16))) (_ bv0 16)))
(assert (bvule x (_ bv32767 16)))
(define-fun aligned () (_ BitVec 16)
  (bvand (bvadd x (bvsub a (_ bv1 16))) (bvnot (bvsub a (_ bv1 16)))))
; Negate: padding >= a
(assert (not (bvult (bvsub aligned x) a)))
(check-sat)
""")

# -- P4: max of two powers-of-2 is a power-of-2 (16-bit BV, UNSAT) -----------
#
# Needed because max_alignment in the stride loop is updated by comparing
# alignment values, all of which must remain powers of 2.

report("  ... proving P4: max of two powers-of-2 is a power-of-2")

ok &= prove("P4: max(pow2 a, pow2 b) is a power of 2", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 16))
(declare-const b (_ BitVec 16))
; Both a and b are powers of 2
(assert (bvugt a (_ bv0 16)))
(assert (= (bvand a (bvsub a (_ bv1 16))) (_ bv0 16)))
(assert (bvugt b (_ bv0 16)))
(assert (= (bvand b (bvsub b (_ bv1 16))) (_ bv0 16)))
(define-fun mx () (_ BitVec 16) (ite (bvuge a b) a b))
; Negate: max is not a power of 2
(assert (not (= (bvand mx (bvsub mx (_ bv1 16))) (_ bv0 16))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: Memtable stride calculation correctness")
    print("    P1: _align(x, a) is a-aligned for any power-of-2 a")
    print("    P2: _align(x, a) >= x for any power-of-2 a")
    print("    P3: padding _align(x,a)-x < a for any power-of-2 a")
    print("    P4: max of two powers-of-2 is a power-of-2")
    print("    P5: memtable_stride >= sum of non-PK sizes (cross-check)")
    print("    P6: memtable_stride %% max_alignment == 0 (cross-check)")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
