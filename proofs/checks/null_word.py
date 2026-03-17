"""Z3 formal proofs for null-word bit-set/test correctness.

Proves four properties of the null-word encoding in gnitz/core/batch.py:
  P1. Bit independence (set-test roundtrip): setting bit i then testing i returns true
  P2. Non-interference: setting bit i does not affect bit j (i != j)
  P3. _payload_idx injectivity on non-PK columns
  P4. _payload_idx range: output in [0, n_cols - 1) for non-PK inputs

Source:
  - gnitz/core/batch.py:174-177 — RowBuilder.put_null(): null_word |= (1 << curr)
  - gnitz/core/batch.py:74-81 — ColumnarBatchAccessor.is_null(): null_word & (1 << payload_idx)
  - gnitz/core/batch.py:67-72 — _payload_idx(col_idx): maps col_idx to [0, n_payload)
  - gnitz/core/types.py:89 — max 64 columns -> max 63 payload columns -> fits in u64

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
print("  Z3 PROOF: null-word bit-set/test correctness")
print("=" * 56)
sys.stdout.flush()

ok = True

MASK64 = (1 << 64) - 1

# -- Cross-check: pure Python reimplementation of put_null/is_null -----------

report("  ... cross-checking null-word set/test against pure Python")

def py_put_null(null_word, idx):
    """Reimplementation of RowBuilder.put_null bit-set."""
    return null_word | (1 << idx)

def py_is_null(null_word, payload_idx):
    """Reimplementation of ColumnarBatchAccessor.is_null bit-test."""
    return bool(null_word & (1 << payload_idx))

def py_payload_idx(col_idx, pk_index):
    """Reimplementation of _payload_idx."""
    if col_idx == pk_index:
        return -1
    if col_idx < pk_index:
        return col_idx
    return col_idx - 1

# Test: set bits at various positions, then test each
test_positions = [0, 1, 31, 62]
for pos in test_positions:
    nw = py_put_null(0, pos)
    # Verify the set bit is detected
    if not py_is_null(nw, pos):
        report("  FAIL  cross-check: set bit %d, test bit %d returned False" % (pos, pos))
        ok = False
        continue
    # Verify other bits are NOT set
    other_ok = True
    for other in test_positions:
        if other != pos:
            if py_is_null(nw, other):
                report("  FAIL  cross-check: set bit %d, test bit %d returned True" % (pos, other))
                other_ok = False
                ok = False
    if other_ok:
        report("  PASS  cross-check set/test bit %d -> %s" % (pos, fmt64(nw)))

# Cross-check Z3 encoding of set/test
report("  ... cross-checking Z3 encoding of bit set/test")
for pos in test_positions:
    # Z3: (bvor (_ bv0 64) (bvshl (_ bv1 64) (_ bvPOS 64)))
    smt_set = "(simplify (bvor (_ bv0 64) (bvshl (_ bv1 64) (_ bv%d 64))))" % pos
    z3_out = run_z3(smt_set)
    z3_val = parse_z3_value(z3_out)
    py_val = py_put_null(0, pos) & MASK64
    if z3_val is None:
        report("  FAIL  cross-check Z3 set(%d): unexpected output: %s" % (pos, z3_out))
        ok = False
    elif z3_val == py_val:
        report("  PASS  cross-check Z3 set(%d) -> %s" % (pos, fmt64(z3_val)))
    else:
        report("  FAIL  cross-check Z3 set(%d): Python=%s Z3=%s" % (pos, fmt64(py_val), fmt64(z3_val)))
        ok = False

# Cross-check _payload_idx
report("  ... cross-checking _payload_idx")
payload_idx_cases = [
    # (col_idx, pk_index, expected)
    (0, 2, 0), (1, 2, 1), (3, 2, 2), (4, 2, 3),  # pk at 2
    (0, 0, -1), (1, 0, 0), (2, 0, 1),  # pk at 0
]
for col_idx, pk_index, expected in payload_idx_cases:
    result = py_payload_idx(col_idx, pk_index)
    if result == expected:
        report("  PASS  cross-check _payload_idx(%d, pk=%d) -> %d" % (col_idx, pk_index, result))
    else:
        report("  FAIL  cross-check _payload_idx(%d, pk=%d): expected %d, got %d" % (col_idx, pk_index, expected, result))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: Bit independence (set-test roundtrip) --------------------------------
#
# For payload_idx i in [0, 62]: setting bit i then testing bit i returns true.
# (null_word | (1 << i)) & (1 << i) != 0
# Negate: assert the expression == 0 for some null_word and i in [0, 62].

report("  ... proving P1: bit set-test roundtrip")
ok &= prove("P1: (nw | (1 << i)) & (1 << i) != 0", """\
(set-logic QF_BV)
(declare-const nw (_ BitVec 64))
(declare-const i (_ BitVec 64))
; i in [0, 62]
(assert (bvule i (_ bv62 64)))
(define-fun bit () (_ BitVec 64) (bvshl (_ bv1 64) i))
(define-fun set_nw () (_ BitVec 64) (bvor nw bit))
; Negate: assert (set_nw & bit) == 0
(assert (= (bvand set_nw bit) (_ bv0 64)))
(check-sat)
""")

# -- P2: Non-interference -----------------------------------------------------
#
# Setting bit i does not affect bit j where i != j.
# ((null_word | (1 << i)) & (1 << j)) == (null_word & (1 << j)) when i != j
# Both i, j in [0, 62].

report("  ... proving P2: bit non-interference")
ok &= prove("P2: setting bit i does not affect bit j (i != j)", """\
(set-logic QF_BV)
(declare-const nw (_ BitVec 64))
(declare-const i (_ BitVec 64))
(declare-const j (_ BitVec 64))
; i, j in [0, 62]
(assert (bvule i (_ bv62 64)))
(assert (bvule j (_ bv62 64)))
; i != j
(assert (not (= i j)))
(define-fun bit_i () (_ BitVec 64) (bvshl (_ bv1 64) i))
(define-fun bit_j () (_ BitVec 64) (bvshl (_ bv1 64) j))
(define-fun set_nw () (_ BitVec 64) (bvor nw bit_i))
; Negate: assert (set_nw & bit_j) != (nw & bit_j)
(assert (not (= (bvand set_nw bit_j) (bvand nw bit_j))))
(check-sat)
""")

# -- P3: _payload_idx injectivity on non-PK columns --------------------------
#
# For two distinct non-PK col indices a, b with a < n_cols and b < n_cols:
# _payload_idx(a) == _payload_idx(b) -> a == b
#
# SMT encoding of _payload_idx(col_idx, pk_index):
#   ite(col_idx < pk_index, col_idx, col_idx - 1)
#
# 8-bit BVs suffice (max 64 columns, values fit in [0, 63]).

report("  ... proving P3: _payload_idx is injective on non-PK columns")
ok &= prove("P3: _payload_idx injective", """\
(set-logic QF_BV)
(declare-const a (_ BitVec 8))
(declare-const b (_ BitVec 8))
(declare-const pk (_ BitVec 8))
(declare-const n_cols (_ BitVec 8))
; n_cols <= 64
(assert (bvule n_cols (_ bv64 8)))
; a, b are valid column indices, not PK
(assert (bvult a n_cols))
(assert (bvult b n_cols))
(assert (not (= a pk)))
(assert (not (= b pk)))
(assert (bvult pk n_cols))
; a != b
(assert (not (= a b)))
; _payload_idx(x) = ite(x < pk, x, x - 1)
(define-fun pidx_a () (_ BitVec 8)
  (ite (bvult a pk) a (bvsub a (_ bv1 8))))
(define-fun pidx_b () (_ BitVec 8)
  (ite (bvult b pk) b (bvsub b (_ bv1 8))))
; Negate injectivity: assert pidx_a == pidx_b (with a != b)
(assert (= pidx_a pidx_b))
(check-sat)
""")

# -- P4: _payload_idx range ---------------------------------------------------
#
# For any non-PK col_idx in [0, n_cols), with n_cols <= 64 and pk < n_cols:
# 0 <= _payload_idx(col_idx) < n_cols - 1
#
# Since BVs are unsigned, >= 0 is automatic. We prove < n_cols - 1.

report("  ... proving P4: _payload_idx range in [0, n_cols-1)")
ok &= prove("P4: _payload_idx(col_idx) < n_cols - 1", """\
(set-logic QF_BV)
(declare-const col_idx (_ BitVec 8))
(declare-const pk (_ BitVec 8))
(declare-const n_cols (_ BitVec 8))
; n_cols >= 2 (at least PK + one payload column) and n_cols <= 64
(assert (bvuge n_cols (_ bv2 8)))
(assert (bvule n_cols (_ bv64 8)))
; col_idx is valid and not PK
(assert (bvult col_idx n_cols))
(assert (not (= col_idx pk)))
(assert (bvult pk n_cols))
; _payload_idx(col_idx) = ite(col_idx < pk, col_idx, col_idx - 1)
(define-fun pidx () (_ BitVec 8)
  (ite (bvult col_idx pk) col_idx (bvsub col_idx (_ bv1 8))))
; Negate: assert pidx >= n_cols - 1
(assert (not (bvult pidx (bvsub n_cols (_ bv1 8)))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: null-word bit operations are correct")
    print("    P1: bit set-test roundtrip (independence)")
    print("    P2: bit non-interference (i != j)")
    print("    P3: _payload_idx is injective on non-PK columns")
    print("    P4: _payload_idx range in [0, n_cols - 1)")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
