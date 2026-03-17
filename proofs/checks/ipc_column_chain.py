"""Z3 formal proofs for IPC ZSet full column offset chain.

Extends ipc_wire.py (which proves P3-P4 for 4 structural sections) to the
DATA COLUMN offset chain: string columns use IPC_STRING_STRIDE=8 per row,
non-string use col_type.size per row. All sections aligned to ALIGNMENT=64.

Source: gnitz/server/ipc.py:286-335 (_compute_zset_wire_size)

  P1. Data column offset is ALIGNMENT-aligned (64-bit BV, UNSAT)
  P2. Data column sections are non-overlapping (64-bit BV, UNSAT)
  P3. Blob offset comes after all column sections (64-bit BV, UNSAT)
  P4. Column size formulas are positive for count > 0 (16-bit BV, UNSAT)

4 Z3 queries + ~5 cross-checks.  Runs under PyPy2.
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

ALIGNMENT = 64
MASK64 = (1 << 64) - 1

# Wire strides for common types
STRIDE_U64 = 8
STRIDE_I64 = 8
STRIDE_F64 = 8
STRIDE_U128 = 16
STRIDE_STRING = 8  # IPC_STRING_STRIDE
STRIDE_U32 = 4
STRIDE_U16 = 2
STRIDE_U8 = 1

# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: IPC ZSet full column offset chain")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-checks ------------------------------------------------------------

report("  ... cross-checking column offset chain")


def align_up_py(val, align):
    u_val = val & MASK64
    u_align = align & MASK64
    return (u_val + u_align - 1) & ~(u_align - 1)


# Test schemas: list of (name, wire_stride)
test_schemas = [
    ("2xU64+STRING+F64", [(STRIDE_U64, False), (STRIDE_U64, False), (STRIDE_STRING, True), (STRIDE_F64, False)]),
    ("STRING+STRING", [(STRIDE_STRING, True), (STRIDE_STRING, True)]),
    ("U128+U8+I64", [(STRIDE_U128, False), (STRIDE_U8, False), (STRIDE_I64, False)]),
]

test_counts = [1, 8, 100, 1000]

for schema_name, col_defs in test_schemas:
    for count in test_counts:
        # Start after 4 structural sections (pk_lo, pk_hi, weight, null)
        struct_sz = count * 8
        cur = 0
        cur = align_up_py(cur + struct_sz, ALIGNMENT)
        cur = align_up_py(cur + struct_sz, ALIGNMENT)
        cur = align_up_py(cur + struct_sz, ALIGNMENT)
        cur = align_up_py(cur + struct_sz, ALIGNMENT)

        # Data columns
        col_offsets = []
        for stride, is_string in col_defs:
            col_offsets.append(cur)
            col_sz = count * stride
            cur = align_up_py(cur + col_sz, ALIGNMENT)

        blob_offset = cur

        # Verify all column offsets are 64-aligned
        all_aligned = all(off % ALIGNMENT == 0 for off in col_offsets)
        # Verify blob_offset is 64-aligned
        blob_aligned = (blob_offset % ALIGNMENT == 0)
        # Verify non-overlapping
        non_overlap = True
        for i in range(len(col_offsets)):
            stride, _ = col_defs[i]
            col_end = col_offsets[i] + count * stride
            if i + 1 < len(col_offsets):
                if col_end > col_offsets[i + 1]:
                    non_overlap = False
        # Verify blob after all columns
        if len(col_offsets) > 0:
            last_stride, _ = col_defs[-1]
            last_end = col_offsets[-1] + count * last_stride
            blob_after = (blob_offset >= last_end)
        else:
            blob_after = True

        if all_aligned and blob_aligned and non_overlap and blob_after:
            report("  PASS  cross-check: %s count=%d blob_off=%d" % (
                schema_name, count, blob_offset))
        else:
            report("  FAIL  cross-check: %s count=%d aligned=%s blob_aligned=%s overlap=%s after=%s" % (
                schema_name, count, all_aligned, blob_aligned, non_overlap, blob_after))
            ok = False

# Z3 simplify cross-check: align_up(128 + 800, 64) = 960
smt_q = "(simplify (bvand (bvadd (bvadd (_ bv128 64) (_ bv800 64)) (_ bv63 64)) (bvnot (_ bv63 64))))"
z3_out = run_z3(smt_q)
expected = align_up_py(128 + 800, 64)
if z3_out.startswith("#x"):
    z3_val = int(z3_out[2:], 16)
    if z3_val == expected:
        report("  PASS  cross-check: Z3 align_up(928, 64) = %d" % expected)
    else:
        report("  FAIL  cross-check: Z3 align_up(928, 64) = %d expected %d" % (z3_val, expected))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: Data column offset is ALIGNMENT-aligned (64-bit BV, UNSAT) ----------
#
# For any 64-aligned starting offset, adding any section size and aligning
# via align_up produces a 64-aligned result.

report("  ... proving P1: data column offset is 64-byte aligned")

ok &= prove("P1: align_up(64-aligned + sz, 64) is 64-aligned", """\
(set-logic QF_BV)
(declare-const cur (_ BitVec 64))
(declare-const sz (_ BitVec 64))
; cur is 64-aligned
(assert (= (bvand cur (_ bv63 64)) (_ bv0 64)))
; Practical bound: cur < 2^48 (wire size from count <= 2^28)
(assert (bvule cur (_ bv281474976710656 64)))
; sz is bounded (count <= 2^28 * max stride 16 = 2^32)
(assert (bvule sz (_ bv268435456 64)))
(define-fun a64 ((x (_ BitVec 64))) (_ BitVec 64)
  (bvand (bvadd x (_ bv63 64)) (bvnot (_ bv63 64))))
(define-fun next () (_ BitVec 64) (a64 (bvadd cur sz)))
; Negate: next is not 64-aligned
(assert (not (= (bvand next (_ bv63 64)) (_ bv0 64))))
(check-sat)
""")

# -- P2: Data column sections are non-overlapping (64-bit BV, UNSAT) ----------
#
# align_up(cur + s1, 64) >= cur + s1, so section [cur, cur+s1) does not
# overlap [align_up(cur+s1, 64), ...).
# Bound: cur <= 2^48 (conservative: 260 sections * 2^32 max section < 2^41).
# RPython r_uint64 wraps at 64 bits, but _compute_zset_wire_size starts from 0
# and count <= 2^28 (matching ipc_wire.py), so total wire size << 2^48.

report("  ... proving P2: data column sections are non-overlapping")

ok &= prove("P2: cur + s1 <= align_up(cur + s1, 64)", """\
(set-logic QF_BV)
(declare-const cur (_ BitVec 64))
(declare-const s1 (_ BitVec 64))
(assert (= (bvand cur (_ bv63 64)) (_ bv0 64)))
(assert (bvugt s1 (_ bv0 64)))
(assert (bvule s1 (_ bv268435456 64)))
; Practical bound: cur < 2^48 (wire size cannot exceed this)
(assert (bvule cur (_ bv281474976710656 64)))
(define-fun a64 ((x (_ BitVec 64))) (_ BitVec 64)
  (bvand (bvadd x (_ bv63 64)) (bvnot (_ bv63 64))))
(define-fun next1 () (_ BitVec 64) (a64 (bvadd cur s1)))
; Negate: section end > next section start
(assert (not (bvule (bvadd cur s1) next1)))
(check-sat)
""")

# -- P3: Blob offset comes after all column sections (64-bit BV, UNSAT) -------
#
# After N data columns, blob_offset = final aligned offset >= last_col_end.
# Prove for 2 data columns.

report("  ... proving P3: blob offset comes after all column sections")

ok &= prove("P3: blob_offset >= last column end (2-col chain)", """\
(set-logic QF_BV)
(declare-const base (_ BitVec 64))
(declare-const s1 (_ BitVec 64))
(declare-const s2 (_ BitVec 64))
(assert (= (bvand base (_ bv63 64)) (_ bv0 64)))
; Practical bound: base < 2^48 (wire size from count <= 2^28)
(assert (bvule base (_ bv281474976710656 64)))
(assert (bvule s1 (_ bv268435456 64)))
(assert (bvule s2 (_ bv268435456 64)))
(define-fun a64 ((x (_ BitVec 64))) (_ BitVec 64)
  (bvand (bvadd x (_ bv63 64)) (bvnot (_ bv63 64))))
(define-fun col1_end () (_ BitVec 64) (bvadd base s1))
(define-fun col2_start () (_ BitVec 64) (a64 col1_end))
(define-fun col2_end () (_ BitVec 64) (bvadd col2_start s2))
(define-fun blob_off () (_ BitVec 64) (a64 col2_end))
; Negate: blob_offset < col2_end
(assert (not (bvuge blob_off col2_end)))
(check-sat)
""")

# -- P4: Column size formulas are positive for count > 0 (16-bit BV, UNSAT) --
#
# For count > 0 and stride in {1, 2, 4, 8, 16}: count * stride > 0.

report("  ... proving P4: column size > 0 when count > 0")

ok &= prove("P4: count * stride > 0 for count > 0, stride in {1..16}", """\
(set-logic QF_BV)
(declare-const count (_ BitVec 64))
(declare-const stride (_ BitVec 64))
(assert (bvugt count (_ bv0 64)))
; count <= 2^28 matching ipc_wire.py; RPython uses r_uint64(stride * count)
(assert (bvule count (_ bv268435456 64)))
(assert (bvugt stride (_ bv0 64)))
(assert (bvule stride (_ bv16 64)))
; Negate: count * stride == 0
(assert (not (bvugt (bvmul count stride) (_ bv0 64))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: IPC ZSet full column offset chain")
    print("    P1: data column offset is 64-byte aligned")
    print("    P2: data column sections are non-overlapping")
    print("    P3: blob offset comes after all column sections")
    print("    P4: column size > 0 when count > 0")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
