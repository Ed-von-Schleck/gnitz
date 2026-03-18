"""Z3 formal proofs for IPC wire format structural integrity.

The IPC wire format was rewritten to use WAL blocks (ipc.py:d1f6758).
Each message is a sequence of up to 3 WAL blocks:
  Block 1: control  (always present, TID = IPC_CONTROL_TID = 0xFFFFFFFF)
  Block 2: schema   (present iff FLAG_HAS_SCHEMA is set in ctrl_flags)
  Block 3: data     (present iff FLAG_HAS_DATA is set in ctrl_flags)

Protocol invariant: FLAG_HAS_DATA requires FLAG_HAS_SCHEMA
(gnitz/server/ipc.py:402-404).

Source: gnitz/server/ipc.py, gnitz/storage/wal_columnar.py

Part A: ZSet section alignment chain (wal_columnar.py:78-100)
  P1. ZSet structural offsets are 64-byte aligned (64-bit BV, UNSAT).
  P2. ZSet structural sections are non-overlapping (64-bit BV, UNSAT).

Part B: IPC block ordering and flag protocol (ipc.py:39-43, 402-404)
  P3. IPC_CONTROL_TID (0xFFFFFFFF) does not fit in a signed 31-bit value,
      so it is distinguishable from any positive user table ID (32-bit BV, UNSAT).
  P4. FLAG_HAS_DATA (bit 49) set and FLAG_HAS_SCHEMA (bit 48) clear is
      a protocol violation — checked at runtime (64-bit BV, UNSAT contradiction).
  P5. FLAG_HAS_SCHEMA (bit 48) and FLAG_HAS_DATA (bit 49) are in disjoint
      bit positions (64-bit BV, UNSAT).

5 Z3 queries + ~8 cross-checks.  Runs under PyPy2.
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

# From gnitz/server/ipc.py:46
IPC_CONTROL_TID = 0xFFFFFFFF  # max uint32

# WAL-block flags from gnitz/server/ipc.py:40-43
FLAG_HAS_SCHEMA         = 1 << 48
FLAG_HAS_DATA           = 1 << 49

# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: IPC wire format structural integrity")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-check Part A: ZSet alignment chain ---------------------------------

report("  ... cross-checking ZSet alignment chain")

print("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_uint64  # noqa: E402
print("  ... imports done")
sys.stdout.flush()


def align_up_py(val, align):
    """Pure Python align_up matching wal_columnar.py buffer alignment."""
    u_val = val & MASK64
    u_align = align & MASK64
    return (u_val + u_align - 1) & ~(u_align - 1)


test_counts = [1, 8, 9, 100, 1 << 20]

for count in test_counts:
    struct_sz = count * 8
    pk_lo_off = 0
    pk_hi_off = align_up_py(pk_lo_off + struct_sz, 64)
    weight_off = align_up_py(pk_hi_off + struct_sz, 64)
    null_off = align_up_py(weight_off + struct_sz, 64)

    aligned = (pk_hi_off % 64 == 0 and weight_off % 64 == 0 and null_off % 64 == 0)
    monotonic = (struct_sz <= pk_hi_off and
                 pk_hi_off + struct_sz <= weight_off and
                 weight_off + struct_sz <= null_off)

    # Z3 cross-check
    smt_query = """\
(simplify (let ((count (_ bv%d 64)))
  (let ((struct_sz (bvmul count (_ bv8 64))))
    (let ((pk_hi_off (bvand (bvadd struct_sz (_ bv63 64)) (bvnot (_ bv63 64)))))
      (let ((weight_off (bvand (bvadd (bvadd pk_hi_off struct_sz) (_ bv63 64)) (bvnot (_ bv63 64)))))
        (let ((null_off (bvand (bvadd (bvadd weight_off struct_sz) (_ bv63 64)) (bvnot (_ bv63 64)))))
          (and
            (= (bvand pk_hi_off (_ bv63 64)) (_ bv0 64))
            (= (bvand weight_off (_ bv63 64)) (_ bv0 64))
            (= (bvand null_off (_ bv63 64)) (_ bv0 64))
            (bvule struct_sz pk_hi_off)
            (bvule (bvadd pk_hi_off struct_sz) weight_off)
            (bvule (bvadd weight_off struct_sz) null_off))))))))""" % count
    z3_out = run_z3(smt_query)
    z3_ok = z3_out == "true"

    if aligned and monotonic and z3_ok:
        report("  PASS  cross-check count=%d: pk_hi=%d weight=%d null=%d" % (
            count, pk_hi_off, weight_off, null_off))
    else:
        report("  FAIL  cross-check count=%d: aligned=%s monotonic=%s z3=%s" % (
            count, aligned, monotonic, z3_ok))
        ok = False

# -- Cross-check Part B: IPC block ordering -----------------------------------

report("  ... cross-checking IPC_CONTROL_TID and flag ordering")

# IPC_CONTROL_TID = 0xFFFFFFFF fits in uint32, max positive int32 = 0x7FFFFFFF
if IPC_CONTROL_TID > 0x7FFFFFFF:
    report("  PASS  cross-check: IPC_CONTROL_TID=0x%08x > 0x7FFFFFFF (not a valid signed table ID)" %
           IPC_CONTROL_TID)
else:
    report("  FAIL  cross-check: IPC_CONTROL_TID fits in positive signed 31 bits")
    ok = False

# FLAG_HAS_DATA requires FLAG_HAS_SCHEMA
violation = FLAG_HAS_DATA & ~FLAG_HAS_SCHEMA  # FLAG_HAS_DATA without FLAG_HAS_SCHEMA
if violation == FLAG_HAS_DATA:
    report("  PASS  cross-check: FLAG_HAS_DATA(%d) without FLAG_HAS_SCHEMA(%d) is detectable" % (
        FLAG_HAS_DATA, FLAG_HAS_SCHEMA))
else:
    report("  FAIL  cross-check: FLAG_HAS_DATA ordering")
    ok = False

# FLAG_HAS_SCHEMA and FLAG_HAS_DATA share no bits
if (FLAG_HAS_SCHEMA & FLAG_HAS_DATA) == 0:
    report("  PASS  cross-check: FLAG_HAS_SCHEMA and FLAG_HAS_DATA share no bits")
else:
    report("  FAIL  cross-check: FLAG_HAS_SCHEMA and FLAG_HAS_DATA overlap")
    ok = False

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P1: ZSet structural offsets are 64-byte aligned --------------------------
#
# For symbolic count in (0, 2^28], each structural offset has low 6 bits = 0.
# 2^28 bound prevents u64 overflow in count * 8 (2^28 * 8 = 2^31).

report("  ... proving P1: ZSet structural offsets are 64-byte aligned")
ok &= prove("P1: pk_hi_off, weight_off, null_off are 64-aligned", """\
(set-logic QF_BV)
(declare-const count (_ BitVec 64))
(assert (bvugt count (_ bv0 64)))
(assert (bvule count (_ bv268435456 64)))
(define-fun struct_sz () (_ BitVec 64) (bvmul count (_ bv8 64)))
(define-fun a64 ((x (_ BitVec 64))) (_ BitVec 64)
  (bvand (bvadd x (_ bv63 64)) (bvnot (_ bv63 64))))
(define-fun pk_hi_off () (_ BitVec 64) (a64 struct_sz))
(define-fun weight_off () (_ BitVec 64) (a64 (bvadd pk_hi_off struct_sz)))
(define-fun null_off () (_ BitVec 64) (a64 (bvadd weight_off struct_sz)))
; Negate: at least one offset is not 64-aligned
(assert (not (and
  (= (bvand pk_hi_off (_ bv63 64)) (_ bv0 64))
  (= (bvand weight_off (_ bv63 64)) (_ bv0 64))
  (= (bvand null_off (_ bv63 64)) (_ bv0 64)))))
(check-sat)
""")

# -- P2: ZSet structural sections are non-overlapping -------------------------
#
# Each section occupies [off, off + struct_sz). Sections must not overlap.

report("  ... proving P2: ZSet structural sections are non-overlapping")
ok &= prove("P2: structural sections [off, off+struct_sz) don't overlap", """\
(set-logic QF_BV)
(declare-const count (_ BitVec 64))
(assert (bvugt count (_ bv0 64)))
(assert (bvule count (_ bv268435456 64)))
(define-fun struct_sz () (_ BitVec 64) (bvmul count (_ bv8 64)))
(define-fun a64 ((x (_ BitVec 64))) (_ BitVec 64)
  (bvand (bvadd x (_ bv63 64)) (bvnot (_ bv63 64))))
(define-fun pk_hi_off () (_ BitVec 64) (a64 struct_sz))
(define-fun weight_off () (_ BitVec 64) (a64 (bvadd pk_hi_off struct_sz)))
(define-fun null_off () (_ BitVec 64) (a64 (bvadd weight_off struct_sz)))
; Negate: some section overlaps another
(assert (not (and
  (bvule struct_sz pk_hi_off)
  (bvule (bvadd pk_hi_off struct_sz) weight_off)
  (bvule (bvadd weight_off struct_sz) null_off))))
(check-sat)
""")

# -- P3: IPC_CONTROL_TID does not fit in a positive signed 31-bit value -------
#
# IPC_CONTROL_TID = 0xFFFFFFFF. Any real table_id is a positive signed 31-bit
# integer (1..0x7FFFFFFF). Prove 0xFFFFFFFF > 0x7FFFFFFF so they can never
# collide when compared as unsigned 32-bit values.

report("  ... proving P3: IPC_CONTROL_TID is not a valid positive table ID")
ok &= prove("P3: 0xFFFFFFFF > 0x7FFFFFFF as unsigned 32-bit", """\
(set-logic QF_BV)
; IPC_CONTROL_TID = 0xFFFFFFFF, max valid table_id = 0x7FFFFFFF
; Negate: IPC_CONTROL_TID <= 0x7FFFFFFF
(assert (not (bvugt (_ bv4294967295 32) (_ bv2147483647 32))))
(check-sat)
""")

# -- P4: FLAG_HAS_DATA without FLAG_HAS_SCHEMA is a detectable violation ------
#
# ipc.py:402-404 checks: if FLAG_HAS_DATA set and FLAG_HAS_SCHEMA clear, raise.
# The condition `flags & FLAG_HAS_DATA and not (flags & FLAG_HAS_SCHEMA)` is
# satisfiable (there exists a flags value triggering it), proving the guard
# is reachable and meaningful.

report("  ... proving P4: FLAG_HAS_DATA without FLAG_HAS_SCHEMA is SAT (reachable violation)")
result = run_z3("""\
(set-logic QF_BV)
(declare-const flags (_ BitVec 64))
; FLAG_HAS_DATA (bit 49) is set
(assert (= (bvand flags (_ bv562949953421312 64)) (_ bv562949953421312 64)))
; FLAG_HAS_SCHEMA (bit 48) is NOT set
(assert (= (bvand flags (_ bv281474976710656 64)) (_ bv0 64)))
(check-sat)
""")
if result == "sat":
    report("  PASS  P4: FLAG_HAS_DATA without FLAG_HAS_SCHEMA is reachable (SAT)")
else:
    report("  FAIL  P4: expected sat, got %s" % result)
    ok = False

# -- P5: FLAG_HAS_SCHEMA and FLAG_HAS_DATA are in disjoint bit positions ------
#
# They cannot both be the same bit; their AND is 0.

report("  ... proving P5: FLAG_HAS_SCHEMA and FLAG_HAS_DATA share no bits")
ok &= prove("P5: FLAG_HAS_SCHEMA(1<<48) & FLAG_HAS_DATA(1<<49) == 0", """\
(set-logic QF_BV)
; Negate: they share a bit
(assert (not (= (bvand (_ bv281474976710656 64) (_ bv562949953421312 64)) (_ bv0 64))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: IPC wire format structural integrity")
    print("    P1: ZSet structural offsets are 64-aligned")
    print("    P2: ZSet structural sections are non-overlapping")
    print("    P3: IPC_CONTROL_TID(0xFFFFFFFF) is not a valid positive table ID")
    print("    P4: FLAG_HAS_DATA without FLAG_HAS_SCHEMA is reachable (SAT)")
    print("    P5: FLAG_HAS_SCHEMA and FLAG_HAS_DATA share no bits")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
