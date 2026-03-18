"""Z3 formal proofs for IPC META_SCHEMA column layout.

The old bespoke IPC string encoding (IPC_STRING_STRIDE, IPC_NULL_STRING_OFFSET,
global_blob_offset) was removed when IPC was rewritten to use WAL blocks
(commit d1f6758).  String columns are now encoded by the WAL columnar encoder
(wal_columnar.py), whose alignment properties are proved in wal_directory.py.

This proof covers the META_SCHEMA used to transmit schema information inside
the schema WAL block (gnitz/server/ipc.py:53-61).

META_SCHEMA (pk_index=0):
  col 0: col_idx   U64 (PK)
  col 1: type_code U64
  col 2: flags     U64
  col 3: name      STRING

META_FLAG_NULLABLE = 1 (bit 0)
META_FLAG_IS_PK    = 2 (bit 1)

  P1. META_SCHEMA has exactly 4 columns (Python cross-check)
  P2. META_FLAG_NULLABLE and META_FLAG_IS_PK are distinct powers of 2 (Python)
  P3. META_FLAG_NULLABLE & META_FLAG_IS_PK == 0 (8-bit BV, UNSAT)
  P4. Any col_idx that fits in [0, 3] is a valid META_SCHEMA column index
      (8-bit BV, UNSAT — all values in [0,3] are < 4)
  P5. col_idx PK (col 0) is distinct from type_code (1), flags (2), name (3)
      (8-bit BV, UNSAT)

5 Z3 queries + ~5 cross-checks.  Runs under PyPy2.
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

# META_SCHEMA column indices (gnitz/server/ipc.py:55-60)
META_COL_IDX       = 0   # col_idx  U64  PK
META_COL_TYPE_CODE = 1   # type_code U64
META_COL_FLAGS     = 2   # flags    U64
META_COL_NAME      = 3   # name     STRING
META_NUM_COLS      = 4
META_PK_INDEX      = 0

# Meta flags from gnitz/server/ipc.py:50-51
META_FLAG_NULLABLE = 1
META_FLAG_IS_PK    = 2

META_COLS = [META_COL_IDX, META_COL_TYPE_CODE, META_COL_FLAGS, META_COL_NAME]

# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: IPC META_SCHEMA column layout")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-check P1: META_SCHEMA has exactly 4 columns ------------------------

if len(META_COLS) == META_NUM_COLS:
    report("  PASS  cross-check: META_SCHEMA has exactly 4 columns")
else:
    report("  FAIL  cross-check: expected 4 columns, got %d" % len(META_COLS))
    ok = False

if len(set(META_COLS)) == len(META_COLS):
    report("  PASS  cross-check: all 4 META_SCHEMA column indices are distinct")
else:
    report("  FAIL  cross-check: META_SCHEMA column indices have duplicates")
    ok = False

# -- Cross-check P2: META flags are distinct powers of 2 ---------------------

for name, val in [("META_FLAG_NULLABLE", META_FLAG_NULLABLE), ("META_FLAG_IS_PK", META_FLAG_IS_PK)]:
    if val > 0 and (val & (val - 1)) == 0:
        report("  PASS  cross-check: %s=%d is a power of 2" % (name, val))
    else:
        report("  FAIL  cross-check: %s=%d is not a power of 2" % (name, val))
        ok = False

if META_FLAG_NULLABLE != META_FLAG_IS_PK:
    report("  PASS  cross-check: META_FLAG_NULLABLE != META_FLAG_IS_PK")
else:
    report("  FAIL  cross-check: META flags are equal")
    ok = False

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P3: META_FLAG_NULLABLE & META_FLAG_IS_PK == 0 (8-bit BV, UNSAT) ---------
#
# 1 & 2 == 0: bit 0 and bit 1 are disjoint.

report("  ... proving P3: META_FLAG_NULLABLE(1) & META_FLAG_IS_PK(2) == 0")
ok &= prove("P3: 1 & 2 == 0", """\
(set-logic QF_BV)
; Negate: they share a bit
(assert (not (= (bvand (_ bv1 8) (_ bv2 8)) (_ bv0 8))))
(check-sat)
""")

# -- P4: All values in [0, 3] are valid META_SCHEMA column indices (8-bit BV) -
#
# A symbolic col c in [0, META_NUM_COLS) = [0, 4) is a valid column index.
# Prove c < 4 for all c in [0, 3].

report("  ... proving P4: valid column index range [0, 3] is within [0, META_NUM_COLS)")
ok &= prove("P4: c in [0,3] implies c < 4 (META_NUM_COLS)", """\
(set-logic QF_BV)
(declare-const c (_ BitVec 8))
(assert (bvule c (_ bv3 8)))
; Negate: c >= META_NUM_COLS (4)
(assert (not (bvult c (_ bv4 8))))
(check-sat)
""")

# -- P5: PK col (0) is distinct from all payload cols (1, 2, 3) ---------------
#
# col_idx (PK) at 0 must not collide with type_code (1), flags (2), name (3).

report("  ... proving P5: PK col 0 is distinct from payload cols 1, 2, 3")
ok &= prove("P5: 0 != 1 AND 0 != 2 AND 0 != 3", """\
(set-logic QF_BV)
; Negate: 0 equals one of {1, 2, 3}
(assert (not (and
  (not (= (_ bv0 8) (_ bv1 8)))
  (not (= (_ bv0 8) (_ bv2 8)))
  (not (= (_ bv0 8) (_ bv3 8))))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: IPC META_SCHEMA column layout")
    print("    P1: META_SCHEMA has exactly 4 columns (cross-check)")
    print("    P2: META_FLAG_NULLABLE and META_FLAG_IS_PK are distinct powers of 2")
    print("    P3: META_FLAG_NULLABLE(1) & META_FLAG_IS_PK(2) == 0")
    print("    P4: valid column index range [0, 3] is within [0, 4)")
    print("    P5: PK col 0 is distinct from all payload column indices")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
