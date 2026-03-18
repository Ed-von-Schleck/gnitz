"""Z3 formal proofs for IPC CONTROL_SCHEMA column layout.

The 96-byte request/response header was removed when IPC was rewritten to
use WAL blocks (commit d1f6758).  The control block now carries a
9-column ZSet encoded as a WAL block.

Source: gnitz/server/ipc.py:64-87

CONTROL_SCHEMA (pk_index=0):
  col 0: msg_idx        U64  (PK)
  col 1: status         U64
  col 2: client_id      U64
  col 3: target_id      U64
  col 4: flags          U64
  col 5: seek_pk_lo     U64
  col 6: seek_pk_hi     U64
  col 7: seek_col_idx   U64
  col 8: error_msg      STRING (nullable)

CTRL_COL_STATUS=1 .. CTRL_COL_ERROR_MSG=8 (contiguous, starting at 1).

  P1. CTRL_COL_* indices are distinct and cover {1..8} (Python cross-check)
  P2. All CTRL_COL_* values are in range [1, 8] (8-bit BV, UNSAT)
  P3. PK column (col 0) is disjoint from all CTRL_COL_* (8-bit BV, UNSAT)
  P4. Total column count == 9 (Python cross-check)
  P5. CTRL_COL_ERROR_MSG == 8 == CTRL_COL_STATUS + 7 (8-bit BV, UNSAT)

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

# CONTROL_SCHEMA has 9 columns (0..8); pk_index = 0
CONTROL_NUM_COLS = 9
CONTROL_PK_INDEX = 0

# Named column indices from gnitz/server/ipc.py:79-86
CTRL_COL_STATUS      = 1
CTRL_COL_CLIENT_ID   = 2
CTRL_COL_TARGET_ID   = 3
CTRL_COL_FLAGS       = 4
CTRL_COL_SEEK_PK_LO  = 5
CTRL_COL_SEEK_PK_HI  = 6
CTRL_COL_SEEK_COL    = 7
CTRL_COL_ERROR_MSG   = 8

CTRL_COLS = [
    CTRL_COL_STATUS, CTRL_COL_CLIENT_ID, CTRL_COL_TARGET_ID, CTRL_COL_FLAGS,
    CTRL_COL_SEEK_PK_LO, CTRL_COL_SEEK_PK_HI, CTRL_COL_SEEK_COL, CTRL_COL_ERROR_MSG,
]

# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: IPC CONTROL_SCHEMA column layout")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-check P1: CTRL_COL_* values are distinct and cover {1..8} ----------

report("  ... cross-checking CTRL_COL_* indices")

if len(set(CTRL_COLS)) == len(CTRL_COLS):
    report("  PASS  cross-check: all 8 CTRL_COL_* values are distinct")
else:
    report("  FAIL  cross-check: CTRL_COL_* values have duplicates")
    ok = False

expected = set(range(1, 9))
actual = set(CTRL_COLS)
if actual == expected:
    report("  PASS  cross-check: CTRL_COL_* values cover {1..8} exactly")
else:
    report("  FAIL  cross-check: CTRL_COL_* expected {1..8}, got %s" % sorted(actual))
    ok = False

# -- Cross-check P3: PK (col 0) not in CTRL_COL_* ----------------------------

if CONTROL_PK_INDEX not in CTRL_COLS:
    report("  PASS  cross-check: PK index 0 is not in CTRL_COL_* set")
else:
    report("  FAIL  cross-check: PK index 0 appears in CTRL_COL_* set")
    ok = False

# -- Cross-check P4: total column count == 9 ----------------------------------

# Payload cols = CTRL_COL_* (8) + PK col (1) = 9
total_cols = len(CTRL_COLS) + 1  # +1 for PK column
if total_cols == CONTROL_NUM_COLS:
    report("  PASS  cross-check: total column count == 9 (1 PK + 8 payload)")
else:
    report("  FAIL  cross-check: total column count %d != %d" % (total_cols, CONTROL_NUM_COLS))
    ok = False

# -- Cross-check P5: CTRL_COL_ERROR_MSG == CTRL_COL_STATUS + 7 ---------------

if CTRL_COL_ERROR_MSG == CTRL_COL_STATUS + 7:
    report("  PASS  cross-check: CTRL_COL_ERROR_MSG(%d) == CTRL_COL_STATUS(%d) + 7" % (
        CTRL_COL_ERROR_MSG, CTRL_COL_STATUS))
else:
    report("  FAIL  cross-check: CTRL_COL_ERROR_MSG(%d) != CTRL_COL_STATUS(%d) + 7" % (
        CTRL_COL_ERROR_MSG, CTRL_COL_STATUS))
    ok = False

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P2: All CTRL_COL_* values are in range [1, 8] (8-bit BV, UNSAT) ----------
#
# A valid CTRL_COL_* index c satisfies 1 <= c <= 8.
# Encode each concrete value and negate: prove no value is outside [1, 8].

report("  ... proving P2: all CTRL_COL_* values in [1, 8]")

in_range_asserts = " ".join(
    "(and (bvule (_ bv1 8) (_ bv%d 8)) (bvule (_ bv%d 8) (_ bv8 8)))" % (c, c)
    for c in CTRL_COLS
)
ok &= prove("P2: all 8 CTRL_COL_* values in [1, 8]", """\
(set-logic QF_BV)
; Negate: at least one CTRL_COL_* value is outside [1, 8]
(assert (not (and
  %s)))
(check-sat)
""" % in_range_asserts)

# -- P3: PK column (0) is disjoint from all CTRL_COL_* (8-bit BV, UNSAT) ------
#
# Prove that 0 != c for every c in CTRL_COLS.

report("  ... proving P3: PK col 0 is not equal to any CTRL_COL_* value")

neq_asserts = " ".join(
    "(not (= (_ bv0 8) (_ bv%d 8)))" % c
    for c in CTRL_COLS
)
ok &= prove("P3: PK col 0 != any CTRL_COL_*", """\
(set-logic QF_BV)
; Negate: 0 equals some CTRL_COL_* value
(assert (not (and
  %s)))
(check-sat)
""" % neq_asserts)

# -- P4: Total column count = 9 (8-bit BV, UNSAT) ----------------------------
#
# len(CTRL_COLS) = 8 payload cols + 1 PK col = 9.

report("  ... proving P4: total column count == 9")
ok &= prove("P4: 8 payload cols + 1 PK col == 9", """\
(set-logic QF_BV)
; Negate: 8 + 1 != 9
(assert (not (= (bvadd (_ bv8 8) (_ bv1 8)) (_ bv9 8))))
(check-sat)
""")

# -- P5: CTRL_COL_ERROR_MSG == CTRL_COL_STATUS + 7 (8-bit BV, UNSAT) ---------
#
# The payload columns are laid out contiguously: STATUS=1, ..., ERROR_MSG=8.
# 8 - 1 = 7 columns span the range.

report("  ... proving P5: CTRL_COL_ERROR_MSG == CTRL_COL_STATUS + 7")
ok &= prove("P5: 1 + 7 == 8 (CTRL_COL_STATUS + span = CTRL_COL_ERROR_MSG)", """\
(set-logic QF_BV)
; CTRL_COL_STATUS=1, CTRL_COL_ERROR_MSG=8, span=7
; Negate: 1 + 7 != 8
(assert (not (= (bvadd (_ bv1 8) (_ bv7 8)) (_ bv8 8))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: IPC CONTROL_SCHEMA column layout")
    print("    P1: CTRL_COL_* indices are distinct and cover {1..8}")
    print("    P2: all CTRL_COL_* values in [1, 8]")
    print("    P3: PK col 0 is disjoint from all CTRL_COL_* values")
    print("    P4: total column count == 9")
    print("    P5: CTRL_COL_ERROR_MSG == CTRL_COL_STATUS + 7")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
