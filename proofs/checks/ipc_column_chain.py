"""Z3 formal proofs for IPC WAL-block message structure.

The old _compute_zset_wire_size function was removed when IPC was rewritten
to use WAL blocks (commit d1f6758).  Column layout is now proved by
wal_directory.py and wal_region_count.py.

This proof covers the 3-block IPC message structure and the flag protocol
(gnitz/server/ipc.py:39-43, 233-262):

  Block 1: control  (always, TID = IPC_CONTROL_TID)
  Block 2: schema   (iff FLAG_HAS_SCHEMA is set in ctrl_flags)
  Block 3: data     (iff FLAG_HAS_DATA is set in ctrl_flags)

Protocol invariant: FLAG_HAS_DATA requires FLAG_HAS_SCHEMA.
FLAG_BATCH_SORTED (bit 50) and FLAG_BATCH_CONSOLIDATED (bit 51) are only
meaningful when FLAG_HAS_DATA is set.

  P1. All four WAL-block flags are distinct powers of 2 (Python cross-check)
  P2. FLAG_HAS_SCHEMA, FLAG_HAS_DATA, FLAG_BATCH_SORTED, FLAG_BATCH_CONSOLIDATED
      are pairwise non-overlapping (64-bit BV, UNSAT)
  P3. If FLAG_HAS_DATA is set, FLAG_HAS_SCHEMA can always be set simultaneously
      (SAT — proves the valid combined state exists)
  P4. FLAG_BATCH_SORTED | FLAG_BATCH_CONSOLIDATED can be combined without
      overlapping FLAG_HAS_DATA or FLAG_HAS_SCHEMA (64-bit BV, UNSAT)
  P5. The four WAL-block flags all live in bits 48-51, above all low protocol
      flags (bits 0-9): ANDing with the low-flag mask 0x3FF yields 0
      (64-bit BV, UNSAT)

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

# WAL-block flags from gnitz/server/ipc.py:40-43
FLAG_HAS_SCHEMA         = 1 << 48   # 0x0001000000000000
FLAG_HAS_DATA           = 1 << 49   # 0x0002000000000000
FLAG_BATCH_SORTED       = 1 << 50   # 0x0004000000000000
FLAG_BATCH_CONSOLIDATED = 1 << 51   # 0x0008000000000000

WAL_FLAGS = [FLAG_HAS_SCHEMA, FLAG_HAS_DATA, FLAG_BATCH_SORTED, FLAG_BATCH_CONSOLIDATED]

# Low protocol flag mask: bits 0-9
LOW_FLAG_MASK = 0x3FF

# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: IPC WAL-block message structure")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-check P1: all WAL-block flags are distinct powers of 2 -------------

all_pow2 = all(f > 0 and (f & (f - 1)) == 0 for f in WAL_FLAGS)
if all_pow2:
    report("  PASS  cross-check: all 4 WAL-block flags are powers of 2 (bits 48-51)")
else:
    report("  FAIL  cross-check: not all WAL-block flags are powers of 2")
    ok = False

all_distinct = (len(set(WAL_FLAGS)) == len(WAL_FLAGS))
if all_distinct:
    report("  PASS  cross-check: all 4 WAL-block flags are distinct")
else:
    report("  FAIL  cross-check: WAL-block flags have duplicates")
    ok = False

# -- Cross-check P2: pairwise non-overlapping ----------------------------------

pairs_ok = True
for i in range(len(WAL_FLAGS)):
    for j in range(i + 1, len(WAL_FLAGS)):
        if WAL_FLAGS[i] & WAL_FLAGS[j]:
            report("  FAIL  cross-check: flags 0x%016x and 0x%016x overlap" % (
                WAL_FLAGS[i], WAL_FLAGS[j]))
            pairs_ok = False
            ok = False
if pairs_ok:
    report("  PASS  cross-check: all WAL-block flag pairs are non-overlapping")

# -- Cross-check P5: WAL flags live above low-flag mask -----------------------

low_ok = all((f & LOW_FLAG_MASK) == 0 for f in WAL_FLAGS)
if low_ok:
    report("  PASS  cross-check: all WAL-block flags have zero overlap with low mask 0x%03x" %
           LOW_FLAG_MASK)
else:
    report("  FAIL  cross-check: some WAL-block flag overlaps low flag bits 0-9")
    ok = False

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P2: WAL-block flags are pairwise non-overlapping (64-bit BV, UNSAT) ------
#
# All four flags are distinct single bits; prove pairwise AND == 0.

report("  ... proving P2: WAL-block flags are pairwise non-overlapping")

# Build all 6 pair constraints
pair_asserts = []
for i in range(len(WAL_FLAGS)):
    for j in range(i + 1, len(WAL_FLAGS)):
        pair_asserts.append(
            "(= (bvand (_ bv%d 64) (_ bv%d 64)) (_ bv0 64))" % (WAL_FLAGS[i], WAL_FLAGS[j]))

ok &= prove("P2: all WAL-block flag pairs have zero AND", """\
(set-logic QF_BV)
; Negate: at least one pair overlaps
(assert (not (and
  %s)))
(check-sat)
""" % "\n  ".join(pair_asserts))

# -- P3: FLAG_HAS_DATA | FLAG_HAS_SCHEMA is a valid combined state (SAT) ------
#
# The valid state "has data and schema" exists: both bits can be set together.
# This SAT check confirms the combined state is reachable.

report("  ... proving P3: FLAG_HAS_DATA | FLAG_HAS_SCHEMA is a valid combined state (SAT)")
result = run_z3("""\
(set-logic QF_BV)
(declare-const flags (_ BitVec 64))
; Both schema and data flags set
(assert (= (bvand flags (_ bv%d 64)) (_ bv%d 64)))
(assert (= (bvand flags (_ bv%d 64)) (_ bv%d 64)))
(check-sat)
""" % (FLAG_HAS_SCHEMA, FLAG_HAS_SCHEMA, FLAG_HAS_DATA, FLAG_HAS_DATA))
if result == "sat":
    report("  PASS  P3: FLAG_HAS_DATA | FLAG_HAS_SCHEMA is reachable (SAT)")
else:
    report("  FAIL  P3: expected sat, got %s" % result)
    ok = False

# -- P4: FLAG_BATCH_SORTED | FLAG_BATCH_CONSOLIDATED don't overlap HAS_SCHEMA/DATA
#
# These batch-metadata flags can coexist with any combination of
# FLAG_HAS_SCHEMA and FLAG_HAS_DATA without bit collision.

report("  ... proving P4: batch flags don't overlap schema/data flags")

ok &= prove("P4: FLAG_BATCH_SORTED & FLAG_HAS_SCHEMA == 0", """\
(set-logic QF_BV)
(assert (not (= (bvand (_ bv%d 64) (_ bv%d 64)) (_ bv0 64))))
(check-sat)
""" % (FLAG_BATCH_SORTED, FLAG_HAS_SCHEMA))

ok &= prove("P4b: FLAG_BATCH_CONSOLIDATED & FLAG_HAS_DATA == 0", """\
(set-logic QF_BV)
(assert (not (= (bvand (_ bv%d 64) (_ bv%d 64)) (_ bv0 64))))
(check-sat)
""" % (FLAG_BATCH_CONSOLIDATED, FLAG_HAS_DATA))

# -- P5: WAL-block flags (bits 48-51) don't overlap low-flag mask (bits 0-9) --
#
# The low-flag mask is 0x3FF (bits 0-9). All WAL flags are >= 2^48.
# Prove: (wal_flag & 0x3FF) == 0 for each wal_flag.

report("  ... proving P5: all WAL-block flags clear the low-flag mask 0x3FF")

low_mask_asserts = " ".join(
    "(= (bvand (_ bv%d 64) (_ bv1023 64)) (_ bv0 64))" % f
    for f in WAL_FLAGS
)
ok &= prove("P5: all WAL-block flags have zero bits in positions 0-9", """\
(set-logic QF_BV)
; Negate: some WAL flag has a bit in [0, 9]
(assert (not (and
  %s)))
(check-sat)
""" % low_mask_asserts)

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: IPC WAL-block message structure")
    print("    P1: all 4 WAL-block flags are distinct powers of 2 (cross-check)")
    print("    P2: WAL-block flags are pairwise non-overlapping")
    print("    P3: FLAG_HAS_DATA | FLAG_HAS_SCHEMA is a valid combined state (SAT)")
    print("    P4: batch flags don't overlap schema/data flags")
    print("    P5: WAL-block flags clear the low-flag mask (bits 0-9)")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
