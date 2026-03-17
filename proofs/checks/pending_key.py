"""Z3 formal proofs for DAG pending key bijectivity.

Proves four properties of gnitz/server/executor.py _pending_key():
  P1. Injectivity under width constraints (view_id < 2^32, source_id < 2^32)
  P2. Roundtrip (shift/mask extraction recovers original fields)
  P3. Masking is identity for source_id < 2^32
  P4. Counterexample: collision if view_id >= 2^32 (SAT)

Source:
  - gnitz/server/executor.py:69-71

  def _pending_key(view_id, source_id):
      return (view_id << 32) | (source_id & 0xFFFFFFFF)

Used as dict key in evaluate_dag.  Collision -> lost pending deltas -> wrong
query results.

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


def check_sat(label, smt_text):
    """Run a query expecting sat. Returns True on success."""
    result = run_z3(smt_text)
    if result == "sat":
        report("  PASS  %s" % label)
        return True
    else:
        report("  FAIL  %s: expected sat, got %s" % (label, result))
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
    return "0x%016x" % (n & MASK64)


# -- Constants ----------------------------------------------------------------

MASK32 = 0xFFFFFFFF
MASK64 = (1 << 64) - 1


# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: DAG pending key bijectivity")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-check: validate Z3 encoding against Python ------------------------

report("  ... cross-checking _pending_key against Python")

def py_pending_key(view_id, source_id):
    return ((view_id << 32) | (source_id & MASK32)) & MASK64

cross_check_vectors = [
    (0, 0),
    (1, 0),
    (0, 1),
    (MASK32, MASK32),
    (100, 200),
    (0, MASK32),
]

for view_id, source_id in cross_check_vectors:
    py_val = py_pending_key(view_id, source_id)
    # Z3: (bvor (bvshl v 32) (bvand s 0xFFFFFFFF))
    smt_query = "(simplify (bvor (bvshl (_ bv%d 64) (_ bv32 64)) (bvand (_ bv%d 64) #x00000000ffffffff)))" % (
        view_id & MASK64, source_id & MASK64)
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check _pending_key(%d, %d): unexpected Z3 output: %s" % (
            view_id, source_id, z3_out))
        ok = False
    elif (int(py_val) & MASK64) == (int(z3_val) & MASK64):
        # Verify roundtrip
        unpack_vid = (py_val >> 32) & MASK32
        unpack_sid = py_val & MASK32
        if view_id < (1 << 32) and unpack_vid == view_id and unpack_sid == source_id:
            report("  PASS  cross-check _pending_key(%d, %d) -> %s [roundtrip OK]" % (
                view_id, source_id, fmt64(py_val)))
        elif view_id >= (1 << 32):
            # Roundtrip not expected to work without width constraint
            report("  PASS  cross-check _pending_key(%d, %d) -> %s [no roundtrip, view_id >= 2^32]" % (
                view_id, source_id, fmt64(py_val)))
        else:
            report("  FAIL  cross-check _pending_key(%d, %d): roundtrip failed vid=%d sid=%d" % (
                view_id, source_id, unpack_vid, unpack_sid))
            ok = False
    else:
        report("  FAIL  cross-check _pending_key(%d, %d): Python=%s Z3=%s" % (
            view_id, source_id, fmt64(py_val), fmt64(z3_val)))
        ok = False

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P1: Injectivity under width constraints ----------------------------------
#
# view_id < 2^32 and source_id < 2^32.
# key = (view_id << 32) | (source_id & 0xFFFFFFFF)
# Assert same key but different inputs -> unsat.

report("  ... proving P1: _pending_key injectivity")
ok &= prove("P1: _pending_key is injective under width constraints", """\
(set-logic QF_BV)
(declare-const v1 (_ BitVec 64))
(declare-const s1 (_ BitVec 64))
(declare-const v2 (_ BitVec 64))
(declare-const s2 (_ BitVec 64))
; Width constraints
(assert (bvult v1 #x0000000100000000))
(assert (bvult v2 #x0000000100000000))
(assert (bvult s1 #x0000000100000000))
(assert (bvult s2 #x0000000100000000))
; Same key
(define-fun k1 () (_ BitVec 64) (bvor (bvshl v1 (_ bv32 64)) (bvand s1 #x00000000ffffffff)))
(define-fun k2 () (_ BitVec 64) (bvor (bvshl v2 (_ bv32 64)) (bvand s2 #x00000000ffffffff)))
(assert (= k1 k2))
; Different inputs
(assert (not (and (= v1 v2) (= s1 s2))))
(check-sat)
""")

# -- P2: Roundtrip -------------------------------------------------------------
#
# Extract: view_id = key >> 32, source_id = key & 0xFFFFFFFF
# Assert NOT(recovered == original) -> unsat.

report("  ... proving P2: _pending_key roundtrip")
ok &= prove("P2: _pending_key roundtrip extraction", """\
(set-logic QF_BV)
(declare-const vid (_ BitVec 64))
(declare-const sid (_ BitVec 64))
; Width constraints
(assert (bvult vid #x0000000100000000))
(assert (bvult sid #x0000000100000000))
(define-fun key () (_ BitVec 64) (bvor (bvshl vid (_ bv32 64)) (bvand sid #x00000000ffffffff)))
; Negate: NOT (extracted_vid == vid AND extracted_sid == sid)
(assert (not (and
  (= (bvlshr key (_ bv32 64)) vid)
  (= (bvand key #x00000000ffffffff) sid))))
(check-sat)
""")

# -- P3: Masking is identity for source_id < 2^32 ----------------------------
#
# source_id & 0xFFFFFFFF == source_id when source_id < 2^32.

report("  ... proving P3: masking identity for source_id < 2^32")
ok &= prove("P3: (sid & 0xFFFFFFFF) == sid when sid < 2^32", """\
(set-logic QF_BV)
(declare-const sid (_ BitVec 64))
(assert (bvult sid #x0000000100000000))
(assert (not (= (bvand sid #x00000000ffffffff) sid)))
(check-sat)
""")

# -- P4: Counterexample — collision if view_id >= 2^32 (SAT) -----------------
#
# Without the width constraint on view_id, the left shift truncates bits,
# causing collisions.

report("  ... proving P4: collision if view_id >= 2^32 (SAT)")
ok &= check_sat("P4: collision exists without view_id width constraint", """\
(set-logic QF_BV)
(declare-const v1 (_ BitVec 64))
(declare-const s1 (_ BitVec 64))
(declare-const v2 (_ BitVec 64))
(declare-const s2 (_ BitVec 64))
; No width constraint on view_ids
(assert (bvult s1 #x0000000100000000))
(assert (bvult s2 #x0000000100000000))
; At least one view_id >= 2^32
(assert (bvuge v1 #x0000000100000000))
; Same key
(define-fun k1 () (_ BitVec 64) (bvor (bvshl v1 (_ bv32 64)) (bvand s1 #x00000000ffffffff)))
(define-fun k2 () (_ BitVec 64) (bvor (bvshl v2 (_ bv32 64)) (bvand s2 #x00000000ffffffff)))
(assert (= k1 k2))
; Different inputs
(assert (not (and (= v1 v2) (= s1 s2))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: DAG pending key bijectivity")
    print("    P1: _pending_key injectivity under width constraints")
    print("    P2: _pending_key roundtrip extraction")
    print("    P3: masking identity for source_id < 2^32")
    print("    P4: collision exists without view_id width constraint (counterexample)")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
