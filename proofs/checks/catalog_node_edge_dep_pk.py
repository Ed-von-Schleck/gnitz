"""Z3 formal proofs for pack_node_pk / pack_edge_pk / pack_dep_pk injectivity.

Proves four properties of gnitz/catalog/system_tables.py lines 43-58:
  P1. Roundtrip: extract(pack(v, x)) recovers (v, x)
  P2. Injectivity: distinct (v1, x1) != (v2, x2) -> distinct packed values
  P3. Upper 64 bits of packed value equal view_id
  P4. Lower 64 bits of packed value equal the object id

All three pack functions (pack_node_pk, pack_edge_pk, pack_dep_pk) use the
identical formula: (r_uint128(r_uint64(view_id)) << 64) | r_uint128(r_uint64(x))
so P1-P4 cover all three simultaneously.  A collision in any of these would
silently merge distinct system table entries (NodeTab, EdgeTab, DepTab).

4 Z3 queries + 6 cross-checks.  Runs under PyPy2.
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


def fmt128(n):
    return "0x%032x" % (n & MASK128)


# -- Constants ----------------------------------------------------------------

MASK64 = (1 << 64) - 1
MASK128 = (1 << 128) - 1


# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: pack_node_pk / pack_edge_pk / pack_dep_pk")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-check: validate Z3 encoding against RPython -----------------------

report("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_uint64                      # noqa: E402
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128  # noqa: E402
report("  ... imports done")

report("  ... cross-checking pack_node_pk / pack_edge_pk / pack_dep_pk against RPython")


def rpy_pack(view_id, x):
    """Shared formula for pack_node_pk, pack_edge_pk, pack_dep_pk."""
    return int((r_uint128(r_uint64(view_id)) << 64) | r_uint128(r_uint64(x))) & MASK128


vectors = [
    (0, 0),
    (1, 0),
    (0, 1),
    ((1 << 32) - 1, (1 << 32) - 1),
    (1, (1 << 32) - 1),
]

packed_vals = []
for view_id, x in vectors:
    rpy_val = rpy_pack(view_id, x)
    smt_query = (
        "(simplify (bvor"
        " (bvshl ((_ zero_extend 64) (_ bv%d 64)) (_ bv64 128))"
        " ((_ zero_extend 64) (_ bv%d 64))))" % (view_id & MASK64, x & MASK64)
    )
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check pack(%d, %d): unexpected Z3 output: %s" % (
            view_id, x, z3_out))
        ok = False
    elif (int(rpy_val) & MASK128) == (int(z3_val) & MASK128):
        # Verify unpack roundtrip
        unpack_v = (rpy_val >> 64) & MASK64
        unpack_x = rpy_val & MASK64
        if unpack_v == view_id and unpack_x == x:
            report("  PASS  cross-check pack(%d, %d) -> %s [roundtrip OK]" % (
                view_id, x, fmt128(rpy_val)))
        else:
            report("  FAIL  cross-check pack(%d, %d): roundtrip failed v=%d x=%d" % (
                view_id, x, unpack_v, unpack_x))
            ok = False
    else:
        report("  FAIL  cross-check pack(%d, %d): RPython=%s Z3=%s" % (
            view_id, x, fmt128(rpy_val), fmt128(z3_val)))
        ok = False
    packed_vals.append(rpy_val)

# Pairwise distinctness check
n = len(packed_vals)
all_distinct = True
for i in range(n):
    for j in range(i + 1, n):
        if (int(packed_vals[i]) & MASK128) == (int(packed_vals[j]) & MASK128):
            report("  FAIL  cross-check: packed_vals[%d] == packed_vals[%d] (collision!)" % (i, j))
            ok = False
            all_distinct = False
if all_distinct:
    report("  PASS  cross-check: all 5 packed values are distinct")

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P1: Roundtrip: extract(pack(v, x)) recovers (v, x) ----------------------
#
# pack(v, x) = (zero_extend(v) << 64) | zero_extend(x)
# extract[127:64](pack) == v  and  extract[63:0](pack) == x

report("  ... proving P1: roundtrip extract(pack(v, x)) == (v, x)")
ok &= prove("P1: roundtrip extract(pack(v, x)) == (v, x)", """\
(set-logic QF_BV)
(declare-const v (_ BitVec 64))
(declare-const x (_ BitVec 64))
(define-fun pk () (_ BitVec 128)
  (bvor (bvshl ((_ zero_extend 64) v) (_ bv64 128))
        ((_ zero_extend 64) x)))
(assert (not (and (= ((_ extract 127 64) pk) v)
                  (= ((_ extract 63 0) pk) x))))
(check-sat)
""")

# -- P2: Injectivity: distinct (v1, x1) != (v2, x2) -> distinct packed values

report("  ... proving P2: pack is injective")
ok &= prove("P2: pack(v, x) is injective over full 64-bit inputs", """\
(set-logic QF_BV)
(declare-const v1 (_ BitVec 64)) (declare-const x1 (_ BitVec 64))
(declare-const v2 (_ BitVec 64)) (declare-const x2 (_ BitVec 64))
(define-fun pk1 () (_ BitVec 128)
  (bvor (bvshl ((_ zero_extend 64) v1) (_ bv64 128)) ((_ zero_extend 64) x1)))
(define-fun pk2 () (_ BitVec 128)
  (bvor (bvshl ((_ zero_extend 64) v2) (_ bv64 128)) ((_ zero_extend 64) x2)))
(assert (= pk1 pk2))
(assert (not (and (= v1 v2) (= x1 x2))))
(check-sat)
""")

# -- P3: Upper 64 bits carry view_id ------------------------------------------

report("  ... proving P3: extract[127:64](pack(v, x)) == v")
ok &= prove("P3: upper 64 bits of packed value equal view_id", """\
(set-logic QF_BV)
(declare-const v (_ BitVec 64)) (declare-const x (_ BitVec 64))
(define-fun pk () (_ BitVec 128)
  (bvor (bvshl ((_ zero_extend 64) v) (_ bv64 128)) ((_ zero_extend 64) x)))
(assert (not (= ((_ extract 127 64) pk) v)))
(check-sat)
""")

# -- P4: Lower 64 bits carry the object id ------------------------------------

report("  ... proving P4: extract[63:0](pack(v, x)) == x")
ok &= prove("P4: lower 64 bits of packed value equal the object id", """\
(set-logic QF_BV)
(declare-const v (_ BitVec 64)) (declare-const x (_ BitVec 64))
(define-fun pk () (_ BitVec 128)
  (bvor (bvshl ((_ zero_extend 64) v) (_ bv64 128)) ((_ zero_extend 64) x)))
(assert (not (= ((_ extract 63 0) pk) x)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: pack_node_pk / pack_edge_pk / pack_dep_pk")
    print("    P1: roundtrip extract(pack(v, x)) == (v, x)")
    print("    P2: pack(v, x) is injective over full 64-bit inputs")
    print("    P3: upper 64 bits of packed value equal view_id")
    print("    P4: lower 64 bits of packed value equal the object id")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
