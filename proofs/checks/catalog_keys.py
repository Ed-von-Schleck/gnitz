"""Z3 formal proofs for system table composite key pack/unpack.

Proves five properties of gnitz/catalog/system_tables.py pack functions:
  P1. pack_column_id injectivity under width constraints
  P2. pack_column_id roundtrip (shift/mask extraction)
  P3. pack_param_pk roundtrip (128-bit, shift/mask extraction)
  P4. pack_gcol_pk roundtrip (128-bit, shift/mask extraction)
  P5. Counterexample: pack_column_id collision without width constraint (SAT)

Source:
  - gnitz/catalog/system_tables.py:38-63 (pack functions)
  - gnitz/catalog/program_cache.py:238-240 (param unpack)
  - gnitz/catalog/program_cache.py:264-265 (gcol unpack)

These pack functions encode multi-field tuples into u64/u128 primary keys.
The field-width assumptions (col_idx < 512, slot < 256, etc.) are implicit -
never checked at runtime.  A collision silently merges distinct catalog entries.

5 Z3 queries + ~12 cross-checks.  Runs under PyPy2.
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


def fmt128(n):
    return "0x%032x" % (n & MASK128)


# -- Constants ----------------------------------------------------------------

MASK64 = (1 << 64) - 1
MASK128 = (1 << 128) - 1


# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: system table composite key pack/unpack")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-check: validate Z3 encoding against RPython -----------------------

report("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_uint64, intmask  # noqa: E402
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128  # noqa: E402
report("  ... imports done")

# --- pack_column_id cross-checks ---

report("  ... cross-checking pack_column_id against RPython")

def rpy_pack_column_id(owner_id, col_idx):
    return (r_uint64(owner_id) << 9) | r_uint64(col_idx)

col_id_vectors = [
    (0, 0),
    (1, 0),
    (0, 511),
    ((1 << 55) - 1, 511),
    (100, 42),
]

for owner_id, col_idx in col_id_vectors:
    rpy_val = int(rpy_pack_column_id(owner_id, col_idx)) & MASK64
    # Z3: (bvor (bvshl o 9) c)
    smt_query = "(simplify (bvor (bvshl (_ bv%d 64) (_ bv9 64)) (_ bv%d 64)))" % (
        owner_id & MASK64, col_idx & MASK64)
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check pack_column_id(%d, %d): unexpected Z3 output: %s" % (
            owner_id, col_idx, z3_out))
        ok = False
    elif (int(rpy_val) & MASK64) == (int(z3_val) & MASK64):
        # Verify unpack roundtrip
        unpack_oid = rpy_val >> 9
        unpack_ci = rpy_val & 0x1FF
        if unpack_oid == owner_id and unpack_ci == col_idx:
            report("  PASS  cross-check pack_column_id(%d, %d) -> %s [roundtrip OK]" % (
                owner_id, col_idx, fmt64(rpy_val)))
        else:
            report("  FAIL  cross-check pack_column_id(%d, %d): roundtrip failed oid=%d ci=%d" % (
                owner_id, col_idx, unpack_oid, unpack_ci))
            ok = False
    else:
        report("  FAIL  cross-check pack_column_id(%d, %d): RPython=%s Z3=%s" % (
            owner_id, col_idx, fmt64(rpy_val), fmt64(z3_val)))
        ok = False

# --- pack_param_pk cross-checks ---

report("  ... cross-checking pack_param_pk against RPython")

def rpy_pack_param_pk(view_id, node_id, slot):
    lo = (r_uint64(node_id) << 8) | r_uint64(slot)
    return (r_uint128(r_uint64(view_id)) << 64) | r_uint128(lo)

param_pk_vectors = [
    (1, 0, 0),
    (1, 100, 7),
    ((1 << 32) - 1, (1 << 32) - 1, 255),
]

for view_id, node_id, slot in param_pk_vectors:
    rpy_val = int(rpy_pack_param_pk(view_id, node_id, slot)) & MASK128
    # Z3: (bvor (bvshl (zext view_id) 64) (zext (bvor (bvshl node_id 8) slot)))
    smt_query = """\
(simplify (bvor
  (bvshl ((_ zero_extend 64) (_ bv%d 64)) (_ bv64 128))
  ((_ zero_extend 64) (bvor (bvshl (_ bv%d 64) (_ bv8 64)) (_ bv%d 64)))))""" % (
        view_id & MASK64, node_id & MASK64, slot & MASK64)
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check pack_param_pk(%d, %d, %d): unexpected Z3 output: %s" % (
            view_id, node_id, slot, z3_out))
        ok = False
    elif (int(rpy_val) & MASK128) == (int(z3_val) & MASK128):
        # Verify unpack roundtrip: lo64 = extract[63:0], node = lo64 >> 8, slot = lo64 & 0xFF
        lo64 = rpy_val & MASK64
        unpack_node = (lo64 >> 8) & MASK64
        unpack_slot = lo64 & 0xFF
        if unpack_node == node_id and unpack_slot == slot:
            report("  PASS  cross-check pack_param_pk(%d, %d, %d) -> %s [roundtrip OK]" % (
                view_id, node_id, slot, fmt128(rpy_val)))
        else:
            report("  FAIL  cross-check pack_param_pk(%d, %d, %d): roundtrip failed node=%d slot=%d" % (
                view_id, node_id, slot, unpack_node, unpack_slot))
            ok = False
    else:
        report("  FAIL  cross-check pack_param_pk(%d, %d, %d): RPython=%s Z3=%s" % (
            view_id, node_id, slot, fmt128(rpy_val), fmt128(z3_val)))
        ok = False

# --- pack_gcol_pk cross-checks ---

report("  ... cross-checking pack_gcol_pk against RPython")

def rpy_pack_gcol_pk(view_id, node_id, col_idx):
    lo = (r_uint64(node_id) << 16) | r_uint64(col_idx)
    return (r_uint128(r_uint64(view_id)) << 64) | r_uint128(lo)

gcol_pk_vectors = [
    (1, 0, 0),
    (1, 100, 42),
    ((1 << 32) - 1, (1 << 32) - 1, 65535),
]

for view_id, node_id, col_idx in gcol_pk_vectors:
    rpy_val = int(rpy_pack_gcol_pk(view_id, node_id, col_idx)) & MASK128
    # Z3: (bvor (bvshl (zext view_id) 64) (zext (bvor (bvshl node_id 16) col_idx)))
    smt_query = """\
(simplify (bvor
  (bvshl ((_ zero_extend 64) (_ bv%d 64)) (_ bv64 128))
  ((_ zero_extend 64) (bvor (bvshl (_ bv%d 64) (_ bv16 64)) (_ bv%d 64)))))""" % (
        view_id & MASK64, node_id & MASK64, col_idx & MASK64)
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  cross-check pack_gcol_pk(%d, %d, %d): unexpected Z3 output: %s" % (
            view_id, node_id, col_idx, z3_out))
        ok = False
    elif (int(rpy_val) & MASK128) == (int(z3_val) & MASK128):
        # Verify unpack roundtrip: lo64 = extract[63:0], node = lo64 >> 16, col = lo64 & 0xFFFF
        lo64 = rpy_val & MASK64
        unpack_node = (lo64 >> 16) & MASK64
        unpack_col = lo64 & 0xFFFF
        if unpack_node == node_id and unpack_col == col_idx:
            report("  PASS  cross-check pack_gcol_pk(%d, %d, %d) -> %s [roundtrip OK]" % (
                view_id, node_id, col_idx, fmt128(rpy_val)))
        else:
            report("  FAIL  cross-check pack_gcol_pk(%d, %d, %d): roundtrip failed node=%d col=%d" % (
                view_id, node_id, col_idx, unpack_node, unpack_col))
            ok = False
    else:
        report("  FAIL  cross-check pack_gcol_pk(%d, %d, %d): RPython=%s Z3=%s" % (
            view_id, node_id, col_idx, fmt128(rpy_val), fmt128(z3_val)))
        ok = False

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P1: pack_column_id injectivity under width constraints ------------------
#
# Under col_idx < 512 (9 bits) and owner_id < 2^55:
# packed = (owner_id << 9) | col_idx
# Two different (owner, col) pairs cannot produce the same packed value.

report("  ... proving P1: pack_column_id injectivity")
ok &= prove("P1: pack_column_id is injective under width constraints", """\
(set-logic QF_BV)
(declare-const o1 (_ BitVec 64))
(declare-const c1 (_ BitVec 64))
(declare-const o2 (_ BitVec 64))
(declare-const c2 (_ BitVec 64))
(assert (bvult c1 (_ bv512 64)))
(assert (bvult c2 (_ bv512 64)))
(assert (bvult o1 #x0080000000000000))
(assert (bvult o2 #x0080000000000000))
(define-fun pk1 () (_ BitVec 64) (bvor (bvshl o1 (_ bv9 64)) c1))
(define-fun pk2 () (_ BitVec 64) (bvor (bvshl o2 (_ bv9 64)) c2))
(assert (= pk1 pk2))
(assert (not (and (= o1 o2) (= c1 c2))))
(check-sat)
""")

# -- P2: pack_column_id roundtrip ---------------------------------------------
#
# packed >> 9 recovers owner_id, packed & 0x1FF recovers col_idx.

report("  ... proving P2: pack_column_id roundtrip")
ok &= prove("P2: pack_column_id roundtrip extraction", """\
(set-logic QF_BV)
(declare-const oid (_ BitVec 64))
(declare-const ci (_ BitVec 64))
(assert (bvult ci (_ bv512 64)))
(assert (bvult oid #x0080000000000000))
(define-fun packed () (_ BitVec 64) (bvor (bvshl oid (_ bv9 64)) ci))
(assert (not (and
  (= (bvlshr packed (_ bv9 64)) oid)
  (= (bvand packed #x00000000000001ff) ci))))
(check-sat)
""")

# -- P3: pack_param_pk roundtrip (128-bit) ------------------------------------
#
# lo = (node_id << 8) | slot
# pk = (view_id << 64) | lo
# Unpack: lo64 = extract[63:0](pk), node = lo64 >> 8, slot = lo64 & 0xFF
# Constraints: slot < 256, node_id < 2^32

report("  ... proving P3: pack_param_pk roundtrip")
ok &= prove("P3: pack_param_pk roundtrip extraction", """\
(set-logic QF_BV)
(declare-const view_id (_ BitVec 64))
(declare-const node_id (_ BitVec 64))
(declare-const slot (_ BitVec 64))
(assert (bvult slot (_ bv256 64)))
(assert (bvult node_id #x0000000100000000))
; Pack
(define-fun lo () (_ BitVec 64) (bvor (bvshl node_id (_ bv8 64)) slot))
(define-fun pk () (_ BitVec 128)
  (bvor (bvshl ((_ zero_extend 64) view_id) (_ bv64 128))
        ((_ zero_extend 64) lo)))
; Unpack
(define-fun lo64 () (_ BitVec 64) ((_ extract 63 0) pk))
(assert (not (and
  (= (bvlshr lo64 (_ bv8 64)) node_id)
  (= (bvand lo64 #x00000000000000ff) slot))))
(check-sat)
""")

# -- P4: pack_gcol_pk roundtrip (128-bit) -------------------------------------
#
# lo = (node_id << 16) | col_idx
# pk = (view_id << 64) | lo
# Unpack: lo64 = extract[63:0](pk), node = lo64 >> 16, col = lo64 & 0xFFFF
# Constraints: col_idx < 65536, node_id < 2^32

report("  ... proving P4: pack_gcol_pk roundtrip")
ok &= prove("P4: pack_gcol_pk roundtrip extraction", """\
(set-logic QF_BV)
(declare-const view_id (_ BitVec 64))
(declare-const node_id (_ BitVec 64))
(declare-const col_idx (_ BitVec 64))
(assert (bvult col_idx (_ bv65536 64)))
(assert (bvult node_id #x0000000100000000))
; Pack
(define-fun lo () (_ BitVec 64) (bvor (bvshl node_id (_ bv16 64)) col_idx))
(define-fun pk () (_ BitVec 128)
  (bvor (bvshl ((_ zero_extend 64) view_id) (_ bv64 128))
        ((_ zero_extend 64) lo)))
; Unpack
(define-fun lo64 () (_ BitVec 64) ((_ extract 63 0) pk))
(assert (not (and
  (= (bvlshr lo64 (_ bv16 64)) node_id)
  (= (bvand lo64 #x000000000000ffff) col_idx))))
(check-sat)
""")

# -- P5: Counterexample — collision without width constraint (SAT) -----------
#
# Without col_idx < 512, two different (owner, col) pairs can pack to the
# same u64.  E.g. (1, 0) vs (0, 512): both pack to 512.

report("  ... proving P5: pack_column_id collision without width constraint (SAT)")
ok &= check_sat("P5: collision exists without width constraint", """\
(set-logic QF_BV)
(declare-const o1 (_ BitVec 64))
(declare-const c1 (_ BitVec 64))
(declare-const o2 (_ BitVec 64))
(declare-const c2 (_ BitVec 64))
; No width constraint on c1, c2 (only < 2^64 implicitly)
(assert (bvult o1 #x0080000000000000))
(assert (bvult o2 #x0080000000000000))
(define-fun pk1 () (_ BitVec 64) (bvor (bvshl o1 (_ bv9 64)) c1))
(define-fun pk2 () (_ BitVec 64) (bvor (bvshl o2 (_ bv9 64)) c2))
(assert (= pk1 pk2))
(assert (not (and (= o1 o2) (= c1 c2))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: system table composite key pack/unpack")
    print("    P1: pack_column_id injectivity under width constraints")
    print("    P2: pack_column_id roundtrip extraction")
    print("    P3: pack_param_pk roundtrip extraction (128-bit)")
    print("    P4: pack_gcol_pk roundtrip extraction (128-bit)")
    print("    P5: collision exists without width constraint (counterexample)")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
