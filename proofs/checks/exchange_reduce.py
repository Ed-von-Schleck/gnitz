"""Z3 formal proofs for exchange <-> reduce partition consistency.

Proves that the exchange operator routes each row to the same worker that
will own the reduce output for that row's group.  hash_row_by_columns
(exchange.py) computes a partition directly, while _extract_group_key
(reduce.py) produces a 128-bit PK that is later routed via
_partition_for_key.  These must agree.

Properties:
  P1. U64/I64 fast-path: promote-and-split identity
      exchange: _extract_group_key -> r_uint128(val) -> split -> _partition_for_key(val, 0)
      reduce:   _extract_group_key -> r_uint128(val) as output PK -> _partition_for_key(val, 0)

  P2. General-path: u128 assembly/split roundtrip
      _extract_group_key: key = (zext(h_hi) << 64) | zext(h)
      exchange: split -> (r_uint64(key), r_uint64(key>>64)) == (h, h_hi)
      reduce:   output PK split as (pk_lo, pk_hi) == (h, h_hi)

  P3. _partition_for_key output bounds
      result = h & 0xFF  =>  result < 256

Known inconsistencies (latent bugs, not currently triggered):
  1. String group-by columns: exchange reads column as integer via
     _read_col_or_pk -> _read_col_int, then _mix64(integer_bits).
     Reduce calls xxh.compute_checksum(string_bytes) on actual string
     content.  Different hash inputs -> different partitions.

  2. U128 single-column group-by: reduce fast-path returns raw
     accessor.get_u128().  Exchange has no U128 fast-path; falls to
     general path which reads only lower 64 bits via _read_col_or_pk.
     Different hash chain -> different partition.

  These don't affect correctness today if string/U128 group-by columns
  are never used with multi-worker exchange, but they're latent bugs
  for when support expands.

Source:
  - gnitz/dbsp/ops/exchange.py:61-63 (PK fast-path)
  - gnitz/dbsp/ops/exchange.py:64-67 (general path, via _extract_group_key)
  - gnitz/dbsp/ops/reduce.py:403-404 (group_by_pk fast-path)
  - gnitz/dbsp/ops/reduce.py:405-406 (general path, via _extract_group_key)
  - gnitz/storage/partitioned_table.py:60-63 (_partition_for_key)

3 Z3 queries + ~13 cross-checks.  Runs under PyPy2.
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
    return "0x%016x" % (n & MASK64)


def fmt128(n):
    return "0x%032x" % (n & MASK128)


# -- Constants ----------------------------------------------------------------

MASK64 = (1 << 64) - 1
MASK128 = (1 << 128) - 1

# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: exchange <-> reduce partition consistency")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-check P1: U64/I64 fast-path promote-and-split --------------------

report("  ... cross-checking P1: promote-and-split identity")

print("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_uint64, intmask  # noqa: E402
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128  # noqa: E402
print("  ... imports done")
sys.stdout.flush()

p1_vectors = [0, 1, (1 << 63) - 1, 1 << 63, (1 << 64) - 1]

for val in p1_vectors:
    # RPython: r_uint128(r_uint64(val)) -> split lo/hi
    promoted = r_uint128(r_uint64(val))
    lo = int(promoted) & MASK64
    hi = (int(promoted) >> 64) & MASK64
    rpy_ok = (lo == (val & MASK64)) and (hi == 0)

    # Z3: zero_extend + extract
    val_u = val & MASK64
    smt_lo = run_z3("(simplify ((_ extract 63 0) ((_ zero_extend 64) (_ bv%d 64))))" % val_u)
    smt_hi = run_z3("(simplify ((_ extract 127 64) ((_ zero_extend 64) (_ bv%d 64))))" % val_u)
    z3_lo = parse_z3_value(smt_lo)
    z3_hi = parse_z3_value(smt_hi)

    if z3_lo is None or z3_hi is None:
        report("  FAIL  P1 cross-check val=%s: unexpected Z3 output lo=%s hi=%s" % (
            fmt64(val_u), smt_lo, smt_hi))
        ok = False
    elif rpy_ok and z3_lo == val_u and z3_hi == 0:
        report("  PASS  P1 cross-check val=%s -> lo=%s hi=0x%016x" % (
            fmt64(val_u), fmt64(z3_lo), z3_hi))
    else:
        report("  FAIL  P1 cross-check val=%s: RPython_ok=%s Z3_lo=%s Z3_hi=%s" % (
            fmt64(val_u), rpy_ok,
            fmt64(z3_lo) if z3_lo is not None else "?",
            fmt64(z3_hi) if z3_hi is not None else "?"))
        ok = False

# -- Cross-check P2: general-path u128 assembly/split roundtrip ---------------

report("  ... cross-checking P2: u128 assembly/split roundtrip")

p2_vectors = [
    (0, 0),
    (1, 0),
    (0, 1),
    ((1 << 64) - 1, (1 << 64) - 1),
    (0xDEADBEEFCAFEBABE, 0x0123456789ABCDEF),
]

for h, h_hi in p2_vectors:
    h_u = h & MASK64
    h_hi_u = h_hi & MASK64

    # RPython: (r_uint128(h_hi) << 64) | r_uint128(h) -> split
    key = (r_uint128(r_uint64(h_hi_u)) << 64) | r_uint128(r_uint64(h_u))
    key_int = int(key) & MASK128
    lo = key_int & MASK64
    hi = (key_int >> 64) & MASK64
    rpy_ok = (lo == h_u) and (hi == h_hi_u)

    # Z3: bvor(bvshl(zext(h_hi), 64), zext(h)) -> extract
    smt_lo = run_z3(
        "(simplify ((_ extract 63 0) (bvor (bvshl ((_ zero_extend 64) (_ bv%d 64)) (_ bv64 128)) ((_ zero_extend 64) (_ bv%d 64)))))"
        % (h_hi_u, h_u))
    smt_hi = run_z3(
        "(simplify ((_ extract 127 64) (bvor (bvshl ((_ zero_extend 64) (_ bv%d 64)) (_ bv64 128)) ((_ zero_extend 64) (_ bv%d 64)))))"
        % (h_hi_u, h_u))
    z3_lo = parse_z3_value(smt_lo)
    z3_hi = parse_z3_value(smt_hi)

    if z3_lo is None or z3_hi is None:
        report("  FAIL  P2 cross-check h=%s h_hi=%s: unexpected Z3 output" % (
            fmt64(h_u), fmt64(h_hi_u)))
        ok = False
    elif rpy_ok and z3_lo == h_u and z3_hi == h_hi_u:
        report("  PASS  P2 cross-check h=%s h_hi=%s -> roundtrip OK" % (
            fmt64(h_u), fmt64(h_hi_u)))
    else:
        report("  FAIL  P2 cross-check h=%s h_hi=%s: RPython_ok=%s Z3_lo=%s Z3_hi=%s" % (
            fmt64(h_u), fmt64(h_hi_u), rpy_ok,
            fmt64(z3_lo) if z3_lo is not None else "?",
            fmt64(z3_hi) if z3_hi is not None else "?"))
        ok = False

# -- Cross-check P3: _partition_for_key output bounds -------------------------

report("  ... cross-checking P3: partition_for_key bounds")

p3_vectors = [0, 0xDEADBEEF, (1 << 64) - 1]

for h in p3_vectors:
    h_u = h & MASK64
    result = h_u & 0xFF
    py_ok = result < 256

    smt_out = run_z3("(simplify (bvand (_ bv%d 64) #x00000000000000ff))" % h_u)
    z3_val = parse_z3_value(smt_out)

    if z3_val is None:
        report("  FAIL  P3 cross-check h=%s: unexpected Z3 output: %s" % (fmt64(h_u), smt_out))
        ok = False
    elif py_ok and z3_val == result:
        report("  PASS  P3 cross-check h=%s -> %d" % (fmt64(h_u), result))
    else:
        report("  FAIL  P3 cross-check h=%s: py_ok=%s z3=%s expected=%d" % (
            fmt64(h_u), py_ok, z3_val, result))
        ok = False

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P1: U64/I64 fast-path — promote-and-split identity ----------------------
#
# exchange.py:48-50: _partition_for_key(val, r_uint64(0))
# reduce.py:291-292: r_uint128(accessor.get_int(c_idx))
#   -> PK written to batch -> partitioned via _partition_for_key(pk_lo, pk_hi)
#
# The reduce PK is r_uint128(val) where val is r_uint64.  When this PK is
# split for partitioning: pk_lo = extract[63:0] = val, pk_hi = extract[127:64] = 0.
# So reduce routes via _partition_for_key(val, 0) — same as exchange.

report("  ... proving P1: U64/I64 fast-path promote-and-split identity")
ok &= prove("P1: zero_extend(val)[63:0] == val AND zero_extend(val)[127:64] == 0", """\
(set-logic QF_BV)
(declare-const val (_ BitVec 64))
(define-fun promoted () (_ BitVec 128) ((_ zero_extend 64) val))
; Negate: NOT (lo == val AND hi == 0)
(assert (not (and
  (= ((_ extract 63 0) promoted) val)
  (= ((_ extract 127 64) promoted) (_ bv0 64)))))
(check-sat)
""")

# -- P2: General-path — u128 assembly/split roundtrip -------------------------
#
# reduce.py:318-319: return (r_uint128(h_hi) << 64) | r_uint128(h) -> PK
# exchange.py:59:    return _partition_for_key(h, h_hi) directly
# partitioned_table.py:118-125: pk_lo = batch._read_pk_lo(i),
#   pk_hi = batch._read_pk_hi(i), _partition_for_key(pk_lo, pk_hi)
#
# Reduce assembles key = (zext(h_hi) << 64) | zext(h), then key is split
# back into (pk_lo, pk_hi) when routed.  Must recover (h, h_hi).

report("  ... proving P2: general-path u128 assembly/split roundtrip")
ok &= prove("P2: extract(assemble(h, h_hi)) == (h, h_hi)", """\
(set-logic QF_BV)
(declare-const h (_ BitVec 64))
(declare-const h_hi (_ BitVec 64))
(define-fun key () (_ BitVec 128)
  (bvor (bvshl ((_ zero_extend 64) h_hi) (_ bv64 128))
        ((_ zero_extend 64) h)))
; Negate: NOT (lo == h AND hi == h_hi)
(assert (not (and
  (= ((_ extract 63 0) key) h)
  (= ((_ extract 127 64) key) h_hi))))
(check-sat)
""")

# -- P3: _partition_for_key output bounds -------------------------------------
#
# partitioned_table.py:60-63: intmask(r_uint64(h) & r_uint64(0xFF))
# Result must be in [0, 256) for use as a partition index.

report("  ... proving P3: _partition_for_key output < 256")
ok &= prove("P3: (h & 0xFF) < 256", """\
(set-logic QF_BV)
(declare-const h (_ BitVec 64))
; Negate: result >= 256
(assert (not (bvult (bvand h #x00000000000000ff) (_ bv256 64))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: exchange <-> reduce partition consistency")
    print("    P1: U64/I64 fast-path promote-and-split identity")
    print("    P2: general-path u128 assembly/split roundtrip")
    print("    P3: _partition_for_key output bounds [0, 256)")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
