"""Z3 formal proofs for reduce accessor sentinel encoding roundtrip.

Proves four properties of gnitz/dbsp/ops/reduce.py:26-89:

Sentinel scheme in mapping_to_input[]:
  >= 0  : input column index (group exemplar)
  -1    : aggregate slot 0
  -2    : synthetic PK (group hash)
  <= -3 : aggregate slot (-src) - 2

Decoder: _agg_idx(src): if src == -1 return 0, else return (-src) - 2.

  P1. _agg_idx roundtrip for all agg slots 0..100 (16-bit BV, UNSAT)
  P2. All agg sentinels are negative — no collision with col indices (16-bit BV, UNSAT)
  P3. Sentinel -2 (synthetic PK) is distinct from all agg sentinels (16-bit BV, UNSAT)
  P4. All agg sentinels are pairwise distinct (16-bit BV, UNSAT)

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


# -- Constants ----------------------------------------------------------------

MASK16 = (1 << 16) - 1

# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: Reduce accessor sentinel encoding roundtrip")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-checks ------------------------------------------------------------

report("  ... cross-checking sentinel encoding")


def encode_sentinel(i):
    """Encode agg slot i into sentinel value (Python, matching reduce.py)."""
    if i == 0:
        return -1
    return -(i + 2)


def decode_sentinel(src):
    """Decode sentinel to agg slot index (_agg_idx from reduce.py)."""
    if src == -1:
        return 0
    return (-src) - 2


test_slots = [0, 1, 2, 5, 10, 50]

for i in test_slots:
    sentinel = encode_sentinel(i)
    decoded = decode_sentinel(sentinel)

    # Roundtrip check
    if decoded != i:
        report("  FAIL  cross-check: roundtrip(slot=%d): sentinel=%d decoded=%d" % (
            i, sentinel, decoded))
        ok = False
        continue

    # Sentinel must be < 0
    if sentinel >= 0:
        report("  FAIL  cross-check: sentinel(slot=%d) = %d >= 0" % (i, sentinel))
        ok = False
        continue

    # Sentinel must not be -2 (synthetic PK)
    if sentinel == -2:
        report("  FAIL  cross-check: sentinel(slot=%d) == -2 (synthetic PK)" % i)
        ok = False
        continue

    # Z3 simplify cross-check: encode as 16-bit two's complement
    sentinel_u16 = sentinel & MASK16
    smt_q = """\
(simplify (ite (= (_ bv%d 16) (_ bv0 16))
  #xffff
  (bvsub (bvsub (_ bv0 16) (_ bv%d 16)) (_ bv2 16))))""" % (i, i)
    z3_out = run_z3(smt_q)
    z3_val = parse_z3_value(z3_out)
    if z3_val is not None and z3_val == sentinel_u16:
        report("  PASS  cross-check: slot=%d sentinel=%d (0x%04x)" % (i, sentinel, sentinel_u16))
    else:
        report("  FAIL  cross-check: slot=%d Z3 got %s expected 0x%04x" % (i, z3_out, sentinel_u16))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: _agg_idx roundtrip for all agg slots (16-bit BV, UNSAT) -------------
#
# For agg index i in [0, 100]: encode sentinel, then _agg_idx decodes back to i.
# Encode: if i == 0, sentinel = -1 (#xffff); else sentinel = -(i+2).
# Decode: if sentinel == -1, result = 0; else result = (-sentinel) - 2.
# Avoid bvneg (Z3 4.15.8 bug) — use bvsub(0, x) instead.

report("  ... proving P1: _agg_idx roundtrip for all agg slots")

ok &= prove("P1: encode then decode = identity for slot 0..100", """\
(set-logic QF_BV)
(declare-const i (_ BitVec 16))
(assert (bvsge i (_ bv0 16)))
(assert (bvsle i (_ bv100 16)))
; Encode
(define-fun sentinel () (_ BitVec 16)
  (ite (= i (_ bv0 16))
       #xffff
       (bvsub (bvsub (_ bv0 16) i) (_ bv2 16))))
; Decode: _agg_idx
(define-fun decoded () (_ BitVec 16)
  (ite (= sentinel #xffff)
       (_ bv0 16)
       (bvsub (bvsub (_ bv0 16) sentinel) (_ bv2 16))))
; Negate: decoded != i
(assert (not (= decoded i)))
(check-sat)
""")

# -- P2: All agg sentinels are negative (16-bit BV, UNSAT) -------------------
#
# Sentinel < 0 (signed), so it can never collide with a column index >= 0.

report("  ... proving P2: all agg sentinels are negative")

ok &= prove("P2: sentinel < 0 for all slots 0..100", """\
(set-logic QF_BV)
(declare-const i (_ BitVec 16))
(assert (bvsge i (_ bv0 16)))
(assert (bvsle i (_ bv100 16)))
(define-fun sentinel () (_ BitVec 16)
  (ite (= i (_ bv0 16))
       #xffff
       (bvsub (bvsub (_ bv0 16) i) (_ bv2 16))))
; Negate: sentinel >= 0
(assert (not (bvslt sentinel (_ bv0 16))))
(check-sat)
""")

# -- P3: Sentinel -2 (synthetic PK) distinct from all agg sentinels -----------
#
# -2 != -1 (agg slot 0) and -2 != -(i+2) for any i >= 1 (since -(1+2)=-3).

report("  ... proving P3: synthetic PK sentinel -2 distinct from agg sentinels")

ok &= prove("P3: -2 != agg_sentinel(i) for i in 0..100", """\
(set-logic QF_BV)
(declare-const i (_ BitVec 16))
(assert (bvsge i (_ bv0 16)))
(assert (bvsle i (_ bv100 16)))
(define-fun agg_sentinel () (_ BitVec 16)
  (ite (= i (_ bv0 16))
       #xffff
       (bvsub (bvsub (_ bv0 16) i) (_ bv2 16))))
; Assert: agg_sentinel == -2 (#xfffe)
(assert (= agg_sentinel #xfffe))
(check-sat)
""")

# -- P4: All agg sentinels are pairwise distinct (16-bit BV, UNSAT) ----------
#
# For i != j, sentinel(i) != sentinel(j).

report("  ... proving P4: agg sentinels are pairwise distinct")

ok &= prove("P4: sentinel(i) != sentinel(j) for i != j", """\
(set-logic QF_BV)
(declare-const i (_ BitVec 16))
(declare-const j (_ BitVec 16))
(assert (bvsge i (_ bv0 16)))
(assert (bvsle i (_ bv100 16)))
(assert (bvsge j (_ bv0 16)))
(assert (bvsle j (_ bv100 16)))
(assert (not (= i j)))
(define-fun si () (_ BitVec 16)
  (ite (= i (_ bv0 16)) #xffff (bvsub (bvsub (_ bv0 16) i) (_ bv2 16))))
(define-fun sj () (_ BitVec 16)
  (ite (= j (_ bv0 16)) #xffff (bvsub (bvsub (_ bv0 16) j) (_ bv2 16))))
; Assert: si == sj (collision)
(assert (= si sj))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: Reduce accessor sentinel encoding is correct")
    print("    P1: _agg_idx roundtrip for all agg slots 0..100")
    print("    P2: all agg sentinels are negative (no col index collision)")
    print("    P3: synthetic PK sentinel -2 distinct from agg sentinels")
    print("    P4: all agg sentinels are pairwise distinct")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
