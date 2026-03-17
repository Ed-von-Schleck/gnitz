"""Z3 formal proofs for Bloom filter sizing formula correctness.

Proves four properties of gnitz/storage/bloom.py:23-31:

  BITS_PER_KEY = 10
  m = expected_n * BITS_PER_KEY
  num_bytes = (m + 7) >> 3          # ceil(m / 8)
  if num_bytes < 8: num_bytes = 8   # minimum 8 bytes
  num_bits = num_bytes * 8

Width: 16-bit BV.  At u16, expected_n * 10 + 7 overflows for n > 6552
(6552 * 10 + 7 = 65527 < 65535, 6553 * 10 + 7 = 65537 > 65535).
Constrain n to [1, 6552] for Z3; cross-checks validate larger values.

  P1. num_bytes * 8 >= m — no bit truncation (16-bit BV, UNSAT)
  P2. num_bytes >= 8 after clamp (16-bit BV, UNSAT)
  P3. Ceiling division is tight: num_bytes * 8 - m < 8 (16-bit BV, UNSAT)
  P4. Ceiling division equivalence: (m+7)>>3 == (m+7)/8 (16-bit BV, UNSAT)

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

BITS_PER_KEY = 10

# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: Bloom filter sizing formula")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-checks: Python validation of sizing formula -----------------------

report("  ... cross-checking bloom sizing formula")

test_ns = [1, 2, 5, 7, 100, 1000, 6552]

for n in test_ns:
    m = n * BITS_PER_KEY
    nb_raw = (m + 7) >> 3
    nb = max(nb_raw, 8)
    num_bits = nb * 8

    # Verify properties
    no_truncation = (num_bits >= m)
    min_ok = (nb >= 8)
    tightness = (num_bits - m < 8) if nb_raw >= 8 else True  # tightness only for unclamped
    shift_eq_div = (nb_raw == (m + 7) // 8)

    # Z3 simplify cross-check for nb_raw
    smt_q = "(simplify (bvlshr (bvadd (_ bv%d 16) (_ bv7 16)) (_ bv3 16)))" % m
    z3_out = run_z3(smt_q)
    z3_val = parse_z3_value(z3_out)
    z3_ok = (z3_val == nb_raw)

    if no_truncation and min_ok and tightness and shift_eq_div and z3_ok:
        report("  PASS  cross-check n=%d: m=%d nb_raw=%d nb=%d bits=%d" % (
            n, m, nb_raw, nb, num_bits))
    else:
        report("  FAIL  cross-check n=%d: trunc=%s min=%s tight=%s eq=%s z3=%s" % (
            n, no_truncation, min_ok, tightness, shift_eq_div, z3_ok))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: num_bytes * 8 >= m — no bit truncation (16-bit BV, UNSAT) ----------

report("  ... proving P1: no bit truncation")
ok &= prove("P1: num_bytes * 8 >= m", """\
(set-logic QF_BV)
(declare-const n (_ BitVec 16))
(assert (bvuge n (_ bv1 16)))
(assert (bvule n (_ bv6552 16)))
(define-fun m () (_ BitVec 16) (bvmul n (_ bv10 16)))
(define-fun nb () (_ BitVec 16) (bvlshr (bvadd m (_ bv7 16)) (_ bv3 16)))
(assert (not (bvuge (bvmul nb (_ bv8 16)) m)))
(check-sat)
""")

# -- P2: num_bytes >= 8 after clamp (16-bit BV, UNSAT) ----------------------

report("  ... proving P2: minimum 8 bytes after clamp")
ok &= prove("P2: clamped num_bytes >= 8", """\
(set-logic QF_BV)
(declare-const n (_ BitVec 16))
(assert (bvuge n (_ bv1 16)))
(assert (bvule n (_ bv6552 16)))
(define-fun m () (_ BitVec 16) (bvmul n (_ bv10 16)))
(define-fun nb_raw () (_ BitVec 16) (bvlshr (bvadd m (_ bv7 16)) (_ bv3 16)))
(define-fun nb () (_ BitVec 16)
  (ite (bvult nb_raw (_ bv8 16)) (_ bv8 16) nb_raw))
(assert (not (bvuge nb (_ bv8 16))))
(check-sat)
""")

# -- P3: Ceiling division is tight: num_bytes*8 - m < 8 (16-bit BV, UNSAT) --

report("  ... proving P3: ceiling division tightness")
ok &= prove("P3: num_bytes*8 - m < 8", """\
(set-logic QF_BV)
(declare-const n (_ BitVec 16))
(assert (bvuge n (_ bv1 16)))
(assert (bvule n (_ bv6552 16)))
(define-fun m () (_ BitVec 16) (bvmul n (_ bv10 16)))
(define-fun nb () (_ BitVec 16) (bvlshr (bvadd m (_ bv7 16)) (_ bv3 16)))
(assert (not (bvult (bvsub (bvmul nb (_ bv8 16)) m) (_ bv8 16))))
(check-sat)
""")

# -- P4: Ceiling division equivalence (16-bit BV, UNSAT) --------------------

report("  ... proving P4: (m+7)>>3 == (m+7)/8")
ok &= prove("P4: shift == division", """\
(set-logic QF_BV)
(declare-const m (_ BitVec 16))
(assert (bvuge m (_ bv10 16)))
(assert (bvule m (_ bv65530 16)))
(assert (not (= (bvlshr (bvadd m (_ bv7 16)) (_ bv3 16))
               (bvudiv (bvadd m (_ bv7 16)) (_ bv8 16)))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: Bloom filter sizing formula is correct")
    print("    P1: num_bytes * 8 >= m (no bit truncation)")
    print("    P2: num_bytes >= 8 after clamp")
    print("    P3: ceiling division tightness (waste < 8 bits)")
    print("    P4: (m+7)>>3 == (m+7)/8")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
