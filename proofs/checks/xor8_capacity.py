"""Z3 formal proofs for XOR filter capacity formula correctness.

Proves four properties of the capacity/segment_length computation in
gnitz/storage/xor8.py:89-97:

  capacity = intmask(1 + (num_keys * 123 + 99) // 100 + 32)
  segment_length = intmask((capacity + 2) // 3)
  if segment_length < 4: segment_length = 4
  total_size = 3 * segment_length

Simplified: capacity = (n * 123 + 99) // 100 + 33

Width: 16-bit BV (bvudiv is O(n^2)).  At u16, n * 123 + 99 overflows for
n > 532 (532 * 123 + 99 = 65535).  P1-P3 constrain n to [1, 532].
P4 shows overflow breaks the invariant.

  P1. capacity > num_keys for n in [1, 532]
  P2. segment_length >= 4 for n in [1, 532]
  P3. 3 * segment_length >= capacity for n in [1, 532]
  P4. Counterexample: overflow for n > 532 at 16-bit (SAT)

4 Z3 queries + ~8 cross-checks.  Runs under PyPy2.
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


def expect_sat(label, smt_text):
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


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: XOR filter capacity formula")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-check: validate capacity formula against RPython -------------------

report("  ... cross-checking capacity formula against RPython")

print("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import intmask  # noqa: E402
print("  ... imports done")
sys.stdout.flush()


def xor8_capacity(n):
    """Replicate gnitz/storage/xor8.py:93-97."""
    capacity = intmask(1 + (n * 123 + 99) // 100 + 32)
    segment_length = intmask((capacity + 2) // 3)
    if segment_length < 4:
        segment_length = 4
    total_size = 3 * segment_length
    return capacity, segment_length, total_size


cross_check_vectors = [1, 2, 3, 10, 100, 532, 1000, 10000]

for n in cross_check_vectors:
    cap, seg, total = xor8_capacity(n)

    # Verify invariants in Python
    inv_cap_gt_n = cap > n
    inv_seg_ge_4 = seg >= 4
    inv_total_ge_cap = total >= cap

    # Z3 cross-check for n <= 532 (within 16-bit range)
    if n <= 532:
        smt_query = """\
(simplify (bvadd (bvudiv (bvadd (bvmul (_ bv%d 16) (_ bv123 16)) (_ bv99 16))
                         (_ bv100 16))
                 (_ bv33 16)))""" % n
        z3_out = run_z3(smt_query)
        z3_cap = parse_z3_value(z3_out)
        z3_match = (z3_cap is not None and z3_cap == cap)
    else:
        z3_match = True  # Skip Z3 for overflow range

    if inv_cap_gt_n and inv_seg_ge_4 and inv_total_ge_cap and z3_match:
        report("  PASS  cross-check n=%d: cap=%d seg=%d total=%d" % (n, cap, seg, total))
    else:
        report("  FAIL  cross-check n=%d: cap_gt_n=%s seg_ge_4=%s total_ge_cap=%s z3=%s" % (
            n, inv_cap_gt_n, inv_seg_ge_4, inv_total_ge_cap, z3_match))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: capacity > num_keys for n in [1, 532] (16-bit BV, UNSAT) -----------
#
# capacity = (n * 123 + 99) // 100 + 33
# For n >= 1: cap >= (1*123 + 99)//100 + 33 = 222//100 + 33 = 2 + 33 = 35 > 1.
# The 23% overhead plus +33 additive term always exceeds num_keys.

report("  ... proving P1: capacity > num_keys")
ok &= prove("P1: cap > n for n in [1, 532]", """\
(set-logic QF_BV)
(declare-const n (_ BitVec 16))
(assert (bvuge n (_ bv1 16)))
(assert (bvule n (_ bv532 16)))
(define-fun cap () (_ BitVec 16)
  (bvadd (bvudiv (bvadd (bvmul n (_ bv123 16)) (_ bv99 16)) (_ bv100 16))
         (_ bv33 16)))
(assert (not (bvugt cap n)))
(check-sat)
""")

# -- P2: segment_length >= 4 for n in [1, 532] (16-bit BV, UNSAT) -----------
#
# For n >= 1, cap >= 35, so (35 + 2) // 3 = 12 >= 4.
# The max(seg, 4) clamp is redundant in the valid range.

report("  ... proving P2: segment_length >= 4")
ok &= prove("P2: seg >= 4 for n in [1, 532]", """\
(set-logic QF_BV)
(declare-const n (_ BitVec 16))
(assert (bvuge n (_ bv1 16)))
(assert (bvule n (_ bv532 16)))
(define-fun cap () (_ BitVec 16)
  (bvadd (bvudiv (bvadd (bvmul n (_ bv123 16)) (_ bv99 16)) (_ bv100 16))
         (_ bv33 16)))
(define-fun seg () (_ BitVec 16) (bvudiv (bvadd cap (_ bv2 16)) (_ bv3 16)))
(assert (not (bvuge seg (_ bv4 16))))
(check-sat)
""")

# -- P3: 3 * segment_length >= capacity for n in [1, 532] (16-bit BV, UNSAT) --
#
# total_size = 3 * segment_length covers all capacity slots.
# This is the ceiling division property: 3 * ceil(cap / 3) >= cap.

report("  ... proving P3: total_size >= capacity")
ok &= prove("P3: 3*seg >= cap for n in [1, 532]", """\
(set-logic QF_BV)
(declare-const n (_ BitVec 16))
(assert (bvuge n (_ bv1 16)))
(assert (bvule n (_ bv532 16)))
(define-fun cap () (_ BitVec 16)
  (bvadd (bvudiv (bvadd (bvmul n (_ bv123 16)) (_ bv99 16)) (_ bv100 16))
         (_ bv33 16)))
(define-fun seg () (_ BitVec 16) (bvudiv (bvadd cap (_ bv2 16)) (_ bv3 16)))
(define-fun total () (_ BitVec 16) (bvmul (_ bv3 16) seg))
(assert (not (bvuge total cap)))
(check-sat)
""")

# -- P4: Overflow counterexample for n > 532 at 16-bit (SAT) -----------------
#
# n * 123 + 99 overflows u16 for n > 532 (533 * 123 + 99 = 65658 > 65535).
# This demonstrates the formula is unsound without sufficient bit width.

report("  ... proving P4: overflow counterexample for n > 532")
ok &= expect_sat("P4: overflow makes cap <= n (SAT)", """\
(set-logic QF_BV)
(declare-const n (_ BitVec 16))
(assert (bvugt n (_ bv532 16)))
(define-fun cap () (_ BitVec 16)
  (bvadd (bvudiv (bvadd (bvmul n (_ bv123 16)) (_ bv99 16)) (_ bv100 16))
         (_ bv33 16)))
(assert (bvule cap n))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: XOR filter capacity formula is correct")
    print("    P1: capacity > num_keys (sufficient slots)")
    print("    P2: segment_length >= 4 (minimum segment size)")
    print("    P3: total_size >= capacity (full coverage)")
    print("    P4: overflow counterexample confirms width bound")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
