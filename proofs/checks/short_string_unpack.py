"""Z3 formal proofs for short-string unpack read correctness.

Source: gnitz/core/strings.py:188-207 (unpack_string, short-string branch)

For length in [1..SHORT_STRING_THRESHOLD=12], unpack_string reads:
  - take_prefix = min(length, 4) bytes from struct offset 4 (prefix slot)
  - suffix_read = max(0, length-4) bytes from struct offset 8 (payload slot)

  P1. Total bytes read equals length: take_prefix + suffix_read == length
      (5-bit BV, UNSAT)
  P2. First read stays within prefix slot: take_prefix <= 4
      (5-bit BV, UNSAT)
  P3. Second read's last byte is within the 16-byte struct bounds (when length > 4):
      last_offset = length + 3 <= 15
      (5-bit BV, UNSAT)

3 Z3 queries + 6 cross-checks.  Runs under PyPy2.
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

SHORT_STRING_THRESHOLD = 12
STRUCT_SIZE = 16


# -- Cross-checks -------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: short-string unpack read correctness")
print("=" * 56)
sys.stdout.flush()

ok = True

report("  ... cross-checking unpack formulas on concrete lengths")

for length in [0, 1, 2, 4, 5, 8, 12]:
    take_prefix = 4 if length > 4 else length
    suffix_read = length - 4 if length > 4 else 0

    # Total bytes == length
    if take_prefix + suffix_read != length:
        report("  FAIL  cross-check length=%d: take_prefix+suffix_read=%d != %d" % (
            length, take_prefix + suffix_read, length))
        ok = False
    else:
        report("  PASS  cross-check length=%d: take_prefix=%d suffix_read=%d total=%d" % (
            length, take_prefix, suffix_read, length))

    # First read within prefix slot [4..8): take_prefix <= 4
    if take_prefix > 4:
        report("  FAIL  cross-check length=%d: take_prefix=%d > 4" % (length, take_prefix))
        ok = False

    # Second read within struct bounds
    if length > 4:
        last_byte_offset = 8 + suffix_read - 1   # = length + 3
        if last_byte_offset >= STRUCT_SIZE:
            report("  FAIL  cross-check length=%d: last_byte_offset=%d >= %d" % (
                length, last_byte_offset, STRUCT_SIZE))
            ok = False
        if last_byte_offset != length + 3:
            report("  FAIL  cross-check length=%d: last_byte_offset formula mismatch" % length)
            ok = False

# Z3-encoding validation: (simplify take_prefix) and (simplify suffix_read) for
# a few concrete lengths
report("  ... validating Z3 take_prefix/suffix_read encoding on concrete values")

for (length, exp_tp, exp_sr) in [(1, 1, 0), (4, 4, 0), (5, 4, 1), (12, 4, 8)]:
    length_bv = "(_ bv%d 5)" % length

    # take_prefix = ite(l > 4, 4, l)
    smt_tp = "(simplify (ite (bvugt %s (_ bv4 5)) (_ bv4 5) %s))" % (length_bv, length_bv)
    z3_tp = run_z3(smt_tp)
    # suffix_read = ite(l > 4, l-4, 0)
    smt_sr = "(simplify (ite (bvugt %s (_ bv4 5)) (bvsub %s (_ bv4 5)) (_ bv0 5)))" % (
        length_bv, length_bv)
    z3_sr = run_z3(smt_sr)

    def bv_to_int(s):
        if s.startswith("#x"):
            return int(s[2:], 16)
        if s.startswith("#b"):
            return int(s[2:], 2)
        return -1

    got_tp = bv_to_int(z3_tp)
    got_sr = bv_to_int(z3_sr)
    if got_tp == exp_tp and got_sr == exp_sr:
        report("  PASS  Z3 encoding length=%d: take_prefix=%d suffix_read=%d" % (
            length, got_tp, got_sr))
    else:
        report("  FAIL  Z3 encoding length=%d: got take_prefix=%d suffix_read=%d, expected %d %d" % (
            length, got_tp, got_sr, exp_tp, exp_sr))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- Shared definitions for P1 and P2 ----------------------------------------
#
# Use 5-bit BV: covers [0..31] ⊇ [1..12].
# l in [1..12], take_prefix = ite(l>4, 4, l), suffix_read = ite(l>4, l-4, 0).

SHARED_DEFS = """\
(set-logic QF_BV)
(declare-const l (_ BitVec 5))
(assert (bvuge l (_ bv1 5)))
(assert (bvule l (_ bv12 5)))
(define-fun take_prefix () (_ BitVec 5)
  (ite (bvugt l (_ bv4 5)) (_ bv4 5) l))
(define-fun suffix_read () (_ BitVec 5)
  (ite (bvugt l (_ bv4 5)) (bvsub l (_ bv4 5)) (_ bv0 5)))
"""

# -- P1: take_prefix + suffix_read == length (5-bit BV, UNSAT) ---------------

report("  ... proving P1: take_prefix + suffix_read == length")

ok &= prove("P1: total bytes read equals length", SHARED_DEFS + """\
(assert (not (= (bvadd take_prefix suffix_read) l)))
(check-sat)
""")

# -- P2: take_prefix <= 4 (first read within prefix slot) (5-bit BV, UNSAT) --

report("  ... proving P2: take_prefix <= 4 (first read within prefix slot)")

ok &= prove("P2: first read stays within prefix slot (take_prefix <= 4)", SHARED_DEFS + """\
(assert (not (bvule take_prefix (_ bv4 5))))
(check-sat)
""")

# -- P3: last byte of second read within 16-byte struct (5-bit BV, UNSAT) -----
#
# When length > 4, last byte offset = 8 + (length-4) - 1 = length + 3.
# Must be <= 15 (within struct).  Range: length in (4..12] => l+3 in [8..15].
# 5-bit arithmetic is sufficient: max value 12+3=15 < 32.

report("  ... proving P3: second-read last byte within 16-byte struct (when length > 4)")

ok &= prove("P3: last byte offset (length+3) <= 15 for length in (4..12]", """\
(set-logic QF_BV)
(declare-const l (_ BitVec 5))
(assert (bvugt l (_ bv4 5)))
(assert (bvule l (_ bv12 5)))
(define-fun last_offset () (_ BitVec 5) (bvadd l (_ bv3 5)))
(assert (not (bvule last_offset (_ bv15 5))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: short-string unpack read correctness")
    print("    P1: take_prefix + suffix_read == length for all l in [1,12]")
    print("    P2: take_prefix <= 4 (first read stays in prefix slot)")
    print("    P3: last byte of second read <= 15 (within 16-byte struct)")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
