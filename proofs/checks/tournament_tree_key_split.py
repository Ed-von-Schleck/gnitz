"""Z3 formal proofs for TournamentTree 128-bit key split/join/ordering.

Proves three properties of gnitz/storage/tournament_tree.py lines 63-99:
  P1. Roundtrip: _get_key(split(k)) == k for all 128-bit keys
  P2. Injectivity: distinct 128-bit keys produce distinct (lo, hi) pairs
  P3. Ordering consistency: k1 < k2 (u128) <-> _compare_nodes (hi, lo) lex order

The tournament tree stores 128-bit PKs as two 64-bit struct fields (key_low,
key_high) to avoid r_uint128 struct-alignment bugs in translated C.
_compare_nodes compares hi words first, then lo words, without reconstructing
the full 128-bit key.  P3 verifies that this lexicographic split ordering is
equivalent to plain 128-bit unsigned ordering.

3 Z3 queries + 5 cross-checks.  Runs under PyPy2.
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
print("  Z3 PROOF: TournamentTree 128-bit key split/join/ordering")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Cross-check: validate Z3 encoding against RPython -----------------------

report("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_uint64, intmask             # noqa: E402
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128  # noqa: E402
report("  ... imports done")

report("  ... cross-checking split/join roundtrip against RPython")


def rpy_split(key):
    """Split a 128-bit key into (lo, hi) as in tournament_tree.py:64-65.

    key_low  = r_uint64(intmask(key))        # lower 64 bits
    key_high = r_uint64(intmask(key >> 64))  # upper 64 bits
    intmask on a 128-bit value takes the low 64 bits as signed, then
    r_uint64 reinterprets as unsigned — net effect is masking to 64 bits.
    """
    k = r_uint128(key)
    lo = int(r_uint64(intmask(k))) & MASK64
    hi = int(r_uint64(intmask(k >> 64))) & MASK64
    return lo, hi


def rpy_join(lo, hi):
    """Reconstruct 128-bit key as in _get_key (tournament_tree.py:77-79).

    low  = r_uint128(r_uint64(key_low))
    high = r_uint128(r_uint64(key_high))
    return (high << 64) | low
    """
    return int((r_uint128(r_uint64(hi)) << 64) | r_uint128(r_uint64(lo))) & MASK128


def split_cmp(lo1, hi1, lo2, hi2):
    """Lexicographic (hi, lo) comparison as in _compare_nodes:88-100."""
    if hi1 < hi2:
        return -1
    if hi1 > hi2:
        return 1
    if lo1 < lo2:
        return -1
    if lo1 > lo2:
        return 1
    return 0


# 5 test vectors: 0, 1, pure-high (1<<64), all-ones, cross-word value
key_vectors = [0, 1, 1 << 64, MASK128, (1 << 64) | 1]

# Roundtrip checks
roundtrip_ok = True
for k in key_vectors:
    lo, hi = rpy_split(k)
    reconstructed = rpy_join(lo, hi)
    if (int(reconstructed) & MASK128) == (int(k) & MASK128):
        report("  PASS  roundtrip %s -> (lo=%s, hi=%s) -> %s" % (
            fmt128(k), fmt128(lo), fmt128(hi), fmt128(reconstructed)))
    else:
        report("  FAIL  roundtrip %s: got %s" % (fmt128(k), fmt128(reconstructed)))
        ok = False
        roundtrip_ok = False

# Ordering consistency: for all pairs, 128-bit ordering matches split_cmp
ordering_ok = True
for i in range(len(key_vectors)):
    for j in range(len(key_vectors)):
        k1 = key_vectors[i]
        k2 = key_vectors[j]
        lo1, hi1 = rpy_split(k1)
        lo2, hi2 = rpy_split(k2)
        cmp_result = split_cmp(lo1, hi1, lo2, hi2)
        direct_lt = (k1 & MASK128) < (k2 & MASK128)
        split_lt = cmp_result < 0
        direct_eq = (k1 & MASK128) == (k2 & MASK128)
        split_eq = cmp_result == 0
        if not ((direct_lt == split_lt) and (direct_eq == split_eq)):
            report("  FAIL  ordering check %s vs %s: direct_lt=%s split_lt=%s" % (
                fmt128(k1), fmt128(k2), direct_lt, split_lt))
            ok = False
            ordering_ok = False
if ordering_ok:
    report("  PASS  ordering consistency: all %d pairs consistent" % (len(key_vectors) ** 2))

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P1: Roundtrip: _get_key(split(k)) == k ----------------------------------
#
# lo = extract[63:0](k),  hi = extract[127:64](k)
# Reconstruct: (zero_extend(hi) << 64) | zero_extend(lo) == k.
# Trivially true by BV extract/concat duality; verified formally.

report("  ... proving P1: roundtrip _get_key(split(k)) == k")
ok &= prove("P1: roundtrip _get_key(split(k)) == k for all 128-bit keys", """\
(set-logic QF_BV)
(declare-const k (_ BitVec 128))
(define-fun lo () (_ BitVec 64) ((_ extract 63 0) k))
(define-fun hi () (_ BitVec 64) ((_ extract 127 64) k))
(define-fun reconstructed () (_ BitVec 128)
  (bvor (bvshl ((_ zero_extend 64) hi) (_ bv64 128))
        ((_ zero_extend 64) lo)))
(assert (not (= reconstructed k)))
(check-sat)
""")

# -- P2: Injectivity: distinct 128-bit keys produce distinct (lo, hi) pairs --
#
# If two keys have the same (lo, hi) split, they must be identical.

report("  ... proving P2: split is injective")
ok &= prove("P2: split(k) is injective (distinct keys -> distinct (lo, hi))", """\
(set-logic QF_BV)
(declare-const k1 (_ BitVec 128)) (declare-const k2 (_ BitVec 128))
(assert (not (= k1 k2)))
(define-fun lo1 () (_ BitVec 64) ((_ extract 63 0) k1))
(define-fun hi1 () (_ BitVec 64) ((_ extract 127 64) k1))
(define-fun lo2 () (_ BitVec 64) ((_ extract 63 0) k2))
(define-fun hi2 () (_ BitVec 64) ((_ extract 127 64) k2))
(assert (and (= lo1 lo2) (= hi1 hi2)))
(check-sat)
""")

# -- P3: Ordering consistency: k1 < k2 (u128) <-> (hi, lo) lex order ---------
#
# _compare_nodes compares hi words first, then lo words.  This is equivalent
# to 128-bit unsigned ordering because the high 64 bits are the most
# significant half of the key.
#
# Negate the biconditional and expect UNSAT: both orderings always agree.

report("  ... proving P3: u128 ordering <-> (hi, lo) lexicographic ordering")
ok &= prove("P3: u128 ordering <-> (hi, lo) lexicographic ordering", """\
(set-logic QF_BV)
(declare-const k1 (_ BitVec 128)) (declare-const k2 (_ BitVec 128))
(define-fun hi1 () (_ BitVec 64) ((_ extract 127 64) k1))
(define-fun lo1 () (_ BitVec 64) ((_ extract 63 0) k1))
(define-fun hi2 () (_ BitVec 64) ((_ extract 127 64) k2))
(define-fun lo2 () (_ BitVec 64) ((_ extract 63 0) k2))
(assert (not (= (bvult k1 k2)
               (or (bvult hi1 hi2)
                   (and (= hi1 hi2) (bvult lo1 lo2))))))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: TournamentTree 128-bit key split/join/ordering")
    print("    P1: roundtrip _get_key(split(k)) == k for all 128-bit keys")
    print("    P2: split(k) is injective (distinct keys -> distinct (lo, hi))")
    print("    P3: u128 ordering <-> (hi, lo) lexicographic ordering")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
