"""Z3 formal proofs for ManifestEntry 128-bit key split/join roundtrip & ordering.

Proves three properties of gnitz/storage/metadata.py lines 26-39:
  P1. Roundtrip: get_min_key(store(k)) == k for all 128-bit keys
  P2. Injectivity: distinct 128-bit keys produce distinct (lo, hi) pairs
  P3. Ordering consistency: k1 < k2 (u128) <-> (hi1, lo1) < (hi2, lo2) lex

Source formulas (ManifestEntry.__init__ + get_min_key):
    mk = r_uint128(min_key)
    self.pk_min_lo = r_uint64(intmask(mk))         # lower 64 bits
    self.pk_min_hi = r_uint64(intmask(mk >> 64))   # upper 64 bits

    def get_min_key(self):
        return (r_uint128(self.pk_min_hi) << 64) | r_uint128(self.pk_min_lo)

P3 is the critical shard-scan property: range pruning in ShardIndex uses
get_min_key()/get_max_key() for shard boundary comparisons.  If the
(hi, lo) split ordering were not equivalent to direct 128-bit ordering,
scans could silently skip shards containing matching rows.

3 Z3 queries + 5 cross-checks (roundtrip) + 20 cross-checks (ordering pairs).
Runs under PyPy2.
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

MASK64 = (1 << 64) - 1
MASK128 = (1 << 128) - 1


def fmt128(n):
    return "0x%032x" % (n & MASK128)


# -- Main ---------------------------------------------------------------------

print("=" * 60)
print("  Z3 PROOF: ManifestEntry 128-bit key split/join/ordering")
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


def rpy_store(key):
    """Split as in ManifestEntry.__init__ (metadata.py:26-29)."""
    mk = r_uint128(key)
    lo = int(r_uint64(intmask(mk))) & MASK64
    hi = int(r_uint64(intmask(mk >> 64))) & MASK64
    return lo, hi


def rpy_reconstruct(lo, hi):
    """Reconstruct as in get_min_key (metadata.py:35-36)."""
    return int((r_uint128(r_uint64(hi)) << 64) | r_uint128(r_uint64(lo))) & MASK128


def split_cmp(lo1, hi1, lo2, hi2):
    """Lexicographic (hi, lo) comparison equivalent to shard boundary check."""
    if hi1 < hi2:
        return -1
    if hi1 > hi2:
        return 1
    if lo1 < lo2:
        return -1
    if lo1 > lo2:
        return 1
    return 0


# 5 key vectors: 0, 1, pure-high (1<<64), all-ones, cross-word value
key_vectors = [0, 1, 1 << 64, MASK128, (1 << 64) | 1]

# Roundtrip checks
cross_ok = True
for k in key_vectors:
    lo, hi = rpy_store(k)
    reconstructed = rpy_reconstruct(lo, hi)
    if (reconstructed & MASK128) == (k & MASK128):
        report("  PASS  roundtrip %s -> (lo=%016x, hi=%016x) -> %s" % (
            fmt128(k), lo, hi, fmt128(reconstructed)))
    else:
        report("  FAIL  roundtrip %s: got %s" % (fmt128(k), fmt128(reconstructed)))
        ok = False
        cross_ok = False

# Ordering consistency: for all pairs, 128-bit ordering matches split_cmp
ordering_ok = True
for i in range(len(key_vectors)):
    for j in range(len(key_vectors)):
        k1 = key_vectors[i]
        k2 = key_vectors[j]
        lo1, hi1 = rpy_store(k1)
        lo2, hi2 = rpy_store(k2)
        cmp_result = split_cmp(lo1, hi1, lo2, hi2)
        direct_lt = (k1 & MASK128) < (k2 & MASK128)
        split_lt = cmp_result < 0
        direct_eq = (k1 & MASK128) == (k2 & MASK128)
        split_eq = cmp_result == 0
        if not ((direct_lt == split_lt) and (direct_eq == split_eq)):
            report("  FAIL  ordering %s vs %s: direct_lt=%s split_lt=%s" % (
                fmt128(k1), fmt128(k2), direct_lt, split_lt))
            ok = False
            ordering_ok = False
if ordering_ok:
    report("  PASS  ordering consistency: all %d pairs consistent" % (len(key_vectors) ** 2))

if not cross_ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch — aborting proof phase")
    print("=" * 60)
    sys.exit(1)

# -- P1: Roundtrip: get_min_key(store(k)) == k --------------------------------
#
# lo = extract[63:0](k),  hi = extract[127:64](k)
# Reconstruct: (zero_extend(hi) << 64) | zero_extend(lo) == k.
# Trivially true by BV extract/concat duality; proved formally here to tie
# the formula to ManifestEntry specifically (distinct from tournament_tree.py).

report("  ... proving P1: roundtrip get_min_key(store(k)) == k for all 128-bit k")
ok &= prove("P1: roundtrip get_min_key(store(k)) == k for all 128-bit keys", """\
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
# If two keys share the same (lo, hi) pair after splitting, they must be
# identical.  Proved by contradiction: assume k1 != k2 and same (lo, hi).

report("  ... proving P2: store(k) is injective")
ok &= prove("P2: store(k) is injective (distinct keys -> distinct (lo, hi))", """\
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

# -- P3: Ordering consistency: k1 < k2 (u128) <-> (hi, lo) lex order --------
#
# ShardIndex.lookup_range uses get_min_key()/get_max_key() for shard boundary
# comparisons.  The stored (hi, lo) fields are compared lexicographically
# (hi first, then lo).  P3 proves this is equivalent to direct 128-bit
# unsigned ordering, so range pruning is correct.
#
# Negate the biconditional and expect UNSAT.

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
    print("  PROVED: ManifestEntry 128-bit key split/join/ordering")
    print("    P1: roundtrip get_min_key(store(k)) == k for all 128-bit keys")
    print("    P2: store(k) is injective (distinct keys -> distinct (lo, hi))")
    print("    P3: u128 ordering <-> (hi, lo) lexicographic ordering")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
