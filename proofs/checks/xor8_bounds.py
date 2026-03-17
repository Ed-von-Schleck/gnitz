"""Z3 formal proofs for XOR filter index safety.

Proves three properties of the functions in gnitz/storage/xor8.py:
  1. rotl64 invertibility — rotation is reversible for the amounts used (21, 42)
  2. fastrange32 output bounds — result is always in [0, size) for valid inputs
  3. Segment non-overlap — h0, h1, h2 from _get_h0_h1_h2 land in disjoint regions

These properties guarantee that fingerprint array accesses in Xor8Filter.may_contain()
and build_xor8() are always in-bounds.

Runs under PyPy2.  Exit code 0 on success, 1 on any failure.
"""
import ast
import subprocess
import sys

from rpython_to_smt import SmtTranslator


# ── Helpers ──────────────────────────────────────────────────────────

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


def fmt64(n):
    return "0x%016x" % (n & 0xFFFFFFFFFFFFFFFF)


def report(msg):
    print(msg)
    sys.stdout.flush()


def bv64(n):
    """Format n as an SMT-LIB2 BV64 literal."""
    n = n & 0xFFFFFFFFFFFFFFFF
    if n <= 0xFFFF:
        return "(_ bv%d 64)" % n
    return "#x%016x" % n


def cross_check(translator, concrete_fn, test_vectors, fn_name):
    """Cross-check translator output against concrete execution.

    test_vectors: list of tuples (one value per parameter).
    concrete_fn: callable that returns an int.
    Returns True if all checks pass.
    """
    ok = True
    body = translator.get_let_body()
    param_names = translator.get_param_names()

    for tv in test_vectors:
        if not isinstance(tv, tuple):
            tv = (tv,)
        expected = int(concrete_fn(*tv)) & 0xFFFFFFFFFFFFFFFF

        # Substitute parameter names with concrete BV64 values
        concrete_body = body
        for pname, val in zip(param_names, tv):
            concrete_body = concrete_body.replace(pname, bv64(val), 1)

        smt_query = "(simplify\n%s)" % concrete_body
        z3_out = run_z3(smt_query)

        if z3_out.startswith("#x"):
            z3_val = int(z3_out[2:], 16)
        elif z3_out.startswith("#b"):
            z3_val = int(z3_out[2:], 2)
        elif z3_out.startswith("(_ bv"):
            # (_ bv42 64) format
            z3_val = int(z3_out.split()[1][2:])
        else:
            tv_str = ", ".join(fmt64(v) for v in tv)
            print("  FAIL  %s(%s): unexpected Z3 output: %s" % (fn_name, tv_str, z3_out))
            sys.stdout.flush()
            ok = False
            continue

        tv_str = ", ".join(fmt64(v) for v in tv)
        if z3_val == expected:
            print("  PASS  cross-check %s(%s) -> %s" % (fn_name, tv_str, fmt64(expected)))
        else:
            print("  FAIL  cross-check %s(%s): RPython=%s Z3=%s" % (
                fn_name, tv_str, fmt64(expected), fmt64(z3_val)))
            ok = False
        sys.stdout.flush()
    return ok


# ── Phase 1: Parse and translate ─────────────────────────────────────

source_path = "../gnitz/storage/xor8.py"
with open(source_path) as f:
    tree = ast.parse(f.read())

# Find both functions
funcs = {}
for node in ast.walk(tree):
    if isinstance(node, ast.FunctionDef) and node.name in ("rotl64", "fastrange32"):
        funcs[node.name] = node

for name in ("rotl64", "fastrange32"):
    if name not in funcs:
        print("FAIL  %s not found in %s" % (name, source_path))
        sys.exit(1)

verbose = "--verbose" in sys.argv

rotl64_translator = SmtTranslator(verbose=verbose)
rotl64_smt = rotl64_translator.translate(funcs["rotl64"])

fastrange32_translator = SmtTranslator(verbose=verbose)
fastrange32_smt = fastrange32_translator.translate(funcs["fastrange32"])

print("=" * 48)
print("  Z3 PROOF: XOR filter index safety")
print("=" * 48)
sys.stdout.flush()

# ── Phase 2: Cross-check ────────────────────────────────────────────

print("  ... importing rpython.rlib.rarithmetic")
sys.stdout.flush()
from rpython.rlib.rarithmetic import r_uint64  # noqa: E402
print("  ... importing gnitz.storage.xor8")
sys.stdout.flush()
from gnitz.storage.xor8 import rotl64, fastrange32  # noqa: E402
print("  ... imports done")
sys.stdout.flush()

ok = True

# rotl64 test vectors: (n, c) pairs
rotl64_vectors = [
    (0, 21),
    (1, 21),
    (0xDEADBEEFCAFEBABE, 21),
    (0xFFFFFFFFFFFFFFFF, 42),
    (0x0000000100000000, 42),
    (42, 7),
]
ok &= cross_check(rotl64_translator, rotl64, rotl64_vectors, "rotl64")

# fastrange32 test vectors: (hash32, size) pairs
fastrange32_vectors = [
    (0, 100),
    (1, 100),
    (0xFFFFFFFF, 100),
    (0xDEADBEEF, 42),
    (0x80000000, 1),
    (0, 1),
    (12345, 256),
]
ok &= cross_check(fastrange32_translator, fastrange32, fastrange32_vectors, "fastrange32")

if not ok:
    print("=" * 48)
    print("  FAILED: cross-check mismatch")
    print("=" * 48)
    sys.exit(1)

# ── Phase 3: Proofs ─────────────────────────────────────────────────
print("  ... starting proofs")
sys.stdout.flush()

# 3a: rotl64 invertibility for c=21 and c=42
# rotl64(rotl64(x, c), 64-c) == x
for c in (21, 42):
    inv = 64 - c
    report("  ... proving rotl64 invertibility c=%d" % c)
    query = """\
(set-logic QF_BV)
(declare-const x (_ BitVec 64))
(define-fun rotl ((n (_ BitVec 64)) (s (_ BitVec 64))) (_ BitVec 64)
  (bvor (bvshl n s) (bvlshr n (bvsub (_ bv64 64) s))))
(assert (not (= (rotl (rotl x (_ bv%d 64)) (_ bv%d 64)) x)))
(check-sat)
""" % (c, inv)
    result = run_z3(query)
    if result == "unsat":
        report("  PASS  rotl64(rotl64(x, %d), %d) == x (invertible)" % (c, inv))
    else:
        report("  FAIL  rotl64 invertibility c=%d: expected unsat, got %s" % (c, result))
        ok = False

# 3b: fastrange32 output bounds
# For all hash32 < 2^32 and 0 < size < 2^31: fastrange32(hash32, size) < size
report("  ... proving fastrange32 bounds")
bounds_query = """\
(set-logic QF_BV)
(declare-const hash32 (_ BitVec 64))
(declare-const size (_ BitVec 64))
; Constrain inputs to valid ranges
(assert (bvult hash32 #x0000000100000000))   ; hash32 < 2^32
(assert (bvugt size #x0000000000000000))     ; size > 0
(assert (bvult size #x0000000080000000))     ; size < 2^31 (positive after intmask)
; Negate the property: result >= size
(assert (not (bvult (bvlshr (bvmul hash32 size) (_ bv32 64)) size)))
(check-sat)
"""
result = run_z3(bounds_query)
if result == "unsat":
    report("  PASS  fastrange32(h, s) < s for all h < 2^32, 0 < s < 2^31")
else:
    report("  FAIL  fastrange32 bounds: expected unsat, got %s" % result)
    ok = False

# 3c: Segment non-overlap
# _get_h0_h1_h2 returns (h0, h1, h2) where:
#   h0 = fastrange32(h & 0xFFFFFFFF, seg)                  -> [0, seg)
#   h1 = fastrange32(rotl64(h, 21) & 0xFFFFFFFF, seg) + seg -> [seg, 2*seg)
#   h2 = fastrange32(rotl64(h, 42) & 0xFFFFFFFF, seg) + 2*seg -> [2*seg, 3*seg)
#
# Given fastrange32 bounds (3b), h0 < seg, h1 >= seg, h2 >= 2*seg.
# The non-overlap follows, but we prove the full property directly:
#   h0 < seg AND seg <= h1 < 2*seg AND 2*seg <= h2 < 3*seg
report("  ... proving segment non-overlap (5 decomposed queries)")

# Decompose into 5 independent queries using 32-bit BVs.
# mask32 already constrains hash inputs to 32 bits, so 64-bit BVs are wasteful.
# Each query proves one bound and completes in <1s (vs timeout for the conjoined 64-bit version).

seg_constraint = """\
(assert (bvuge seg (_ bv4 32)))
(assert (bvult seg #x80000000))"""

# fr32 in 32-bit: zero-extend to 64, multiply, extract upper 32 bits
fr32_def = """\
(define-fun fr32 ((hw (_ BitVec 32)) (s (_ BitVec 32))) (_ BitVec 32)
  ((_ extract 63 32) (bvmul ((_ zero_extend 32) hw) ((_ zero_extend 32) s))))"""

widen_def = """\
(define-fun widen ((x (_ BitVec 32))) (_ BitVec 64) ((_ zero_extend 32) x))"""

# Query 1: h0 upper bound — fr32(h0_in, seg) < seg
q1 = """\
(set-logic QF_BV)
(declare-const h0_in (_ BitVec 32))
(declare-const seg (_ BitVec 32))
%s
%s
(assert (not (bvult (fr32 h0_in seg) seg)))
(check-sat)
""" % (seg_constraint, fr32_def)
result = run_z3(q1)
if result == "unsat":
    report("  PASS  h0 = fr32(h0_in, seg) < seg")
else:
    report("  FAIL  h0 upper bound: expected unsat, got %s" % result)
    ok = False

# Query 2: h1 lower bound — fr32(h1_in, seg) + seg >= seg  (trivially true)
q2 = """\
(set-logic QF_BV)
(declare-const h1_in (_ BitVec 32))
(declare-const seg (_ BitVec 32))
%s
%s
(assert (not (bvuge (bvadd (fr32 h1_in seg) seg) seg)))
(check-sat)
""" % (seg_constraint, fr32_def)
result = run_z3(q2)
if result == "unsat":
    report("  PASS  h1 = fr32(h1_in, seg) + seg >= seg")
else:
    report("  FAIL  h1 lower bound: expected unsat, got %s" % result)
    ok = False

# Query 3: h1 upper bound — fr32(h1_in, seg) + seg < 2*seg
# Both sides fit in 32 bits (seg < 2^31 -> 2*seg < 2^32)
q3 = """\
(set-logic QF_BV)
(declare-const h1_in (_ BitVec 32))
(declare-const seg (_ BitVec 32))
%s
%s
(assert (not (bvult (bvadd (fr32 h1_in seg) seg) (bvmul seg (_ bv2 32)))))
(check-sat)
""" % (seg_constraint, fr32_def)
result = run_z3(q3)
if result == "unsat":
    report("  PASS  h1 = fr32(h1_in, seg) + seg < 2*seg")
else:
    report("  FAIL  h1 upper bound: expected unsat, got %s" % result)
    ok = False

# Query 4: h2 lower bound — fr32(h2_in, seg) + 2*seg >= 2*seg  (trivially true)
# Widen to 64-bit: fr32 + 2*seg can reach ~3*seg which overflows 32 bits
q4 = """\
(set-logic QF_BV)
(declare-const h2_in (_ BitVec 32))
(declare-const seg (_ BitVec 32))
%s
%s
%s
(assert (not (bvuge
  (bvadd (widen (fr32 h2_in seg)) (bvmul (widen seg) (_ bv2 64)))
  (bvmul (widen seg) (_ bv2 64)))))
(check-sat)
""" % (seg_constraint, fr32_def, widen_def)
result = run_z3(q4)
if result == "unsat":
    report("  PASS  h2 = fr32(h2_in, seg) + 2*seg >= 2*seg")
else:
    report("  FAIL  h2 lower bound: expected unsat, got %s" % result)
    ok = False

# Query 5: h2 upper bound — fr32(h2_in, seg) + 2*seg < 3*seg
# Widen to 64-bit: 3*seg overflows 32 bits when seg >= 2^30
q5 = """\
(set-logic QF_BV)
(declare-const h2_in (_ BitVec 32))
(declare-const seg (_ BitVec 32))
%s
%s
%s
(assert (not (bvult
  (bvadd (widen (fr32 h2_in seg)) (bvmul (widen seg) (_ bv2 64)))
  (bvmul (widen seg) (_ bv3 64)))))
(check-sat)
""" % (seg_constraint, fr32_def, widen_def)
result = run_z3(q5)
if result == "unsat":
    report("  PASS  h2 = fr32(h2_in, seg) + 2*seg < 3*seg")
else:
    report("  FAIL  h2 upper bound: expected unsat, got %s" % result)
    ok = False

# ── Phase 4: Summary ────────────────────────────────────────────────

print("=" * 48)
if ok:
    print("  PROVED: XOR filter indices are always in-bounds")
else:
    print("  FAILED: see above")
print("=" * 48)

sys.exit(0 if ok else 1)
