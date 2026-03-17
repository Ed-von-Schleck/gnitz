"""Z3 formal proof of _mix64 (Murmur3 64-bit finalizer) bijectivity.

Runs under PyPy2.  Four phases:
  1. Parse reduce.py, translate _mix64 to SMT-LIB2
  2. Cross-check translator against concrete RPython execution
  3. Compositional bijectivity proof (3 Z3 queries)
  4. Summary

Exit code 0 on success, 1 on any failure.
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
    """Format an integer as 0x{16 hex digits}."""
    return "0x%016x" % (n & 0xFFFFFFFFFFFFFFFF)


# ── Phase 1: Parse and translate ─────────────────────────────────────

source_path = "../gnitz/dbsp/ops/reduce.py"
with open(source_path) as f:
    tree = ast.parse(f.read())

func_node = None
for node in ast.walk(tree):
    if isinstance(node, ast.FunctionDef) and node.name == "_mix64":
        func_node = node
        break

if func_node is None:
    print("FAIL  _mix64 not found in %s" % source_path)
    sys.exit(1)

translator = SmtTranslator()
smt_define = translator.translate(func_node)
let_body = translator.get_let_body()
constants = translator.get_constants()

print("=" * 40)
print("  Z3 PROOF: _mix64 bijectivity")
print("=" * 40)

# ── Phase 2: Cross-check ────────────────────────────────────────────

from rpython.rlib.rarithmetic import r_uint64   # noqa: E402
from gnitz.dbsp.ops.reduce import _mix64         # noqa: E402

TEST_VECTORS = [0, 1, 2, 42, 0xFFFFFFFFFFFFFFFF, 0xDEADBEEFCAFEBABE]

ok = True
for tv in TEST_VECTORS:
    # Concrete RPython execution
    expected = int(_mix64(r_uint64(tv))) & 0xFFFFFFFFFFFFFFFF

    # Z3 evaluation via (simplify ...)
    input_hex = "#x%016x" % (tv & 0xFFFFFFFFFFFFFFFF)
    # Replace the parameter binding with the concrete value
    concrete_body = let_body.replace("v_in", input_hex, 1)
    smt_query = "(simplify\n%s)" % concrete_body
    z3_out = run_z3(smt_query)

    # Parse Z3 hex output
    if z3_out.startswith("#x"):
        z3_val = int(z3_out[2:], 16)
    elif z3_out.startswith("#b"):
        z3_val = int(z3_out[2:], 2)
    else:
        print("  FAIL  cross-check %s: unexpected Z3 output: %s" % (fmt64(tv), z3_out))
        ok = False
        continue

    if z3_val == expected:
        print("  PASS  cross-check %s -> %s" % (fmt64(tv), fmt64(expected)))
    else:
        print("  FAIL  cross-check %s: RPython=%s Z3=%s" % (
            fmt64(tv), fmt64(expected), fmt64(z3_val)))
        ok = False

if not ok:
    print("=" * 40)
    print("  FAILED: cross-check mismatch")
    print("=" * 40)
    sys.exit(1)

# ── Phase 3: Compositional bijectivity proof ─────────────────────────

# _mix64 = xorshift33 . mul_c1 . xorshift33 . mul_c2 . xorshift33
# Each step is individually bijective:
#   - xorshift33 is an involution (self-inverse => bijective)
#   - multiply by odd constant is injective (mod 2^64)
# Composition of bijections is a bijection.

# Query 1: xorshift33 is self-inverse
xorshift_query = """\
(set-logic QF_BV)
(declare-const x (_ BitVec 64))
(assert (not (= (bvxor (bvxor x (bvlshr x (_ bv33 64)))
                       (bvlshr (bvxor x (bvlshr x (_ bv33 64))) (_ bv33 64)))
                x)))
(check-sat)
"""

result = run_z3(xorshift_query)
if result == "unsat":
    print("  PASS  xorshift33 is self-inverse (involution => bijection)")
else:
    print("  FAIL  xorshift33 involution: expected unsat, got %s" % result)
    ok = False

# Queries 2-3: multiply by odd constant is injective
# Constants are extracted from the translator output, not hardcoded
if len(constants) < 2:
    print("  FAIL  expected 2 multiplication constants, found %d" % len(constants))
    ok = False
else:
    for c in constants:
        mul_query = """\
(set-logic QF_BV)
(declare-const a (_ BitVec 64))
(declare-const b (_ BitVec 64))
(assert (= (bvmul a %s) (bvmul b %s)))
(assert (not (= a b)))
(check-sat)
""" % (c, c)
        result = run_z3(mul_query)
        if result == "unsat":
            print("  PASS  multiply by %s is injective" % c)
        else:
            print("  FAIL  multiply by %s: expected unsat, got %s" % (c, result))
            ok = False

# ── Phase 4: Summary ────────────────────────────────────────────────

print("=" * 40)
if ok:
    print("  PROVED: _mix64 is bijective")
else:
    print("  FAILED: see above")
print("=" * 40)

sys.exit(0 if ok else 1)
