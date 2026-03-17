"""Z3 formal proofs for tournament tree heap index arithmetic.

Proves seven properties of gnitz/storage/tournament_tree.py:72,114,124-125:

  parent = (idx - 1) // 2       # _sift_up line 114
  left   = 2 * idx + 1          # _sift_down line 124
  right  = 2 * idx + 2          # _sift_down line 125
  build: for i in range((heap_size // 2) - 1, -1, -1)  # line 72

Width: 16-bit BV (bvudiv for // 2).  Constrain i <= 32766 so 2*i+2
doesn't overflow u16 (2*32766+2 = 65534 < 65536).

  P1. parent(left(i)) == i
  P2. parent(right(i)) == i
  P3. left(i) != right(i)
  P4. left(i) > i
  P5. right(i) > i
  P6a. Build start index has a child (n > 1)
  P6b. First leaf has no child

7 Z3 queries + ~16 cross-checks.  Runs under PyPy2.
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


# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: Tournament tree heap index arithmetic")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-check: parent/child roundtrip in Python ---------------------------

report("  ... cross-checking parent/child index arithmetic")

index_vectors = [0, 1, 2, 3, 10, 100, 1000, 32766]

for i in index_vectors:
    left = 2 * i + 1
    right = 2 * i + 2
    parent_left = (left - 1) // 2
    parent_right = (right - 1) // 2

    # Z3 cross-check via (simplify ...)
    smt_left = "(simplify (bvudiv (bvsub (bvadd (bvmul (_ bv2 16) (_ bv%d 16)) (_ bv1 16)) (_ bv1 16)) (_ bv2 16)))" % i
    smt_right = "(simplify (bvudiv (bvsub (bvadd (bvmul (_ bv2 16) (_ bv%d 16)) (_ bv2 16)) (_ bv1 16)) (_ bv2 16)))" % i

    z3_pl = parse_z3_value(run_z3(smt_left))
    z3_pr = parse_z3_value(run_z3(smt_right))

    if parent_left == i and parent_right == i and z3_pl == i and z3_pr == i:
        report("  PASS  cross-check i=%d: left=%d right=%d parent(left)=%d parent(right)=%d" % (
            i, left, right, parent_left, parent_right))
    else:
        report("  FAIL  cross-check i=%d: p(l)=%d p(r)=%d z3_p(l)=%s z3_p(r)=%s" % (
            i, parent_left, parent_right, z3_pl, z3_pr))
        ok = False

# Cross-check: build start index
heap_size_vectors = [1, 2, 3, 4, 7, 15, 100, 1000]

for n in heap_size_vectors:
    if n > 1:
        start_idx = (n // 2) - 1
        left_of_start = 2 * start_idx + 1
        first_leaf = n // 2
        left_of_leaf = 2 * first_leaf + 1
        has_child = left_of_start < n
        leaf_no_child = left_of_leaf >= n

        if has_child and leaf_no_child:
            report("  PASS  cross-check build n=%d: start=%d left=%d first_leaf=%d" % (
                n, start_idx, left_of_start, first_leaf))
        else:
            report("  FAIL  cross-check build n=%d: has_child=%s leaf_no_child=%s" % (
                n, has_child, leaf_no_child))
            ok = False
    else:
        report("  PASS  cross-check build n=%d: skipped (heap_size <= 1)" % n)

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: parent(left(i)) == i (16-bit BV, UNSAT) ----------------------------
#
# ((2*i+1) - 1) // 2 = (2*i) // 2 = i

report("  ... proving P1: parent(left(i)) == i")
ok &= prove("P1: parent(left(i)) == i", """\
(set-logic QF_BV)
(declare-const i (_ BitVec 16))
(assert (bvule i (_ bv32766 16)))
(define-fun l () (_ BitVec 16) (bvadd (bvmul (_ bv2 16) i) (_ bv1 16)))
(define-fun p () (_ BitVec 16) (bvudiv (bvsub l (_ bv1 16)) (_ bv2 16)))
(assert (not (= p i)))
(check-sat)
""")

# -- P2: parent(right(i)) == i (16-bit BV, UNSAT) ---------------------------
#
# ((2*i+2) - 1) // 2 = (2*i+1) // 2 = i

report("  ... proving P2: parent(right(i)) == i")
ok &= prove("P2: parent(right(i)) == i", """\
(set-logic QF_BV)
(declare-const i (_ BitVec 16))
(assert (bvule i (_ bv32766 16)))
(define-fun r () (_ BitVec 16) (bvadd (bvmul (_ bv2 16) i) (_ bv2 16)))
(define-fun p () (_ BitVec 16) (bvudiv (bvsub r (_ bv1 16)) (_ bv2 16)))
(assert (not (= p i)))
(check-sat)
""")

# -- P3: left(i) != right(i) (16-bit BV, UNSAT) -----------------------------
#
# 2*i+1 != 2*i+2 trivially (differ by 1).

report("  ... proving P3: left(i) != right(i)")
ok &= prove("P3: left(i) != right(i)", """\
(set-logic QF_BV)
(declare-const i (_ BitVec 16))
(assert (bvule i (_ bv32766 16)))
(assert (= (bvadd (bvmul (_ bv2 16) i) (_ bv1 16))
           (bvadd (bvmul (_ bv2 16) i) (_ bv2 16))))
(check-sat)
""")

# -- P4: left(i) > i (16-bit BV, UNSAT) -------------------------------------
#
# 2*i+1 > i for i >= 0. Since 2*i >= i and we add 1, strictly greater.

report("  ... proving P4: left(i) > i")
ok &= prove("P4: left(i) > i", """\
(set-logic QF_BV)
(declare-const i (_ BitVec 16))
(assert (bvule i (_ bv32766 16)))
(define-fun l () (_ BitVec 16) (bvadd (bvmul (_ bv2 16) i) (_ bv1 16)))
(assert (not (bvugt l i)))
(check-sat)
""")

# -- P5: right(i) > i (16-bit BV, UNSAT) ------------------------------------
#
# 2*i+2 > i for i >= 0.

report("  ... proving P5: right(i) > i")
ok &= prove("P5: right(i) > i", """\
(set-logic QF_BV)
(declare-const i (_ BitVec 16))
(assert (bvule i (_ bv32766 16)))
(define-fun r () (_ BitVec 16) (bvadd (bvmul (_ bv2 16) i) (_ bv2 16)))
(assert (not (bvugt r i)))
(check-sat)
""")

# -- P6a: Build start index has a child (n > 1) (16-bit BV, UNSAT) ----------
#
# For idx = (n // 2) - 1, left(idx) = 2*((n//2)-1)+1 < n.
# This ensures the build loop starts at a non-leaf node.

report("  ... proving P6a: build start has a child")
ok &= prove("P6a: left(start_idx) < n", """\
(set-logic QF_BV)
(declare-const n (_ BitVec 16))
(assert (bvugt n (_ bv1 16)))
(assert (bvule n (_ bv32767 16)))
(define-fun start_idx () (_ BitVec 16) (bvsub (bvudiv n (_ bv2 16)) (_ bv1 16)))
(define-fun l () (_ BitVec 16) (bvadd (bvmul (_ bv2 16) start_idx) (_ bv1 16)))
(assert (not (bvult l n)))
(check-sat)
""")

# -- P6b: First leaf has no child (16-bit BV, UNSAT) -------------------------
#
# For idx = n // 2, left(idx) = 2*(n//2)+1 >= n.
# The node at n//2 is the first leaf — no children in the heap.

report("  ... proving P6b: first leaf has no child")
ok &= prove("P6b: left(first_leaf) >= n", """\
(set-logic QF_BV)
(declare-const n (_ BitVec 16))
(assert (bvugt n (_ bv1 16)))
(assert (bvule n (_ bv32767 16)))
(define-fun leaf () (_ BitVec 16) (bvudiv n (_ bv2 16)))
(define-fun l () (_ BitVec 16) (bvadd (bvmul (_ bv2 16) leaf) (_ bv1 16)))
(assert (not (bvuge l n)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: Tournament tree heap index arithmetic is correct")
    print("    P1: parent(left(i)) == i")
    print("    P2: parent(right(i)) == i")
    print("    P3: left(i) != right(i)")
    print("    P4: left(i) > i (strict ordering)")
    print("    P5: right(i) > i (strict ordering)")
    print("    P6a: build start has a child")
    print("    P6b: first leaf has no child")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
