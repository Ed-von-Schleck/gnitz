"""Z3 formal proofs for partition assignment coverage.

Proves three SMT properties and one exhaustive cross-check for
gnitz/server/master.py PartitionAssignment:
  P1. chunk >= 1 for N in [1, 256] (16-bit BV)
  P2. Last worker's range is non-empty (16-bit BV)
  P3. Counterexample: chunk == 0 for N > 256 (SAT)
  Exhaustive: All 256 partitions covered exactly once for every N in [1, 256]

Source:
  - gnitz/dbsp/ops/exchange.py (PartitionAssignment class)

  chunk = 256 // num_workers
  starts[w] = w * chunk
  ends[w] = (w+1)*chunk if w < N-1 else 256

Partitions 0-255 are divided among N workers.  Missing partition = lost data.
Overlapping partition = duplicated data.  Both are silent correctness bugs.

3 Z3 queries + 256-iteration exhaustive check + ~8 Z3 cross-checks.
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


def check_sat(label, smt_text):
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

print("=" * 60)
print("  Z3 PROOF: partition assignment coverage")
print("=" * 60)
sys.stdout.flush()

ok = True

# -- Exhaustive cross-check: all N in [1, 256] --------------------------------
#
# For every possible num_workers, verify:
# 1. chunk >= 1
# 2. starts[0] == 0
# 3. ends[N-1] == 256
# 4. Adjacent ranges are contiguous: ends[w] == starts[w+1]
# 5. All 256 LUT entries are assigned exactly once
# 6. Each LUT entry maps to a valid worker ID in [0, N)

report("  ... exhaustive cross-check: all N in [1, 256]")

for num_workers in range(1, 257):
    chunk = 256 // num_workers
    if chunk < 1:
        report("  FAIL  exhaustive N=%d: chunk=%d < 1" % (num_workers, chunk))
        ok = False
        continue

    starts = [0] * num_workers
    ends = [0] * num_workers
    for w in range(num_workers):
        starts[w] = w * chunk
        if w == num_workers - 1:
            ends[w] = 256
        else:
            ends[w] = (w + 1) * chunk

    # Check starts[0] == 0
    if starts[0] != 0:
        report("  FAIL  exhaustive N=%d: starts[0]=%d != 0" % (num_workers, starts[0]))
        ok = False
        continue

    # Check ends[N-1] == 256
    if ends[num_workers - 1] != 256:
        report("  FAIL  exhaustive N=%d: ends[N-1]=%d != 256" % (num_workers, ends[num_workers - 1]))
        ok = False
        continue

    # Check contiguity
    contiguous = True
    for w in range(num_workers - 1):
        if ends[w] != starts[w + 1]:
            report("  FAIL  exhaustive N=%d: ends[%d]=%d != starts[%d]=%d" % (
                num_workers, w, ends[w], w + 1, starts[w + 1]))
            ok = False
            contiguous = False
            break
    if not contiguous:
        continue

    # Build LUT and verify all 256 assigned exactly once
    lut = [0] * 256
    assigned = [0] * 256
    for w in range(num_workers):
        for p in range(starts[w], ends[w]):
            lut[p] = w
            assigned[p] += 1

    all_once = True
    for p in range(256):
        if assigned[p] != 1:
            report("  FAIL  exhaustive N=%d: partition %d assigned %d times" % (
                num_workers, p, assigned[p]))
            ok = False
            all_once = False
            break
    if not all_once:
        continue

    # Check valid worker IDs
    valid_ids = True
    for p in range(256):
        if lut[p] < 0 or lut[p] >= num_workers:
            report("  FAIL  exhaustive N=%d: partition %d -> worker %d out of range" % (
                num_workers, p, lut[p]))
            ok = False
            valid_ids = False
            break

report("  PASS  exhaustive cross-check: all 256 worker counts verified")

# -- Z3 cross-check: bvudiv for selected N values ----------------------------

report("  ... Z3 cross-check: bvudiv for selected N values")

z3_cross_vectors = [1, 2, 3, 4, 7, 128, 255, 256]

for n in z3_cross_vectors:
    py_chunk = 256 // n
    smt_query = "(simplify (bvudiv (_ bv256 16) (_ bv%d 16)))" % n
    z3_out = run_z3(smt_query)
    z3_val = parse_z3_value(z3_out)
    if z3_val is None:
        report("  FAIL  Z3 cross-check N=%d: unexpected output: %s" % (n, z3_out))
        ok = False
    elif z3_val == py_chunk:
        report("  PASS  Z3 cross-check N=%d: 256//%d = %d" % (n, n, py_chunk))
    else:
        report("  FAIL  Z3 cross-check N=%d: Python=%d Z3=%d" % (n, py_chunk, z3_val))
        ok = False

if not ok:
    print("=" * 60)
    print("  FAILED: cross-check mismatch")
    print("=" * 60)
    sys.exit(1)

# -- P1: chunk >= 1 for N in [1, 256] (16-bit BV) ----------------------------
#
# Uses 16-bit BV because bvudiv is O(n^2). Width is sufficient (max value 256).

report("  ... proving P1: chunk >= 1 for N in [1, 256]")
ok &= prove("P1: 256 // N >= 1 for N in [1, 256]", """\
(set-logic QF_BV)
(declare-const N (_ BitVec 16))
(assert (bvugt N (_ bv0 16)))
(assert (bvule N (_ bv256 16)))
(define-fun chunk () (_ BitVec 16) (bvudiv (_ bv256 16) N))
(assert (not (bvuge chunk (_ bv1 16))))
(check-sat)
""")

# -- P2: Last worker's range is non-empty (16-bit BV) ------------------------
#
# (N-1) * chunk < 256 ensures [starts[N-1], 256) is non-empty.

report("  ... proving P2: last worker's range is non-empty")
ok &= prove("P2: (N-1) * (256 // N) < 256 for N in [1, 256]", """\
(set-logic QF_BV)
(declare-const N (_ BitVec 16))
(assert (bvugt N (_ bv0 16)))
(assert (bvule N (_ bv256 16)))
(define-fun chunk () (_ BitVec 16) (bvudiv (_ bv256 16) N))
(define-fun last_start () (_ BitVec 16) (bvmul (bvsub N (_ bv1 16)) chunk))
(assert (not (bvult last_start (_ bv256 16))))
(check-sat)
""")

# -- P3: Counterexample — chunk == 0 for N > 256 (SAT) ----------------------
#
# Shows the algorithm breaks if num_workers exceeds 256.

report("  ... proving P3: chunk == 0 for N > 256 (SAT)")
ok &= check_sat("P3: 256 // N == 0 exists for N > 256", """\
(set-logic QF_BV)
(declare-const N (_ BitVec 16))
(assert (bvugt N (_ bv256 16)))
(define-fun chunk () (_ BitVec 16) (bvudiv (_ bv256 16) N))
(assert (= chunk (_ bv0 16)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 60)
if ok:
    print("  PROVED: partition assignment coverage")
    print("    Exhaustive: all 256 worker counts -> full [0,256) coverage")
    print("    P1: chunk >= 1 for N in [1, 256]")
    print("    P2: last worker's range is non-empty")
    print("    P3: chunk == 0 exists for N > 256 (counterexample)")
else:
    print("  FAILED: see above")
print("=" * 60)

sys.exit(0 if ok else 1)
