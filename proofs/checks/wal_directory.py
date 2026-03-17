"""Z3 formal proofs for WAL columnar directory entry layout.

Proves five properties of gnitz/storage/wal_columnar.py:64-67,119-124:

  dir_start = WAL_BLOCK_HEADER_SIZE  # = 48
  dir_size = num_data_regions * 8    # 8 bytes per entry

  Each entry: u32 offset (4 bytes) + u32 size (4 bytes) = 8 bytes
  dir_entry_ptr = rffi.ptradd(base_ptr, dir_start + ri * 8)
  u32p[0] = region_offsets[ri]   # u32
  u32p[1] = region_sizes[ri]     # u32

  P1. Entry size is 8 bytes (16-bit BV, UNSAT)
  P2. Entries non-overlapping — stride matches entry size (16-bit BV, UNSAT)
  P3. u32 fields within entry are non-overlapping (16-bit BV, UNSAT)
  P4. Directory starts after header (16-bit BV, UNSAT)
  P5. Directory end is 8-byte aligned (32-bit BV, UNSAT)

5 Z3 queries + ~5 cross-checks.  Runs under PyPy2.
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

DIR_START = 48  # WAL_BLOCK_HEADER_SIZE
ENTRY_SIZE = 8  # u32 offset + u32 size

# -- Main ---------------------------------------------------------------------

print("=" * 56)
print("  Z3 PROOF: WAL columnar directory entry layout")
print("=" * 56)
sys.stdout.flush()

ok = True

# -- Cross-checks: directory layout for various region counts ----------------

report("  ... cross-checking directory layout")

test_region_counts = [5, 6, 8, 10, 20]

for n in test_region_counts:
    dir_size = n * ENTRY_SIZE
    dir_end = DIR_START + dir_size

    # All entries non-overlapping
    entries_ok = True
    for i in range(n):
        entry_start = DIR_START + i * ENTRY_SIZE
        entry_end = entry_start + ENTRY_SIZE
        if i + 1 < n:
            next_start = DIR_START + (i + 1) * ENTRY_SIZE
            if entry_end > next_start:
                entries_ok = False

    # Directory end is 8-byte aligned
    aligned = (dir_end % 8 == 0)

    # Z3 simplify cross-check for dir_end alignment
    smt_q = "(simplify (= (bvand (bvadd (_ bv48 32) (bvmul (_ bv%d 32) (_ bv8 32))) (_ bv7 32)) (_ bv0 32)))" % n
    z3_out = run_z3(smt_q)
    z3_ok = (z3_out == "true")

    if entries_ok and aligned and z3_ok:
        report("  PASS  cross-check n=%d: dir_end=%d aligned=%s" % (n, dir_end, aligned))
    else:
        report("  FAIL  cross-check n=%d: entries=%s aligned=%s z3=%s" % (
            n, entries_ok, aligned, z3_ok))
        ok = False

if not ok:
    print("=" * 56)
    print("  FAILED: cross-check mismatch")
    print("=" * 56)
    sys.exit(1)

# -- P1: Entry size is 8 bytes (16-bit BV, UNSAT) ---------------------------

report("  ... proving P1: entry size is 8 bytes")

ok &= prove("P1: u32 + u32 == 8", """\
(set-logic QF_BV)
(assert (not (= (bvadd (_ bv4 16) (_ bv4 16)) (_ bv8 16))))
(check-sat)
""")

# -- P2: Entries non-overlapping (16-bit BV, UNSAT) -------------------------
#
# For any index i, entry i ends at or before entry i+1 starts:
# dir_start + i*8 + 8 <= dir_start + (i+1)*8

report("  ... proving P2: entries non-overlapping")

ok &= prove("P2: entry[i] ends <= entry[i+1] starts", """\
(set-logic QF_BV)
(declare-const i (_ BitVec 16))
(assert (bvule i (_ bv254 16)))
(define-fun entry_end () (_ BitVec 16)
  (bvadd (bvadd (_ bv48 16) (bvmul i (_ bv8 16))) (_ bv8 16)))
(define-fun next_start () (_ BitVec 16)
  (bvadd (_ bv48 16) (bvmul (bvadd i (_ bv1 16)) (_ bv8 16))))
(assert (not (bvule entry_end next_start)))
(check-sat)
""")

# -- P3: u32 fields within entry non-overlapping (16-bit BV, UNSAT) ---------
#
# Within one 8-byte entry: offset occupies [0, 4), size occupies [4, 8).

report("  ... proving P3: intra-entry fields non-overlapping")

ok &= prove("P3: byte in [0,8) belongs to offset or size field", """\
(set-logic QF_BV)
(declare-const b (_ BitVec 16))
(assert (bvuge b (_ bv0 16)))
(assert (bvult b (_ bv8 16)))
(assert (not (or
  (and (bvuge b (_ bv0 16)) (bvult b (_ bv4 16)))
  (and (bvuge b (_ bv4 16)) (bvult b (_ bv8 16))))))
(check-sat)
""")

# -- P4: Directory starts after header (16-bit BV, UNSAT) -------------------

report("  ... proving P4: directory starts after header")

ok &= prove("P4: dir_start >= WAL_BLOCK_HEADER_SIZE", """\
(set-logic QF_BV)
(assert (not (bvuge (_ bv48 16) (_ bv48 16))))
(check-sat)
""")

# -- P5: Directory end is 8-byte aligned (32-bit BV, UNSAT) -----------------
#
# Since dir_start=48 is 8-aligned and dir_size=n*8 is 8-aligned,
# dir_end = 48 + n*8 is 8-aligned.  Prove: align_up(dir_end, 8) == dir_end.

report("  ... proving P5: directory end is 8-byte aligned")

ok &= prove("P5: 48 + n*8 is 8-aligned", """\
(set-logic QF_BV)
(declare-const n (_ BitVec 32))
(assert (bvuge n (_ bv5 32)))
(assert (bvule n (_ bv256 32)))
(define-fun dir_end () (_ BitVec 32) (bvadd (_ bv48 32) (bvmul n (_ bv8 32))))
(assert (not (= (bvand (bvadd dir_end (_ bv7 32)) (bvnot (_ bv7 32))) dir_end)))
(check-sat)
""")

# -- Summary ------------------------------------------------------------------

print("=" * 56)
if ok:
    print("  PROVED: WAL columnar directory entry layout is correct")
    print("    P1: entry size is 8 bytes")
    print("    P2: entries non-overlapping")
    print("    P3: intra-entry fields non-overlapping")
    print("    P4: directory starts after header")
    print("    P5: directory end is 8-byte aligned")
else:
    print("  FAILED: see above")
print("=" * 56)

sys.exit(0 if ok else 1)
