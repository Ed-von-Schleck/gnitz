# Proof-Writing Guidelines

Lessons learned from writing and debugging the mix64, xor8, and bloom proofs.

## Bitvector width selection

Use the **minimum width that covers the property's range**.

`bvurem` and `bvudiv` bit-blast into O(n^2) gates. A 64-bit `bvurem` query can take
minutes; the same property at 16-bit is instant. Width-independent properties (like
`urem(x, n) < n`) can be proven at any width — choose the smallest.

Rule of thumb:
- Property involves only `& mask`, `>> shift`, `<< shift` → use the mask/shift width
- Property involves `urem` or `udiv` → drop to 16–32 bits if the property is width-independent
- Property involves `bvmul` of two symbolic values → keep widths minimal; 64-bit `bvmul` is tolerable but slow

## Query decomposition

**One property per `(check-sat)`**. Never conjoin independent properties into a single
query. Each query should complete in < 1 second. If a query takes > 5 seconds, decompose
it or reduce the bitvector width.

## Cross-checks

**Always** validate the Z3 encoding against concrete RPython execution with test vectors
before the proof phase. This prevents proving properties of the wrong formula.

Use at least 5 test vectors including edge cases (0, 1, MAX, typical values). The
cross-check should run the RPython function under PyPy2 and compare against Z3's
`(simplify ...)` evaluation.

## Script structure

Follow this phase order:

1. **Parse & translate** — read the source, build SMT-LIB2 definitions
2. **Cross-check** — validate Z3 encoding against concrete RPython execution
3. **Proofs** — one `(check-sat)` per property, expect `unsat` for safety proofs
4. **Summary** — report PASS/FAIL per query, exit 0 on success, 1 on failure

Use the `run_z3()` helper pattern (pipe SMT-LIB2 to `z3 -smt2 -in`).

## Avoiding expensive operations

In order of cost (cheapest to most expensive):
- `bvand`, `bvor`, `bvxor`, `bvnot` — essentially free
- `bvshl`, `bvlshr`, `bvashr` — cheap
- `bvadd`, `bvsub` — cheap
- `bvmul` (one symbolic operand) — moderate
- `bvmul` (both operands symbolic) — expensive at 64-bit
- `bvurem`, `bvudiv` — very expensive; avoid at 64-bit

Prefer `bvand` masks over `bvurem` when the divisor is a power of 2. Prefer shifts over
multiplication by powers of 2.

## RPython-to-SMT correspondence

When the RPython code uses `r_uint64`, the natural encoding is 64-bit BV. But if the
*property* only depends on low bits (like `& 7`), prove it at the narrowest width that
captures those bits. Document why the reduced width is sound (e.g., "the property is
width-independent" or "only the low N bits participate").

## File naming

Proof scripts live in `proofs/checks/`. Name them after the module or function being
proved (e.g., `bloom_safety.py`, `mix64.py`). No `prove_` prefix — the directory makes
the purpose clear.
