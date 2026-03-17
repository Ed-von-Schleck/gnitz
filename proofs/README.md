# Z3 Formal Proofs

Mechanically verified properties of gnitz low-level functions using Z3's SMT solver.

## Layout

```
proofs/
  Makefile              # build targets
  README.md             # this file
  GUIDELINES.md         # proof-writing guidelines
  rpython_to_smt.py     # shared RPython-to-SMT translator
  checks/
    mix64.py            # Murmur3 finalizer bijectivity
    xor8_bounds.py      # XOR filter index safety
    bloom_safety.py     # Bloom filter memory safety
```

## What this proves

**`_mix64` bijectivity** (`checks/mix64.py`) — the Murmur3 64-bit finalizer
(`gnitz/dbsp/ops/reduce.py`) maps every 64-bit input to a unique 64-bit output.
Proved compositionally: xorshift33 is an involution, multiplication by an odd constant
mod 2^64 is injective, and composition of bijections is a bijection. Cross-checked
against concrete RPython execution.

**XOR filter index safety** (`checks/xor8_bounds.py`) — three properties of
`gnitz/storage/xor8.py`: rotl64 invertibility, fastrange32 output bounds, and segment
non-overlap for h0/h1/h2. Together these guarantee that fingerprint array accesses in
`Xor8Filter.may_contain()` and `build_xor8()` are always in-bounds. 10 Z3 queries
total + cross-check.

**Bloom filter memory safety** (`checks/bloom_safety.py`) — five property groups for
`gnitz/storage/bloom.py`: h2 is always odd (stride never zero), probe position < num_bits
(urem bound), byte index < num_bytes (array in-bounds), bit mask is a valid single-bit
byte value, and all 7 probes are distinct. 11 Z3 queries + cross-check. Uses reduced
bitvector widths (8–32 bit) to avoid expensive `bvurem` bit-blasting.

## Prerequisites

- `pypy2` (RPython host interpreter)
- `z3` CLI binary (tested with 4.15.8)
- PyPy source at `../../pypy` (or set `PYPY_PATH`)

## Usage

```
cd proofs
make prove
```

Exit code 0 = all proofs pass. Exit code 1 = failure.

Individual targets: `make prove-mix64`, `make prove-xor8`, `make prove-bloom`.

## How divergence is prevented

1. SMT-LIB2 is generated from the current source on every run — no separate model
2. Cross-checks validate the translator against concrete RPython execution
3. Multiplication constants are extracted from translator output, not hardcoded

## Writing new proofs

See [GUIDELINES.md](GUIDELINES.md) for lessons learned on bitvector width selection,
query decomposition, cross-checking, and avoiding expensive SMT operations.
