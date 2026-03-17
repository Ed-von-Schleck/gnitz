# Z3 Formal Proofs

Mechanically verified properties of gnitz low-level functions using Z3's SMT solver.
Each proof script documents what it proves in its docstring.

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
    string_prefix.py    # compute_prefix byte-packing correctness
    align_up.py         # align_up formula correctness
    u128_roundtrip.py   # write/read u128 serialization roundtrip
    promote_key.py      # promote_to_index_key injectivity
    bounds_check.py     # MappedBuffer bounds check safety
    serialize_align.py  # serialize.py alignment formula correctness
    promote_group.py    # promote_group_col_to_u64 injectivity
    null_word.py        # null-word bit-set/test correctness
    load_const.py       # LOAD_CONST i64 split/join roundtrip
    exchange_reduce.py  # exchange <-> reduce partition consistency
    shard_bounds.py     # shard region bounds check safety
    distinct_bounds.py  # distinct operator output bounds
    ipc_wire.py         # IPC wire format structural integrity
    catalog_keys.py     # system table composite key pack/unpack
    pending_key.py      # DAG pending key bijectivity
    partition_assign.py # partition assignment coverage
    weight_arith.py     # DBSP weight arithmetic
```

## Prerequisites

- `pypy2` (RPython host interpreter)
- `z3` CLI binary (tested with 4.15.8)
- PyPy source at `../../pypy` (or set `PYPY_PATH`)

## Usage

```
cd proofs
make prove          # full suite
make prove-mix64    # individual target (one per script)
```

Exit code 0 = all proofs pass. Exit code 1 = failure.

## How divergence is prevented

1. SMT-LIB2 is generated from the current source on every run — no separate model
2. Cross-checks validate the translator against concrete RPython execution
3. Multiplication constants are extracted from translator output, not hardcoded

## Writing new proofs

See [GUIDELINES.md](GUIDELINES.md) for lessons learned on bitvector width selection,
query decomposition, cross-checking, and avoiding expensive SMT operations.
