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
    xor8_capacity.py    # XOR filter capacity formula correctness
    manifest_layout.py  # Manifest entry/header field layout integrity
    heap_index.py       # Tournament tree heap index arithmetic
    xor8_fingerprint.py # XOR filter fingerprint XOR properties
    float_const.py      # Float constant encoding roundtrip & injectivity
    wal_header.py       # WAL block header field layout integrity
    bloom_sizing.py     # Bloom filter sizing formula correctness
    byte_encoding.py    # U16/U8/U32 byte-level encoding roundtrip
    bsearch_midpoint.py # Binary search midpoint safety
    range_key.py        # U128 range key scanning correctness
    wal_directory.py    # WAL columnar directory entry layout
    shard_directory.py  # Shard file header + directory entry layout
    ipc_string_offsets.py # IPC string encoding offset safety
    reduce_sentinel.py  # Reduce accessor sentinel encoding roundtrip
    serialize_offsets.py # Serialization two-phase offset consistency
    ipc_column_chain.py # IPC ZSet full column offset chain
    agg_spec_pack.py    # Aggregation spec packing roundtrip
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
