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
    ipc_flags.py        # IPC protocol flag bit-safety
    opcode_slots.py     # Opcode distinctness and parameter slot non-overlap
    type_constants.py   # Type code distinctness and size/alignment consistency
    join_mapping.py     # CompositeAccessor column mapping correctness
    group_index_key.py  # Reduce group index key composition roundtrip
    schema_stride.py    # Memtable stride calculation correctness
    distinct_sign.py        # DBSP distinct operator sign function correctness
    ipc_header_layout.py    # IPC v2 96-byte request/response header field layout
    german_string_layout.py # German string 16-byte struct layout correctness
    mergesort_partition.py  # Merge sort range partition invariants
    wal_region_count.py     # WAL columnar block region count formula
    expr_opcodes.py         # Expression VM opcode namespace properties
    mix64_consistency.py    # Murmur3 mix64 consistency with avalanche
    short_string_unpack.py  # Short string unpack byte extraction
    reduce_mapping_count.py # Reduce accessor mapping count bounds
    expr_instr_width.py     # Expression instruction width encoding
    catalog_node_edge_dep_pk.py # Catalog node/edge/dep composite PK roundtrip
    tournament_tree_key_split.py # TournamentTree 128-bit key split/join/ordering
    promote_col_to_u128.py  # promote_col_to_u128 injectivity
    promote_group_narrow.py # promote_group_narrow injectivity
    partition_key_routing.py # _partition_for_key bitmask: output always in [0, 255]
    name_tid_hash.py        # _name_to_tid output range [1, 0x7FFFFFFF]
    manifest_key_split.py   # ManifestEntry 128-bit key split/join roundtrip & ordering
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
