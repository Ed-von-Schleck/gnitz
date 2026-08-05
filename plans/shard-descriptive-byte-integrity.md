# Shard and manifest self-consistency: derive what is derivable, witness what is not

A shard's *payload* bytes are checksummed per region. The bytes that decide **how those
payloads are interpreted** — the region directory, the header fields it hangs off, and the
manifest that names the shard — are largely redundant copies of quantities the writer already
derives, and the reader trusts them instead of re-deriving them. Eight forgeries of those bytes are
accepted today; every shard-level one returns a wrong answer rather than an error.

The fix is not to checksum the redundancy. It is to remove it: **a descriptive byte should
either not exist, because it is derivable, or have a second, independent witness.** A stored
offset that the writer computed by a fixed rule is not information — it is a second copy that
can disagree, and every forgery below is a disagreement nobody checks.

## 1. What is accepted today

Measured through `MappedShard::open` at both settings of `validate_checksums` (the flag is
`true` at exactly one production site, `storage/lsm/compact.rs:25`; every other
production open — including boot's `load_manifest` via `ShardEntry::open`,
`storage/lsm/shard_index/mod.rs:78` — passes `false`):

| forgery | `validate = false` | `validate = true` |
|---|---|---|
| `OFF_ROW_COUNT` 10 → 5 | `Ok(5)` — half the rows vanish | `Ok(5)` |
| payload entry encoding `RAW`→`CONSTANT` (**one bit**) | `Ok(10)` — column collapses to row 0 | `Ok(10)` |
| payload entry encoding `RAW`→`FOR`, `count ∈ {1,2,4,8}` | `Ok(n)` — garbage values | `Ok(n)` |
| null-bitmap ↔ 8-byte-payload directory entries swapped | `Ok(n)` — column collapses | `ChecksumMismatch` |
| pk entry `offset` → weight region | `Ok(10)` — wrong | `ChecksumMismatch` |
| blob region `size` flipped within its `align64` bucket | `Ok` — string heap truncated | `ChecksumMismatch` |
| XOR8 `seed` flipped (**one bit**) | `Ok` — shard invisible to every point lookup | `Ok` |
| manifest `OFF_ENTRY_COUNT` 5 → 4 | shard silently drops out of the index | — |

Five mechanisms produce these:

**Lower-bound size checks.** `direct_region` accepts any region at least as large as it needs
(`storage/lsm/shard_reader/open.rs:112`, `if e.size < needed`), and `manifest::parse` accepts
any manifest at least as large as its entry count implies (`if buf.len() < expected_data`).
Shard region sizes are *exact*: `Batch::region_slice` (`storage/repr/batch.rs:1637-1650`) hands
the writer exactly `count * stride`, `write_as_shard` → `regions()`
(`storage/repr/batch_wire.rs:87-95`) is the only production entry to `write_shard_streaming`,
the writer `debug_assert_eq!(orig_sz, n * width)` (`storage/repr/shard_file.rs:425`) and
records `actual_sizes[i] = orig_sz` for RAW (`:449-452`) — `align64` affects region *offsets*,
never the directory's `size` field. The sibling decoder for the same writer↔reader contract
already checks exactly (`batch_wire.rs:340`, `if sizes[r] as usize != n * strides[r] as usize`).
The shard and manifest readers do not.

**Stored offsets nobody re-derives.** The writer computes every region offset by a fixed fold
(`shard_file.rs:463-472`):

```rust
let dir_size = num_regions * DIR_ENTRY_SIZE;
let dir_offset = HEADER_SIZE;
let mut pos = align64(dir_offset + dir_size);
for &actual_sz in actual_sizes.iter().take(num_regions) {
    region_offsets.push(pos);
    pos = align64(pos + actual_sz);
}
```

Every offset in the file is a function of the sizes. The reader reads them back verbatim, so
redirecting one is undetectable.

**Nothing binds a directory slot to the role it describes.** `direct_region` is called with
`FIXED_REGION_BYTES = 8` for the null bitmap (`open.rs:170`) and with the column's stride for
every payload column (`:175-179`), and both accept `{RAW, CONSTANT}`. An all-zero null bitmap
is Constant at size 8; a varying 8-byte payload column is Raw at size `count * 8`. Swapping
those two entries whole gives each the size its new encoding demands, so every per-role check
passes and the payload column collapses to a single wrong value. A per-region checksum cannot
catch this at `validate = false` even in principle, because the checksum travels *inside* the
entry and moves with it.

**An inferred `bw`.** `build_payload_region` derives the FoR byte-width from the region size
(`open.rs:143`, `let bw = (e.size - 8) / count`) and then checks the size against its own
inference. Relabelling a RAW region as `ENCODING_FOR` leaves the size unchanged, so whenever
`count * (elem_width - bw) == 8` the inference lands on a consistent `bw` and the region
decodes as garbage. `ENCODING_CONSTANT` (`0x01`) → `ENCODING_FOR` (`0x03`) is a one-bit flip.

**A filter outside the directory entirely.** The XOR8 filter is written past every region at
`align64(data_end)` (`shard_file.rs:476-478`) with **no directory entry**, so no per-region
checksum can reach it, and `open` admits it on bounds alone (`:191-195`). Its failure is
authoritative: `probe_pk_bytes` (`shard_index/mod.rs:92`) returns `None` on a filter miss, so
one flipped `seed` byte makes an entire shard invisible to every point lookup. Downstream that
is not a slowdown — `enforce_unique_pk` (`query/dag/ingest.rs:330-338`) drops the retraction,
leaving two live rows at one PK and permanently stale views.

### No filesystem backstop

`set_nocow_dir` (`storage/lsm/table/mod.rs`) applies `FS_NOCOW_FL` via `try_set_nocow`
(`foundation/posix_io.rs:165-180`) to the table directory, so every shard inside inherits it.
It is called for `RecoverySource::SalReplay` (base and master system tables, whose shards are
the sole copy) and for `RederiveCheckpointed`. On btrfs `FS_NOCOW_FL` disables data
checksumming — nodatacow implies nodatasum. On ext4/xfs the ioctl returns `EOPNOTSUPP` and
there was never a data checksum. So for exactly the tables that can least afford it, gnitz has
opted out of the only mainstream mechanism that would make this redundant.

## 2. Scope of the guarantee

Every check below is a consistency relation between two derivations, not a hash. That makes it
stronger than a checksum against a wrong writer — a checksum is computed from the same
in-memory bytes it protects, so a self-consistent encoder bug validates perfectly, whereas a
cross-check constrains the writer too. It is weaker against a *coordinated* forgery: an
adversary editing every related field together passes. Storage faults do not do that; a
malicious local writer would. The threat model here is the former — the storage stack returned
different bytes than we wrote.

Three classes stay out of reach, and are equally out of reach of a directory checksum, so they
are not arguments for one design over the other:

- Transplanting a whole region *together with* its directory entry from another shard of the
  same shape. The entry's size still equals `count * stride`, the fold is unchanged, and the
  checksum travels with it.
- A coordinated edit of every field in a consistency relation.
- Corruption *inside* the PK region. It has the same authoritative-miss failure as a corrupt
  filter — `find_lower_bound_bytes` binary-searches over `compare_pk_bytes`, so one flipped
  byte early in a sorted region can make lookups miss anywhere in the shard, and
  `enforce_unique_pk` then drops the retraction. It is a payload region, covered by its
  existing `validate_checksums`-gated checksum and by the demand-paging non-goal in §6, not by
  anything here. §3.4 checksums the filter unconditionally because the filter is fully read at
  open anyway — not because its failure is authoritative, which would give the PK region an
  equal claim.

## 3. Making the forgeries unrepresentable

These change the on-disk format. `SHARD_VERSION` `8 → 9` (`storage/repr/layout.rs:4`).

**This requires deleting existing data directories.** `CatalogEngine::open`
(`runtime/bootstrap.rs:621`) constructs every system table through
`Table::new(...).map_err(...)?` (`catalog/bootstrap.rs:18-26`), so an `InvalidVersion` out of
`load_manifest` (`shard_index/persist.rs:70`) fails the boot on the *first system table*.
Pre-alpha, no deployments, so this is a development cost, not a migration problem — but it is a
real one and the change should not be described as free.

Do **not** bump `STATE_FORMAT`. Its only consumer, `compute_invalid_views`, runs at
`runtime/bootstrap.rs:677` — *after* the `:621` catalog open has already failed — so on a
version bump the per-view verdict never executes and the bump is dead code.

### 3.1 The directory entry

`DIR_ENTRY_SIZE` `32 → 24`:

| field | bytes | why |
|---|---|---|
| `size` | `[0,8)` | the only non-derivable geometry |
| `checksum` | `[8,16)` | unchanged, still `validate_checksums`-gated except for the filter (§3.4) |
| `encoding` | `[16]` | unchanged |
| `bw` | `[17]` | §3.3 |
| `role` | `[18]` | §3.2 |
| reserved | `[19,24)` | zero-validated, as `[25,32)` is today (`open.rs:71-74`) |

The 8-byte `offset` field is **deleted**. Nothing requires 32 or a power of two;
`read_u64_le` is a byte copy (`gnitz-wire/src/lib.rs`), so entry alignment is irrelevant.
`DIR_ENTRY_SIZE` has ten sites — `layout.rs`, three in `shard_file.rs`, two in `open.rs`, and
four in `shard_reader/mod.rs`'s tests — all of which move.

### 3.2 Offsets are derived; a role tag binds each slot to its meaning

Delete `OFF_DIR_OFFSET` (the constant `HEADER_SIZE`) and `OFF_XOR8_OFFSET` / `OFF_XOR8_SIZE`.
The reader replays the writer's fold, exactly as quoted in §1. Offset redirection becomes
unrepresentable: an entry's bytes are located by where the entry sits.

Zero-validate the header bytes this frees, together with the already-reserved `[32,40)` and
`[56,64)` — `[24,64)`, exactly 40 of the 64 header bytes. Not because they could otherwise
change what the reader returns (they cannot; that is why §6 rejects a header checksum), but for
the same reason the entry's `[25,32)` is zero-validated today (`open.rs:71-74`): an undefined
byte that is never checked is one a later reader can start trusting without noticing it was
never written. **This plan owns `[24,64)` at `SHARD_VERSION 9`.** A later version that defines
a field there drops that byte's zero check — normal format evolution, not a foreclosure, since
pre-alpha means no migration.

Derived offsets alone do **not** make a swap safe. The derived offset of the last entry depends
only on the *multiset* `{align64(sᵢ)}` of the preceding sizes, so the fold end is invariant
under any permutation that leaves the last entry in place; and two roles of equal `elem_width`
accept the same encoding set, so the null-bitmap ↔ payload swap in §1 passes every size and
encoding check. The missing constraint is that nothing ties slot *i* to the role the reader
expects there. Add it:

```rust
// A slot's position is its identity, so the slot must say which role it is.
// Without this, two roles of equal element width (the null bitmap and any
// 8-byte payload column) accept each other's entries: the Constant one takes
// the Raw one's size and vice versa, and every other check passes.
if e.role != expected_role_for_slot(i) { return Err(StorageError::InvalidShard); }
```

`expected_role_for_slot` is the existing `REG_*` mapping — `REG_PK`, `REG_WEIGHT`,
`REG_NULL_BMP`, then **payload columns by index**, then the filter, then the blob. Encoding the
payload *index* rather than a generic "payload" kind is load-bearing: two 8-byte payload
columns, one Constant and one Raw, are the same hole.

The role byte is derivable from the slot index, which is exactly what makes it a witness under
§3's rule rather than a violation of it — structurally the same move as §3.3's persisted `bw`.
The alternatives do not work: the null bitmap's width is `FIXED_REGION_BYTES = 8` by definition
and I64/U64/F64 columns are 8 wide, so per-role width sets cannot be made disjoint;
role-seeding the region checksums would only catch the swap at `validate = true`, which is one
production site; and forbidding `ENCODING_CONSTANT` on payload columns would forfeit real
compression while still accepting a Raw entry in the null bitmap's slot.

Close the geometry with exact tiling. This is the **unpadded** end of the last region, not the
fold variable: `pos` is `align64`'d at every step, so after the loop it sits one bucket past a
shard whose file ends unpadded (measured on a real 10-row shard: `file_size = 830`, unpadded
end `830`, fold `pos` `832`).

```rust
// The regions tile the file exactly; the last one ends at EOF. `Mmap::open_ro`
// takes len from fstat and the writer ftruncates to exactly this value.
if region_offsets[last] + sizes[last] != file_size { return Err(StorageError::InvalidShard); }
```

Exact because `Mmap::open_ro` takes `len` from `fstat` (`foundation/posix_io.rs:479-483`) and
`write_shard_streaming_inner` `ftruncate`s to exactly `total_size`. It depends on §3.4 having
made the blob region last — the two are a package.

**Why the cover is complete.** Since the last entry's offset depends only on the multiset of
preceding `align64(sᵢ)`, every size must be pinned or the geometry can shift, and `count` must
be pinned or the relations that pin the sizes are vacuous. So:

| quantity | pinned by |
|---|---|
| `count` | §3.2a's file-size bound, then the PK region's exact size (§3.5) |
| fixed-width region sizes | §4's exact `count * stride` |
| blob size | tiling — it is last (§3.4) |
| filter size | its unconditionally verified checksum (§3.4) |
| every region offset | derived; not stored |
| slot ↔ role | the role byte |
| directory length | the schema, cross-checked by tiling |

Two links are easy to lose. The filter's size is the one no `count * stride` relation
constrains, so without its unconditional checksum a coordinated filter `+64` / blob `−64` edit
satisfies tiling and slides the entire string heap — making that checksum
`validate_checksums`-gated for symmetry reopens the hole. And without §3.2a the `count` row is
false, which makes every row below it false too.

Strides stay schema-derived: `size == count * stride` is only a real cross-check while `stride`
comes from a witness independent of the file. A region *count* is likewise not worth a field —
the reader needs the schema anyway (`pk_stride`, `strides`, `col.type_code` in `open.rs`), and a
file whose arity disagrees with the schema is already rejected by tiling plus §4.

The reader now replays the writer's padding fold, a coupling that does not exist today. It is
the same shape as `strides_from_schema`, already described in-tree as "the writer↔reader
region-layout contract" (`batch_wire.rs:312-314`), and it replaces a coupling that was silent
with one that is checked.

### 3.2a `count` must be bounded before it is multiplied

`count` is `read_u64_le(data, OFF_ROW_COUNT) as usize` (`open.rs`), and every size relation
below is an unchecked `usize` multiply — `count * elem_width`, `8 + count * bw`. So
`count' = count + 2^k` yields an **identical product** whenever `k + v₂(stride) ≥ 64`: at
`pk_stride = 8`, bits 61, 62 and 63 of `OFF_ROW_COUNT` can each be flipped with every size
relation still satisfied. Verified: `(10 + 2^61) * 8 mod 2^64 == 80 == 10 * 8`. In release the
shard opens claiming `2^61 + 10` rows and the next `find_lower_bound_bytes` segfaults; in debug
the multiply panics.

Nothing else in this plan catches it — tiling and the role byte are functions of the sizes and
the slot index, both untouched, and `(filter.size == 0) != (count == 0)` passes because both
sides are false. One bound fixes it, and keeps the derive-don't-store rule: a row costs at
least one PK byte, so

```rust
// Every subsequent size relation multiplies `count`. Bound it against the file
// first: a row costs >= 1 PK byte, so no honest count can exceed the file size,
// and no product can then overflow.
if count > file_size { return Err(StorageError::InvalidShard); }
```

placed immediately after `OFF_ROW_COUNT` is read, before any region builder. Every product is
then at most `file_size * 16`.

### 3.3 `bw` is persisted, not inferred

The entry's `bw` byte carries the FoR byte-width. Add the parity constraint and delete the
inference block (`open.rs:140-146`):

```rust
// bw is written, not recovered from the size — inferring it let a relabelled
// RAW region land on a self-consistent width whenever count*(w-bw) == 8.
if (e.encoding == ENCODING_FOR) != (e.bw != 0) { return Err(StorageError::InvalidShard); }
```

`bw` fits a byte with room to spare: `is_fixed_int` implies an element width in `{1,2,4,8}`
(`gnitz-wire/src/types.rs`) and the reader requires `bw < elem_width`, so `bw ≤ 7`. Keep the
`is_fixed_int(type_code)` gate at `open.rs:130` — it is what bounds `elem_width ≤ 8` and keeps
`decode_for_region`'s `to_le_bytes()[..elem_width]` in range. The surviving bound at `:144` is
`bw < 1 || bw >= elem_width`.

### 3.4 The XOR8 filter becomes a directory region, second to last

The filter gets a directory entry, always present — size 0 when `count == 0` — and sits
**before** the blob, so the blob is the last region.

- **Always present** keeps the directory length the constant `nr + 2`, `expected_role_for_slot`
  static, and the blob's entry index a constant. The alternative, deriving the filter's presence
  from `count > 0`, makes all three count-dependent, so a forged `count` would shift every
  region offset in the file — an amplification that does not exist today. The empty-region
  machinery already handles this shape: `shard_file.rs:416-419` short-circuits any zero-size
  region to Raw, `:493-497` records checksum 0, `:541` skips the pwrite, and `empty_shard`
  (`shard_file.rs:610-626`) already writes and opens four size-0 regions. The cross-check the
  length-derivation would have given moves into the entry, at the same strength:
  `if (filter.size == 0) != (count == 0) { return Err(StorageError::InvalidShard) }`.
- **Second to last** makes the blob last, and the blob is the one region whose size is *not*
  `count * stride` — it is data, so nothing constrains it except tiling. With the blob last,
  `blob_off + blob_len == file_size` pins it exactly. With the filter last the blob would be
  pinned only to its `align64` bucket, and a flip shortening `blob_len` by up to 63 bytes is
  accepted and sends German long-string cells past the heap slice (`access.rs:122`, `:350-355`).

The filter's legal encoding is `ENCODING_RAW` only, like the blob's. Its checksum is verified
**unconditionally**, not under `validate_checksums`: this adds no page faults, since
`xor8::deserialize` already copies every one of those bytes out of the mapping (`xor8.rs:85`),
and §3.2 depends on it for geometric completeness. `xor8.rs` itself is untouched.

**A size-0 filter region is skipped entirely** — no checksum, no `deserialize`, `xor8_filter =
None`. Both steps would otherwise reject every `count == 0` shard: the writer records checksum
`0` for a zero-size region while `xxh3_64(&[])` is `0x2d06800538d394c2`, and
`deserialize(&[])` returns `None`, which `open` now treats as `InvalidShard`. The
`(filter.size == 0) != (count == 0)` cross-check is what makes the skip safe — a filter may be
absent only when there are no keys to filter.

This deletes `OFF_XOR8_OFFSET`, `OFF_XOR8_SIZE`, the special-case bounds test, and the separate
`pwrite`.

Writer restructuring: build the directory source as `regions[..nr] ++ [filter] ++ [regions[nr]]`
and drive everything from that. Everything that indexes `regions[i]` by directory index has to
come off that vector — phase 1's encoding/size loop, the checksum/directory pass, the pwrite
loop, and `data_end`'s `actual_sizes[num_regions - 1]` — and `debug_assert_eq!(num_regions, nr + 1)`
becomes `nr + 2`. The filter is built in phase 2 today, *after* phase 1; it must move ahead of
phase 1 so its bytes exist when the directory source is assembled. That move is free — it reads
only `regions[REG_PK]`. The existing variable-length-tail test `i >= nr` already covers both
trailing regions, which is what keeps the filter out of the FoR-eligibility gate
(`i >= REG_PAYLOAD_START`). Exactly one production reader assumes the blob sits at index `nr`
— the blob read in `open`; everything downstream reads `MappedShard`'s `blob_off`/`blob_len`
fields, never a directory slot.

### 3.5 The PK region is always `ENCODING_RAW`

Two halves, both required:

- **Writer**: force Raw for the PK region at the *call site* — add an `i == REG_PK` arm to the
  existing `if i == REG_WEIGHT { … } else { … }` encoding-selection chain, turning it three
  ways. **Not** inside `detect_encoding`, which is the shared Constant detector for the null
  bitmap and every payload column
  and would regress all of them to Raw. It only changes `pk_stride ≤ 16`, since
  `detect_encoding` already returns Raw above that; `pk_stride` can reach 80.
- **Reader**: reject a PK entry whose encoding is not `ENCODING_RAW`. This is the half that
  makes the property hold. Without it, §4's exact size check still accepts `size == pk_stride`
  for a Constant PK entry and `count` is unpinned again.

Then `size == count * pk_stride` makes `OFF_ROW_COUNT` and the PK region's size witnesses for
each other, and `count` is pinned for every shard — including the single-row and
single-distinct-PK cases a size check alone leaves open. `pk_stride ≥ 1` always (at least one
PK column, each at least one byte wide), so the relation is injective in `count` **over the
range §3.2a admits**; without that bound it is injective only modulo 2⁶⁴. The weight region
needs no equivalent treatment.

Measured cost across the entire e2e suite: 101 shards had an all-identical PK region with more
than one row; 97 changed by **0 bytes** and the total across the whole suite was **+69,376
bytes**. A region contributes `align64(size)` to the file length, so the overhead is
`align64(count * pk_stride) − align64(pk_stride)` — zero for every `count ≤ 8` at
`pk_stride = 8`, which is why almost all of the 101 were free.

`u64_pk_constant_shard` (`shard_file.rs:721-761`) asserts the PK region *must* be
`ENCODING_CONSTANT`; it must be inverted.

## 4. Exact sizes

Format-free, and independently worth landing. In `direct_region` (`open.rs:106-119`) and
`build_weight_region`'s TwoValue guard (`:154-166`), tighten the lower bound to equality:

```rust
// Region sizes are exact by construction (`Batch::region_slice` hands the writer
// exactly count*stride), so `count` is cross-checked here rather than trusted.
if e.size != needed { return Err(StorageError::InvalidShard); }
```

```rust
if e.size != 16 + count.div_ceil(8) { return Err(StorageError::InvalidShard); }
```

This alone rejects the `RAW`→`CONSTANT` forgery — `ENCODING_CONSTANT` demands
`size == elem_width` while a RAW region carries `count * elem_width` — and was verified in
isolation against the full suite: 1381/1381 engine tests, clippy clean.

Apply the same correction to `manifest::parse`, which today accepts any buffer at least
`HEADER_SIZE + count * ENTRY_SIZE` long:

```rust
// A manifest is written at exactly `serialized_size(count)`. Split by direction so
// each error keeps its meaning: short is a genuinely truncated file, long is a
// forged count that would silently drop trailing shards out of the LSM index.
if buf.len() < expected_data { return Err(StorageError::Truncated); }
if buf.len() > expected_data { return Err(StorageError::InvalidShard); }
```

Reusing `InvalidShard` for a manifest is a naming compromise, not a functional one: no
production caller discriminates on any `StorageError` variant today, and `error.rs`'s own rule
is to add variants only when a caller needs to discriminate. §5.1 introduces the first such
caller, and the variant it discriminates on is `Io`, not this one. Widen `InvalidShard`'s doc
comment, which currently describes only shard directory entries.

`prepare_file` allocates exactly `serialized_size(count)` and writes `&buf[..written]` with
`written == total`, so equality holds by construction. Use `InvalidShard`, not `Truncated` —
the check now covers the too-long direction too, where `Truncated` would be a lie.

## 5. The manifest

Filenames, levels, guard keys, `max_lsn` and `generation` are opaque, non-derivable data with
no second witness anywhere. A checksum is the only available tool, and unlike the shard's
directory there is no derivation to replace it with.

Add `OFF_CHECKSUM = 40` in the free header slot, XXH3-64 over the serialized manifest with the
checksum field's eight bytes excluded, bump `VERSION` `7 → 8`. `serialize` and `parse` are the
sole codec, and every reader routes through `parse` (`read_file`, `load_manifest` via
`persist.rs:59`, and `child_dir.rs:117`; `child_dir.rs:135` hard-links a manifest byte-for-byte,
so the digest survives seeding). Hash two spans rather than copying the buffer to zero eight
bytes:

```rust
/// XXH3-64 over the serialized manifest with the checksum field excluded. The
/// one derivation `serialize` and `parse` share.
fn digest(buf: &[u8], total: usize) -> u64 {
    let mut h = xxhash_rust::xxh3::Xxh3Default::default();
    h.update(&buf[..OFF_CHECKSUM]);
    h.update(&buf[OFF_CHECKSUM + 8..total]);
    h.digest()
}
```

**`parse`'s check order is fixed**: magic → version → exact length (§4) → digest → entries. The
digest covers the entry-count field, so verifying it first would make a forged count report
`ChecksumMismatch` and shadow the structural check that names the actual defect. Same principle
§6 applies to the shard.

### 5.1 A live boot failure, independent of any of the above

`Table::new`'s `RederiveCheckpointed` arm is `if peek_generation(&cpath)? == Some(committed)`.
`peek_header_u64` returns `Err(Truncated)` for a short file and `Err(InvalidMagic)` for a bad
one, so the `?` fires **today** on a damaged manifest and fails the boot, where the `else`
branch (`erase_stale_shards` + `remove_file`) is the correct, curative verdict. Separately,
`peek_header_u64` reaches the file through `open_owned`, which delegates to `openat_owned` and
discards errno (`posix_io.rs:88-92`), so an `EMFILE` at boot returns `Ok(None)` and silently
takes the *destructive* branch.

Route `peek_generation` through the one validated parser — `read_file` uses `std::fs::read` and
distinguishes `NotFound` from every other error — and match instead of propagating:

```rust
pub fn peek_generation(path: &std::ffi::CStr) -> Result<Option<u64>, StorageError> {
    Ok(read_file(path)?.map(|(_, header)| header.generation))
}
```

```rust
// A malformed manifest is not a generation match — rebuild, don't fail the boot.
// A transient I/O error is not evidence of staleness, so it must not erase.
let generation_valid = match super::manifest::peek_generation(&cpath) {
    Ok(g) => g == Some(committed),
    Err(StorageError::Io) => return Err(StorageError::Io),
    Err(_) => false,
};
```

This deletes `peek_header_u64`, and with it `posix_io::read_all_fd` — whose only other caller
is inside `posix_io.rs`'s own `#[cfg(test)] mod tests`, so leaving it would fail `make verify`
on `-D warnings`. That test reads back an `O_TMPFILE` inode and must be repointed at another
read helper.

The cost, which is intrinsic rather than accidental: `peek_generation` reads 48 bytes today and
will read and parse the whole manifest (160 B/entry). §5's digest spans the entries, so no
header-only peek can validate it. This runs per cluster child per view pre-fork
(`partition_lsn.rs:270-273`) and again per `RederiveCheckpointed` table immediately before
`load_manifest` reads the same file. `compute_invalid_views`'s `at_g` already treats any
non-`Ok(Some(g))` as invalid with no `?`, so the master-side verdict needs no change: a
checksum-failing manifest rebuilds its view, and phase 2 cascades to dependents.

## 6. Non-goals

- **A header/directory checksum.** Once §3.2a, §3.2's role byte and §4 are all in place, every
  byte it would catch that they do not is a byte whose corruption cannot change what the reader
  returns — a `checksum` field at `validate = false`, or a reserved header byte (which §3.2
  zero-validates anyway). That is the same property §7's single-bit sweep asserts, and it is
  contingent on all three: drop §3.2a and the high bits of `OFF_ROW_COUNT` become a
  counterexample a checksum would catch; drop the role byte and the whole-entry-swap class
  becomes one. Against that margin,
  a checksum *shadows* the format's structural checks — a forgery test would have to re-stamp it
  before its forgery could reach the decode-site check it exists to exercise. (§3 shadows too:
  §3.5's PK-RAW gate and §4's exact sizes now reject several forgeries before their original
  decode site sees them. But it substitutes an earlier structural check, not an opaque hash, and
  §7 pins down which site must reject what.)
- **The corruption *response*.** A shard that fails to open is handled four different ways
  today, including being swallowed at `query/vm/mod.rs:204` and `query/vm/exec.rs:337`. That
  incoherence predates this plan and already applies to `InvalidShard`, `Truncated`,
  `InvalidMagic` and `InvalidVersion`. A correct policy has to be keyed on where a table's data
  can be rebuilt from and has to cover the boot path, which no `&mut self` method on a
  constructed `Table` can reach. Separate plan.
- **Payload-region validation on the read path**, including the PK region (§2). Regions are
  demand-paged (`MAP_SHARED`, no `MAP_POPULATE`, `posix_io.rs:459-473`), so validating them at
  open would fault in a whole shard for a point lookup. Their per-region checksums stay
  compaction-gated. The filter is the sole exception, and only because it is fully read at open
  regardless.
- **No background scrub**; at-rest rot between two opens is undetected.
- **Encoder bugs** remain invisible where the writer is wrong in both derivations (§2).
- **A schema-free `MappedShard::open`.** Persisting strides would remove the reader's
  independent witness (§3.2), and the ALTER-driven case for a schema-invariant reader belongs to
  whichever plan needs it.

## 7. Tests

Rust, `gnitz-engine`:

- **Forgery matrix** at both `validate_checksums` settings. The expected verdict differs per row
  and must be asserted per row:
  - `OFF_ROW_COUNT` down, up, and to `0`; **each of bits 61, 62 and 63 of `OFF_ROW_COUNT` set**
    (these leave every size product unchanged and are rejected only by §3.2a's bound — without
    it the shard opens claiming 2⁶¹ rows and the next lookup segfaults); payload encoding
    `RAW`→`CONSTANT`; payload encoding `RAW`→`FOR` at `count ∈ {1,2,4,8}` and at `count = 10`;
    a region `size` altered within and across its `align64` bucket; the blob `size` flipped
    within its bucket; a filter `size` `+64` paired with a blob `size` `−64` (tiling-neutral,
    caught only by the filter's unconditional checksum); the file truncated by one region; a
    directory entry's `role` byte altered — each `InvalidShard`.
  - **PK entry forced to `ENCODING_CONSTANT` on a `count == 1` shard.** The encoding-only flip
    must be tested at `count == 1`, where `count * pk_stride == pk_stride` so §4's size check
    passes and §3.5's reader gate is the rejecting site. At any larger `count` the size check
    fires first and the gate — "the half that makes the property hold" — would have no test.
  - **Three swap cases**, because they are not one behaviour:
    (i) two entries of identical `(size, encoding, bw)` — `InvalidShard` via the role tag. This
    is the case every other check is blind to;
    (ii) a Constant↔Raw swap between the null bitmap and an 8-byte payload column —
    `InvalidShard` via the role tag. Accepted today, and still accepted by derived offsets
    alone, which is why §3.2 exists;
    (iii) `(size, checksum, encoding, bw)` swapped between two entries **with the role bytes
    left in place**, chosen so the last entry moves — `InvalidShard` via tiling. The role bytes
    must stay put or the role check rejects first and tiling gets no test.
- **Exhaustive single-bit sweep** over the whole header plus directory: every accepted open must
  be observationally identical to the clean open (row count, every PK, every payload value,
  every weight, every null bit). Run it over an all-Constant shard, a TwoValue-weight shard, a
  FoR-packed shard, **and a STRING-payload shard with a non-empty blob** — without the last, the
  sweep passes vacuously while the blob-size hole §3.4 closes is still open. A one-payload-column
  schema has six directory slots (pk, weight, null, payload0, filter, blob), so header+directory
  is `64 + 6*24 = 208` bytes — ~1.7k opens per shape. The sweep must run in **both** profiles:
  the §3.2a counterexample panics on an unchecked multiply in dev and segfaults in release, so a
  release-only sweep would miss the panic and a dev-only sweep would miss the segfault.
- **Randomized multi-byte corruption**, 1–8 contiguous bytes over the same span, same invariant
  — the shape of a torn or misdirected write.
- **PK-always-RAW**: a shard whose rows share one PK opens with the PK region `ENCODING_RAW`,
  and forging its `count` is rejected. Invert `u64_pk_constant_shard` (`shard_file.rs:721-761`).
- **XOR8 as a region**: flip a fingerprint byte and separately the `seed` — `open` returns
  `InvalidShard` at both `validate_checksums` settings. A `count == 0` shard opens with a
  size-0 filter entry still present, and a filter entry whose emptiness disagrees with `count`
  is rejected. Point lookups against an intact filter are unchanged.
- **Manifest**: forging `OFF_ENTRY_COUNT` down returns `Truncated` (the length check, not the
  digest — this pins `parse`'s check order); appending trailing bytes returns `InvalidShard`;
  flipping a byte in an entry's `FILENAME`, `LEVEL`, `GUARD_KEY` and in the header's
  `OFF_GENERATION` and `OFF_COMPACT_SEQ` returns `ChecksumMismatch`; `serialize`/`parse`
  round-trips at zero, one and many entries.
- **Boot verdict**: a `RederiveCheckpointed` `Table::new` against a truncated, bad-magic or
  checksum-failing manifest **opens empty and rebuilds** rather than returning `Err`; against an
  `Io` failure it returns `Err` and erases nothing. All three of the first cases fail the boot
  today.
- **Every decode-site check keeps a test that reaches it.** The existing forgery tests
  (`shard_reader/mod.rs:383, 404, 425, 466, 964, 989`, plus `checksum_catches_corrupted_packed_region`
  at `:1097`) all need mechanical updates — `OFF_DIR_OFFSET` is gone and `DIR_ENTRY_SIZE`
  changed, so every hard-coded directory offset moves, `:1097` must fold to find its region
  rather than reading an offset field, and `:964`'s `4 /* blob */` slot is now the filter. That
  is expected. What is **not** acceptable is a test that still returns `InvalidShard` while no
  longer reaching the check it was written for: `unknown_encoding_rejected`,
  `two_value_pk_region_rejected`, `forged_for_on_non_payload_rejected` and
  `forged_for_payload_bad_size_rejected` are each shadowed by §3.5 or §4 under a naive update.
  Each must assert the *rejecting site*, not merely the error kind, so shadowing shows up as a
  failure. No forgery may need re-legalizing — a re-stamped checksum or a re-derived field — to
  reach its decode site.
- `make verify` green; `make e2e` at `GNITZ_WORKERS=4` green.
