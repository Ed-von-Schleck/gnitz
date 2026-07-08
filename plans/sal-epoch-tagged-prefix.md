# SAL epoch-tagged group prefix — fix the torn-header read on epoch transitions

## The bug

A worker can crash (debug) or silently desync its SAL cursor (release) by
reading a group header that the master is concurrently overwriting in place
after a checkpoint reset. Captured live under CPU oversubscription
(`tests/test_checkpoint.py`, GNITZ_WORKERS=4, ~2 hits per ~85 file-runs under
3× concurrent load):

```
thread 'main' panicked at gnitz-engine/src/runtime/protocol/sal.rs:635:
SAL group header corrupt: my_offset=1872 my_size=3776 payload_size=2304
  sal_read_group_header → SalReader::try_read
  → WorkerProcess::next_sal_message → drain_sal
```

`payload_size` (2304) is the group-size prefix, Acquire-loaded at the read
cursor; `my_offset`/`my_size` (1872 + 3776 = 5648 > 2304) are directory words
read afterwards with plain loads. No consistently-observed group can show
this: the prefix is the **stale pre-reset group's** size while the directory
already belongs to the **larger new group** being written over the same bytes.
The master then reports "Worker N crashed, shutting down" and every client
sees `connection closed` — the flaky failure mode of the checkpoint e2e tests.

### Mechanism

Writer protocol (`sal.rs`): `sal_begin_group` plain-writes the group header +
per-worker directory at `base+8`; `SalGroup::commit()` writes the next slot's
sentinel (0) and then Release-stores the size prefix at `base`. Reader
protocol (`sal_read_group_header`): Acquire-load the prefix at the cursor;
0 → no message; nonzero → plain-read the header fields.

Mid-epoch this is torn-free: a slot the reader can reach ahead of the writer
always has a 0 prefix (each `commit()` sentinels the next slot before
publishing the current one), so the reader never dereferences header bytes of
a slot under construction.

The single hole is **offset 0 across an epoch transition**. When a worker
consumes a `FLAG_FLUSH`/`FLAG_FLUSH_EPH` group (or a backfill CHECKPOINT
relay decision) it rewinds `read_cursor` to 0 and bumps `expected_epoch`
(`advance_read_epoch`, worker/mod.rs) — while the **previous epoch's first
group still sits at offset 0 with a nonzero prefix**. The worker's next
`try_read(0)` parses that stale group and relies on the *in-band* epoch field
(header offset +28) to park. Once the last worker ACKs, the master resets
(`checkpoint_reset`: epoch += 1, write_cursor = 0, prefix at 0 zeroed) and
immediately writes the next group in place at offset 0 with plain stores.
A parked reader descheduled between its prefix load and its header reads
observes an old/new mix. Three master reset sites feed the race, all with the
same shape:

- steady-state base round: `checkpoint_post_ack` (master/dispatch.rs:1751)
- ephemeral round: `checkpoint_reset_only` (master/dispatch.rs:1762)
- backfill inline reclaim: `pending_reset` → `checkpoint_reset()`
  (master/dispatch.rs:446)

In debug builds the `debug_assert` at sal.rs:635 kills the worker. **Release
builds are worse**: the assert is compiled out, `try_read` does no
revalidation, and a torn read whose (new) epoch field happens to equal
`expected_epoch` is *consumed* — `advance = 8 + align8(stale payload_size)`
lands the cursor mid-payload of the new group, permanently desyncing the
worker (garbage ingestion, hangs, or silent data loss).

## Design

Tag the epoch into the group-size prefix — the one word that is already
atomically published — and gate on it **before any header byte is read**.
A reader then never dereferences a slot that does not provably belong to its
epoch, and the Release/Acquire pair on the prefix orders every group byte it
subsequently reads. There is no ABA: a slot's prefix transitions
`0 → (epoch | size)` at most once per epoch (single writer, monotonic cursor
within an epoch; reuse only happens after a reset, which bumps the epoch and
therefore changes the published word).

### Prefix format

```
u64 prefix = (epoch as u64) << 32 | payload_size as u64
```

- `payload_size` fits u32 with headroom: `SAL_MMAP_SIZE = 1 << 30` bounds any
  group below 2^30 bytes.
- The sentinel stays plain 0 (`sal_write_sentinel`, `checkpoint_reset`'s
  zero-store): epoch 0 is never valid — the boot path calls
  `dispatcher.reset_sal(0, 1)` (bootstrap.rs:738) before the first group is
  written, and workers start at `expected_epoch: 1`. `sal_begin_group` gains
  `debug_assert!(epoch >= 1)`.
- "Empty slot" remains `size == 0`; the epoch lane of a zeroed slot is 0 and
  is never inspected when size is 0.

### Header repack (delete the redundant fields)

The in-band epoch field is subsumed by the prefix, and two further header
fields are written but never read anywhere (verified: the only reads of the
header are in `sal_read_group_header`, which touches lsn/flags/target/dir):

| field | old offset (from hdr_off = base+8) | fate |
|---|---|---|
| payload_size copy | +0 (u64) | delete — duplicate of the prefix |
| lsn | +8 (u64) | keep, moves to +0 |
| num_workers | +16 (u32) | delete — never read |
| flags | +20 (u32) | keep, moves to +8 |
| target_id | +24 (u32) | keep, moves to +12 |
| epoch | +28 (u32) | delete — lives in the prefix |
| offsets dir | +32 (MAX_WORKERS × u32) | keep, moves to +16 |
| sizes dir | +288 (MAX_WORKERS × u32) | keep, moves to +16 + MAX_WORKERS*4 |

```rust
pub(crate) const GROUP_HEADER_SIZE: usize = 16 + 2 * MAX_WORKERS * 4; // 528, multiple of 8
```

(Old value 576 = 32 bytes of fields + 512 dir + 32 slack; the constant is
confined to `protocol/sal.rs`, nothing else depends on the number.)

### Reader gate

`sal_read_group_header` takes the expected epoch as an `Option`:

```rust
pub(crate) unsafe fn sal_read_group_header(
    sal_ptr: *const u8,
    read_cursor: u64,
    worker_id: u32,
    expected_epoch: Option<u32>,
) -> SalReadResult {
    let rc = read_cursor as usize;
    let word = atomic_load_u64(sal_ptr.add(rc));
    let payload_size = (word & 0xFFFF_FFFF) as usize;
    let epoch = (word >> 32) as u32;
    if payload_size == 0 {
        return NO_MESSAGE_RESULT;
    }
    // The load-bearing gate: on an epoch mismatch, return before ANY plain
    // read of the header region. A stale slot at offset 0 after an epoch
    // transition may be concurrently overwritten by the master; its bytes
    // are unreadable until the prefix proves the slot belongs to our epoch.
    if let Some(exp) = expected_epoch {
        if epoch != exp {
            return NO_MESSAGE_RESULT;
        }
    }
    let hdr_off = rc + 8;
    let lsn = read_u64_raw(sal_ptr, hdr_off);
    let flags = read_u32_raw(sal_ptr, hdr_off + 8);
    let target_id = read_u32_raw(sal_ptr, hdr_off + 12);
    let advance = (8 + align8(payload_size)) as u64;
    let my_offset = read_u32_raw(sal_ptr, hdr_off + 16 + wid * 4) as usize;
    let my_size = read_u32_raw(sal_ptr, hdr_off + 16 + MAX_WORKERS * 4 + wid * 4) as usize;
    // ... unchanged from here: my_size/my_offset validation, SalReadResult
    //     construction with `epoch` (now from the prefix).
}
```

`NO_MESSAGE_RESULT` is the existing all-zero `SalReadResult` with
`status: SAL_STATUS_NO_MESSAGE` (factor the current literal into a `const` or
helper — it is currently written out twice).

The `debug_assert!(my_offset + my_size <= payload_size, ...)` at the old line
635 **stays**: with the gate it is a true invariant again (a violation means
real corruption, not an expected race). The release-mode clamp to
`data_size = 0` also stays as defense in depth.

- `expected_epoch: Some(e)` — the live worker path. Mismatch (either
  direction) returns `NO_MESSAGE` without touching header bytes; the caller
  parks exactly as today.
- `expected_epoch: None` — the recovery walkers (`collect_committed_lsns`,
  `recover_sal`, bootstrap.rs). They run against a quiescent SAL (master
  pre-fork / worker replay; no concurrent writer exists), walk from offset 0
  without knowing the epoch in advance, and terminate on the first epoch
  *decrease*. They keep that exact logic, reading `msg.epoch` — now sourced
  from the prefix. Stale groups beyond the newest epoch's frontier still
  carry their own committed `(epoch | size)` word, so the fence walk is
  unchanged.

### Writer

`SalGroup` carries the epoch to `commit()`:

```rust
pub(crate) struct SalGroup {
    // ... existing fields ...
    epoch: u32,
}

pub(crate) unsafe fn commit(mut self) -> u64 {
    sal_write_sentinel(self.sal_ptr, self.base + self.total, self.mmap_size);
    atomic_store_u64(
        self.sal_ptr.add(self.base),
        (self.epoch as u64) << 32 | self.payload_size as u64,
    );
    let cursor = (self.base + self.total) as u64;
    self.committed = true;
    cursor
}
```

`sal_begin_group` keeps its `epoch: u32` parameter (all five callers —
`write_group_direct`, `scatter_wire_group`, `write_broadcast_direct`,
`write_commit_sentinel`, test-only `sal_write_group` — already pass
`self.epoch` / an explicit epoch), stops writing the three deleted header
fields, writes lsn/flags/target_id at the new offsets, and stashes `epoch`
into the returned `SalGroup`.

### Worker

`next_sal_message` (worker/mod.rs:572) passes the expectation down and drops
its post-hoc check:

```rust
let (msg, new_cursor) = self.sal_reader.try_read(self.read_cursor, Some(self.expected_epoch))?;
self.read_cursor = new_cursor;
```

The deleted `if msg.epoch != self.expected_epoch { return None; }` is the
whole diff there; the `FlushEph` generation latch and everything else stays.
`SalMessage` keeps its `epoch` field (the recovery walkers fence on it).

`SalReader::try_read(cursor, expected_epoch: Option<u32>)` forwards the new
parameter. Call sites:

| caller | argument |
|---|---|
| `WorkerProcess::next_sal_message` (worker/mod.rs:576) | `Some(self.expected_epoch)` |
| `collect_committed_lsns` (bootstrap.rs:67) | `None` |
| `recover_sal` (bootstrap.rs:102) | `None` |
| bootstrap.rs test walker (~:923) | `None` |

## What this fixes, precisely

- The reader invariant becomes: **header/payload bytes of a slot are
  dereferenced only after an Acquire load of its prefix whose epoch equals
  the reader's expected epoch.** Since a slot is published at most once per
  epoch and only ever rewritten under a bumped epoch, a matching prefix
  proves the slot is immutable and fully visible. All three reset sites are
  covered by the one reader-side gate; no ordering change is needed on the
  master.
- The release-mode mis-advance path (torn epoch matches, stale size advances
  the cursor) is structurally impossible: epoch and size arrive in one atomic
  word.
- Net deletion: three dead/redundant header fields, one redundant runtime
  check, 48 bytes per group.

## Non-changes

- W2M ring: separate protocol with its own slot sequencing — untouched.
- The backfill decision stamp (`seek_col_idx` rewritten in a relay's ctrl
  block) is payload-level and consumed under its own relay handshake —
  untouched.
- `SalWriter::reset` / `checkpoint_reset` / eventfd signaling — untouched.
- Wire format (`gnitz-wire`), shard/WAL formats — untouched. The SAL is
  re-created at boot with a recovery replay by the same binary; pre-alpha, no
  cross-version SAL compatibility exists or is added.

## Tests

1. **Existing** `test_next_sal_message_epoch_gating` (worker/mod.rs:2307)
   passes unchanged: same observables (mismatch → `None`, cursor parked,
   epoch bump makes the same group consumable). It now exercises the prefix
   gate instead of the deleted in-band check.
2. **Existing** bootstrap replay tests (`collect_committed_lsns` fence,
   commit-sentinel tests) pass with `try_read(offset, None)` — mechanical
   call-site update in the test walker.
3. **New unit test** in `protocol/sal.rs` — the gate itself:
   - write one group at offset 0 with epoch 1 via `sal_write_group`;
   - `try_read(0, Some(2))` → `None` (parked, no panic);
   - `try_read(0, Some(1))` → `Some`, `msg.epoch == 1`;
   - `try_read(0, None)` → `Some` (ungated recovery read).
4. **New unit test** — prefix packing round-trip at the boundaries:
   `payload_size = GROUP_HEADER_SIZE` and a multi-MiB size, epoch 1 and
   `u32::MAX`; assert unpacked (epoch, size) and `advance`.
5. **E2E stress acceptance** (the reproducer from the investigation): three
   concurrent loops of `GNITZ_WORKERS=4 uv run pytest tests/test_checkpoint.py -q`
   against the debug server on a saturated machine (load ≥ 2× cores),
   server stderr captured via a `GNITZ_SERVER_BIN` wrapper and worker logs
   mirrored out of the fixture tmpdirs before teardown. Baseline at the
   pre-fix HEAD: 2 torn-header worker panics in ≈85 file-runs. Acceptance:
   ≥ 120 file-runs, zero `SAL group header corrupt` panics and zero
   "Worker N crashed" master lines.
6. `make verify` and a full `make e2e`.

## Sequencing

- [ ] Commit 1: prefix packing + reader gate + header repack + call-site
      updates + unit tests (3, 4) + `async-invariants.md`: extend the
      checkpoint/epoch paragraph with the prefix-epoch invariant (readers
      gate on the atomically-published `(epoch | size)` word before touching
      header bytes; slot reuse always changes the word).
