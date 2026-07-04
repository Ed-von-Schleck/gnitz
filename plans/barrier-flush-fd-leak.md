# Close the barrier-flush directory-fd leak

The checkpoint flush path collects open directory fds as raw `libc::c_int` (no
`Drop`) and closes them only on the success path. An I/O error mid-loop
(`renameat` / `fstat` / `fdatasync` failure) returns via `?`, dropping the
collected raw ints unclosed. The worker survives the error (`send_error` +
`Continue`) and the committer only warns and continues, so **repeated failed
checkpoints under a persistent disk error leak fds cumulatively**.

Fix: own every collected dir fd as `OwnedFd` from the syscall that opens it, so
every exit — success, error `?`, panic unwind — closes it via `Drop`.

Pre-alpha: no compatibility concern.

---

## Current mechanics (verified against source)

Two commit loops accumulate raw dir fds and close them only at the end:

- `PartitionedTable::flush_commit_batch` (`partitioned_table.rs:359-367`)
  accumulates one dirfd per committed partition into a `Vec<libc::c_int>`.
- The worker's `handle_flush_all` / `flush_chunk` (`worker/mod.rs:1459-1551`)
  accumulate one dirfd per committed family into `dir_inodes`, closing the set
  only in the final dir-fsync sweep (`worker/mod.rs:1502-1506`).

`flush_commit` (`storage/lsm/table/flush.rs:184-230`) returns
`Result<Option<libc::c_int>, StorageError>`; `flush_commit_batch` returns
`Result<Vec<libc::c_int>, StorageError>`; `flush_family_commit_batch`
(`catalog/store_io.rs:422-434`) returns `Result<Vec<libc::c_int>, String>`. Every
one is a raw int, so any mid-loop `?` drops the collected/in-flight fds unclosed.
The two loops leak disjoint fd sets (within a family, before the failing
partition; across families, before the failing chunk).

## Design

Return `OwnedFd` / `Vec<OwnedFd>` natively from `flush_commit`,
`flush_commit_batch`, and `flush_family_commit_batch`, and store
`dir_inodes: Vec<(u64, u64, OwnedFd)>` in `flush_chunk`. Ownership is then RAII
end-to-end from the syscall that opens each dir fd to the sweep that consumes it;
the `dedup_dirfds` fsync sweep borrows the fds and `handle_flush_all` drops the
vec on every path. The manual close loop (`worker/mod.rs:1502-1506`) is deleted.

Returning owned fds natively (rather than wrapping raw ints at a collection
boundary) leaves **no** window where a raw `c_int` is in flight — in particular
the `Vec<c_int>` that `flush_family_commit_batch` hands `flush_chunk`, whose
un-yielded tail would leak if a mid-loop `fstat` `?`-returned before each int were
wrapped.

```rust
// storage/lsm/partitioned_table.rs — flush_commit returns Option<OwnedFd>, so a
// mid-loop `?` drops (closes) every fd already collected and no raw c_int is
// ever in flight.
pub fn flush_commit_batch(
    &mut self,
    works: Vec<(usize, FlushWork)>,
) -> Result<Vec<OwnedFd>, StorageError> {
    let mut dirfds: Vec<OwnedFd> = Vec::with_capacity(works.len());
    for (idx, w) in works {
        if let Some(fd) = self.tables[idx].flush_commit(w)? {
            dirfds.push(fd);
        }
    }
    Ok(dirfds)
}
```

`flush_commit`'s signature becomes `Result<Option<OwnedFd>, StorageError>` (it
already `open`s the dir fd it returns); `flush_family_commit_batch` becomes
`Result<Vec<OwnedFd>, String>`. `flush_chunk` takes each returned `OwnedFd`
directly into `dir_inodes`.

## Tests

- **fd hygiene under error.** A debug-only injection seam forces `flush_commit`
  (within a family) and `fstat` (across chunks) to fail on the Nth fd; assert the
  flush returns `Err` and the process fd count (`/proc/self/fd`) is unchanged.
- **Success-path regression.** Repeated checkpoints over several partitioned
  tables leave the fd count flat.

## Sequencing

One commit; the tree is green (`make verify` + `make e2e`) after it.

- [ ] 1. **Own barrier-flush dir fds as `OwnedFd`.** `flush_commit`,
  `flush_commit_batch`, `flush_family_commit_batch` return `OwnedFd` /
  `Vec<OwnedFd>`; `flush_chunk` stores `dir_inodes: Vec<(u64, u64, OwnedFd)>`;
  delete the manual close loop. Add the fd-hygiene injection test and the
  success-path regression.
