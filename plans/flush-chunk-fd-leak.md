# Fix the directory-fd leak in the worker checkpoint flush

## Problem

`WorkerProcess::flush_chunk` (`crates/gnitz-engine/src/runtime/worker.rs:1410`)
leaks every accumulated directory fd when `fstat` fails on any dirfd:

```rust
for (tid, works) in pending.drain(..) {
    let dirfds = self.cat().flush_family_commit_batch(tid, works)
        .map_err(|e| format!("flush_commit tid={tid}: {e}"))?;
    for dirfd in dirfds {
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(dirfd, &mut stat) } < 0 {
            let err = std::io::Error::last_os_error();
            return Err(format!("fstat: {err}"));   // ← leaks dirfd + remaining dirfds
        }
        dir_inodes.push((stat.st_dev, stat.st_ino, dirfd));
    }
}
```

`flush_family_commit_batch` returns owned raw dir fds (opened per flush in
`flush_prepare`, `O_RDONLY|O_DIRECTORY`). On the `fstat` failure path the function
returns without closing the current `dirfd` or the remaining entries of `dirfds`.
Worse, the caller `flush_all_inner` invokes `flush_chunk` with `?`
(`worker.rs:1387`, `:1390`), so the error propagates straight out of
`flush_all_inner`, **skipping its own close-all loop** (`worker.rs:1401-1403`).
Every dir fd already pushed into `dir_inodes` across all prior chunks leaks too.

`fstat` on a freshly opened directory fd realistically only fails with `EBADF`,
`EFAULT`, or `ENOMEM`, so this is low-probability — but it is an unconditional
leak on a checkpoint path that runs for the life of the process, and a worker
that checkpoints repeatedly under memory pressure can exhaust its fd table.

The same ownership smell exists on the *success* path: `dir_inodes` holds raw
`libc::c_int`, closed by a manual loop in `flush_all_inner` that is bypassed by
any early `?` return between the push and the close.

## Fix

Make the dir fds owned (`std::os::fd::OwnedFd`) so every control-flow path —
success, `fstat` failure, `flush_family_commit_batch` failure, panic unwind —
closes them via `Drop`. This removes the manual close loop entirely.

### 1. `flush_chunk` — own the fds at the boundary

```rust
use std::os::fd::{FromRawFd, AsRawFd, OwnedFd};

fn flush_chunk(
    &mut self,
    ring: &mut io_uring::IoUring,
    pending: &mut Vec<(i64, Vec<(usize, FlushWork)>)>,
    dir_inodes: &mut Vec<(u64, u64, OwnedFd)>,          // was (_, _, libc::c_int)
) -> Result<(), String> {
    if pending.is_empty() { return Ok(()); }

    let fds: Vec<libc::c_int> = /* unchanged: shard/manifest fds for fdatasync */;
    uring_batch_fdatasync(ring, &fds)?;
    for (_, ws) in pending.iter_mut() {
        for (_, w) in ws { w.close_fds(); }
    }

    for (tid, works) in pending.drain(..) {
        // Own every returned dir fd immediately: any early return below now
        // drops (closes) the current fd AND every not-yet-consumed fd.
        let dirfds: Vec<OwnedFd> = self.cat()
            .flush_family_commit_batch(tid, works)
            .map_err(|e| format!("flush_commit tid={tid}: {e}"))?
            .into_iter()
            .map(|fd| unsafe { OwnedFd::from_raw_fd(fd) })
            .collect();
        for owned in dirfds {
            let mut stat: libc::stat = unsafe { std::mem::zeroed() };
            if unsafe { libc::fstat(owned.as_raw_fd(), &mut stat) } < 0 {
                let err = std::io::Error::last_os_error();
                return Err(format!("fstat: {err}"));   // owned + the rest drop → closed
            }
            dir_inodes.push((stat.st_dev, stat.st_ino, owned));
        }
    }
    Ok(())
}
```

`flush_family_commit_batch` keeps returning `Vec<libc::c_int>` — the ownership
transfer happens at the `from_raw_fd` boundary in the worker, the sole consumer.

### 2. `flush_all_inner` — drop replaces the manual close loop

```rust
let mut dir_inodes: Vec<(u64, u64, OwnedFd)> = Vec::new();
// ... flush_chunk(...)? calls unchanged ...

// Fsync one fd per unique directory inode (dedup unchanged), borrowing the
// OwnedFd. No manual close loop: dropping `dir_inodes` closes every fd, on
// the success path and on any `?` unwind above.
let mut fsync_err = None;
for &(dev, ino, ref fd) in dedup_by_inode(&dir_inodes) {
    if fsync_eintr(fd.as_raw_fd()).is_err() { fsync_err = Some(/* … */); break; }
}
drop(dir_inodes);   // explicit for clarity; closes all fds
if let Some(err) = fsync_err { return Err(format!("dir fsync failed: {err}")); }
Ok(())
```

Update `dedup_dir_inodes` (the `(dev, ino, fd)` dedup helper near `worker.rs:199`)
to take/borrow `OwnedFd` and yield the representative fd per inode by reference;
it must not consume the fds (they are still needed for the rest of the flush).

## Tests

- **Rust unit test, injected `fstat` failure.** Add a debug-only injection seam
  (env var, mirroring `GNITZ_INJECT_UNIQUE_PREFLIGHT_ERROR`) that forces the
  `fstat` branch to fail on the Nth dirfd. Assert `flush_chunk` returns `Err` and
  that the process fd count is unchanged afterward (snapshot `/proc/self/fd`
  count before/after, or count open fds via `fcntl(F_GETFD)` over a range).
- **Multi-chunk leak.** Seed enough user tables/partitions that
  `FD_CHUNK_THRESHOLD` (256) splits the flush into ≥2 chunks; inject the failure
  in the second chunk and assert the first chunk's dir fds (already in
  `dir_inodes`) are also closed on the unwind.
- **Success-path regression.** A normal checkpoint over several partitioned
  tables leaves the fd count unchanged across repeated `make e2e` checkpoints.

## Scope

Pure resource-safety fix in the worker checkpoint path. No protocol, schema, or
on-disk format change. Unrelated to any index/seek feature.
