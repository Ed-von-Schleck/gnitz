//! The mapping of `wal.sal` — the anchor page and the ring behind it — and
//! what a reader of it sees: a group verified at its own offset, and the walk
//! over consecutive groups.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};

use super::format::{
    anchor_word, dir_sizes, directory, group_total_size, header_len, payload_rows, payload_spans, word_epoch,
    GroupHead, Header, HDR_PREFIX, PREFIX_BYTES,
};
use gnitz_foundation::posix_io;
use gnitz_wire::MAX_WORKERS;
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;

const SAL_MMAP_SIZE: usize = 1 << 30;

/// The page holding the anchor, at the head of `wal.sal` so it does not move
/// when `GNITZ_SAL_BYTES` does.
pub(super) const ANCHOR_BYTES: usize = 4096;

const _: () = assert!(
    SAL_MMAP_SIZE as u64 <= 1 << 32,
    "a zone LSN and a group header's BASE pack a ring offset into 32 bits"
);

/// Floor for a `GNITZ_SAL_BYTES` override — must comfortably exceed one DDL zone
/// plus the checkpoint headroom.
pub(super) const MIN_SAL_BYTES: usize = 16 << 20;

/// `SAL_MMAP_SIZE`, or `GNITZ_SAL_BYTES` clamped into
/// `[MIN_SAL_BYTES, SAL_MMAP_SIZE]` and logged when the clamp moves it — for
/// test servers sharing a tmpfs, since each `fallocate`s its whole SAL.
fn sal_mmap_size() -> usize {
    let asked = gnitz_foundation::env::env_num("GNITZ_SAL_BYTES", SAL_MMAP_SIZE);
    let size = asked.clamp(MIN_SAL_BYTES, SAL_MMAP_SIZE);
    if size != asked {
        gnitz_info!("GNITZ_SAL_BYTES={asked} is outside [{MIN_SAL_BYTES}, {SAL_MMAP_SIZE}]; using {size}");
    }
    size
}

/// The word at `offset` as an atomic: a publication prefix, or the anchor.
///
/// # Safety
/// `offset + PREFIX_BYTES` must lie within the mapping, and `sal_ptr + offset`
/// must be aligned for an `AtomicU64`. The mapping is page-aligned and the
/// ring starts one page into it, so the anchor is aligned, and so is the word
/// at every ring offset that is a multiple of `PREFIX_BYTES` — which every
/// group base is: 0, or the end of a group, whose size
/// `group_total_size` keeps a multiple of it.
#[inline]
unsafe fn word_atomic<'a>(sal_ptr: *const u8, offset: usize) -> &'a AtomicU64 {
    AtomicU64::from_ptr(sal_ptr.add(offset).cast_mut().cast())
}

/// One SAL group as a reader sees it: the header verified at its own offset,
/// and the payload bytes its directory describes.
pub(crate) struct SalMessage<'a> {
    pub(super) head: GroupHead,
    /// The group's byte offset in the ring — what a recovery error names.
    pub(super) base: u64,
    /// The offset past the group.
    pub(super) end: u64,
    /// The group's payload bytes, from the header's end to the end of the group.
    payload: &'a [u8],
    /// The checksum the writer stored of a zone member's payload bytes.
    payload_sum: u64,
    /// The directory: one `DIR_ENTRY_BYTES` entry per payload.
    dir: &'a [u8],
    /// The worker count the group was written at.
    width: u8,
}

impl<'a> SalMessage<'a> {
    /// Whether the payload bytes are what the writer checksummed; only a zone
    /// member's are checksummed.
    pub(super) fn intact(&self) -> bool {
        debug_assert!(self.head.zoned(), "only a zone member's payloads carry a checksum");
        gnitz_wire::checksum(self.payload) == self.payload_sum
    }

    /// Each payload's bytes, in directory order.
    pub(crate) fn payloads(&self) -> impl Iterator<Item = &'a [u8]> + use<'a> {
        let payload = self.payload;
        payload_spans(dir_sizes(self.dir)).map(move |(off, sz)| &payload[off..off + sz])
    }

    /// The worker count the group was written at.
    pub(crate) fn width(&self) -> u32 {
        self.width as u32
    }

    /// Worker `w`'s bytes, `None` when the group does not address it.
    /// Outlined: inlined into `SalReader::next`, it costs the drain more
    /// instructions per group than the call does.
    #[inline(never)]
    pub(crate) fn slot(&self, w: u32) -> Option<&'a [u8]> {
        let w = w as usize;
        if w >= MAX_WORKERS {
            return None;
        }
        let i = self.head.targets.position(w)?;
        let i = if self.head.shared() { 0 } else { i };
        let (off, sz) = payload_spans(dir_sizes(self.dir)).nth(i)?;
        Some(&self.payload[off..off + sz])
    }

    /// The rows `payload`, one of this group's, carries; `known` as
    /// [`payload_rows`]'s. The error names the group's offset and target.
    pub(crate) fn rows(
        &self,
        payload: &[u8],
        known: impl FnOnce(u64, &[u8]) -> Option<SchemaDescriptor>,
    ) -> Result<Option<Batch>, String> {
        let tid = self.head.request.target_id;
        payload_rows(payload, |record| known(tid, record))
            .map(|(_, batch)| batch)
            .map_err(|e| format!("SAL replay: corrupt block at offset={} target={tid}: {e}", self.base))
    }
}

/// Ask btrfs to overwrite the file in place (`FS_NOCOW_FL`): a copy-on-write
/// rewrite needs new blocks, so the reservation alone would not keep a store
/// from failing for want of space. Btrfs honours the flag on an empty file
/// only. Best-effort: other filesystems refuse the ioctl.
fn set_nocow(fd: libc::c_int) {
    const FS_NOCOW_FL: libc::c_int = 0x0080_0000;
    let mut flags: libc::c_int = 0;
    // SAFETY: both ioctls read or write one `int` through the pointer.
    unsafe {
        if libc::ioctl(fd, libc::FS_IOC_GETFLAGS, &mut flags) == 0 && flags & FS_NOCOW_FL == 0 {
            flags |= FS_NOCOW_FL;
            libc::ioctl(fd, libc::FS_IOC_SETFLAGS, &flags);
        }
    }
}

/// The opened SAL: its mapping and the file behind it.
///
/// Mapped once, by [`SalLog::open`], and inherited across the `fork()`; it is
/// never unmapped, so this is `Copy` and has no `Drop`.
#[derive(Clone, Copy)]
pub(crate) struct SalLog {
    pub(super) ring: *mut u8,
    pub(super) ring_len: usize,
    /// What the writer syncs.
    pub(super) file: &'static File,
}

impl SalLog {
    /// Open `data_dir`'s SAL, creating it on a first boot. A fresh file reads
    /// all-zero, the empty log recovery expects. The file is leaked: the writer
    /// and the reactor's fsync SQEs name it for the process's life.
    pub(super) fn open(data_dir: &str) -> Result<SalLog, String> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o644)
            .open(format!("{data_dir}/wal.sal"))
            .map_err(|e| format!("failed to open SAL file: {e}"))?;
        let len = sal_mmap_size();
        let log =
            Self::map(Box::leak(Box::new(file)), len).map_err(|e| format!("failed to map SAL ({len} bytes): {e}"))?;
        // The file's name in `data_dir`, before any group written to it is ACKed.
        posix_io::fsync_dir(data_dir).map_err(|e| format!("failed to fsync '{data_dir}': {e}"))?;
        Ok(log)
    }

    /// Map `file` shared and writable: `len` bytes, or all of a longer file —
    /// one written under a larger `GNITZ_SAL_BYTES`.
    pub(super) fn map(file: &'static File, len: usize) -> io::Result<SalLog> {
        let len = len.max(file.metadata()?.len().min(SAL_MMAP_SIZE as u64) as usize);
        assert!(len > ANCHOR_BYTES, "the SAL holds a ring behind its anchor page");
        let fd = file.as_raw_fd();
        set_nocow(fd);
        // A store past the file's end raises `SIGBUS`, and one into an
        // unreserved block can fail for want of space.
        // SAFETY: a syscall on the open descriptor of `file`; it takes no pointer.
        posix_io::retry_eintr(|| unsafe { libc::fallocate(fd, 0, 0, len as libc::off_t) })?;
        let ptr = posix_io::map_shared(Some(file.as_fd()), len)?;
        Ok(SalLog {
            // SAFETY: `len > ANCHOR_BYTES`.
            ring: unsafe { ptr.add(ANCHOR_BYTES) },
            ring_len: len - ANCHOR_BYTES,
            file,
        })
    }

    /// From here on, a fault on the mapping reads no pages ahead, so it caches
    /// the one page it faulted on: a store to a page no write cached before
    /// dirties, and the next `fdatasync` writes, that page alone. The advice is
    /// the mapping's, so the workers' reads through it, after the `fork()`,
    /// take it too. Given once recovery has read the committed tail, which a
    /// cold page cache then reads ahead.
    pub(crate) fn advise_random(&self) {
        let (base, len) = (self.ring.wrapping_sub(ANCHOR_BYTES), self.ring_len + ANCHOR_BYTES);
        // SAFETY: the whole mapping, anchor page included.
        unsafe { libc::madvise(base.cast(), len, libc::MADV_RANDOM) };
    }

    /// The `len` ring bytes at `off`, or `None` when the ring does not hold
    /// them all.
    #[inline]
    fn span(&self, off: usize, len: usize) -> Option<&[u8]> {
        if off.checked_add(len)? > self.ring_len {
            return None;
        }
        // SAFETY: bounded directly above.
        Some(unsafe { std::slice::from_raw_parts(self.ring.add(off), len) })
    }

    /// The `len` ring bytes at `off`, to write.
    ///
    /// # Safety
    /// The ring holds the span, it lies past the published end, and nothing
    /// else references it.
    #[allow(clippy::mut_from_ref)]
    #[inline]
    pub(super) unsafe fn span_mut(&self, off: usize, len: usize) -> &mut [u8] {
        debug_assert!(off.checked_add(len).is_some_and(|end| end <= self.ring_len));
        std::slice::from_raw_parts_mut(self.ring.add(off), len)
    }

    /// The header of the group laid out at `base`, sized by its own payload
    /// count.
    ///
    /// # Safety
    /// An unpublished group's header is laid out at `base`, and nothing else
    /// references it.
    #[allow(clippy::mut_from_ref)]
    #[inline]
    pub(super) unsafe fn header_mut(&self, base: u64) -> &mut [u8] {
        let at = base as usize + PREFIX_BYTES;
        let fixed = self.span_mut(at, HDR_PREFIX).first_chunk().expect("HDR_PREFIX bytes");
        self.span_mut(at, header_len(fixed))
    }

    pub(super) fn anchor_atomic(&self) -> &'static AtomicU64 {
        // SAFETY: the anchor page is mapped ahead of the ring, at the
        // mapping's page-aligned start.
        unsafe { word_atomic(self.ring.wrapping_sub(ANCHOR_BYTES), 0) }
    }

    /// The walk epoch.
    pub(super) fn anchor(&self) -> Result<u32, String> {
        match self.anchor_atomic().load(Ordering::Relaxed) {
            0 => Ok(0), // The fresh file.
            w if w == anchor_word(word_epoch(w)) => Ok(word_epoch(w)),
            _ => Err("SAL anchor fails its check: the head of wal.sal is damaged".into()),
        }
    }

    /// The publication prefix word at `base`, or 0 when nothing was published
    /// there — which is also the answer for a `base` the mapping cannot hold.
    /// Acquire-loaded, so a group whose prefix is visible has its whole header
    /// visible too (the publishing store lands last).
    #[inline]
    pub(super) fn prefix_word(&self, base: u64) -> u64 {
        if base as usize + PREFIX_BYTES > self.ring_len {
            return 0;
        }
        // SAFETY: bounded directly above, and aligned: `base` is 0 or the end
        // of a group read off this ring, a multiple of `PREFIX_BYTES` whatever
        // its directory says.
        unsafe { word_atomic(self.ring, base as usize).load(Ordering::Acquire) }
    }

    /// Store the publication prefix word at `base`: 0 ends the log there, an
    /// [`epoch_word`](super::format::epoch_word) publishes the group laid out
    /// there.
    ///
    /// # Safety
    /// `base` is a group base the ring holds the word of — a multiple of
    /// `PREFIX_BYTES` with `base + PREFIX_BYTES` inside the ring — and no
    /// reader has been admitted past it.
    #[inline]
    pub(super) unsafe fn set_prefix_word(&self, base: u64, word: u64) {
        debug_assert!(base as usize + PREFIX_BYTES <= self.ring_len);
        word_atomic(self.ring, base as usize).store(word, Ordering::Release);
    }

    /// Whether a group of `epoch` is published at `base`: its prefix word is
    /// set and carries that epoch. The word's epoch half is outside every
    /// digest, so this admits a header to be read and verifies nothing of it.
    #[inline]
    pub(super) fn published_at(&self, base: u64, epoch: u32) -> bool {
        let word = self.prefix_word(base);
        word != 0 && word_epoch(word) == epoch
    }

    /// The group of `epoch` whose header is at `cursor`, or `None` when the bytes
    /// there are not one: a failed digest, a header naming another offset or
    /// epoch, scalars the writer does not produce, or a size the mapping cannot
    /// hold. Each bound precedes the read it guards.
    ///
    /// Reads header bytes without consulting the publication prefix. The caller
    /// has seen a non-zero prefix word at `cursor`; on a live log, one carrying
    /// `epoch`.
    ///
    /// Always inlined: the live drain reads every group through it, and as a
    /// call it hands the group back through memory.
    #[inline(always)]
    pub(super) fn group_at(&self, cursor: u64, epoch: u32) -> Option<SalMessage<'_>> {
        let hdr_off = cursor as usize + PREFIX_BYTES;
        let fixed = self.span(hdr_off, HDR_PREFIX)?.first_chunk()?;
        let hdr = self.span(hdr_off, header_len(fixed))?;
        let h = Header::verify(hdr, cursor)?;
        if h.epoch != epoch {
            return None;
        }

        let dir = directory(hdr);
        let payload_off = hdr_off + hdr.len();
        let end = cursor as usize + group_total_size(hdr.len(), dir_sizes(dir));
        // The payloads run from the header's end to the group's end.
        let payload = self.span(payload_off, end - payload_off)?;
        Some(SalMessage {
            head: h.head,
            base: cursor,
            end: end as u64,
            payload,
            payload_sum: h.payload_sum,
            dir,
            width: h.width,
        })
    }

    /// Every group readable from offset 0 at `epoch`, up to the first that is not.
    /// Any non-zero prefix word counts as published: its epoch half is outside the
    /// digest, and only the digested header's epoch decides.
    pub(super) fn walk(&self, epoch: u32) -> impl Iterator<Item = SalMessage<'_>> {
        let mut at = 0;
        std::iter::from_fn(move || {
            if self.prefix_word(at) == 0 {
                return None;
            }
            let msg = self.group_at(at, epoch)?;
            at = msg.end;
            Some(msg)
        })
    }
}

#[cfg(test)]
#[path = "tests/log.rs"]
mod tests;
