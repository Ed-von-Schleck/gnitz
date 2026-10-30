//! SAL (shared append-only log): master→worker broadcast channel.
//!
//! Owns the layout of `wal.sal` — the anchor page and the ring of groups
//! behind it — and every reader and writer of it. The recovery walk over the
//! ring is the [`zone`] child module.
//!
//! A group's kind byte and its frame head are this module's alone: callers name
//! a [`SalRequest`], and the encode/decode below and in [`request`] is the only
//! code entitled to know how either is stored.

mod request;
mod zone;

pub(crate) use request::{Apply, Read, SalRequest};
pub(crate) use zone::CommittedTail;

use std::cell::Cell;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io;
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::catalog::SysFamily;
use crate::runtime::frame::WireMsg;
use crate::runtime::park::{WorkerParks, WorkerSet};
use crate::runtime::reactor::{GroupTargets, Reactor, ReadyAcked};
use crate::runtime::sync::{AsyncRwLock, WriteGuard};
use crate::runtime::w2m::RequestId;
use gnitz_foundation::fault::Seam;
use gnitz_foundation::posix_io;
use gnitz_wire::{
    frame_head_size, peek_control_block, read_u32_le, read_u64_le, write_u32_le, write_u64_le, DecodedControl,
    WireFault, WireStatus, MAX_WORKERS,
};
use gnitz_zset::repr::{Batch, WireRows};
use gnitz_zset::schema::{decode_schema_block, SchemaDescriptor};

/// `GNITZ_INJECT_SAL_ZONE_PANIC=<scope tag>`: crash the master between a zone's
/// groups publishing and its closing member. The tag (`"ddl"` / `"commit"`)
/// picks which scope aborts.
static ZONE_PANIC: Seam = Seam::new("GNITZ_INJECT_SAL_ZONE_PANIC");

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// The publication prefix in front of every group header, one atomically stored
/// u64 ([`epoch_word`]); also the alignment of every group base.
const PREFIX_BYTES: usize = std::mem::size_of::<AtomicU64>();

// Group header field offsets, relative to the start of the header. The byte
// behind WIDTH is zero.

/// XXH3-64 of every header byte behind it.
const OFF_DIGEST: usize = 0;
/// The kind's ordinal (`SalMessageKind::as_wire`), one byte.
const OFF_KIND: usize = 8;
/// `FLAG_*` bits, one byte. A bit outside them is corruption.
const OFF_FLAGS: usize = 9;
/// On a zone's last member: the zone is closed.
const FLAG_ZONE_END: u8 = 1;
/// The group is a later member of its [`Cut`].
const FLAG_JOINS_CUT: u8 = 2;
/// The group holds one payload, which every addressed worker reads.
const FLAG_SHARED: u8 = 4;
const KNOWN_FLAGS: u8 = FLAG_ZONE_END | FLAG_JOINS_CUT | FLAG_SHARED;
/// The worker count the group was written at, one byte.
const OFF_WIDTH: usize = 10;
/// The group's [`RequestId`], 0 for none.
const OFF_REQUEST_ID: usize = 12;
/// The group's own byte offset in the ring.
const OFF_BASE: usize = 16;
const OFF_EPOCH: usize = 20;
/// The zone LSN of a zone member, `0` outside every zone.
const OFF_LSN: usize = 24;
const OFF_TARGET_ID: usize = 32;
/// The [`WorkerSet`] the group addresses, within its width.
const OFF_TARGETS: usize = 40;
/// XXH3-64 of the group's payload bytes, header end to group end, on a zone
/// member; `0` on every other group.
const OFF_PAYLOAD_SUM: usize = 48;
/// One `u64` size per payload.
const OFF_DIRECTORY: usize = 56;
const DIR_ENTRY_BYTES: usize = 8;

/// The header's fixed prefix — every scalar field, ending where the directory
/// begins. Read as a `&[u8; HDR_PREFIX]` so the field reads are provably in
/// bounds at const offsets.
const HDR_PREFIX: usize = OFF_DIRECTORY;

/// How many payloads a group holds: one every addressed worker reads, or one
/// per addressed worker in ascending worker order.
const fn payload_count(flags: u8, targets: WorkerSet) -> usize {
    if flags & FLAG_SHARED != 0 {
        1
    } else {
        targets.len()
    }
}

/// Header + directory bytes for a group of `payloads` payloads.
#[inline]
const fn group_header_size(payloads: usize) -> usize {
    OFF_DIRECTORY + payloads * DIR_ENTRY_BYTES
}

// Group bases stay aligned for the publication prefix's atomic store.
const _: () = assert!(
    OFF_DIRECTORY.is_multiple_of(PREFIX_BYTES) && DIR_ENTRY_BYTES.is_multiple_of(PREFIX_BYTES),
    "a group header must be a multiple of PREFIX_BYTES, or group bases stop being aligned"
);

/// Header + directory bytes of the header whose fixed prefix is `fixed`: at most
/// `group_header_size(MAX_WORKERS)` whatever the bytes say, so it needs no check
/// ahead of the digest.
#[inline]
fn header_len(fixed: &[u8; HDR_PREFIX]) -> usize {
    group_header_size(payload_count(
        fixed[OFF_FLAGS],
        WorkerSet(read_u64_le(fixed, OFF_TARGETS)),
    ))
}

/// Each payload as `(payload-relative offset, size)`, in directory order.
/// The offset is a pure function of the sizes before it — a payload starts where
/// the 8-byte-padded ones ahead of it end — so the directory stores sizes alone
/// and an inconsistent offset is unrepresentable.
fn payload_spans(sizes: impl Iterator<Item = usize>) -> impl Iterator<Item = (usize, usize)> {
    let mut off = 0;
    sizes.map(move |sz| {
        let at = off;
        off += sz.next_multiple_of(8);
        (at, sz)
    })
}

/// The SAL bytes a group occupies: prefix, a `hdr_len`-byte header and
/// directory, and 8-byte-padded payloads — what the write cursor advances by,
/// and where [`payload_spans`] leaves off.
fn group_total_size(hdr_len: usize, sizes: impl Iterator<Item = usize>) -> usize {
    PREFIX_BYTES + hdr_len + sizes.map(|sz| sz.next_multiple_of(8)).sum::<usize>()
}

/// A group's directory bytes read back as payload sizes.
fn dir_sizes(dir: &[u8]) -> impl Iterator<Item = usize> + '_ {
    dir.as_chunks::<DIR_ENTRY_BYTES>()
        .0
        .iter()
        .map(|c| read_u64_le(c, 0) as usize)
}

/// What a group sends each worker.
#[derive(Clone, Copy)]
pub(crate) enum GroupData<'a> {
    /// Every worker is sent these rows.
    Same(Option<WireRows<'a>>),
    /// Worker `w` is sent the rows `each[w]`; one entry per worker.
    Each(&'a [Option<WireRows<'a>>]),
    /// Worker `w` is sent no rows and `blobs[w]` in place of the request's blob;
    /// one entry per worker.
    Blobs(&'a [Vec<u8>]),
}

impl<'a> GroupData<'a> {
    /// True when no worker is sent a row — the one shape allowed to omit a schema.
    fn is_dataless(&self) -> bool {
        match *self {
            Self::Same(d) => d.is_none(),
            Self::Each(each) => each.iter().all(Option::is_none),
            Self::Blobs(_) => true,
        }
    }

    /// The workers this data has rows for; every worker for the one payload all share.
    pub(crate) fn holders(&self) -> WorkerSet {
        match *self {
            Self::Same(_) => WorkerSet::ALL,
            Self::Blobs(_) => WorkerSet::EMPTY,
            Self::Each(each) => each
                .iter()
                .enumerate()
                .filter_map(|(w, rows)| rows.map(|_| w))
                .collect(),
        }
    }
}

/// One SAL group as [`SalExcl::lay_out`] encodes it: the request every payload
/// carries, plus what varies by worker.
pub(crate) struct SalGroup<'a> {
    pub(crate) request: SalRequest<'a>,
    /// The schema record of the rows in `data`; dropped from a payload that
    /// carries none.
    pub(crate) schema: Option<&'a [u8]>,
    pub(crate) data: GroupData<'a>,
    pub(crate) targets: GroupTargets,
}

impl<'a> SalGroup<'a> {
    /// A control-only silent broadcast of `request`. Callers override what varies
    /// by struct update.
    pub(crate) fn new(request: impl Into<SalRequest<'a>>) -> Self {
        SalGroup {
            request: request.into(),
            schema: None,
            data: GroupData::Same(None),
            targets: GroupTargets::SILENT,
        }
    }

    /// A push of `data` into `tid`, whose schema record is `record`, written to
    /// `targets`.
    pub(crate) fn push(tid: u64, record: &'a [u8], data: GroupData<'a>, targets: GroupTargets) -> Self {
        SalGroup {
            schema: Some(record),
            data,
            targets,
            ..Self::new(Apply::Push { tid })
        }
    }

    /// A catalog mutation of `family`: `batch`, sent whole to every worker. A
    /// family that reaches no worker is logged and addresses none.
    pub(crate) fn ddl_sync(family: SysFamily, batch: &'a Batch) -> Self {
        SalGroup {
            schema: Some(family.record()),
            data: GroupData::Same(batch.wire_whole()),
            targets: if family.master_only() {
                GroupTargets {
                    set: WorkerSet::EMPTY,
                    ..GroupTargets::SILENT
                }
            } else {
                GroupTargets::SILENT
            },
            ..Self::new(Apply::DdlSync { family })
        }
    }

    /// Worker `w`'s message over `head`, the request's template. The one
    /// definition — sizing and encoding both go through it, so a payload's size
    /// and its bytes cannot disagree.
    #[inline]
    fn msg<'m>(&'m self, head: &WireMsg<'m>, w: usize) -> WireMsg<'m> {
        let (data, blob) = match self.data {
            GroupData::Same(d) => (d, head.blob),
            GroupData::Each(each) => (each[w], head.blob),
            GroupData::Blobs(blobs) => (None, &blobs[w][..]),
        };
        WireMsg {
            data,
            schema_block: self.schema.filter(|_| data.is_some()),
            blob,
            ..*head
        }
    }
}

/// XXH3-64 over a group header behind its digest field. The header names its own
/// ring offset and its epoch, so it verifies only where and when it was published.
fn group_digest(hdr: &[u8]) -> u64 {
    gnitz_wire::checksum(&hdr[OFF_DIGEST + 8..])
}

/// Stamp the digest of the group header `hdr`.
fn stamp_digest(hdr: &mut [u8]) {
    let digest = group_digest(hdr);
    write_u64_le(hdr, OFF_DIGEST, digest);
}

const SAL_MMAP_SIZE: usize = 1 << 30;

/// The page holding the anchor, at the head of `wal.sal` so it does not move
/// when `GNITZ_SAL_BYTES` does.
const ANCHOR_BYTES: usize = 4096;

const _: () = assert!(
    SAL_MMAP_SIZE as u64 <= 1 << 32,
    "a zone LSN and a group header's BASE pack a ring offset into 32 bits"
);

/// The epoch after `epoch`: what a checkpoint moves the writer and every reader
/// to, and what a boot starts at above the recovered tail's epoch.
const fn next_epoch(epoch: u32) -> u32 {
    epoch + 1
}

/// Space only a terminal group — a checkpoint round's `Flush`/`FlushEph`, the
/// `Shutdown` broadcast — may spend, so those always fit.
const CHECKPOINT_RESERVE: usize = 64 << 10;

const _: () = {
    let terminal = PREFIX_BYTES + group_header_size(1) + frame_head_size(0, None).next_multiple_of(8);
    assert!(
        CHECKPOINT_RESERVE >= 2 * terminal,
        "the checkpoint band must hold two control-only groups"
    );
};

/// The highest byte a group of `kind` may occupy: its end prefix stays free,
/// and so does the checkpoint band unless nothing follows the group in its epoch.
fn group_limit(kind: SalMessageKind, ring_len: usize) -> usize {
    let band = if kind.ends_epoch() || matches!(kind, SalMessageKind::Shutdown) {
        0
    } else {
        CHECKPOINT_RESERVE
    };
    ring_len - PREFIX_BYTES - band
}

/// Floor for a `GNITZ_SAL_BYTES` override — must comfortably exceed one DDL zone
/// plus the checkpoint headroom.
const MIN_SAL_BYTES: usize = 16 << 20;
const _: () = assert!(
    PREFIX_BYTES + CHECKPOINT_RESERVE < MIN_SAL_BYTES / 64,
    "the checkpoint reserve must leave ordinary groups nearly all of the smallest SAL"
);

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

// ---------------------------------------------------------------------------
// Group kinds
// ---------------------------------------------------------------------------

gnitz_wire::wire_enum! {
    /// What a SAL group asks a worker to do: the [`SalRequest`] variant its
    /// payloads decode to. Stored as one ordinal byte in the group header;
    /// `ALL`/`from_wire` come from the one variant list, so a decode cannot fall
    /// behind the enum.
    pub(crate) enum SalMessageKind: u8 {
        Shutdown = 1,
        Flush = 2,
        FlushEph = 3,
        DdlSync = 4,
        Backfill = 6,
        HasPk = 7,
        KeySpans = 8,
        Push = 9,
        Tick = 10,
        ScanSpec = 11,
        Delta = 12,
        Pin = 13,
        Unpin = 14,
        SnapshotPage = 15,
        Backup = 16,
        ReleasePreflight = 17,
        HasPkColumn = 18,
        HasPkIndex = 19,
    }
}

impl SalMessageKind {
    /// Whether a group of this kind is the last of its epoch.
    const fn ends_epoch(self) -> bool {
        matches!(self, SalMessageKind::Flush | SalMessageKind::FlushEph)
    }
}

/// `(epoch << 32) | low`.
#[inline]
const fn epoch_word(epoch: u32, low: u64) -> u64 {
    (epoch as u64) << 32 | low
}

/// The low half of every publication prefix word: a presence marker, so zeroing a
/// published word takes at least 33 bit flips.
const PRESENT: u64 = u32::MAX as u64;

/// The epoch half of an [`epoch_word`].
#[inline]
const fn word_epoch(word: u64) -> u32 {
    (word >> 32) as u32
}

/// The anchor of `epoch`: its complement in the low half, so no flipped bit
/// verifies.
const fn anchor_word(epoch: u32) -> u64 {
    epoch_word(epoch, !epoch as u64)
}

/// The word at `offset` as an atomic: a publication prefix, or the anchor.
///
/// # Safety
/// `offset + PREFIX_BYTES` must lie within the mapping.
#[inline]
unsafe fn prefix_atomic<'a>(sal_ptr: *const u8, offset: usize) -> &'a AtomicU64 {
    AtomicU64::from_ptr(sal_ptr.add(offset).cast_mut().cast())
}

/// Zero the prefix word at `offset`, ending the log there.
///
/// # Safety
/// `offset + PREFIX_BYTES` must lie within the mapping.
#[inline]
unsafe fn write_end_prefix(sal_ptr: *mut u8, offset: usize) {
    prefix_atomic(sal_ptr, offset).store(0, Ordering::Release);
}

// ---------------------------------------------------------------------------
// SAL read
// ---------------------------------------------------------------------------

/// A group header's scalars: all of it but the digest, the payload checksum and
/// the base and epoch, which the writer stamps.
#[derive(Clone, Copy)]
pub(crate) struct GroupHead {
    /// The zone LSN of a zone member, `0` outside every zone.
    pub(crate) lsn: u64,
    pub(crate) kind: SalMessageKind,
    /// `FLAG_*` bits.
    flags: u8,
    /// The worker count the group was written at.
    width: u8,
    pub(crate) reply: Option<RequestId>,
    pub(crate) target_id: u64,
    /// The workers the group addresses, within `width`.
    pub(crate) targets: WorkerSet,
}

impl GroupHead {
    /// Whether this group is its zone's last member, the one that closes it.
    fn zone_end(&self) -> bool {
        self.flags & FLAG_ZONE_END != 0
    }

    /// Whether the group is a later member of its [`Cut`].
    fn joins_cut(&self) -> bool {
        self.flags & FLAG_JOINS_CUT != 0
    }

    /// Whether the group holds one payload for every addressed worker.
    fn shared(&self) -> bool {
        self.flags & FLAG_SHARED != 0
    }

    /// The worker count the group was written at.
    pub(crate) fn width(&self) -> u32 {
        self.width as u32
    }

    /// Every scalar field into `hdr`, with the base and epoch it is published at.
    fn encode(&self, hdr: &mut [u8], base: u64, epoch: u32) {
        hdr[OFF_KIND..OFF_KIND + 4].copy_from_slice(&[self.kind.as_wire(), self.flags, self.width, 0]);
        write_u32_le(hdr, OFF_REQUEST_ID, RequestId::to_wire(self.reply));
        write_u32_le(hdr, OFF_BASE, base as u32);
        write_u32_le(hdr, OFF_EPOCH, epoch);
        write_u64_le(hdr, OFF_LSN, self.lsn);
        write_u64_le(hdr, OFF_TARGET_ID, self.target_id);
        write_u64_le(hdr, OFF_TARGETS, self.targets.0);
    }

    /// The scalars of a digest-verified fixed prefix; `None` when they are
    /// nothing the writer produces.
    fn decode(fixed: &[u8; HDR_PREFIX]) -> Option<GroupHead> {
        let head = GroupHead {
            lsn: read_u64_le(fixed, OFF_LSN),
            kind: SalMessageKind::from_wire(fixed[OFF_KIND])?,
            flags: fixed[OFF_FLAGS],
            width: fixed[OFF_WIDTH],
            reply: RequestId::from_wire(read_u32_le(fixed, OFF_REQUEST_ID)),
            target_id: read_u64_le(fixed, OFF_TARGET_ID),
            targets: WorkerSet(read_u64_le(fixed, OFF_TARGETS)),
        };
        (head.flags & !KNOWN_FLAGS == 0
            && !(head.zone_end() && head.lsn == 0)
            && head.width as usize <= MAX_WORKERS
            && head.targets == head.targets.within(head.width as usize))
        .then_some(head)
    }
}

/// One SAL group as a reader sees it: the header verified at its own offset,
/// and the payload bytes its directory describes.
pub(crate) struct SalMessage {
    pub(crate) head: GroupHead,
    /// The group's byte offset in the ring — what a recovery error names.
    pub(crate) base: u64,
    /// The offset past the group.
    end: u64,
    /// The group's payload bytes, from the header's end to the end of the group.
    payload: &'static [u8],
    /// What the writer stored at `OFF_PAYLOAD_SUM`.
    payload_sum: u64,
    /// The directory: one `DIR_ENTRY_BYTES` entry per payload.
    dir: &'static [u8],
}

impl SalMessage {
    /// Whether the payload bytes are what the writer checksummed; only a zone
    /// member's are checksummed.
    fn intact(&self) -> bool {
        debug_assert!(self.head.lsn != 0, "only a zone member's payloads carry a checksum");
        gnitz_wire::checksum(self.payload) == self.payload_sum
    }

    /// Each payload's bytes, in directory order.
    pub(crate) fn payloads(&self) -> impl Iterator<Item = &'static [u8]> + '_ {
        let payload = self.payload;
        payload_spans(dir_sizes(self.dir)).map(move |(off, sz)| &payload[off..off + sz])
    }

    /// The one payload of a shared group, `None` on a group with one per worker.
    pub(crate) fn shared_payload(&self) -> Option<&'static [u8]> {
        if self.head.shared() {
            self.payloads().next()
        } else {
            None
        }
    }

    /// Worker `w`'s bytes, `None` when the group does not address it.
    /// Outlined: inlined into [`SalReader::next`], it costs the drain more
    /// instructions per group than the call does.
    #[inline(never)]
    pub(crate) fn slot(&self, w: u32) -> Option<&'static [u8]> {
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
    /// [`decode_sal_frame`]'s. The error names the group's offset, LSN and target.
    pub(crate) fn rows(
        &self,
        payload: &[u8],
        known: impl FnOnce(u64, &[u8]) -> Option<SchemaDescriptor>,
    ) -> Result<Option<Batch>, String> {
        decode_sal_frame(payload, known).map(|(_, batch)| batch).map_err(|e| {
            format!(
                "SAL replay: corrupt block at offset={} lsn={} target={}: {e}",
                self.base, self.head.lsn, self.head.target_id
            )
        })
    }
}

/// A payload's control block, and the rows of its data block under the payload's
/// own schema record; `known` answers what that record decodes to when the
/// caller already knows.
fn decode_sal_frame(
    bytes: &[u8],
    known: impl FnOnce(u64, &[u8]) -> Option<SchemaDescriptor>,
) -> Result<(DecodedControl, Option<Batch>), String> {
    let control = peek_control_block(bytes)?;
    let Some(r) = control.data.clone() else {
        return Ok((control, None));
    };
    let record = &bytes[control.schema.clone().ok_or("a data block without a schema block")?];
    let schema = match known(control.hdr.target_id, record) {
        Some(s) => s,
        None => decode_schema_block(record)?,
    };
    let batch = Batch::decode_from_wal_block(&bytes[r], &schema)?;
    batch.debug_verify_null_bits();
    Ok((control, Some(batch)))
}

/// What the bytes at an offset are.
enum SalStep {
    /// Nothing published here, or, on a walk, a verified header stamped with
    /// another epoch. Either way, the end of the log.
    Absent,
    /// A published header at this offset that fails its digest, does not decode,
    /// or sizes a group the mapping cannot hold.
    Corrupt,
    Group(SalMessage),
}

/// How a reader treats the epoch a group is stamped with.
#[derive(Clone, Copy)]
enum EpochGate {
    /// Live worker drain. Rejects on the prefix's epoch copy **before a single
    /// header byte is read**: a parked slot may be overwritten by the master
    /// under us, so its bytes are unreadable until the prefix proves the slot is
    /// ours. The header's own epoch is then checked too, since the prefix copy is
    /// outside the digest: a mismatch behind a passing prefix is damage.
    Live(u32),
    /// Quiescent recovery walk. The prefix's epoch half is not consulted, so a
    /// flipped prefix epoch changes nothing; only the digested header's epoch
    /// decides.
    Walk(u32),
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
    ring: *mut u8,
    ring_len: usize,
    /// What the writer syncs.
    file: &'static File,
}

impl SalLog {
    /// Open `data_dir`'s SAL, creating it on a first boot. A fresh file reads
    /// all-zero, the empty log recovery expects. The file is leaked: the writer
    /// and the reactor's fsync SQEs name it for the process's life.
    pub(crate) fn open(data_dir: &str) -> Result<SalLog, String> {
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
    fn map(file: &'static File, len: usize) -> io::Result<SalLog> {
        let len = len.max(file.metadata()?.len().min(SAL_MMAP_SIZE as u64) as usize);
        assert!(len > ANCHOR_BYTES, "the SAL holds a ring behind its anchor page");
        let fd = file.as_raw_fd();
        set_nocow(fd);
        // A store past the file's end raises `SIGBUS`, and one into an
        // unreserved block can fail for want of space.
        posix_io::retry_eintr(|| unsafe { libc::fallocate(fd, 0, 0, len as libc::off_t) })?;
        let ptr = posix_io::map_shared(Some(file.as_fd()), len)?;
        Ok(SalLog {
            // SAFETY: `len > ANCHOR_BYTES`.
            ring: unsafe { ptr.add(ANCHOR_BYTES) },
            ring_len: len - ANCHOR_BYTES,
            file,
        })
    }

    /// The header of the group laid out at `base`, sized by its own payload
    /// count.
    ///
    /// # Safety
    /// A group's header is laid out at `base`, and nothing else references it.
    #[allow(clippy::mut_from_ref)]
    unsafe fn header_mut(&self, base: u64) -> &'static mut [u8] {
        let at = self.ring.add(base as usize + PREFIX_BYTES);
        std::slice::from_raw_parts_mut(at, header_len(&*(at as *const [u8; HDR_PREFIX])))
    }

    fn anchor_atomic(&self) -> &'static AtomicU64 {
        // SAFETY: the anchor page is mapped ahead of the ring.
        unsafe { prefix_atomic(self.ring.wrapping_sub(ANCHOR_BYTES), 0) }
    }

    /// The walk epoch.
    fn anchor(&self) -> Result<u32, String> {
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
    fn prefix_word(&self, base: u64) -> u64 {
        if base as usize + PREFIX_BYTES > self.ring_len {
            return 0;
        }
        unsafe { prefix_atomic(self.ring, base as usize).load(Ordering::Acquire) }
    }

    /// Classify the bytes at `cursor`, building the message when they are a
    /// readable group. Each bound precedes the read it guards.
    ///
    /// The caller decides what a `Corrupt` verdict means: the live drain
    /// fail-stops (see [`SalReader::next`]), the recovery walk stops there.
    fn read_at(&self, cursor: u64, gate: EpochGate) -> SalStep {
        let word = self.prefix_word(cursor);
        if word == 0 {
            return SalStep::Absent;
        }
        let epoch = match gate {
            EpochGate::Live(e) if word_epoch(word) != e => return SalStep::Absent,
            EpochGate::Live(e) | EpochGate::Walk(e) => e,
        };

        let hdr_off = cursor as usize + PREFIX_BYTES;
        let m = self.ring_len;
        if hdr_off + HDR_PREFIX > m {
            return SalStep::Corrupt;
        }
        // SAFETY: the bound above admits the fixed prefix.
        let fixed: &'static [u8; HDR_PREFIX] = unsafe { &*(self.ring.add(hdr_off) as *const [u8; HDR_PREFIX]) };
        let hdr_len = header_len(fixed);
        if hdr_off + hdr_len > m {
            return SalStep::Corrupt;
        }
        // SAFETY: bounded directly above.
        let hdr = unsafe { std::slice::from_raw_parts(self.ring.add(hdr_off), hdr_len) };
        if group_digest(hdr) != read_u64_le(hdr, OFF_DIGEST) || u64::from(read_u32_le(fixed, OFF_BASE)) != cursor {
            return SalStep::Corrupt;
        }
        let Some(head) = GroupHead::decode(fixed) else {
            return SalStep::Corrupt;
        };
        if read_u32_le(fixed, OFF_EPOCH) != epoch {
            return match gate {
                EpochGate::Live(_) => SalStep::Corrupt,
                EpochGate::Walk(_) => SalStep::Absent,
            };
        }

        let dir = &hdr[OFF_DIRECTORY..];
        let end = cursor as usize + group_total_size(hdr_len, dir_sizes(dir));
        if end > m {
            return SalStep::Corrupt;
        }

        // SAFETY: bounded directly above; the payloads run from the header's end
        // to the group's end.
        let payload_off = hdr_off + hdr_len;
        let payload = unsafe { std::slice::from_raw_parts(self.ring.add(payload_off), end - payload_off) };
        SalStep::Group(SalMessage {
            head,
            base: cursor,
            end: end as u64,
            payload,
            payload_sum: read_u64_le(fixed, OFF_PAYLOAD_SUM),
            dir,
        })
    }

    /// Every group readable from offset 0 at `epoch`, up to the first that is not.
    fn walk(self, epoch: u32) -> impl Iterator<Item = SalMessage> {
        let mut at = 0;
        std::iter::from_fn(move || match self.read_at(at, EpochGate::Walk(epoch)) {
            SalStep::Group(msg) => {
                at = msg.end;
                Some(msg)
            }
            SalStep::Absent | SalStep::Corrupt => None,
        })
    }
}

// ---------------------------------------------------------------------------
// SalWriter
// ---------------------------------------------------------------------------

/// The tag of a [`SalWriter`]'s lock.
enum SalLock {}

pub(crate) struct SalWriter {
    log: SalLog,
    write_cursor: Cell<u64>,
    epoch: Cell<u32>,
    /// The LSN of the latest scope that published anything; see
    /// [`Self::watermark`].
    watermark: Cell<u64>,
    checkpoint_threshold: u64,
    /// A group was refused since the last reset that an empty log would admit,
    /// so a checkpoint is warranted whatever the write cursor says. Cleared by
    /// [`Self::rewind`].
    refused_transient: Cell<bool>,
    /// A zone member pushing rows was published in this epoch. Cleared by
    /// [`Self::rewind`].
    holds_pushes: Cell<bool>,
    /// Taken by every [`SalExcl`].
    excl: AsyncRwLock<SalLock>,
    /// Every worker's park; its length is the worker count groups are written at.
    parks: WorkerParks,
}

impl SalWriter {
    /// The writer of the boot that recovered `tail`, rewound to its live epoch.
    pub(crate) fn boot(tail: CommittedTail, parks: WorkerParks, _: ReadyAcked) -> Self {
        Self::rewound(tail.log(), parks, tail.live_epoch())
    }

    /// A writer over `log` rewound to cursor 0 of `epoch`, its watermark below
    /// every LSN of `epoch` and above every earlier epoch's.
    fn rewound(log: SalLog, parks: WorkerParks, epoch: u32) -> Self {
        assert!(
            log.ring_len > PREFIX_BYTES + CHECKPOINT_RESERVE,
            "the ring must hold the checkpoint band"
        );
        let checkpoint_threshold =
            gnitz_foundation::env::env_num("GNITZ_CHECKPOINT_BYTES", (log.ring_len as u64 * 3) >> 2);
        let writer = SalWriter {
            log,
            write_cursor: Cell::new(0),
            epoch: Cell::new(epoch),
            watermark: Cell::new(epoch_word(epoch, 0) - 1),
            checkpoint_threshold,
            refused_transient: Cell::new(false),
            holds_pushes: Cell::new(false),
            excl: AsyncRwLock::default(),
            parks,
        };
        writer.rewind(epoch);
        writer
    }

    /// Sole write access, once no other task holds it.
    pub(crate) async fn lock(&self) -> SalExcl<'_> {
        SalExcl {
            writer: self,
            reached: WorkerSet::EMPTY,
            _guard: self.excl.write().await,
        }
    }

    /// The worker count every group is written at.
    pub(crate) fn num_workers(&self) -> usize {
        self.parks.len()
    }

    /// Lay one group out at the write cursor, `fill` writing each payload's bytes
    /// in directory order, and return its base — the group is complete on the
    /// log but for its digest, and unpublished; [`Self::publish`] does both. A
    /// group that does not fit is refused with [`WireStatus::SalFull`] and leaves
    /// the log untouched; the refusal reaches clients verbatim, so the cursor goes
    /// to the operator log instead. A zone member (`lsn != 0`) checksums its
    /// payload bytes.
    fn write_slots(&self, head: GroupHead, sizes: &[usize], mut fill: impl FnMut(&mut [u8])) -> Result<u64, WireFault> {
        let epoch = self.epoch.get();
        assert!(
            epoch >= 1,
            "SAL group epoch must be >= 1 — epoch 0 is indistinguishable from the empty prefix"
        );
        debug_assert_eq!(sizes.len(), payload_count(head.flags, head.targets));
        debug_assert_eq!(
            head.width as usize,
            self.parks.len(),
            "a group is written at the launched width"
        );
        debug_assert_eq!(
            head.targets,
            head.targets.within(head.width as usize),
            "a group addresses launched workers"
        );

        let hdr_size = group_header_size(sizes.len());
        let total = group_total_size(hdr_size, sizes.iter().copied());
        let cap = group_limit(head.kind, self.log.ring_len);
        let base = self.write_cursor.get();
        if total > cap.saturating_sub(base as usize) {
            // On the writer, not on the scope: the group did not fit whether or
            // not a rolled-back transaction contained it. One that fits an empty
            // log is a checkpoint away from fitting.
            if total <= cap {
                self.refused_transient.set(true);
            }
            gnitz_debug!(
                "SAL group refused: cursor={} mmap={} epoch={} need={} cap={}",
                base,
                self.log.ring_len,
                epoch,
                total,
                cap
            );
            return Err(WireFault {
                status: WireStatus::SalFull,
                text: format!("SAL full: {:?} group did not fit", head.kind),
            });
        }

        let hdr_off = base as usize + PREFIX_BYTES;
        // SAFETY: `cursor + total <= ring_len`, so the whole group is mapped, and
        // nothing else references the span past the write cursor. One
        // `&mut [u8]` over the header, so every field write below is
        // bounds-checked against it rather than argued in prose.
        let hdr = unsafe { std::slice::from_raw_parts_mut(self.log.ring.add(hdr_off), hdr_size) };
        // `wal.sal` is never truncated, so a byte of the digested span left
        // unwritten would read as whatever group last occupied this offset:
        // every one is written.
        head.encode(hdr, base, epoch);
        for (i, (off, sz)) in payload_spans(sizes.iter().copied()).enumerate() {
            write_u64_le(hdr, OFF_DIRECTORY + i * DIR_ENTRY_BYTES, sz as u64);
            // SAFETY: `off + sz <= total - PREFIX_BYTES - hdr_size`, inside the mapped
            // span past `hdr`, and the spans are disjoint.
            let slot = unsafe { std::slice::from_raw_parts_mut(self.log.ring.add(hdr_off + hdr_size + off), sz) };
            fill(slot);
        }
        // The span includes the padding behind each payload, which nothing
        // writes: the checksum covers whatever bytes are there, and nothing
        // rewrites a laid-out group's span before the cursor passes it again.
        // SAFETY: the payload span of the group laid out above.
        let payload = unsafe {
            std::slice::from_raw_parts(self.log.ring.add(hdr_off + hdr_size), total - PREFIX_BYTES - hdr_size)
        };
        let sum = if head.lsn != 0 {
            gnitz_wire::checksum(payload)
        } else {
            0
        };
        write_u64_le(hdr, OFF_PAYLOAD_SUM, sum);

        let end = base + total as u64;
        // SAFETY: `end <= cap <= ring_len`, inside the mapping, and the word at
        // `end` is past the write cursor, so no reader has reached it.
        unsafe { write_end_prefix(self.log.ring, end as usize) };
        self.write_cursor.set(end);
        Ok(base)
    }

    /// Stamp the digest of the group laid out at `base` and store its prefix: the
    /// group is readable from here on. Returns its end.
    fn publish(&self, base: u64) -> u64 {
        // SAFETY: laid out by this writer and, its prefix word still zero,
        // reached by no reader.
        let hdr = unsafe { self.log.header_mut(base) };
        stamp_digest(hdr);
        let end = base + group_total_size(hdr.len(), dir_sizes(&hdr[OFF_DIRECTORY..])) as u64;
        let word = epoch_word(self.epoch.get(), PRESENT);
        // SAFETY: the group's prefix word, inside the mapping.
        unsafe { prefix_atomic(self.log.ring, base as usize).store(word, Ordering::Release) };
        end
    }

    /// Publish every group laid out in `[from, end)` in layout order.
    fn publish_range(&self, from: u64, end: u64) {
        let mut off = from;
        while off < end {
            off = self.publish(off);
        }
    }

    /// Mark the unpublished group at `base` as its zone's last member.
    fn mark_zone_end(&self, base: u64) {
        // SAFETY: as `publish`.
        unsafe { self.log.header_mut(base)[OFF_FLAGS] |= FLAG_ZONE_END };
    }

    /// Whether a checkpoint is warranted: the write cursor has crossed the
    /// configured threshold, or a group a checkpoint would admit has been refused
    /// since the last reset, on a log whose cursor may still sit below the
    /// threshold.
    pub(crate) fn needs_checkpoint(&self) -> bool {
        self.write_cursor.get() >= self.checkpoint_threshold || self.refused_transient.get()
    }

    /// Rewind to cursor 0 of `epoch`, which is anchored durably before any of
    /// its groups can exist.
    fn rewind(&self, epoch: u32) {
        debug_assert!(epoch >= 1, "the first live SAL epoch is 1");
        self.write_cursor.set(0);
        self.epoch.set(epoch);
        self.refused_transient.set(false);
        self.holds_pushes.set(false);
        // Emptied only once the new epoch is durable: a kill before that must
        // leave the old epoch's tail whole.
        self.write_anchor(epoch);
        if let Err(e) = self.log.file.sync_data() {
            gnitz_fatal_abort!("SAL fdatasync (rewind to epoch {epoch}) failed: {e}");
        }
        unsafe { write_end_prefix(self.log.ring, 0) };
    }

    /// Anchor `epoch` as the one the walk reads.
    fn write_anchor(&self, epoch: u32) {
        self.log.anchor_atomic().store(anchor_word(epoch), Ordering::Relaxed);
    }

    /// Whether a zone member pushing rows was published in this epoch: the next
    /// base round moves a base store.
    pub(crate) fn holds_pushes(&self) -> bool {
        self.holds_pushes.get()
    }

    /// Every scope that published anything took an LSN at or below this; every
    /// scope opened later takes one above it. Moves only when a scope commits.
    pub(crate) fn watermark(&self) -> u64 {
        self.watermark.get()
    }
}

/// Sole write access. Dropping it wakes each worker a group written under it
/// reached that no earlier wake did. Held across no await except a checkpoint
/// round's ACKs.
pub(crate) struct SalExcl<'a> {
    writer: &'a SalWriter,
    /// The workers groups laid out since the last [`Self::wake`] reached.
    reached: WorkerSet,
    _guard: WriteGuard<SalLock>,
}

impl<'a> SalExcl<'a> {
    /// Lay `g` out at the write cursor, unpublished, stamped with `lsn` and,
    /// when `joins_cut`, as a later member of its [`Cut`]. The one encode path.
    fn lay_out(&mut self, g: &SalGroup, lsn: u64, joins_cut: bool) -> Result<u64, WireFault> {
        let nw = self.writer.parks.len();
        let per_worker = match g.data {
            GroupData::Same(_) => nw,
            GroupData::Each(each) => each.len(),
            GroupData::Blobs(blobs) => blobs.len(),
        };
        assert_eq!(per_worker, nw, "per-worker entries={per_worker} != num_workers={nw}");
        debug_assert!(
            g.schema.is_some() || g.data.is_dataless(),
            "data without a schema — `decode_sal_frame` rejects a data block without a schema block",
        );

        let set = g.targets.set.within(nw);
        let shared = matches!(g.data, GroupData::Same(_));
        let (kind, template) = g.request.encode();
        debug_assert!(
            shared || g.data.holders().union(set) == set,
            "a group addresses every worker it has rows for"
        );
        // A shared group carries one payload, which `msg` builds alike for every
        // worker; any other carries one per addressed worker, in set order.
        let payloads = if shared { WorkerSet::one(0) } else { set };
        let mut sizes = [0usize; MAX_WORKERS];
        for (i, w) in payloads.iter().enumerate() {
            sizes[i] = g.msg(&template, w).size();
        }
        let head = GroupHead {
            lsn,
            kind,
            flags: if shared { FLAG_SHARED } else { 0 } | if joins_cut { FLAG_JOINS_CUT } else { 0 },
            width: nw as u8,
            reply: g.targets.reply,
            target_id: template.target_id,
            targets: set,
        };
        let mut workers = payloads.iter();
        let base = self.writer.write_slots(head, &sizes[..payloads.len()], |slot| {
            g.msg(&template, workers.next().expect("one payload per size"))
                .encode(slot)
        })?;
        self.reached = self.reached.union(set);
        Ok(base)
    }

    /// Write and publish one group, outside every zone, starting a [`Cut`].
    pub(crate) fn write(&mut self, g: &SalGroup) -> Result<(), WireFault> {
        self.write_group(g, false)
    }

    /// [`Self::write`], joining the [`Cut`] of the group laid out before it.
    pub(crate) fn join(&mut self, g: &SalGroup) -> Result<(), WireFault> {
        self.write_group(g, true)
    }

    fn write_group(&mut self, g: &SalGroup, joins_cut: bool) -> Result<(), WireFault> {
        let base = self.lay_out(g, 0, joins_cut)?;
        self.writer.publish(base);
        Ok(())
    }

    /// Open a publication scope at the log's next zone LSN: groups laid out in
    /// it are invisible until [`SalScope::commit`], and an uncommitted drop
    /// discards the span. `tag` names the scope for [`ZONE_PANIC`]. `&mut`, so
    /// no scope overlaps another or a rewind.
    pub(crate) fn begin(&mut self, tag: &'static str) -> SalScope<'_, 'a> {
        SalScope {
            from: self.writer.write_cursor.get(),
            excl: self,
            tag,
            last_member: None,
            zoned_push: false,
            committed: false,
        }
    }

    /// `fdatasync` the log, fatal on failure: every group published before the
    /// call is durable once it resolves. The abort names the caller's location.
    #[track_caller]
    pub(crate) fn sync(&self, reactor: &Reactor) -> impl Future<Output = ()> {
        let at = std::panic::Location::caller();
        let done = reactor.fsync(self.writer.log.file.as_fd());
        async move {
            let rc = done.await;
            if rc < 0 {
                gnitz_fatal_abort!("{at}: SAL fdatasync failed rc={rc}");
            }
        }
    }

    /// Rewind into the next epoch — the checkpoint's reclaim — and return
    /// that epoch.
    pub(crate) fn checkpoint_reset(&mut self) -> u32 {
        let epoch = next_epoch(self.writer.epoch.get());
        self.writer.rewind(epoch);
        epoch
    }

    /// Wake every worker a group written since the last wake reached.
    pub(crate) fn wake(&mut self) {
        self.writer
            .parks
            .wake(std::mem::replace(&mut self.reached, WorkerSet::EMPTY));
    }
}

impl Drop for SalExcl<'_> {
    fn drop(&mut self) {
        self.wake()
    }
}

/// One publication span on the SAL, holding at most one zone: its `zoned`
/// groups. Nothing is visible until [`Self::commit`].
pub(crate) struct SalScope<'s, 'w> {
    excl: &'s mut SalExcl<'w>,
    /// Where the scope opened — the start of its publish range, and where an
    /// uncommitted drop puts the write cursor back.
    from: u64,
    /// Which scope this is, for [`ZONE_PANIC`]: `"ddl"` or `"commit"`.
    tag: &'static str,
    /// The base of the latest zoned group; `Some` iff a zone is open.
    last_member: Option<u64>,
    /// A zoned group carries a push. Only `true` while a zone is open.
    zoned_push: bool,
    committed: bool,
}

impl SalScope<'_, '_> {
    /// The LSN this scope's zone members carry: its opening position on the
    /// log, in an epoch that cannot move while the scope borrows the hold. The
    /// epoch is the high word, so LSNs follow SAL order across checkpoints and
    /// restarts; two scopes that published anything never share one.
    pub(crate) fn lsn(&self) -> u64 {
        epoch_word(self.excl.writer.epoch.get(), self.from)
    }

    /// Lay `g` out inside the scope, unpublished. `zoned` puts it in the
    /// scope's atomic zone at the scope's LSN; otherwise it is written at LSN 0.
    pub(crate) fn write(&mut self, g: &SalGroup, zoned: bool) -> Result<(), WireFault> {
        let base = self.excl.lay_out(g, if zoned { self.lsn() } else { 0 }, false)?;
        if zoned {
            self.last_member = Some(base);
            self.zoned_push |= matches!(g.request, SalRequest::Apply(Apply::Push { .. }));
        }
        Ok(())
    }

    /// Run `f`, and take back every group it laid out when it fails: nothing in
    /// a scope is published, so restoring the cursor, the zone's last member and
    /// whether it pushes is the whole undo.
    pub(crate) fn all_or_nothing<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, WireFault>,
    ) -> Result<T, WireFault> {
        let (cursor, last_member, zoned_push) =
            (self.excl.writer.write_cursor.get(), self.last_member, self.zoned_push);
        let done = f(self);
        if done.is_err() {
            self.excl.writer.write_cursor.set(cursor);
            self.last_member = last_member;
            self.zoned_push = zoned_push;
        }
        done
    }

    /// Publish the span in layout order. Answers whether a zone closed.
    pub(crate) fn commit(mut self) -> bool {
        self.committed = true;
        let w = self.excl.writer;
        let end = w.write_cursor.get();
        // An empty scope's LSN is the next scope's too, so it must not count.
        if end > self.from {
            w.watermark.set(self.lsn());
        }
        let Some(last) = self.last_member else {
            w.publish_range(self.from, end);
            return false;
        };
        if self.zoned_push {
            w.holds_pushes.set(true);
        }
        w.mark_zone_end(last);
        w.publish_range(self.from, last);
        if ZONE_PANIC.at(self.tag) {
            std::process::abort();
        }
        w.publish_range(last, end);
        true
    }
}

impl Drop for SalScope<'_, '_> {
    /// An uncommitted scope publishes nothing: the write cursor goes back to
    /// where it opened and the bytes stay for the next group to overwrite.
    fn drop(&mut self) {
        if !self.committed {
            self.excl.writer.write_cursor.set(self.from);
        }
    }
}

// ---------------------------------------------------------------------------
// SalReader — one worker's live drain over a SalLog
// ---------------------------------------------------------------------------

/// A run of consecutive groups in the log: one that starts it, then each that
/// [`SalExcl::join`] writes behind it, carrying [`FLAG_JOINS_CUT`]. A worker
/// sends a cut's replies in request order: the master drains a cut's leases in
/// that order, and a W2M ring frees only a released prefix. Replies of different
/// cuts go to different leases, and may pass each other.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Cut(u64);

#[cfg(test)]
impl Cut {
    /// The `n`th cut a drain passes.
    pub(crate) fn nth(n: u64) -> Cut {
        Cut(n)
    }
}

/// Where the reply to a read goes.
#[derive(Clone, Copy)]
pub(crate) struct ReplyRoute {
    pub(crate) target_id: u64,
    pub(crate) request_id: RequestId,
    /// The cut the read was written in.
    pub(crate) cut: Cut,
}

/// One group addressed to a worker, decoded and owned: a parked read outlives the
/// SAL bytes it came in, which an inline `Flush` lets the master rewrite.
pub(crate) enum Inbound {
    Read {
        route: ReplyRoute,
        read: Read<'static>,
    },
    Apply {
        /// The id the change is acknowledged on; `None` when nothing waits for it.
        ack: Option<RequestId>,
        apply: Apply<'static>,
        /// Boxed, so a request moves from its decode to where it is served as a
        /// few words: held inline, the batch would be copied whole at each of
        /// those moves, for a request that carries none as well.
        rows: Option<Box<Batch>>,
    },
    Shutdown,
}

pub(crate) struct SalReader {
    log: SalLog,
    worker_id: u32,
    /// The live drain's position and epoch — the read-side mirror of
    /// `SalWriter`'s `write_cursor` / `epoch`, so no caller does SAL address
    /// arithmetic.
    read_cursor: u64,
    expected_epoch: u32,
    /// The cut of the group [`Self::next`] last returned: how many groups
    /// opening a cut the drain has passed.
    cut: Cut,
}

impl SalReader {
    /// A live drain from cursor 0 of `live_epoch`.
    pub(crate) fn new(log: SalLog, worker_id: u32, live_epoch: u32) -> Self {
        SalReader {
            log,
            worker_id,
            read_cursor: 0,
            expected_epoch: live_epoch,
            cut: Cut(0),
        }
    }

    /// The next group addressed to this worker, with this worker's bytes; groups
    /// addressed to others are stepped over. Stepping past a group that ends its
    /// epoch moves the drain to the start of the next one; a group from another
    /// epoch parks.
    ///
    /// A published header that fails verification behind a matching prefix epoch
    /// can only be damage, and parking on it would read as end-of-drain, so it
    /// fail-stops.
    fn next(&mut self) -> Option<(SalMessage, &'static [u8])> {
        loop {
            let cursor = self.read_cursor;
            match self.log.read_at(cursor, EpochGate::Live(self.expected_epoch)) {
                SalStep::Group(msg) => {
                    if msg.head.kind.ends_epoch() {
                        self.read_cursor = 0;
                        self.expected_epoch = next_epoch(self.expected_epoch);
                    } else {
                        self.read_cursor = msg.end;
                    }
                    // Counted whether or not the group addresses this worker, so
                    // every worker numbers a cut alike.
                    if !msg.head.joins_cut() {
                        self.cut.0 += 1;
                    }
                    if let Some(slot) = msg.slot(self.worker_id) {
                        return Some((msg, slot));
                    }
                }
                SalStep::Corrupt => {
                    gnitz_fatal_abort!("SAL group header at offset={cursor} is corrupt — the log is damaged")
                }
                SalStep::Absent => return None,
            }
        }
    }

    /// The next group addressed to this worker. The master authored it, so bytes
    /// that do not decode fail-stop, as a corrupt header does in [`Self::next`].
    /// `catalog_schema` answers what a relation's catalog record decodes to.
    #[inline]
    pub(crate) fn next_request(
        &mut self,
        catalog_schema: impl FnOnce(u64, &[u8]) -> Option<SchemaDescriptor>,
    ) -> Option<Inbound> {
        let (msg, wire) = self.next()?;
        // Fail-stop: a dropped group diverges this worker from the master.
        let undecodable = |e: String| -> ! {
            gnitz_fatal_abort!(
                "failed to decode {:?} for tid={}: {e}",
                msg.head.kind,
                msg.head.target_id
            )
        };
        // Read in place: the rows move once, out of the decoded frame.
        let mut decoded = decode_sal_frame(wire, catalog_schema);
        let (control, rows) = match &mut decoded {
            Ok(frame) => frame,
            Err(e) => undecodable(std::mem::take(e)),
        };
        // [`SalGroup::push`] and [`SalGroup::ddl_sync`] frame the only groups
        // that carry rows.
        if rows.is_some() && !matches!(msg.head.kind, SalMessageKind::Push | SalMessageKind::DdlSync) {
            undecodable(format!("a {:?} group carries no rows", msg.head.kind));
        }
        let what = match SalRequest::decode(msg.head.kind, &control.hdr, &wire[control.blob.clone()]) {
            Ok(what) => what,
            Err(e) => undecodable(e),
        };
        Some(match what {
            SalRequest::Read(read) => {
                let Some(request_id) = msg.head.reply else {
                    undecodable("a read group carries no request id".into())
                };
                let route = ReplyRoute {
                    target_id: control.hdr.target_id,
                    request_id,
                    cut: self.cut,
                };
                Inbound::Read { route, read }
            }
            SalRequest::Apply(apply) => Inbound::Apply {
                ack: msg.head.reply,
                apply,
                rows: rows.take().map(Box::new),
            },
            SalRequest::Shutdown => Inbound::Shutdown,
        })
    }

    /// No group of this epoch is published at the cursor. A published header that
    /// does not verify reads as non-empty, so [`Self::next`] reaches it and fail-stops.
    pub(crate) fn is_empty(&self) -> bool {
        let word = self.log.prefix_word(self.read_cursor);
        word == 0 || word_epoch(word) != self.expected_epoch
    }
}

/// The SAL fixture.
#[cfg(test)]
#[path = "tests/fixtures.rs"]
pub(crate) mod fixtures;

#[cfg(test)]
#[path = "tests/sal.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/sal.rs"]
mod bench;
