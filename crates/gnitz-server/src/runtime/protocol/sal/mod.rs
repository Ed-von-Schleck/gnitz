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
pub(crate) mod zone;

pub(crate) use request::{Apply, Read, SalRequest};

use std::cell::Cell;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io;
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::runtime::park::WorkerParks;
use crate::runtime::reactor::{AsyncRwLock, Reactor, WriteGuard};
use crate::runtime::wire::WireMsg;
use gnitz_foundation::fault::Seam;
use gnitz_foundation::posix_io;
use gnitz_wire::control::{frame_head_size, peek_control_block, DecodedControl};
use gnitz_wire::{low_bits_mask, read_u32_le, read_u64_le, write_u32_le, write_u64_le, BitIter};
use gnitz_wire::{WireFault, WireStatus};
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
/// Each reply must reach the ring in request order.
const FLAG_IN_REQUEST_ORDER: u8 = 2;
/// The group holds one payload, which every addressed worker reads.
const FLAG_SHARED: u8 = 4;
const KNOWN_FLAGS: u8 = FLAG_ZONE_END | FLAG_IN_REQUEST_ORDER | FLAG_SHARED;
/// The worker count the group was written at, one byte.
const OFF_WIDTH: usize = 10;
/// `0` = nothing answers.
const OFF_REQUEST_ID: usize = 12;
/// The group's own byte offset in the ring.
const OFF_BASE: usize = 16;
const OFF_EPOCH: usize = 20;
/// The zone LSN of a zone member, `0` outside every zone.
const OFF_LSN: usize = 24;
const OFF_TARGET_ID: usize = 32;
/// The [`WorkerSet`] the group addresses, within its width.
const OFF_TARGETS: usize = 40;
/// One entry per payload, 8-byte-padded.
const OFF_DIRECTORY: usize = 48;

/// One directory entry: `[u32 size][u64 payload checksum]`. The checksum is 0
/// outside a zone member.
const DIR_ENTRY_BYTES: usize = 12;
const DIR_CHECKSUM_AT: usize = 4;

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
    OFF_DIRECTORY + (payloads * DIR_ENTRY_BYTES).next_multiple_of(8)
}

// Group bases stay aligned for the publication prefix's atomic store.
const _: () = {
    let mut payloads = 0;
    while payloads <= MAX_WORKERS {
        assert!(
            group_header_size(payloads).is_multiple_of(PREFIX_BYTES),
            "a group header must be a multiple of PREFIX_BYTES, or group bases stop being aligned"
        );
        payloads += 1;
    }
};

/// Each payload as `(index, payload-relative offset, size)`, in directory order.
/// The offset is a pure function of the sizes before it — a payload starts where
/// the 8-byte-padded ones ahead of it end — so the directory stores sizes alone
/// and an inconsistent offset is unrepresentable.
fn payload_spans(sizes: impl Iterator<Item = usize>) -> impl Iterator<Item = (usize, usize, usize)> {
    let mut off = 0;
    sizes.enumerate().map(move |(i, sz)| {
        let at = off;
        off += sz.next_multiple_of(8);
        (i, at, sz)
    })
}

/// The SAL bytes a group occupies: prefix, header, directory and 8-byte-padded
/// payloads — what the write cursor advances by, and where [`payload_spans`]
/// leaves off.
fn group_total_size(payloads: usize, sizes: impl Iterator<Item = usize>) -> usize {
    PREFIX_BYTES + group_header_size(payloads) + sizes.map(|sz| sz.next_multiple_of(8)).sum::<usize>()
}

/// A group's directory bytes read back as payload sizes.
fn dir_sizes(dir: &[u8]) -> impl Iterator<Item = usize> + '_ {
    dir.as_chunks::<DIR_ENTRY_BYTES>()
        .0
        .iter()
        .map(|c| read_u32_le(c, 0) as usize)
}

/// The most workers a cluster runs: [`WorkerSet`] holds one bit per worker.
pub(crate) const MAX_WORKERS: usize = 64;

/// A subset of the workers. `ALL` is unbounded; every other set holds only
/// launched workers.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct WorkerSet(u64);

const _: () = assert!(MAX_WORKERS <= u64::BITS as usize);

impl WorkerSet {
    pub(crate) const EMPTY: Self = WorkerSet(0);
    pub(crate) const ALL: Self = WorkerSet(u64::MAX);

    pub(crate) const fn one(w: usize) -> Self {
        WorkerSet(1 << w)
    }

    pub(crate) const fn with(self, w: usize) -> Self {
        WorkerSet(self.0 | 1 << w)
    }

    pub(crate) const fn without(self, w: usize) -> Self {
        WorkerSet(self.0 & !(1 << w))
    }

    pub(crate) const fn within(self, nw: usize) -> Self {
        WorkerSet(self.0 & low_bits_mask(nw))
    }

    pub(crate) const fn contains(self, w: usize) -> bool {
        self.0 >> w & 1 == 1
    }

    pub(crate) const fn len(self) -> usize {
        self.0.count_ones() as usize
    }

    pub(crate) const fn union(self, other: Self) -> Self {
        WorkerSet(self.0 | other.0)
    }

    /// How many members of the set are below `w`: `w`'s position among them.
    pub(crate) const fn rank(self, w: usize) -> usize {
        (self.0 & low_bits_mask(w)).count_ones() as usize
    }

    pub(crate) fn iter(self) -> BitIter {
        BitIter(self.0)
    }
}

/// The workers a group is written to, the request id they answer on (`0`:
/// nothing answers), and whether each reply must reach the ring in request order.
#[derive(Clone, Copy)]
pub(crate) struct GroupTargets {
    pub(crate) set: WorkerSet,
    pub(crate) request_id: u32,
    pub(crate) in_request_order: bool,
}

impl GroupTargets {
    /// Every worker, nothing answers.
    pub(crate) const UNADDRESSED: Self = GroupTargets {
        set: WorkerSet::ALL,
        request_id: 0,
        in_request_order: false,
    };
}

/// The rows a group carries, by worker.
#[derive(Clone, Copy)]
pub(crate) enum GroupData<'a> {
    /// Every worker is sent these rows.
    Same(Option<WireRows<'a>>),
    /// Worker `w` is sent `each[w]`; one entry per worker.
    Each(&'a [Option<WireRows<'a>>]),
}

impl<'a> GroupData<'a> {
    /// A control-only group: every payload is a bare control block.
    pub(crate) const NONE: Self = Self::Same(None);

    /// True when no worker is sent a row — the one shape allowed to omit a schema.
    fn is_dataless(&self) -> bool {
        match *self {
            Self::Same(d) => d.is_none(),
            Self::Each(each) => each.iter().all(Option::is_none),
        }
    }

    /// The workers this data has rows for; every worker for the one payload all share.
    pub(crate) fn holders(&self) -> WorkerSet {
        match *self {
            Self::Same(_) => WorkerSet::ALL,
            Self::Each(each) => each
                .iter()
                .enumerate()
                .filter(|(_, rows)| rows.is_some())
                .fold(WorkerSet::EMPTY, |set, (w, _)| set.with(w)),
        }
    }
}

/// One SAL group as [`SalWriter::lay_out`] encodes it: the request every payload
/// carries, plus what varies by worker.
#[derive(Clone)]
pub(crate) struct DirectGroup<'a> {
    pub(crate) request: SalRequest<'a>,
    /// The schema record of the rows in `data`; dropped from a payload that
    /// carries none.
    pub(crate) schema: Option<&'a [u8]>,
    pub(crate) data: GroupData<'a>,
    /// Worker `w`'s blob in place of the request's, when every worker is sent its own.
    pub(crate) extras: Option<&'a [Vec<u8>]>,
    pub(crate) targets: GroupTargets,
}

impl<'a> DirectGroup<'a> {
    /// A control-only silent broadcast of `request`. Callers override what varies
    /// by struct update.
    pub(crate) fn new(request: impl Into<SalRequest<'a>>) -> Self {
        DirectGroup {
            request: request.into(),
            schema: None,
            data: GroupData::NONE,
            extras: None,
            targets: GroupTargets::UNADDRESSED,
        }
    }

    /// A push of `data` into `tid`, whose schema record is `record`, written to
    /// `targets`.
    pub(crate) fn push(tid: u64, record: &'a [u8], data: GroupData<'a>, targets: GroupTargets) -> Self {
        DirectGroup {
            schema: Some(record),
            data,
            targets,
            ..Self::new(Apply::Push { tid })
        }
    }

    /// A catalog mutation of `family`, whose schema record is `record`: `batch`,
    /// sent whole to every worker.
    pub(crate) fn ddl_sync(family: u64, record: &'a [u8], batch: &'a Batch) -> Self {
        DirectGroup {
            schema: Some(record),
            data: GroupData::Same(batch.wire_whole()),
            ..Self::new(Apply::DdlSync { family })
        }
    }

    /// Whether every addressed worker is sent the same bytes.
    fn shared(&self) -> bool {
        matches!(self.data, GroupData::Same(_)) && self.extras.is_none()
    }

    /// Worker `w`'s message over `head`, the request's template. The one
    /// definition — sizing and encoding both go through it, so a payload's size
    /// and its bytes cannot disagree.
    #[inline]
    fn msg<'m>(&'m self, head: &WireMsg<'m>, w: usize) -> WireMsg<'m> {
        let data = match self.data {
            GroupData::Same(d) => d,
            GroupData::Each(each) => each[w],
        };
        WireMsg {
            data,
            schema_block: self.schema.filter(|_| data.is_some()),
            blob: self.extras.map_or(head.blob, |e| &e[w]),
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

/// The page at the head of `wal.sal` holding the anchor: the walk epoch, and the
/// ring offset a completed `fdatasync` covers. At the head, so it does not move
/// when `GNITZ_SAL_BYTES` does.
const ANCHOR_BYTES: usize = 4096;

const _: () = assert!(
    SAL_MMAP_SIZE as u64 <= 1 << 32,
    "the anchor and a group header's BASE pack a ring offset into 32 bits"
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
/// `[MIN_SAL_BYTES, SAL_MMAP_SIZE]` and logged when the clamp moves it. The
/// override exists because each server `fallocate`s its whole SAL at startup, so
/// dozens of test servers sharing a tmpfs would exhaust it. Called once, by
/// [`SalLog::open`].
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
        DeltaRead = 12,
    }
}

impl SalMessageKind {
    /// Whether a group of this kind is the last of its epoch.
    pub(crate) const fn ends_epoch(self) -> bool {
        matches!(self, SalMessageKind::Flush | SalMessageKind::FlushEph)
    }
}

/// `(epoch << 32) | low`: `low` = [`PRESENT`] for a prefix, the synced offset
/// for the anchor.
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

/// The prefix word at `offset` as an atomic, for the Acquire/Release pair that
/// publishes a group across processes.
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

/// One SAL group as a reader sees it: the header verified at its own offset,
/// and the payload bytes its directory describes.
pub(crate) struct SalMessage {
    /// The zone LSN of a zone member, `0` outside every zone.
    pub(crate) lsn: u64,
    pub(crate) kind: SalMessageKind,
    /// Whether this group is its zone's last member, the one that closes it.
    zone_end: bool,
    pub(crate) target_id: u64,
    /// The group's byte offset in the ring — what a recovery error names.
    pub(crate) base: u64,
    /// The offset past the group.
    end: u64,
    /// `0` = nothing answers.
    pub(crate) request_id: u32,
    /// Whether the group is a later member of its cut.
    in_request_order: bool,
    /// The workers the group addresses.
    pub(crate) targets: WorkerSet,
    /// Whether the group holds one payload for every addressed worker.
    shared: bool,
    width: u8,
    /// The group's payload bytes, from the header's end to the end of the group.
    payload: &'static [u8],
    /// The directory: one `DIR_ENTRY_BYTES` entry per payload.
    dir: &'static [u8],
}

impl SalMessage {
    /// Whether every payload is what the writer checksummed; only a zone member's are.
    fn intact(&self) -> bool {
        debug_assert!(self.lsn != 0, "only a zone member's payloads carry checksums");
        self.payloads()
            .zip(self.dir.as_chunks::<DIR_ENTRY_BYTES>().0)
            .all(|(bytes, entry)| gnitz_wire::checksum(bytes) == read_u64_le(entry, DIR_CHECKSUM_AT))
    }

    /// The worker count the group was written at.
    pub(crate) fn width(&self) -> u32 {
        self.width as u32
    }

    /// Each payload's bytes, in directory order.
    pub(crate) fn payloads(&self) -> impl Iterator<Item = &'static [u8]> + '_ {
        let payload = self.payload;
        payload_spans(dir_sizes(self.dir)).map(move |(_, off, sz)| &payload[off..off + sz])
    }

    /// Worker `w`'s bytes, `None` when the group does not address it.
    pub(crate) fn slot(&self, w: u32) -> Option<&'static [u8]> {
        let w = w as usize;
        (w < MAX_WORKERS && self.targets.contains(w))
            .then(|| self.payloads().nth(if self.shared { 0 } else { self.targets.rank(w) }))
            .flatten()
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
                self.base, self.lsn, self.target_id
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
    /// Nothing published here, the group's own stride runs past this mapping
    /// (the log was written under a larger `GNITZ_SAL_BYTES`), or a verified
    /// header stamped with another epoch — the ring's leftovers. Either way, the
    /// end of the log.
    Absent,
    /// A published header at this offset that fails its digest, or does not
    /// decode.
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
    /// outside the digest and a leftover whose copy flipped up would otherwise be
    /// consumed as current.
    Live(u32),
    /// Quiescent recovery walk. The prefix's epoch half is not consulted, so a
    /// flipped prefix epoch changes nothing; only the digested header's epoch
    /// decides.
    Walk(u32),
}

impl EpochGate {
    fn epoch(self) -> u32 {
        match self {
            EpochGate::Live(e) | EpochGate::Walk(e) => e,
        }
    }
}

/// The anchor's bytes: the [`epoch_word`] in force, that word's checksum, and a
/// spare word — the one in force before the last write began.
const ANCHOR_RECORD: usize = 24;

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

/// The stores of an anchor write from the word `in_force` to `word`, as
/// `(byte offset, value)` in the order they are made. A kill between any two
/// leaves a record [`SalLog::anchor_word`] resolves to one of the two words: the
/// spare takes the word in force before the front word is touched, and the
/// checksum moves to the new word last.
fn anchor_stores(in_force: u64, word: u64) -> [(usize, u64); 3] {
    [
        (16, in_force),
        (0, word),
        (8, gnitz_wire::checksum(&word.to_le_bytes())),
    ]
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
        File::open(data_dir)
            .and_then(|dir| dir.sync_all())
            .map_err(|e| format!("failed to fsync '{data_dir}': {e}"))?;
        Ok(log)
    }

    /// Map `len` bytes of `file` shared and writable. The file is extended to
    /// `len` first, since a store past its end raises `SIGBUS`, and its blocks
    /// are reserved, so a store cannot fail for want of disk space. A file
    /// already longer keeps its length and contents.
    fn map(file: &'static File, len: usize) -> io::Result<SalLog> {
        assert!(len > ANCHOR_BYTES, "the SAL holds a ring behind its anchor page");
        let fd = file.as_raw_fd();
        set_nocow(fd);
        posix_io::retry_eintr(|| unsafe { libc::fallocate(fd, 0, 0, len as libc::off_t) })?;
        // SAFETY: a fresh mapping of a file at least `len` bytes long.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(SalLog {
            // SAFETY: `len > ANCHOR_BYTES`.
            ring: unsafe { ptr.cast::<u8>().add(ANCHOR_BYTES) },
            ring_len: len - ANCHOR_BYTES,
            file,
        })
    }

    fn anchor_record(&self) -> *mut [u8; ANCHOR_RECORD] {
        self.ring.wrapping_sub(ANCHOR_BYTES).cast()
    }

    /// The anchor word in force: the front word if the checksum verifies it, else
    /// the spare. A zero word verifies against a zero checksum — the fresh file.
    fn anchor_word(&self) -> Option<u64> {
        // SAFETY: the anchor page is mapped ahead of the ring.
        let a = unsafe { &*self.anchor_record() };
        let sum = read_u64_le(a, 8);
        [0, 16].into_iter().map(|at| read_u64_le(a, at)).find(|&w| match w {
            0 => sum == 0,
            w => gnitz_wire::checksum(&w.to_le_bytes()) == sum,
        })
    }

    /// The walk epoch, and the ring offset a completed `fdatasync` covers.
    fn anchor(&self) -> Result<(u32, u64), String> {
        let word = self
            .anchor_word()
            .ok_or("SAL anchor fails its checksum: the head of wal.sal is damaged")?;
        Ok((word_epoch(word), word & u64::from(u32::MAX)))
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
        if let EpochGate::Live(exp) = gate {
            if word_epoch(word) != exp {
                return SalStep::Absent;
            }
        }

        let hdr_off = cursor as usize + PREFIX_BYTES;
        let m = self.ring_len;
        if hdr_off + HDR_PREFIX > m {
            return SalStep::Corrupt;
        }
        // SAFETY: the bound above admits the fixed prefix.
        let fixed: &'static [u8; HDR_PREFIX] = unsafe { &*(self.ring.add(hdr_off) as *const [u8; HDR_PREFIX]) };
        let flags = fixed[OFF_FLAGS];
        let targets = WorkerSet(read_u64_le(fixed, OFF_TARGETS));
        // At most `MAX_WORKERS` whatever the bytes say, so the header's length
        // needs no check ahead of the digest.
        let payloads = payload_count(flags, targets);
        let hdr_len = group_header_size(payloads);
        if hdr_off + hdr_len > m {
            return SalStep::Corrupt;
        }
        // SAFETY: bounded directly above.
        let hdr = unsafe { std::slice::from_raw_parts(self.ring.add(hdr_off), hdr_len) };
        if group_digest(hdr) != read_u64_le(hdr, OFF_DIGEST) || u64::from(read_u32_le(fixed, OFF_BASE)) != cursor {
            return SalStep::Corrupt;
        }
        let Some(kind) = SalMessageKind::from_wire(fixed[OFF_KIND]) else {
            return SalStep::Corrupt;
        };
        let lsn = read_u64_le(fixed, OFF_LSN);
        let width = fixed[OFF_WIDTH];
        if flags & !KNOWN_FLAGS != 0
            || (flags & FLAG_ZONE_END != 0 && lsn == 0)
            || width as usize > MAX_WORKERS
            || targets != targets.within(width as usize)
        {
            return SalStep::Corrupt;
        }
        if read_u32_le(fixed, OFF_EPOCH) != gate.epoch() {
            return SalStep::Absent;
        }

        let dir = &hdr[OFF_DIRECTORY..OFF_DIRECTORY + payloads * DIR_ENTRY_BYTES];
        let end = cursor as usize + group_total_size(payloads, dir_sizes(dir));
        if end > m {
            return SalStep::Absent;
        }

        // SAFETY: bounded directly above; the payloads run from the header's end
        // to the group's end.
        let payload_off = hdr_off + hdr_len;
        let payload = unsafe { std::slice::from_raw_parts(self.ring.add(payload_off), end - payload_off) };
        SalStep::Group(SalMessage {
            lsn,
            kind,
            zone_end: flags & FLAG_ZONE_END != 0,
            target_id: read_u64_le(fixed, OFF_TARGET_ID),
            request_id: read_u32_le(fixed, OFF_REQUEST_ID),
            in_request_order: flags & FLAG_IN_REQUEST_ORDER != 0,
            targets,
            shared: flags & FLAG_SHARED != 0,
            width,
            base: cursor,
            end: end as u64,
            payload,
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

/// The cursor and zone state a group laid out in a scope can be rolled back to.
/// Taken before a group is laid out and consumed only by
/// [`SalScope::roll_back`].
#[derive(Clone, Copy)]
pub(crate) struct Savepoint {
    cursor: u64,
    last_member: Option<u64>,
}

/// A group's header scalars. The width, base, epoch and digest are the writer's.
struct GroupHead {
    lsn: u64,
    kind: SalMessageKind,
    flags: u8,
    request_id: u32,
    target_id: u64,
    targets: WorkerSet,
}

pub(crate) struct SalWriter {
    log: SalLog,
    write_cursor: Cell<u64>,
    epoch: Cell<u32>,
    /// The anchored synced offset of the live epoch.
    synced: Cell<u64>,
    /// The LSN of the latest scope that published anything; see
    /// [`Self::watermark`].
    watermark: Cell<u64>,
    checkpoint_threshold: u64,
    /// A group was refused since the last reset that an empty log would admit,
    /// so a checkpoint is warranted whatever the write cursor says. Cleared by
    /// [`Self::rewind`].
    refused_transient: Cell<bool>,
    /// Taken by every [`SalExcl`].
    excl: AsyncRwLock,
    /// Every worker's park; its length is the worker count groups are written at.
    parks: WorkerParks,
    /// The workers groups written since the last [`SalExcl::wake`] reached.
    reached: Cell<WorkerSet>,
}

impl SalWriter {
    /// Starts at epoch 0, which [`Self::write_slots`] refuses: nothing can be
    /// written before the boot [`SalExcl::boot_rewind`] sets the live epoch.
    pub(crate) fn new(log: SalLog, parks: WorkerParks) -> Self {
        assert!(
            parks.len() <= MAX_WORKERS,
            "a SAL group cannot address more than MAX_WORKERS workers"
        );
        assert!(
            log.ring_len > PREFIX_BYTES + CHECKPOINT_RESERVE,
            "the ring must hold the checkpoint band"
        );
        let checkpoint_threshold =
            gnitz_foundation::env::env_num("GNITZ_CHECKPOINT_BYTES", (log.ring_len as u64 * 3) >> 2);
        SalWriter {
            log,
            write_cursor: Cell::new(0),
            epoch: Cell::new(0),
            synced: Cell::new(0),
            watermark: Cell::new(0),
            checkpoint_threshold,
            refused_transient: Cell::new(false),
            excl: AsyncRwLock::default(),
            parks,
            reached: Cell::new(WorkerSet::EMPTY),
        }
    }

    /// Sole write access, once no other task holds it.
    pub(crate) async fn lock(&self) -> SalExcl<'_> {
        SalExcl {
            writer: self,
            _guard: self.excl.write().await,
        }
    }

    /// The worker count every group is written at.
    pub(crate) fn num_workers(&self) -> usize {
        self.parks.len()
    }

    /// Lay one group out at the write cursor, `fill(index, bytes)` per payload,
    /// and return its base — the group is complete on the log but for its
    /// digest, and unpublished; [`Self::publish`] does both. A group that does
    /// not fit is refused with [`WireStatus::SalFull`] and leaves the log
    /// untouched; the refusal reaches clients verbatim, so the cursor goes to the
    /// operator log instead. A zone member (`lsn != 0`) checksums every payload.
    fn write_slots(
        &self,
        head: GroupHead,
        sizes: &[usize],
        mut fill: impl FnMut(usize, &mut [u8]),
    ) -> Result<u64, WireFault> {
        let epoch = self.epoch.get();
        assert!(
            epoch >= 1,
            "SAL group epoch must be >= 1 — epoch 0 is indistinguishable from the empty prefix"
        );
        let width = self.parks.len();
        debug_assert_eq!(sizes.len(), payload_count(head.flags, head.targets));
        debug_assert_eq!(
            head.targets,
            head.targets.within(width),
            "a group addresses launched workers"
        );

        let hdr_size = group_header_size(sizes.len());
        let total = group_total_size(sizes.len(), sizes.iter().copied());
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
        // every one is written, the pad bytes included.
        hdr[OFF_KIND..OFF_KIND + 4].copy_from_slice(&[head.kind.as_wire(), head.flags, width as u8, 0]);
        write_u32_le(hdr, OFF_REQUEST_ID, head.request_id);
        write_u32_le(hdr, OFF_BASE, base as u32);
        write_u32_le(hdr, OFF_EPOCH, epoch);
        write_u64_le(hdr, OFF_LSN, head.lsn);
        write_u64_le(hdr, OFF_TARGET_ID, head.target_id);
        write_u64_le(hdr, OFF_TARGETS, head.targets.0);
        // The group fits a ring of at most `SAL_MMAP_SIZE` bytes, so no size
        // is past `u32`.
        for (i, &sz) in sizes.iter().enumerate() {
            let entry = OFF_DIRECTORY + i * DIR_ENTRY_BYTES;
            write_u32_le(hdr, entry, sz as u32);
            write_u64_le(hdr, entry + DIR_CHECKSUM_AT, 0);
        }
        hdr[OFF_DIRECTORY + sizes.len() * DIR_ENTRY_BYTES..].fill(0);

        for (i, off, sz) in payload_spans(sizes.iter().copied()) {
            // SAFETY: `off + sz <= total - PREFIX_BYTES - hdr_size`, inside the mapped
            // span, and the spans are disjoint.
            let slot = unsafe { std::slice::from_raw_parts_mut(self.log.ring.add(hdr_off + hdr_size + off), sz) };
            fill(i, &mut *slot);
            if head.lsn != 0 {
                let at = OFF_DIRECTORY + i * DIR_ENTRY_BYTES + DIR_CHECKSUM_AT;
                write_u64_le(hdr, at, gnitz_wire::checksum(slot));
            }
        }

        let end = base + total as u64;
        unsafe { write_end_prefix(self.log.ring, end as usize) };
        self.write_cursor.set(end);
        Ok(base)
    }

    /// Stamp the digest of the group laid out at `base` and store its prefix: the
    /// group is readable from here on. Returns its end.
    fn publish(&self, base: u64) -> u64 {
        let at = base as usize + PREFIX_BYTES;
        // SAFETY: laid out by this writer, so mapped; its prefix word is still
        // zero, so no reader has reached it.
        let (flags, targets) = unsafe {
            let fixed = &*(self.log.ring.add(at) as *const [u8; HDR_PREFIX]);
            (fixed[OFF_FLAGS], WorkerSet(read_u64_le(fixed, OFF_TARGETS)))
        };
        let n = payload_count(flags, targets);
        // SAFETY: as above.
        let hdr = unsafe { std::slice::from_raw_parts_mut(self.log.ring.add(at), group_header_size(n)) };
        stamp_digest(hdr);
        let dir = &hdr[OFF_DIRECTORY..OFF_DIRECTORY + n * DIR_ENTRY_BYTES];
        let end = base + group_total_size(n, dir_sizes(dir)) as u64;
        let word = epoch_word(self.epoch.get(), PRESENT);
        // SAFETY: as above.
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
        unsafe { *self.log.ring.add(base as usize + PREFIX_BYTES + OFF_FLAGS) |= FLAG_ZONE_END };
    }

    /// Lay `g` out at the write cursor, unpublished, stamped with `lsn`. The one
    /// encode path.
    fn lay_out(&self, g: &DirectGroup, lsn: u64) -> Result<u64, WireFault> {
        let nw = self.parks.len();
        let per_worker = match g.data {
            GroupData::Same(_) => nw,
            GroupData::Each(each) => each.len(),
        };
        assert_eq!(per_worker, nw, "per-worker payloads={per_worker} != num_workers={nw}");
        debug_assert!(
            g.schema.is_some() || g.data.is_dataless(),
            "data without a schema — `decode_sal_frame` rejects a data block without a schema block",
        );

        let set = g.targets.set.within(nw);
        let shared = g.shared();
        let template = g.request.template();
        debug_assert!(
            matches!(g.data, GroupData::Same(_)) || g.data.holders().union(set) == set,
            "a group addresses every worker it has rows for"
        );
        let mut sizes = [0usize; MAX_WORKERS];
        let n = if shared {
            sizes[0] = g.msg(&template, 0).size();
            1
        } else {
            for (i, w) in set.iter().enumerate() {
                sizes[i] = g.msg(&template, w).size();
            }
            set.len()
        };
        let head = GroupHead {
            lsn,
            kind: g.request.kind(),
            flags: if shared { FLAG_SHARED } else { 0 }
                | if g.targets.in_request_order {
                    FLAG_IN_REQUEST_ORDER
                } else {
                    0
                },
            request_id: g.targets.request_id,
            target_id: template.target_id,
            targets: set,
        };
        let mut workers = set.iter();
        let base = self.write_slots(head, &sizes[..n], |_, slot| {
            let w = if shared {
                0
            } else {
                workers.next().expect("one payload per addressed worker")
            };
            g.msg(&template, w).encode(slot)
        })?;
        self.reached.set(self.reached.get().union(set));
        Ok(base)
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
        self.synced.set(0);
        self.refused_transient.set(false);
        // Emptied only once the new epoch is durable: a kill before that must
        // leave the old epoch's tail whole.
        self.write_anchor(epoch, 0);
        if let Err(e) = self.log.file.sync_data() {
            gnitz_fatal_abort!("SAL fdatasync (rewind to epoch {epoch}) failed: {e}");
        }
        unsafe { write_end_prefix(self.log.ring, 0) };
    }

    /// Anchor `synced` in `epoch`.
    fn write_anchor(&self, epoch: u32, synced: u64) {
        let in_force = self.log.anchor_word().expect("the anchor verified at boot");
        let record = self.log.anchor_record().cast::<u8>();
        for (at, value) in anchor_stores(in_force, epoch_word(epoch, synced)) {
            // SAFETY: the anchor page is mapped ahead of the ring, and only this
            // writer writes it. Volatile, so the stores reach the mapping in
            // this order.
            unsafe { record.add(at).cast::<[u8; 8]>().write_volatile(value.to_le_bytes()) };
        }
    }

    /// Raise the anchored offset to `through`, unless the log was rewound out of
    /// `epoch` since.
    fn mark_synced(&self, epoch: u32, through: u64) {
        if self.epoch.get() == epoch && through > self.synced.get() {
            self.synced.set(through);
            self.write_anchor(epoch, through);
        }
    }

    pub(crate) fn epoch(&self) -> u32 {
        self.epoch.get()
    }

    /// The zone LSN a scope opened now would take: its position on the log.
    /// The epoch is the high word, so LSNs follow SAL order across checkpoints
    /// and restarts. Two scopes that published anything never share one.
    fn next_zone_lsn(&self) -> u64 {
        epoch_word(self.epoch.get(), self.write_cursor.get())
    }

    /// Every scope that published anything took an LSN at or below this; every
    /// scope opened later takes one above it. Moves only when a scope commits.
    pub(crate) fn watermark(&self) -> u64 {
        self.watermark.get()
    }
}

/// Sole write access. Dropping it wakes each worker a group written under it
/// reached. Held across no await except a checkpoint round's ACKs.
pub(crate) struct SalExcl<'a> {
    writer: &'a SalWriter,
    _guard: WriteGuard,
}

impl<'a> SalExcl<'a> {
    /// Write and publish one group, outside every zone.
    pub(crate) fn write(&self, g: &DirectGroup) -> Result<(), WireFault> {
        self.writer.publish(self.writer.lay_out(g, 0)?);
        Ok(())
    }

    /// Open a publication scope at the log's next zone LSN: groups laid out in
    /// it are invisible until [`SalScope::commit`], and an uncommitted drop
    /// discards the span. `tag` names the scope for [`ZONE_PANIC`]. `&mut`, so
    /// no scope overlaps another, a rewind or a [`Self::sync`].
    pub(crate) fn begin(&mut self, tag: &'static str) -> SalScope<'_> {
        let w = self.writer;
        SalScope {
            writer: w,
            from: w.write_cursor.get(),
            lsn: w.next_zone_lsn(),
            tag,
            last_member: Cell::new(None),
            committed: false,
        }
    }

    /// `fdatasync` the log, fatal on failure; once done, anchor the cursor at
    /// submission as synced. No scope is open, so every byte below it is published.
    pub(crate) fn sync(&self, reactor: &Reactor, op: &'static str) -> impl Future<Output = ()> + 'a {
        let w = self.writer;
        let (epoch, through) = (w.epoch.get(), w.write_cursor.get());
        let done = reactor.fsync(w.log.file.as_fd());
        async move {
            let rc = done.await;
            if rc < 0 {
                gnitz_fatal_abort!("SAL fdatasync ({op}) failed rc={rc}");
            }
            w.mark_synced(epoch, through);
        }
    }

    /// Rewind into the next epoch — the checkpoint's reclaim.
    pub(crate) fn checkpoint_reset(&mut self) {
        self.writer.rewind(next_epoch(self.writer.epoch.get()));
    }

    /// The boot rewind, to the live epoch the recovered tail names. Seeds the
    /// watermark below every LSN this boot, above every earlier boot's.
    pub(crate) fn boot_rewind(&mut self, live_epoch: u32) {
        let w = self.writer;
        w.rewind(live_epoch);
        w.watermark.set(w.next_zone_lsn() - 1);
    }

    /// Wake every worker a group written since the last wake reached.
    pub(crate) fn wake(&self) {
        let w = self.writer;
        for worker in w.reached.replace(WorkerSet::EMPTY).iter() {
            w.parks.wake(worker);
        }
    }
}

impl Drop for SalExcl<'_> {
    fn drop(&mut self) {
        self.wake()
    }
}

/// One publication span on the SAL, holding at most one zone: its `zoned`
/// groups. Nothing is visible until [`Self::commit`].
pub(crate) struct SalScope<'a> {
    writer: &'a SalWriter,
    /// Where the scope opened — the start of its publish range, and where an
    /// uncommitted drop puts the write cursor back.
    from: u64,
    /// The zone LSN every zoned group in the scope is stamped with.
    lsn: u64,
    /// Which scope this is, for [`ZONE_PANIC`]: `"ddl"` or `"commit"`.
    tag: &'static str,
    /// The base of the latest zoned group; `Some` iff a zone is open.
    last_member: Cell<Option<u64>>,
    committed: bool,
}

impl SalScope<'_> {
    /// The LSN this scope's zone members carry.
    pub(crate) fn lsn(&self) -> u64 {
        self.lsn
    }

    /// Lay `g` out inside the scope, unpublished. `zoned` puts it in the
    /// scope's atomic zone at the scope's LSN; otherwise it is written at LSN 0.
    pub(crate) fn write(&self, g: &DirectGroup, zoned: bool) -> Result<(), WireFault> {
        let base = self.writer.lay_out(g, if zoned { self.lsn } else { 0 })?;
        if zoned {
            self.last_member.set(Some(base));
        }
        Ok(())
    }

    /// The point a group laid out in this scope can be rolled back to, taken
    /// before laying it out.
    pub(crate) fn savepoint(&self) -> Savepoint {
        Savepoint {
            cursor: self.writer.write_cursor.get(),
            last_member: self.last_member.get(),
        }
    }

    /// Take back every group laid out since `sp`: nothing was published, so
    /// restoring the cursor and the zone's last member is the whole undo.
    pub(crate) fn roll_back(&self, sp: Savepoint) {
        self.writer.write_cursor.set(sp.cursor);
        self.last_member.set(sp.last_member);
    }

    /// Publish the span in layout order. Answers whether a zone closed.
    pub(crate) fn commit(mut self) -> bool {
        self.committed = true;
        let end = self.writer.write_cursor.get();
        // An empty scope's LSN is the next scope's too, so it must not count.
        if end > self.from {
            self.writer.watermark.set(self.lsn);
        }
        let Some(last) = self.last_member.get() else {
            self.writer.publish_range(self.from, end);
            return false;
        };
        self.writer.mark_zone_end(last);
        self.writer.publish_range(self.from, last);
        if ZONE_PANIC.at(self.tag) {
            // SAFETY: `libc::abort` is the whole reason this block is unsafe; it
            // takes no argument and cannot violate an invariant.
            unsafe { libc::abort() };
        }
        self.writer.publish_range(last, end);
        true
    }
}

impl Drop for SalScope<'_> {
    /// An uncommitted scope publishes nothing: the write cursor goes back to
    /// where it opened and the bytes stay for the next group to overwrite.
    fn drop(&mut self) {
        if !self.committed {
            self.writer.write_cursor.set(self.from);
        }
    }
}

// ---------------------------------------------------------------------------
// SalReader — one worker's live drain over a SalLog
// ---------------------------------------------------------------------------

/// Where the reply to a group goes.
#[derive(Clone, Copy)]
pub(crate) struct ReplyRoute {
    pub(crate) target_id: u64,
    /// `0`: nothing answers.
    pub(crate) request_id: u32,
    /// The cut the group was written in: how many groups not flagged in request
    /// order the drain had passed. A cut's groups are consecutive and only its
    /// first is unflagged, so every member reads one count and no two cuts share one.
    pub(crate) cut: u64,
}

/// One group addressed to a worker, decoded and owned: a parked read outlives the
/// SAL bytes it came in, which an inline `Flush` lets the master rewrite.
pub(crate) struct Inbound {
    pub(crate) route: ReplyRoute,
    pub(crate) what: SalRequest<'static>,
    /// Boxed, so a request moves from its decode to where it is served or parked
    /// as a few words: held inline, the batch would be copied whole at each of
    /// those moves, for a request that carries none as well.
    pub(crate) rows: Option<Box<Batch>>,
}

pub(crate) struct SalReader {
    log: SalLog,
    worker_id: u32,
    /// The live drain's position and epoch — the read-side mirror of
    /// `SalWriter`'s `write_cursor` / `epoch`, so no caller does SAL address
    /// arithmetic.
    read_cursor: Cell<u64>,
    expected_epoch: Cell<u32>,
    /// The cut of the group [`Self::next`] last returned.
    cut: Cell<u64>,
}

impl SalReader {
    /// A live drain from cursor 0 of `live_epoch`.
    pub(crate) fn new(log: SalLog, worker_id: u32, live_epoch: u32) -> Self {
        SalReader {
            log,
            worker_id,
            read_cursor: Cell::new(0),
            expected_epoch: Cell::new(live_epoch),
            cut: Cell::new(0),
        }
    }

    /// The next group addressed to this worker, with this worker's bytes; groups
    /// addressed to others are stepped over. Stepping past a group that ends its
    /// epoch moves the drain to the start of the next one; a group from another
    /// epoch parks.
    /// A digest failure under a passing epoch gate can only be corruption, and
    /// parking on it would read as end-of-drain, so it fail-stops.
    fn next(&self) -> Option<(SalMessage, &'static [u8])> {
        loop {
            let cursor = self.read_cursor.get();
            match self.log.read_at(cursor, EpochGate::Live(self.expected_epoch.get())) {
                SalStep::Group(msg) => {
                    if msg.kind.ends_epoch() {
                        self.read_cursor.set(0);
                        self.expected_epoch.set(next_epoch(self.expected_epoch.get()));
                    } else {
                        self.read_cursor.set(msg.end);
                    }
                    // Counted whether or not the group addresses this worker, so
                    // every worker numbers a cut alike.
                    if !msg.in_request_order {
                        self.cut.set(self.cut.get() + 1);
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

    /// The cut of the group [`Self::next`] last returned; see [`ReplyRoute::cut`].
    fn cut(&self) -> u64 {
        self.cut.get()
    }

    /// The next group addressed to this worker. The master authored it, so bytes
    /// that do not decode fail-stop, as a corrupt header does in [`Self::next`].
    /// `catalog_schema` answers what a relation's catalog record decodes to.
    #[inline]
    pub(crate) fn next_request(
        &self,
        catalog_schema: impl FnOnce(u64, &[u8]) -> Option<SchemaDescriptor>,
    ) -> Option<Inbound> {
        let (msg, wire) = self.next()?;
        // The kinds [`DirectGroup::push`] and [`DirectGroup::ddl_sync`] frame with
        // their target's catalog record.
        let catalog_record = matches!(msg.kind, SalMessageKind::Push | SalMessageKind::DdlSync);
        let known = |tid, record: &[u8]| catalog_record.then(|| catalog_schema(tid, record)).flatten();
        // Fail-stop: a dropped group diverges this worker from the master.
        let undecodable =
            |e: String| -> ! { gnitz_fatal_abort!("failed to decode {:?} for tid={}: {e}", msg.kind, msg.target_id) };
        // Read in place: the rows move once, out of the decoded frame.
        let mut decoded = decode_sal_frame(wire, known);
        let (control, rows) = match &mut decoded {
            Ok(frame) => frame,
            Err(e) => undecodable(std::mem::take(e)),
        };
        let what = match SalRequest::decode(msg.kind, &control.hdr, &wire[control.blob.clone()]) {
            Ok(what) => what,
            Err(e) => undecodable(e),
        };
        Some(Inbound {
            route: ReplyRoute {
                target_id: control.hdr.target_id,
                request_id: msg.request_id,
                cut: self.cut(),
            },
            what,
            rows: rows.take().map(Box::new),
        })
    }

    /// No group is readable or corrupt at the cursor.
    pub(crate) fn is_empty(&self) -> bool {
        matches!(
            self.log
                .read_at(self.read_cursor.get(), EpochGate::Live(self.expected_epoch.get())),
            SalStep::Absent
        )
    }
}

/// The SAL fixture.
#[cfg(test)]
#[path = "tests/fixtures.rs"]
pub(crate) mod fixtures;

#[cfg(test)]
#[path = "tests/sal.rs"]
mod tests;
