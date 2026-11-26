//! The bytes of the log: the publication prefix, anchor and format words, the
//! kind ordinals, the group header with its directory, and the payloads behind
//! it.
//! Every encode and decode of a group's bytes is here; the header's field
//! offsets and flag bits are named nowhere else.

use std::sync::atomic::AtomicU64;

use crate::runtime::park::WorkerSet;
use crate::runtime::w2m::RequestId;
use gnitz_wire::{MAX_WORKERS, WIRE_VERSION, read_u32_le, read_u64_le, write_u32_le, write_u64_le};
use gnitz_zset::repr::{Batch, MemBatch, WireRows};
use gnitz_zset::schema::{SchemaDescriptor, decode_schema_record};

/// The publication prefix in front of every group header, one atomically stored
/// u64 ([`epoch_word`]); also the alignment of every group base.
pub(super) const PREFIX_BYTES: usize = std::mem::size_of::<AtomicU64>();

/// `(epoch << 32) | low`.
#[inline]
pub(super) const fn epoch_word(epoch: u32, low: u64) -> u64 {
    (epoch as u64) << 32 | low
}

/// The low half of every publication prefix word: a presence marker, so zeroing a
/// published word takes at least 33 bit flips.
pub(super) const PRESENT: u64 = u32::MAX as u64;

/// The epoch half of an [`epoch_word`].
#[inline]
pub(super) const fn word_epoch(word: u64) -> u32 {
    (word >> 32) as u32
}

/// The anchor of `epoch`: its complement in the low half, so no flipped bit
/// verifies.
pub(super) const fn anchor_word(epoch: u32) -> u64 {
    epoch_word(epoch, !epoch as u64)
}

/// The epoch of the log's own layout: the group header, its directory and the
/// payload codec. The header's byte-pin test fails until it moves with one.
const SAL_LAYOUT: u32 = 2;

/// The format word beside the anchor: the log's layout, and the
/// [`WIRE_VERSION`] of the shape records and data blocks its payloads hold.
/// Never 0, which is a fresh file's.
pub(crate) const SAL_FORMAT: u64 = (SAL_LAYOUT as u64) << 32 | WIRE_VERSION as u64;

/// The epoch after `epoch`: what a checkpoint's base round moves the writer and
/// every reader to, and what a boot starts at above the recovered tail's epoch.
pub(super) const fn next_epoch(epoch: u32) -> u32 {
    epoch + 1
}

gnitz_wire::wire_enum! {
    /// What a SAL group asks a worker to do: the
    /// [`SalRequest`](super::request::SalRequest) variant its payloads decode to. Stored as one ordinal byte in the group header;
    /// `ALL`/`from_wire` come from the one variant list, so a decode cannot fall
    /// behind the enum.
    pub(super) enum SalMessageKind: u8 {
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
        HasPkIndex = 19,
    }
}

/// The scalars of a request, which its group's header carries: all of the
/// request but its blob. [`Read::encode`](super::request::Read::encode),
/// [`Apply::encode`](super::request::Apply::encode) and
/// [`Ingest::encode`](super::request::Ingest::encode) say what each kind
/// stores in the two arguments.
///
/// Laid out in declaration order: in the order the compiler otherwise picks,
/// reading a verified header costs the live drain more instructions per group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub(super) struct RequestHead {
    pub(super) kind: SalMessageKind,
    /// The relation the request names, or 0.
    pub(super) target_id: u64,
    pub(super) arg0: u64,
    pub(super) arg1: u64,
}

impl RequestHead {
    pub(super) const fn new(kind: SalMessageKind, target_id: u64, arg0: u64, arg1: u64) -> Self {
        RequestHead { kind, target_id, arg0, arg1 }
    }
}

// Group header field offsets, relative to the start of the header. The byte
// behind WIDTH is zero.

/// XXH3-64 of every header byte behind it.
const OFF_DIGEST: usize = 0;
/// The kind's ordinal (`SalMessageKind::as_wire`), one byte.
const OFF_KIND: usize = 8;
/// `FLAG_*` bits, one byte. A bit outside them is corruption.
const OFF_FLAGS: usize = 9;
/// Set on a zone's last member, only together with [`FLAG_ZONE`]: the zone is
/// closed.
const FLAG_ZONE_END: u8 = 1;
/// The group holds one payload, which every addressed worker reads.
const FLAG_SHARED: u8 = 4;
/// The group is a member of its scope's zone.
const FLAG_ZONE: u8 = 8;
const KNOWN_FLAGS: u8 = FLAG_ZONE_END | FLAG_SHARED | FLAG_ZONE;
/// The worker count the group was written at, one byte.
const OFF_WIDTH: usize = 10;
/// The group's [`RequestId`], 0 for none.
const OFF_REQUEST_ID: usize = 12;
/// The group's own byte offset in the ring.
const OFF_BASE: usize = 16;
const OFF_EPOCH: usize = 20;
const OFF_TARGET_ID: usize = 24;
/// The request's two arguments.
const OFF_ARG0: usize = 32;
const OFF_ARG1: usize = 40;
/// The [`WorkerSet`] the group addresses, within its width.
const OFF_TARGETS: usize = 48;
/// XXH3-64 of the group's payload bytes, header end to group end, on a zone
/// member; `0` on every other group.
const OFF_PAYLOAD_SUM: usize = 56;
/// One `u64` size per payload.
const OFF_DIRECTORY: usize = 64;
const DIR_ENTRY_BYTES: usize = 8;

/// The header's fixed prefix — every scalar field, ending where the directory
/// begins. Read as a `&[u8; HDR_PREFIX]` so the field reads are provably in
/// bounds at const offsets.
pub(super) const HDR_PREFIX: usize = OFF_DIRECTORY;

/// How many payloads a group holds: one every addressed worker reads, or one
/// per addressed worker in ascending worker order.
const fn payload_count(flags: u8, targets: WorkerSet) -> usize {
    if flags & FLAG_SHARED != 0 { 1 } else { targets.len() }
}

/// Header + directory bytes for a group of `payloads` payloads.
#[inline]
pub(super) const fn group_header_size(payloads: usize) -> usize {
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
pub(super) fn header_len(fixed: &[u8; HDR_PREFIX]) -> usize {
    group_header_size(payload_count(
        fixed[OFF_FLAGS],
        WorkerSet(read_u64_le(fixed, OFF_TARGETS)),
    ))
}

/// Each payload as `(payload-relative offset, size)`, in directory order.
/// The offset is a pure function of the sizes before it — a payload starts where
/// the 8-byte-padded ones ahead of it end — so the directory stores sizes alone
/// and an inconsistent offset is unrepresentable.
pub(super) fn payload_spans(sizes: impl Iterator<Item = usize>) -> impl Iterator<Item = (usize, usize)> {
    let mut off = 0;
    sizes.map(move |sz| {
        let at = off;
        off += sz.next_multiple_of(8);
        (at, sz)
    })
}

/// The SAL bytes a group occupies: prefix, a `hdr_len`-byte header and
/// directory, and 8-byte-padded payloads — what it advances the log by,
/// and where [`payload_spans`] leaves off.
pub(super) fn group_total_size(hdr_len: usize, sizes: impl Iterator<Item = usize>) -> usize {
    PREFIX_BYTES + hdr_len + sizes.map(|sz| sz.next_multiple_of(8)).sum::<usize>()
}

/// The payload sizes the directory of the header `hdr` holds, in directory
/// order.
pub(super) fn payload_sizes(hdr: &[u8]) -> impl Iterator<Item = usize> {
    hdr[OFF_DIRECTORY..]
        .as_chunks::<DIR_ENTRY_BYTES>()
        .0
        .iter()
        .map(|c| read_u64_le(c, 0) as usize)
}

/// XXH3-64 over a group header behind its digest field. The header names its own
/// ring offset and its epoch, so it verifies only where and when it was published.
fn group_digest(hdr: &[u8]) -> u64 {
    gnitz_wire::checksum(&hdr[OFF_DIGEST + 8..])
}

/// What a group's author says of it: every header scalar but the ones stamped
/// where it is laid out, which [`Header`] adds.
#[derive(Clone, Copy)]
pub(super) struct GroupHead {
    /// The request the group carries, but for its blob.
    pub(super) request: RequestHead,
    /// `FLAG_*` bits.
    flags: u8,
    pub(super) reply: Option<RequestId>,
    /// The workers the group addresses.
    pub(super) targets: WorkerSet,
}

impl GroupHead {
    /// The head of an open group carrying `request` to `targets`, answered on
    /// `reply`: a member of its scope's zone when `zoned`, holding one payload
    /// for all of `targets` when `shared`.
    pub(super) fn new(
        request: RequestHead,
        zoned: bool,
        shared: bool,
        reply: Option<RequestId>,
        targets: WorkerSet,
    ) -> Self {
        GroupHead {
            request,
            flags: if zoned { FLAG_ZONE } else { 0 } | if shared { FLAG_SHARED } else { 0 },
            reply,
            targets,
        }
    }

    /// Whether the group is a member of its scope's zone.
    pub(super) fn zoned(&self) -> bool {
        self.flags & FLAG_ZONE != 0
    }

    /// Whether this group is its zone's last member, the one that closes it.
    pub(super) fn zone_end(&self) -> bool {
        self.flags & FLAG_ZONE_END != 0
    }

    /// Whether the group holds one payload for every addressed worker.
    pub(super) fn shared(&self) -> bool {
        self.flags & FLAG_SHARED != 0
    }

    /// How many payloads the group holds.
    pub(super) fn payload_count(&self) -> usize {
        payload_count(self.flags, self.targets)
    }
}

/// A group header's scalars: all of it but the digest and the directory. The
/// one codec of the header's bytes.
#[derive(Clone, Copy)]
pub(super) struct Header {
    pub(super) head: GroupHead,
    /// The group's own byte offset in the ring.
    pub(super) base: u32,
    pub(super) epoch: u32,
    /// The worker count the group was written at.
    pub(super) width: u8,
    /// XXH3-64 of a zone member's payload bytes, `0` on every other group.
    pub(super) payload_sum: u64,
}

impl Header {
    /// Write every scalar into the fixed prefix of `hdr`: every byte of it
    /// behind the digest, so none keeps what a group laid out there earlier
    /// left. The directory behind the prefix is [`Self::set_payload_size`]'s.
    ///
    /// Always inlined: the writer encodes through it once per group, and as a
    /// call it takes the header through memory.
    #[inline(always)]
    pub(super) fn encode(&self, hdr: &mut [u8]) {
        let fixed = hdr
            .first_chunk_mut::<HDR_PREFIX>()
            .expect("a header holds its fixed prefix");
        let head = &self.head;
        let request = &head.request;
        fixed[OFF_KIND..OFF_KIND + 4].copy_from_slice(&[request.kind.as_wire(), head.flags, self.width, 0]);
        write_u32_le(fixed, OFF_REQUEST_ID, RequestId::to_wire(head.reply));
        write_u32_le(fixed, OFF_BASE, self.base);
        write_u32_le(fixed, OFF_EPOCH, self.epoch);
        write_u64_le(fixed, OFF_TARGET_ID, request.target_id);
        write_u64_le(fixed, OFF_ARG0, request.arg0);
        write_u64_le(fixed, OFF_ARG1, request.arg1);
        write_u64_le(fixed, OFF_TARGETS, head.targets.0);
        write_u64_le(fixed, OFF_PAYLOAD_SUM, self.payload_sum);
    }

    /// Write the directory entry of payload `i` of the header `hdr`: its size.
    /// A header's writer calls it once per payload, so every directory byte is
    /// written too.
    #[inline(always)]
    pub(super) fn set_payload_size(hdr: &mut [u8], i: usize, size: usize) {
        write_u64_le(hdr, OFF_DIRECTORY + i * DIR_ENTRY_BYTES, size as u64);
    }

    /// Mark the encoded, unsealed header `hdr` as its zone's last member.
    pub(super) fn set_zone_end(hdr: &mut [u8]) {
        debug_assert!(hdr[OFF_FLAGS] & FLAG_ZONE != 0, "only a zone member closes a zone");
        hdr[OFF_FLAGS] |= FLAG_ZONE_END;
    }

    /// Stamp the digest of the encoded header `hdr`: it verifies from here on.
    pub(super) fn seal(hdr: &mut [u8]) {
        let digest = group_digest(hdr);
        write_u64_le(hdr, OFF_DIGEST, digest);
    }

    /// The scalars of the sealed header `hdr`, read at ring offset `at`; `None`
    /// when its digest fails, when it was published at another offset, or when
    /// its scalars are nothing a writer produces.
    #[inline]
    pub(super) fn verify(hdr: &[u8], at: u64) -> Option<Header> {
        let fixed = hdr.first_chunk::<HDR_PREFIX>()?;
        if group_digest(hdr) != read_u64_le(fixed, OFF_DIGEST) || u64::from(read_u32_le(fixed, OFF_BASE)) != at {
            return None;
        }
        let head = GroupHead {
            request: RequestHead {
                kind: SalMessageKind::from_wire(fixed[OFF_KIND])?,
                target_id: read_u64_le(fixed, OFF_TARGET_ID),
                arg0: read_u64_le(fixed, OFF_ARG0),
                arg1: read_u64_le(fixed, OFF_ARG1),
            },
            flags: fixed[OFF_FLAGS],
            reply: RequestId::from_wire(read_u32_le(fixed, OFF_REQUEST_ID)),
            targets: WorkerSet(read_u64_le(fixed, OFF_TARGETS)),
        };
        let width = fixed[OFF_WIDTH];
        (head.flags & !KNOWN_FLAGS == 0
            && (head.zoned() || !head.zone_end())
            && width as usize <= MAX_WORKERS
            && head.targets == head.targets.within(width as usize)
            && (head.request.kind != SalMessageKind::DdlSync || head.shared()))
        .then_some(Header {
            head,
            base: at as u32,
            epoch: read_u32_le(fixed, OFF_EPOCH),
            width,
            payload_sum: read_u64_le(fixed, OFF_PAYLOAD_SUM),
        })
    }
}

// A payload is `[blob_len u64][blob][data block?]`: the blob of the group's
// request, or the worker's own in its place, then the rows the worker is sent
// as a WAL block. A payload carries rows iff bytes follow its blob, and its
// blob is then the shape record that lays those rows out.

/// The blob length in front of every payload; a `u64`, so a blob starts and
/// ends 8-aligned within its payload.
pub(super) const PAYLOAD_HEAD: usize = 8;

/// The size of the payload [`payload_encode`] writes.
#[inline]
pub(super) fn payload_size(blob: &[u8], rows: Option<WireRows<'_>>) -> usize {
    PAYLOAD_HEAD + blob.len() + rows.map_or(0, |d| d.byte_size())
}

/// Write the payload of `blob` and `rows` into `out`, which is exactly
/// [`payload_size`] bytes, every one of which is written. A payload with rows
/// has their shape record as its blob.
///
/// Always inlined: the writer encodes through it once per payload, and left to
/// the inliner's judgement it becomes a call there as its other callers grow.
#[inline(always)]
pub(super) fn payload_encode(out: &mut [u8], blob: &[u8], rows: Option<WireRows<'_>>) {
    debug_assert!(
        rows.is_none() || !blob.is_empty(),
        "rows need their shape record as the blob"
    );
    write_u64_le(out, 0, blob.len() as u64);
    let mut pos = PAYLOAD_HEAD;
    out[pos..pos + blob.len()].copy_from_slice(blob);
    pos += blob.len();
    if let Some(d) = rows {
        pos += d.encode(&mut out[pos..]);
    }
    assert_eq!(pos, out.len(), "payload_size and payload_encode disagree");
}

/// A payload read back: its blob, and the data block of its rows.
type ParsedPayload<'a> = (&'a [u8], Option<&'a [u8]>);

/// The parts of the payload `bytes`: its blob, and, when bytes follow the blob,
/// those bytes as the data block, whose length the block's decoder holds to the
/// size the block declares.
///
/// Always inlined: left to the inliner, it becomes a call in the live drain
/// whenever the worker's own code around it changes shape.
#[inline(always)]
fn payload_parse(bytes: &[u8]) -> Result<ParsedPayload<'_>, String> {
    let Some((head, body)) = bytes.split_first_chunk::<PAYLOAD_HEAD>() else {
        return Err(format!("a payload of {} bytes holds no payload head", bytes.len()));
    };
    let blob_len = read_u64_le(head, 0);
    let Some((blob, rest)) = usize::try_from(blob_len).ok().and_then(|n| body.split_at_checked(n)) else {
        return Err(format!("a blob of {blob_len} bytes runs past its payload"));
    };
    Ok((blob, (!rest.is_empty()).then_some(rest)))
}

/// A payload's blob, and the rows of its data block under the shape record
/// that is the blob; `known` answers what that record decodes to when the
/// caller already knows.
///
/// Inlined: the live drain decodes every group it is addressed in through it,
/// and without the attribute it is a call there.
#[inline]
pub(super) fn payload_rows(
    bytes: &[u8],
    known: impl FnOnce(&[u8]) -> Option<SchemaDescriptor>,
) -> Result<(&[u8], Option<Batch>), String> {
    let (blob, block) = payload_parse(bytes)?;
    let Some(block) = block else {
        return Ok((blob, None));
    };
    let schema = match known(blob) {
        Some(s) => s,
        None => decode_schema_record(blob)?,
    };
    let batch = MemBatch::of_wal_block(block, &schema)?.to_batch();
    batch.debug_verify_null_bits();
    Ok((blob, Some(batch)))
}

#[cfg(test)]
#[path = "tests/format.rs"]
mod tests;
