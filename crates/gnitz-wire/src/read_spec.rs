// ---------------------------------------------------------------------------
// ReadSpec — the parameterized-scan descriptor for an ad-hoc bounded SELECT.
//
// One wire descriptor carries bound extraction, predicate evaluation, and the
// sink — an optional map, then row forwarding (ORDER BY / LIMIT top-k) or an
// aggregate fold — out to the workers, executed single-pass over each worker's merged
// partition cursor. Both client (gnitz-core) and engine (gnitz-server worker)
// share this encoder/decoder — the same drift-safety rule `RangeDescriptor`
// follows. The master forwards the encoded blob verbatim (it never decodes the
// bound); the worker decodes it at the trust boundary and rejects any
// malformed frame.
// ---------------------------------------------------------------------------

use crate::circuit::{read_aggs, read_cols, read_compute_map, write_aggs, write_cols, write_compute_map};
use crate::circuit::{AggDescriptor, ComputeMap};
use crate::codec::{Reader, Writer};
use crate::range::{read_index_bound, read_range_descriptor, write_index_bound, write_range_descriptor};
use crate::range::{IndexBound, IndexWalk, RangeDescriptor};
use crate::MAX_PK_BYTES;

/// ORDER BY keys apply in sequence; a spec carries at most this many.
pub const MAX_ORDER_KEYS: usize = 16;
/// Decode-side ceiling on one `pk IN (…)` gather. A longer list is served as
/// several gathers or as an ordinary predicate scan, never rejected.
pub const MAX_PK_SET_KEYS: usize = 65_536;
/// Decode-side ceiling on a full encoded `ReadSpec` blob.
pub(crate) const MAX_READ_SPEC_BYTES: usize = 2 << 20;

const VERSION: u8 = 6;

const BOUND_NONE: u8 = 0;
const BOUND_PK_RANGE: u8 = 1;
const BOUND_INDEX_RANGE: u8 = 2;
const BOUND_PK_SET: u8 = 3;
const BOUND_DELTA: u8 = 4;

const SINK_ROWS: u8 = 0;
const SINK_FOLD: u8 = 1;

const ORDER_DESC: u8 = 1 << 0;
const ORDER_NULLS_FIRST: u8 = 1 << 1;

/// One ORDER BY key. `col` is a **full reply-schema column index** (not a dense
/// payload index): the worker's top-k sink and the client's finish both resolve
/// it through the same `is_pk_col` / `payload_idx` split, so a projected PK
/// column and a hidden appended order column each land in the right window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderKey {
    pub col: u16,
    pub desc: bool,
    pub nulls_first: bool,
}

/// One order key's four wire bytes: column, flags, one reserved byte. Shared by
/// the rows sink and a circuit's `TopN` node, so the two cannot drift.
pub(crate) fn write_order_key(w: &mut Writer, key: &OrderKey) {
    let mut flags = 0u8;
    if key.desc {
        flags |= ORDER_DESC;
    }
    if key.nulls_first {
        flags |= ORDER_NULLS_FIRST;
    }
    w.u16(key.col).u8(flags).u8(0);
}

/// [`write_order_key`]'s inverse; an unknown flag bit is a refusal.
pub(crate) fn read_order_key(r: &mut Reader) -> Result<OrderKey, String> {
    let col = r.u16()?;
    let flags = r.u8()?;
    if flags & !(ORDER_DESC | ORDER_NULLS_FIRST) != 0 {
        return Err(format!("order key has unknown flag bits {flags:#04x}"));
    }
    let _rsv = r.u8()?;
    Ok(OrderKey {
        col,
        desc: flags & ORDER_DESC != 0,
        nulls_first: flags & ORDER_NULLS_FIRST != 0,
    })
}

/// The fold sink's per-worker hash-fold. `aggs` is the physical reduce layout,
/// not the SELECT list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggReadSpec {
    pub group_cols: Vec<u32>,
    pub aggs: Vec<AggDescriptor>,
}

/// What the worker does with the rows surviving `bound` + `predicate`: an
/// optional map, then exactly one of the two sink kinds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSink {
    /// The map between the predicate and the sink, compiled over the SOURCE
    /// schema: its output is the source's PK columns followed by the declared
    /// slots. `None` = the sink reads the source rows themselves.
    pub map: Option<ComputeMap>,
    pub kind: SinkKind,
}

/// The two sinks. A fold carries no ORDER BY / LIMIT because all SQL-level
/// finishing on an aggregate result is client-side; the enum makes that
/// unrepresentable rather than decode-checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkKind {
    /// Forward rows, ORDER BY / LIMIT top-k.
    Rows {
        /// ORDER BY keys applied in sequence; `col` indexes the sink input (=
        /// the reply layout). `len ≤ MAX_ORDER_KEYS`.
        order: Vec<OrderKey>,
        /// OFFSET + LIMIT in logical rows (summed weight). 0 = unbounded.
        /// (`LIMIT 0` never reaches the wire — the SQL layer short-circuits an
        /// empty window.)
        limit_k: u64,
    },
    /// Fold rows into per-group accumulators (GROUP BY / global aggregate /
    /// DISTINCT) and emit partial reduce-output rows. `group_cols` /
    /// `aggs[].col_idx` index the sink input.
    Fold(AggReadSpec),
}

impl ReadSink {
    /// The identity sink: forward every surviving row unmapped, unordered,
    /// unbounded.
    pub fn all_rows() -> Self {
        ReadSink {
            map: None,
            kind: SinkKind::Rows { order: Vec::new(), limit_k: 0 },
        }
    }
}

/// OPK keys of one stride, strictly ascending — the order a forward gather
/// sweeps. Fields private: every constructor sorts and dedups, or validates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PkKeys {
    stride: u8,
    bytes: Vec<u8>,
}

impl PkKeys {
    /// Sort and dedup `keys`, each exactly `stride` bytes.
    pub fn from_keys<'a>(stride: usize, keys: impl IntoIterator<Item = &'a [u8]>) -> Self {
        assert!((1..=MAX_PK_BYTES).contains(&stride), "PkKeys: stride {stride}");
        let mut ks: Vec<&[u8]> = keys
            .into_iter()
            .inspect(|k| assert_eq!(k.len(), stride, "PkKeys: key width"))
            .collect();
        ks.sort_unstable();
        ks.dedup();
        PkKeys { stride: stride as u8, bytes: ks.concat() }
    }

    /// The most keys of `stride` one request carries: `MAX_PK_SET_KEYS`, and
    /// no more bytes than that many 16-byte keys, so a wide compound key cannot
    /// push a spec past `MAX_READ_SPEC_BYTES`.
    pub const fn max_per_request(stride: usize) -> usize {
        let by_bytes = MAX_PK_SET_KEYS * 16 / stride;
        if by_bytes < MAX_PK_SET_KEYS {
            by_bytes
        } else {
            MAX_PK_SET_KEYS
        }
    }

    pub fn stride(&self) -> usize {
        self.stride as usize
    }

    pub fn len(&self) -> usize {
        self.bytes.len() / self.stride()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn iter(&self) -> std::slice::ChunksExact<'_, u8> {
        self.bytes.chunks_exact(self.stride())
    }

    /// Consecutive sub-lists of at most [`Self::max_per_request`] keys.
    pub fn per_request(&self) -> impl Iterator<Item = PkKeys> + '_ {
        let n = Self::max_per_request(self.stride()) * self.stride();
        self.bytes
            .chunks(n)
            .map(|c| PkKeys { stride: self.stride, bytes: c.to_vec() })
    }
}

/// The bound a `ReadSpec` walks before the predicate and the sink. The range
/// bounds' `RangeDescriptor` carries **native** values (packed LE `u128`), for
/// which the worker is the sole OPK encoder; a `PkSet` carries OPK keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadBound {
    /// Full merged cursor — a plain scan with a server-side predicate.
    None,
    /// Range over the PK column list (`RangeDescriptor` semantics). A full-PK
    /// point lookup is `n_eq = pk_count − 1` with degenerate cuts. Exact — no
    /// residual is needed for the bound itself.
    PkRange(RangeDescriptor),
    /// Secondary-index range walk.
    IndexRange { bound: IndexBound, walk: IndexWalk },
    /// `pk IN (…)`: the listed keys, at any PK arity.
    PkSet(PkKeys),
    /// Every delta a fed view emitted after tick round `after_tick`, walked over
    /// the view's delta store rather than its output store. `after_tick = 0` is
    /// the bootstrap: it names the view's whole history, which is what the output
    /// store already holds, so that arm reads the output store in the view's own
    /// schema.
    ///
    /// A distinct tag rather than a `PkRange` over the delta store's leading
    /// `_tick` column: the tag is what the master's router reads, and a
    /// `PkRange`-tagged delta read would be hashed against the *view's* schema
    /// and unicast to one worker — a silently partial answer.
    Delta { after_tick: u64 },
}

impl ReadBound {
    const fn kind(&self) -> u8 {
        match self {
            ReadBound::None => BOUND_NONE,
            ReadBound::PkRange(_) => BOUND_PK_RANGE,
            ReadBound::IndexRange { .. } => BOUND_INDEX_RANGE,
            ReadBound::PkSet(_) => BOUND_PK_SET,
            ReadBound::Delta { .. } => BOUND_DELTA,
        }
    }
}

/// A parameterized bounded scan: bound → predicate → sink (row forwarding or
/// aggregate fold), executed on the workers over one merged partition cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSpec {
    pub bound: ReadBound,
    /// Compiled predicate over the SOURCE schema ("EXPR" blob). Empty = the
    /// bound is exact and no residual filter runs.
    pub predicate: Vec<u8>,
    pub sink: ReadSink,
}

/// Pack a delta reply's terminal watermark: the cursor tag in the high half of
/// `seek_pk`, the tick round `T` in the low half. Defined here, beside the bound
/// it answers, because the encoder is in the server and the decoder in the
/// client — the same reason [`pack_scan_spec_extra`] and `pack_pk_cols` are
/// single-sourced in this crate.
///
/// One field, because "which cursor is this?" and "how far does it reach?" are
/// answered together: a client that does not recognise the tag discards its copy
/// and re-reads at `after_tick = 0` whatever `T` says.
pub fn pack_delta_watermark(tag: u64, tick: u64) -> u128 {
    ((tag as u128) << 64) | tick as u128
}

/// The inverse of [`pack_delta_watermark`]: `(tag, tick)`.
pub fn unpack_delta_watermark(watermark: u128) -> (u64, u64) {
    ((watermark >> 64) as u64, watermark as u64)
}

/// Pack a ScanSpec request's control-block `seek_pk_extra` blob: the encoded
/// `ReadSpec` followed by the reply-schema wire block, each `u32`-length-
/// prefixed. Bundling both in the arbitrary-length `seek_pk_extra` BLOB lets the
/// master forward the blob to the workers without decoding the spec (it peeks
/// only the bound header, [`peek_pk_range`], to route the request). The block
/// does not travel back: the worker decodes it into the read's output shape,
/// and the client decodes the reply against its own copy.
pub fn pack_scan_spec_extra(spec: &[u8], reply_block: &[u8]) -> Vec<u8> {
    let mut w = Writer::with_capacity(scan_spec_extra_len(spec, reply_block));
    w.bytes32(spec).bytes32(reply_block);
    w.into_vec()
}

/// The packed length of [`pack_scan_spec_extra`]'s output: the two `u32` length
/// prefixes plus both payloads. A frame that packs the pair into itself needs it
/// twice — for its own length prefix and for the capacity hint.
pub(crate) fn scan_spec_extra_len(spec: &[u8], reply_block: &[u8]) -> usize {
    8 + spec.len() + reply_block.len()
}

/// The bytes of one encoded [`ReadSpec`], distinct from the packed
/// `seek_pk_extra` blob it travels inside — which at `spec.len() == 0x0103`
/// opens with a length prefix that spells a valid `(VERSION, BOUND_PK_RANGE)`
/// header. Both are `&[u8]`; only this one may be peeked.
#[derive(Clone, Copy, Debug)]
pub struct SpecBytes<'a>(pub &'a [u8]);

/// Split a ScanSpec `seek_pk_extra` blob back into `(spec_bytes, reply_block)`
/// at the trust boundary — rejecting a truncated / trailing-byte frame.
pub fn unpack_scan_spec_extra(extra: &[u8]) -> Result<(SpecBytes<'_>, &[u8]), String> {
    let mut r = Reader::new(extra, "scan_spec extra");
    let spec = r.bytes32()?;
    let block = r.bytes32()?;
    r.expect_consumed()?;
    Ok((SpecBytes(spec), block))
}

/// The `PkRange` descriptor of an encoded `ReadSpec`, or `None` for every other
/// bound kind, an unknown version, or a truncated prefix. Lets the master derive
/// a routing decision without decoding the spec.
///
/// Reads only the header and the range descriptor, never the predicate / map /
/// order / PkSet sections, so a `PkSet` spec at the key cap costs one byte
/// compare rather than a megabyte-scale parse. [`ReadSpec::decode`] remains the
/// worker's full validating parse.
pub fn peek_pk_range(spec: SpecBytes<'_>) -> Option<RangeDescriptor> {
    read_range_descriptor(&mut peek_bound(spec, BOUND_PK_RANGE)?).ok()
}

/// A reader positioned just past the fixed header of an encoded `ReadSpec`, if
/// that spec carries a `kind` bound. `None` for every other bound kind, an
/// unknown version, or a truncated prefix.
///
/// Both peeks read the header through here rather than each spelling it out:
/// they are the master's routing and gating inputs, and a header that grew a
/// field would make a second hand-written parse return `None` silently —
/// broadcasting every read and gating no poll, with no error anywhere.
fn peek_bound(spec: SpecBytes<'_>, kind: u8) -> Option<Reader<'_>> {
    let mut r = Reader::new(spec.0, "read_spec");
    if r.u8().ok()? != VERSION || r.u8().ok()? != kind {
        return None;
    }
    r.u8().ok()?; // sink tag
    r.u8().ok()?; // reserved
    Some(r)
}

/// The `after_tick` of an encoded `ReadSpec` carrying a delta bound, or `None`
/// for every other bound kind, an unknown version, or a truncated prefix.
///
/// Read once per request by the master and its answer used for all three of the
/// dispatch classification, the routing and the idle-poll gate, so the bound is
/// decoded once rather than three times. [`ReadSpec::decode`] remains the
/// worker's full validating parse and the sole trust boundary.
pub fn peek_delta_bound(spec: SpecBytes<'_>) -> Option<u64> {
    peek_bound(spec, BOUND_DELTA)?.u64().ok()
}

impl ReadSpec {
    /// Serialise to a version-prefixed LE byte sequence (§ layout below), from
    /// borrowed parts: the planner holds the bound, predicate and sink
    /// separately and re-sends one spec under several bounds, so nothing is
    /// cloned per request.
    pub fn encode_parts(bound: &ReadBound, predicate: &[u8], sink: &ReadSink) -> Vec<u8> {
        // Header: version | bound_kind | sink_tag | reserved. One up-front
        // reservation covering every section (64 covers the fixed header,
        // range descriptors, and section length prefixes).
        let sink_tag = match sink.kind {
            SinkKind::Rows { .. } => SINK_ROWS,
            SinkKind::Fold(_) => SINK_FOLD,
        };
        let cap = 64
            + predicate.len()
            + match bound {
                ReadBound::PkSet(keys) => keys.as_bytes().len(),
                ReadBound::PkRange(desc) => RangeDescriptor::encoded_len(desc.eq_vals().len()),
                ReadBound::IndexRange { bound, .. } => RangeDescriptor::encoded_len(bound.desc.eq_vals().len()),
                ReadBound::None | ReadBound::Delta { .. } => 0,
            }
            + sink.map.as_ref().map_or(0, |m| m.program.len() + 2 * m.out_cols.len())
            + match &sink.kind {
                SinkKind::Rows { order, .. } => 4 * order.len(),
                SinkKind::Fold(agg) => 4 * agg.group_cols.len() + 5 * agg.aggs.len(),
            };

        let mut w = Writer::with_capacity(cap);
        w.u8(VERSION).u8(bound.kind()).u8(sink_tag).u8(0);

        match bound {
            ReadBound::None => {}
            ReadBound::PkRange(desc) => {
                write_range_descriptor(&mut w, desc);
            }
            ReadBound::IndexRange { bound, walk } => {
                write_index_bound(&mut w, bound);
                w.u8(match walk {
                    IndexWalk::Optional => 0,
                    IndexWalk::Required => 1,
                });
            }
            ReadBound::PkSet(keys) => {
                w.u8(keys.stride).u32(keys.len() as u32).raw(keys.as_bytes());
            }
            ReadBound::Delta { after_tick } => {
                w.u64(*after_tick);
            }
        }

        w.bytes32(predicate);

        match &sink.map {
            Some(m) => {
                w.u8(1);
                write_compute_map(&mut w, &m.out_cols, &m.program);
            }
            None => {
                w.u8(0);
            }
        }

        match &sink.kind {
            SinkKind::Rows { order, limit_k } => {
                w.u8(order.len() as u8).u64(*limit_k);
                for key in order {
                    write_order_key(&mut w, key);
                }
            }
            SinkKind::Fold(agg) => {
                write_cols(&mut w, &agg.group_cols);
                write_aggs(&mut w, &agg.aggs);
            }
        }
        w.into_vec()
    }

    /// Decode and validate at the trust boundary: a malformed frame is an `Err`.
    pub fn decode(buf: &[u8]) -> Result<Self, String> {
        if buf.len() > MAX_READ_SPEC_BYTES {
            return Err(format!(
                "read_spec: {} bytes exceeds cap {MAX_READ_SPEC_BYTES}",
                buf.len()
            ));
        }
        let mut r = Reader::new(buf, "read_spec");
        let version = r.u8()?;
        if version != VERSION {
            return Err(format!("read_spec: unknown version {version}"));
        }
        let bound_kind = r.u8()?;
        let sink_tag = r.u8()?;
        let _reserved = r.u8()?;

        let bound = match bound_kind {
            BOUND_NONE => ReadBound::None,
            BOUND_PK_RANGE => ReadBound::PkRange(read_range_descriptor(&mut r)?),
            BOUND_INDEX_RANGE => {
                let bound = read_index_bound(&mut r).map_err(|e| format!("read_spec: {e}"))?;
                let walk = match r.u8()? {
                    0 => IndexWalk::Optional,
                    1 => IndexWalk::Required,
                    other => return Err(format!("read_spec: IndexRange walk byte {other} is not 0 or 1")),
                };
                ReadBound::IndexRange { bound, walk }
            }
            BOUND_PK_SET => {
                let stride = r.u8()? as usize;
                if !(1..=MAX_PK_BYTES).contains(&stride) {
                    return Err(format!("read_spec: PkSet stride {stride} outside 1..={MAX_PK_BYTES}"));
                }
                let count = r.u32()? as usize;
                let cap = PkKeys::max_per_request(stride);
                if count > cap {
                    return Err(format!("read_spec: PkSet count {count} exceeds cap {cap}"));
                }
                let bytes = r.take(count * stride)?;
                if bytes.chunks_exact(stride).is_sorted_by(|a, b| a < b) {
                    ReadBound::PkSet(PkKeys {
                        stride: stride as u8,
                        bytes: bytes.to_vec(),
                    })
                } else {
                    return Err("read_spec: PkSet keys are not strictly ascending".to_string());
                }
            }
            BOUND_DELTA => ReadBound::Delta { after_tick: r.u64()? },
            other => return Err(format!("read_spec: unknown bound kind {other}")),
        };

        let predicate = r.bytes32()?.to_vec();

        let map = match r.u8()? {
            0 => None,
            1 => Some(read_compute_map(&mut r).map_err(|e| format!("read_spec: {e}"))?),
            other => return Err(format!("read_spec: map presence byte {other} is not 0 or 1")),
        };

        let kind = match sink_tag {
            SINK_ROWS => {
                let n_order = r.u8()? as usize;
                if n_order > MAX_ORDER_KEYS {
                    return Err(format!("read_spec: {n_order} order keys exceeds cap {MAX_ORDER_KEYS}"));
                }
                let limit_k = r.u64()?;
                let mut order = Vec::with_capacity(n_order);
                for _ in 0..n_order {
                    order.push(read_order_key(&mut r)?);
                }
                SinkKind::Rows { order, limit_k }
            }
            SINK_FOLD => {
                // Both counted sections take the circuit codec's caps and
                // domain checks.
                let group_cols = read_cols(&mut r).map_err(|e| format!("read_spec: {e}"))?;
                let aggs = read_aggs(&mut r).map_err(|e| format!("read_spec: {e}"))?;
                SinkKind::Fold(AggReadSpec { group_cols, aggs })
            }
            other => return Err(format!("read_spec: unknown sink tag {other}")),
        };

        r.expect_consumed()?;

        Ok(ReadSpec {
            bound,
            predicate,
            sink: ReadSink { map, kind },
        })
    }
}

#[cfg(test)]
#[path = "tests/read_spec.rs"]
mod tests;
