// ---------------------------------------------------------------------------
// ReadSpec — the parameterized-scan descriptor for an ad-hoc bounded SELECT.
//
// One wire descriptor carries bound extraction, predicate evaluation, and the
// sink — row forwarding (projection + ORDER BY / LIMIT top-k) or an aggregate
// fold — out to the workers, executed single-pass over each worker's merged
// partition cursor. Both client (gnitz-core) and engine (gnitz-server worker)
// share this encoder/decoder — the same drift-safety rule `RangeDescriptor`
// follows. The master forwards the encoded blob verbatim (it never decodes the
// bound); the worker decodes it at the trust boundary and rejects any
// malformed frame.
// ---------------------------------------------------------------------------

use crate::catalog::MAX_COLUMNS;
use crate::circuit::AggFunc;
use crate::codec::{Reader, Writer};
use crate::range::RangeDescriptor;

/// ORDER BY keys apply in sequence; a spec carries at most this many.
pub const MAX_ORDER_KEYS: usize = 16;
/// Decode-side ceiling on one `pk IN (…)` gather. A longer list is served as
/// several gathers or as an ordinary predicate scan, never rejected.
pub const MAX_PK_SET_KEYS: usize = 65_536;
/// Decode-side ceiling on a full encoded `ReadSpec` blob.
pub(crate) const MAX_READ_SPEC_BYTES: usize = 2 << 20;

const VERSION: u8 = 3;

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

/// One physical aggregate item in a fold sink. This is the physical reduce
/// layout the view planner's `push_agg_specs` produces — **not** the SELECT
/// list: AVG contributes `[Sum, CountNonNull]`, a nullable SUM carries its
/// `CountNonNull` companion, HAVING-only aggregates append items. `src_col` is
/// a source-schema column index (`col 0` for COUNT(*), whose arm never reads
/// the column).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggReadItem {
    pub op: AggFunc,
    pub src_col: u16,
}

/// The fold sink's aggregate spec: a per-worker hash-fold over the scanned
/// rows. `group_cols` are source-schema indices (empty = a global aggregate;
/// `aggs = []` = `SELECT DISTINCT` over `group_cols`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggReadSpec {
    pub group_cols: Vec<u16>,
    pub aggs: Vec<AggReadItem>,
}

/// What the worker does with the rows surviving `bound` + `predicate` —
/// exactly one of the two sinks. A fold carries no projection / ORDER BY /
/// LIMIT because all SQL-level finishing on an aggregate result is
/// client-side; the enum makes that unrepresentable rather than decode-checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadSink {
    /// Forward rows: projection → ORDER BY / LIMIT top-k.
    Rows {
        /// Compiled projection map program over the SOURCE schema → reply
        /// payload columns. Empty = identity (reply schema == source schema).
        projection: Vec<u8>,
        /// ORDER BY keys applied in sequence; `col` indices refer to the REPLY
        /// schema. `len ≤ MAX_ORDER_KEYS`.
        order: Vec<OrderKey>,
        /// OFFSET + LIMIT in logical rows (summed weight). 0 = unbounded.
        /// (`LIMIT 0` never reaches the wire — the SQL layer short-circuits an
        /// empty window.)
        limit_k: u64,
    },
    /// Fold rows into per-group accumulators (GROUP BY / global aggregate /
    /// DISTINCT) and emit partial reduce-output rows.
    Fold(AggReadSpec),
}

impl ReadSink {
    /// The identity sink: forward every surviving row unprojected, unordered,
    /// unbounded.
    pub fn all_rows() -> Self {
        ReadSink::Rows {
            projection: Vec::new(),
            order: Vec::new(),
            limit_k: 0,
        }
    }
}

/// The bound a `ReadSpec` walks before predicate/projection. `RangeDescriptor`
/// carries **native** values (packed LE `u128`); the worker is the sole OPK
/// encoder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadBound {
    /// Full merged cursor — a plain scan with a server-side predicate.
    None,
    /// Range over the PK column list (`RangeDescriptor` semantics). A full-PK
    /// point lookup is `n_eq = pk_count − 1` with degenerate cuts. Exact — no
    /// residual is needed for the bound itself.
    PkRange(RangeDescriptor),
    /// Secondary-index range: the packed index column list, whether the walk
    /// must be exact, and the descriptor.
    ///
    /// `exact` is the SQL layer's statement that it **stripped** the bounded
    /// conjuncts from `predicate` — so the walk is the only thing that applies
    /// them and must not be traded away. The worker then runs an un-gated
    /// byte-exact `BoundedIndexCursor`. With `exact = false` the conjuncts are
    /// still in `predicate`, so the walk is only an access optimization and the
    /// worker may fall back to a full cursor when the range covers too much of
    /// the table.
    IndexRange {
        idx_cols: u64,
        exact: bool,
        desc: RangeDescriptor,
    },
    /// `pk IN (…)` for a single-column PK. Values are raw native keys widened to
    /// `u128` (`FixedInt::pack`), and must be **distinct after truncation to the
    /// PK's width** — the worker rejects the rest. Wire order is irrelevant: the
    /// worker OPK-sorts the keys before its forward gather.
    PkSet(Vec<u128>),
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
/// client — the same reason [`pack_scan_spec_extra`], `pack_col_id` and
/// `pack_pk_cols` are single-sourced in this crate.
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
    let mut w = Writer::with_capacity(8 + spec.len() + reply_block.len());
    w.bytes32(spec).bytes32(reply_block);
    w.into_vec()
}

/// Split a ScanSpec `seek_pk_extra` blob back into `(spec_bytes, reply_block)`
/// at the trust boundary — rejecting a truncated / trailing-byte frame.
pub fn unpack_scan_spec_extra(extra: &[u8]) -> Result<(&[u8], &[u8]), String> {
    let mut r = Reader::new(extra, "scan_spec extra");
    let spec = r.bytes32()?;
    let block = r.bytes32()?;
    r.expect_consumed()?;
    Ok((spec, block))
}

/// The `PkRange` descriptor of an encoded `ReadSpec`, or `None` for every other
/// bound kind, an unknown version, or a truncated prefix. Lets the master derive
/// a routing decision without decoding the spec.
///
/// Reads only the header and the range descriptor, never the predicate / order /
/// projection / PkSet sections, so a `PkSet` spec at the key cap costs one byte
/// compare rather than a megabyte-scale parse. [`ReadSpec::decode`] remains the
/// worker's full validating parse.
pub fn peek_pk_range(buf: &[u8]) -> Option<RangeDescriptor> {
    read_range_descriptor(&mut peek_bound(buf, BOUND_PK_RANGE)?).ok()
}

/// A reader positioned just past the fixed header of an encoded `ReadSpec`, if
/// that spec carries a `kind` bound. `None` for every other bound kind, an
/// unknown version, or a truncated prefix.
///
/// Both peeks read the header through here rather than each spelling it out.
/// They are the master's routing and gating inputs and their failure mode is
/// silent — a header that grew a field would make one of two hand-written parses
/// return `None`, which routes every read as a broadcast and gates no poll, with
/// no error anywhere.
fn peek_bound<'a>(buf: &'a [u8], kind: u8) -> Option<Reader<'a>> {
    let mut r = Reader::new(buf, "read_spec");
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
pub fn peek_delta_bound(buf: &[u8]) -> Option<u64> {
    peek_bound(buf, BOUND_DELTA)?.u64().ok()
}

/// Read an embedded `RangeDescriptor`: peek its `n_eq` to learn its span, slice
/// exactly that many bytes, and defer full validation to `RangeDescriptor::decode`.
/// (`n_eq` is one byte, so `encoded_len` cannot overflow; a pathological value
/// exceeds `remaining` and `take` rejects it, and a valid one is re-validated —
/// arity, flags, exact length — by the descriptor decoder.)
fn read_range_descriptor(r: &mut Reader) -> Result<RangeDescriptor, String> {
    let n_eq = r.peek_u8()? as usize;
    let bytes = r.take(RangeDescriptor::encoded_len(n_eq))?;
    RangeDescriptor::decode(bytes)
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
        let sink_tag = match sink {
            ReadSink::Rows { .. } => SINK_ROWS,
            ReadSink::Fold(_) => SINK_FOLD,
        };
        let cap = 64
            + predicate.len()
            + match bound {
                ReadBound::PkSet(keys) => 16 * keys.len(),
                _ => 0,
            }
            + match sink {
                ReadSink::Rows { projection, order, .. } => projection.len() + 4 * order.len(),
                ReadSink::Fold(agg) => 2 * agg.group_cols.len() + 3 * agg.aggs.len(),
            };
        let mut w = Writer::with_capacity(cap);
        w.u8(VERSION).u8(bound.kind()).u8(sink_tag).u8(0);

        match bound {
            ReadBound::None => {}
            ReadBound::PkRange(desc) => {
                w.raw(&desc.encode());
            }
            ReadBound::IndexRange { idx_cols, exact, desc } => {
                w.u64(*idx_cols).u8(*exact as u8).raw(&desc.encode());
            }
            ReadBound::PkSet(keys) => {
                // One memcpy: on a little-endian target the `u128` slice already
                // IS its wire image. A `pk IN (…)` set reaches MAX_PK_SET_KEYS.
                w.u32(keys.len() as u32).raw(crate::as_le_bytes(keys));
            }
            ReadBound::Delta { after_tick } => {
                w.u64(*after_tick);
            }
        }

        w.bytes32(predicate);

        match sink {
            ReadSink::Rows {
                projection,
                order,
                limit_k,
            } => {
                w.u8(order.len() as u8).u64(*limit_k);
                for key in order {
                    let mut flags = 0u8;
                    if key.desc {
                        flags |= ORDER_DESC;
                    }
                    if key.nulls_first {
                        flags |= ORDER_NULLS_FIRST;
                    }
                    w.u16(key.col).u8(flags).u8(0); // trailing byte reserved
                }
                w.bytes32(projection);
            }
            ReadSink::Fold(agg) => {
                w.u16(agg.group_cols.len() as u16);
                for &c in &agg.group_cols {
                    w.u16(c);
                }
                w.u8(agg.aggs.len() as u8);
                for item in &agg.aggs {
                    w.u8(item.op as u8).u16(item.src_col);
                }
            }
        }
        w.into_vec()
    }

    /// Decode and validate at the trust boundary. Rejects: an over-cap blob,
    /// unknown version/kind/sink tag, order-key / PkSet / fold-section count
    /// over cap, an unknown order flag bit or aggregate op, length
    /// overflows/truncation, and trailing bytes. Duplicate PkSet keys are NOT
    /// rejected here — that check needs the PK width and lives in the worker's
    /// schema-aware gather.
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
                let idx_cols = r.u64()?;
                let exact = match r.u8()? {
                    0 => false,
                    1 => true,
                    other => return Err(format!("read_spec: IndexRange exact flag {other} is not 0 or 1")),
                };
                let desc = read_range_descriptor(&mut r)?;
                ReadBound::IndexRange { idx_cols, exact, desc }
            }
            BOUND_PK_SET => {
                let count = r.u32()? as usize;
                if count > MAX_PK_SET_KEYS {
                    return Err(format!("read_spec: PkSet count {count} exceeds cap {MAX_PK_SET_KEYS}"));
                }
                // Duplicates are NOT rejected here: this decoder has no schema, so
                // a raw-`u128` set cannot see the wire keys that collide once
                // truncated to the PK's width. The reject lives in the worker's
                // schema-aware gather.
                let raw = r.take(count * 16)?;
                let keys = raw
                    .chunks_exact(16)
                    .map(|c| u128::from_le_bytes(c.try_into().unwrap()))
                    .collect();
                ReadBound::PkSet(keys)
            }
            BOUND_DELTA => ReadBound::Delta { after_tick: r.u64()? },
            other => return Err(format!("read_spec: unknown bound kind {other}")),
        };

        let predicate = r.bytes32()?.to_vec();

        let sink = match sink_tag {
            SINK_ROWS => {
                let n_order = r.u8()? as usize;
                if n_order > MAX_ORDER_KEYS {
                    return Err(format!("read_spec: {n_order} order keys exceeds cap {MAX_ORDER_KEYS}"));
                }
                let limit_k = r.u64()?;
                let mut order = Vec::with_capacity(n_order);
                for _ in 0..n_order {
                    let col = r.u16()?;
                    let flags = r.u8()?;
                    if flags & !(ORDER_DESC | ORDER_NULLS_FIRST) != 0 {
                        return Err(format!("read_spec: order key has unknown flag bits {flags:#04x}"));
                    }
                    let _rsv = r.u8()?;
                    order.push(OrderKey {
                        col,
                        desc: flags & ORDER_DESC != 0,
                        nulls_first: flags & ORDER_NULLS_FIRST != 0,
                    });
                }
                let projection = r.bytes32()?.to_vec();
                ReadSink::Rows {
                    projection,
                    order,
                    limit_k,
                }
            }
            SINK_FOLD => {
                // Trust-boundary caps only: a legitimate fold's reply schema is
                // width-checked by the SQL layer (`1 + groups + aggs ≤
                // MAX_COLUMNS`), so either count exceeding one schema's column
                // cap marks a malformed frame.
                let n_group_cols = r.u16()? as usize;
                if n_group_cols > MAX_COLUMNS {
                    return Err(format!(
                        "read_spec: {n_group_cols} group cols exceeds cap {MAX_COLUMNS}"
                    ));
                }
                let mut group_cols = Vec::with_capacity(n_group_cols);
                for _ in 0..n_group_cols {
                    group_cols.push(r.u16()?);
                }
                let n_aggs = r.u8()? as usize;
                if n_aggs > MAX_COLUMNS {
                    return Err(format!("read_spec: {n_aggs} agg items exceeds cap {MAX_COLUMNS}"));
                }
                let mut aggs = Vec::with_capacity(n_aggs);
                for _ in 0..n_aggs {
                    let op_byte = r.u8()?;
                    let op = AggFunc::from_wire(op_byte as u64)
                        .ok_or_else(|| format!("read_spec: unknown aggregate op {op_byte}"))?;
                    let src_col = r.u16()?;
                    aggs.push(AggReadItem { op, src_col });
                }
                ReadSink::Fold(AggReadSpec { group_cols, aggs })
            }
            other => return Err(format!("read_spec: unknown sink tag {other}")),
        };

        r.expect_consumed()?;

        Ok(ReadSpec { bound, predicate, sink })
    }
}

#[cfg(test)]
#[path = "tests/read_spec.rs"]
mod tests;
