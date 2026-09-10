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

use crate::circuit::{read_aggs, read_cols, read_compute_map, write_aggs, write_cols, write_compute_map};
use crate::circuit::{AggDescriptor, ComputeMap};
use crate::codec::{Reader, Writer};
use crate::range::{read_index_bound, read_range_descriptor, write_index_bound, write_range_descriptor};
use crate::range::{IndexBound, RangeDescriptor};

/// ORDER BY keys apply in sequence; a spec carries at most this many.
pub const MAX_ORDER_KEYS: usize = 16;
/// Decode-side ceiling on one `pk IN (…)` gather. A longer list is served as
/// several gathers or as an ordinary predicate scan, never rejected.
pub const MAX_PK_SET_KEYS: usize = 65_536;
/// Decode-side ceiling on a full encoded `ReadSpec` blob.
pub(crate) const MAX_READ_SPEC_BYTES: usize = 2 << 20;

const VERSION: u8 = 5;

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

/// The fold sink's aggregate spec: a per-worker hash-fold over the scanned rows.
/// `group_cols` and `aggs[].col_idx` index the **reduce input** — `pre`'s output
/// when one is present, else the source schema (empty `group_cols` = a global
/// aggregate; `aggs = []` = `SELECT DISTINCT`). `aggs` is the physical reduce
/// layout `push_agg_specs` produces, not the SELECT list: AVG contributes
/// `[Sum, CountNonNull]`, and HAVING-only aggregates append items.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggReadSpec {
    pub group_cols: Vec<u32>,
    pub aggs: Vec<AggDescriptor>,
    /// The map between the predicate and the fold, so the reduce can group by or
    /// aggregate an expression (`GROUP BY a + b`). Compiled over the SOURCE
    /// schema; the worker rebuilds the reduce input as the source's PK columns
    /// followed by the map's declared slots. `None` = the source *is* the input.
    pub pre: Option<ComputeMap>,
}

impl AggReadSpec {
    /// A fold that reads source columns directly — the shape every grouped
    /// SELECT has until it groups by, or aggregates, an expression.
    pub fn direct(group_cols: Vec<u32>, aggs: Vec<AggDescriptor>) -> Self {
        AggReadSpec { group_cols, aggs, pre: None }
    }
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
    /// Secondary-index range walk, and whether it must be exact.
    ///
    /// `exact` is the SQL layer's statement that it **stripped** the bounded
    /// conjuncts from `predicate` — so the walk is the only thing that applies
    /// them and must not be traded away. The worker then runs an un-gated
    /// byte-exact `BoundedIndexCursor`. With `exact = false` the conjuncts are
    /// still in `predicate`, so the walk is only an access optimization and the
    /// worker may fall back to a full cursor when the range covers too much of
    /// the table.
    IndexRange { bound: IndexBound, exact: bool },
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
/// Reads only the header and the range descriptor, never the predicate / order /
/// projection / PkSet sections, so a `PkSet` spec at the key cap costs one byte
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
        let sink_tag = match sink {
            ReadSink::Rows { .. } => SINK_ROWS,
            ReadSink::Fold(_) => SINK_FOLD,
        };
        let cap = 64
            + predicate.len()
            + match bound {
                ReadBound::PkSet(keys) => 16 * keys.len(),
                ReadBound::PkRange(desc) => RangeDescriptor::encoded_len(desc.eq_vals().len()),
                ReadBound::IndexRange { bound, .. } => RangeDescriptor::encoded_len(bound.desc.eq_vals().len()),
                ReadBound::None | ReadBound::Delta { .. } => 0,
            }
            + match sink {
                ReadSink::Rows { projection, order, .. } => projection.len() + 4 * order.len(),
                ReadSink::Fold(agg) => {
                    4 * agg.group_cols.len()
                        + 5 * agg.aggs.len()
                        + agg.pre.as_ref().map_or(0, |p| p.program.len() + 2 * p.out_cols.len())
                }
            };

        let mut w = Writer::with_capacity(cap);
        w.u8(VERSION).u8(bound.kind()).u8(sink_tag).u8(0);

        match bound {
            ReadBound::None => {}
            ReadBound::PkRange(desc) => {
                write_range_descriptor(&mut w, desc);
            }
            ReadBound::IndexRange { bound, exact } => {
                write_index_bound(&mut w, bound);
                w.u8(*exact as u8);
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
            ReadSink::Rows { projection, order, limit_k } => {
                w.u8(order.len() as u8).u64(*limit_k);
                for key in order {
                    write_order_key(&mut w, key);
                }
                w.bytes32(projection);
            }
            ReadSink::Fold(agg) => {
                write_cols(&mut w, &agg.group_cols);
                write_aggs(&mut w, &agg.aggs);
                // `None` is spelled as an empty program, so the pre-map section
                // is unconditional.
                let (program, out_cols) = match &agg.pre {
                    Some(p) => (p.program.as_slice(), p.out_cols.as_slice()),
                    None => (&[][..], &[][..]),
                };
                write_compute_map(&mut w, out_cols, program);
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
                let bound = read_index_bound(&mut r).map_err(|e| format!("read_spec: {e}"))?;
                let exact = match r.u8()? {
                    0 => false,
                    1 => true,
                    other => return Err(format!("read_spec: IndexRange exact flag {other} is not 0 or 1")),
                };
                ReadBound::IndexRange { bound, exact }
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
                    .as_chunks::<16>()
                    .0
                    .iter()
                    .map(|c| u128::from_le_bytes(*c))
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
                    order.push(read_order_key(&mut r)?);
                }
                let projection = r.bytes32()?.to_vec();
                ReadSink::Rows { projection, order, limit_k }
            }
            SINK_FOLD => {
                // The three counted sections take the circuit codec's caps and
                // domain checks; only the pre-map's `Option` encoding is this
                // sink's own.
                let group_cols = read_cols(&mut r).map_err(|e| format!("read_spec: {e}"))?;
                let aggs = read_aggs(&mut r).map_err(|e| format!("read_spec: {e}"))?;
                let map = read_compute_map(&mut r).map_err(|e| format!("read_spec: {e}"))?;
                // The two halves describe one reduce input: a program with no
                // declared output slots cannot be resolved against a schema, and
                // declared slots with no program would leave every one unwritten.
                // A circuit `MapKind::Compute` has no such rule — it is
                // unconditional, and a PK-only projection legitimately declares
                // no slots — so this stays here rather than in the shared codec.
                if map.program.is_empty() != map.out_cols.is_empty() {
                    return Err("read_spec: fold pre-map program and column declarations disagree".to_string());
                }
                let pre = (!map.program.is_empty()).then_some(map);
                ReadSink::Fold(AggReadSpec { group_cols, aggs, pre })
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
