// ---------------------------------------------------------------------------
// ReadSpec — the parameterized-scan descriptor for an ad-hoc bounded SELECT.
//
// One wire descriptor carries bound extraction, predicate evaluation, and the
// sink — row forwarding (projection + ORDER BY / LIMIT top-k) or an aggregate
// fold — out to the workers, executed single-pass over each worker's merged
// partition cursor. Both client (gnitz-core) and engine (gnitz-engine worker)
// share this encoder/decoder — the same drift-safety rule `RangeDescriptor`
// follows. The master forwards the encoded blob verbatim (it never decodes the
// bound); the worker decodes it at the trust boundary and rejects any
// malformed frame.
// ---------------------------------------------------------------------------

use crate::catalog::MAX_COLUMNS;
use crate::circuit::AggFunc;
use crate::range::RangeDescriptor;

/// ORDER BY keys apply in sequence; a spec carries at most this many.
pub const MAX_ORDER_KEYS: usize = 16;
/// A `pk IN (…)` list larger than this rejects cleanly at bind time.
pub const MAX_PK_SET_KEYS: usize = 65_536;
/// Decode-side ceiling on a full encoded `ReadSpec` blob.
pub const MAX_READ_SPEC_BYTES: usize = 2 << 20;

const VERSION: u8 = 2;

const BOUND_NONE: u8 = 0;
const BOUND_PK_RANGE: u8 = 1;
const BOUND_INDEX_RANGE: u8 = 2;
const BOUND_PK_SET: u8 = 3;

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
    /// Secondary-index range: the packed index column list and the descriptor.
    /// The worker decides the walk from the range column's type: a wide-int
    /// (U128/UUID) index — the one index-eligible family the predicate VM
    /// cannot express, whose conjunct the SQL layer therefore strips — runs an
    /// un-gated byte-exact `BoundedIndexCursor`; a ≤8-byte-integer index is
    /// selectivity-gated (may fall back to a full cursor) and the conjunct
    /// stays in the predicate. Both sides derive the split from the same
    /// column type, so a stripped conjunct is always re-imposed by the walk.
    IndexRange { idx_cols: u64, desc: RangeDescriptor },
    /// `pk IN (…)` for a single-column PK. Values are raw native keys widened to
    /// `u128` (`FixedInt::pack`), and must be **distinct after truncation to the
    /// PK's width** — the worker rejects the rest. Wire order is irrelevant: the
    /// worker OPK-sorts the keys before its forward gather.
    PkSet(Vec<u128>),
}

impl ReadBound {
    const fn kind(&self) -> u8 {
        match self {
            ReadBound::None => BOUND_NONE,
            ReadBound::PkRange(_) => BOUND_PK_RANGE,
            ReadBound::IndexRange { .. } => BOUND_INDEX_RANGE,
            ReadBound::PkSet(_) => BOUND_PK_SET,
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

/// Append a `u32`-length-prefixed byte section — the one variable-length
/// section shape this format uses, and the exact inverse of
/// [`Reader::bytes32`].
fn put_bytes32(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// Pack a ScanSpec request's control-block `seek_pk_extra` blob: the encoded
/// `ReadSpec` followed by the reply-schema wire block, each `u32`-length-
/// prefixed. Bundling both in the arbitrary-length `seek_pk_extra` BLOB lets the
/// master forward the blob to the workers without decoding the spec (it peeks
/// only the bound header, [`peek_pk_range`], to route the request). The block
/// does not travel back: the worker decodes it into the read's output shape,
/// and the client decodes the reply against its own copy.
pub fn pack_scan_spec_extra(spec: &[u8], reply_block: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + spec.len() + reply_block.len());
    put_bytes32(&mut out, spec);
    put_bytes32(&mut out, reply_block);
    out
}

/// Split a ScanSpec `seek_pk_extra` blob back into `(spec_bytes, reply_block)`
/// at the trust boundary — rejecting a truncated / trailing-byte frame.
pub fn unpack_scan_spec_extra(extra: &[u8]) -> Result<(&[u8], &[u8]), String> {
    let mut r = Reader::new(extra);
    let spec = r.bytes32()?;
    let block = r.bytes32()?;
    if r.remaining() != 0 {
        return Err(format!("scan_spec extra: {} trailing bytes", r.remaining()));
    }
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
    let mut r = Reader::new(buf);
    if r.u8().ok()? != VERSION || r.u8().ok()? != BOUND_PK_RANGE {
        return None;
    }
    r.u8().ok()?; // sink tag
    r.u8().ok()?; // reserved
    read_range_descriptor(&mut r).ok()
}

/// A bounds-checked forward reader over the encoded blob — every field access
/// is a `take` that rejects a truncated frame rather than panicking.
struct Reader<'a> {
    buf: &'a [u8],
    off: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, off: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.off.checked_add(n).ok_or("read_spec: length overflow")?;
        if end > self.buf.len() {
            return Err(format!(
                "read_spec: truncated (need {n} bytes at offset {}, {} remain)",
                self.off,
                self.buf.len() - self.off
            ));
        }
        let s = &self.buf[self.off..end];
        self.off = end;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn u128(&mut self) -> Result<u128, String> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }

    /// A `u32`-length-prefixed byte section — the inverse of [`put_bytes32`].
    /// The length is bounds-checked by `take`, so a hostile prefix is a clean
    /// `Err` rather than an over-large allocation.
    fn bytes32(&mut self) -> Result<&'a [u8], String> {
        let n = self.u32()? as usize;
        self.take(n)
    }

    /// The next byte without consuming it — used to compute a variable-length
    /// `RangeDescriptor`'s span from its leading `n_eq`.
    fn peek_u8(&self) -> Result<u8, String> {
        self.buf
            .get(self.off)
            .copied()
            .ok_or_else(|| "read_spec: truncated reading descriptor length".to_string())
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.off
    }
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
    /// Serialise to a version-prefixed LE byte sequence (§ layout below).
    pub fn encode(&self) -> Vec<u8> {
        // Header: version | bound_kind | sink_tag | reserved. One up-front
        // reservation covering every section (64 covers the fixed header,
        // range descriptors, and section length prefixes).
        let sink_tag = match &self.sink {
            ReadSink::Rows { .. } => SINK_ROWS,
            ReadSink::Fold(_) => SINK_FOLD,
        };
        let cap = 64
            + self.predicate.len()
            + match &self.bound {
                ReadBound::PkSet(keys) => 16 * keys.len(),
                _ => 0,
            }
            + match &self.sink {
                ReadSink::Rows { projection, order, .. } => projection.len() + 4 * order.len(),
                ReadSink::Fold(agg) => 2 * agg.group_cols.len() + 3 * agg.aggs.len(),
            };
        let mut out = Vec::with_capacity(cap);
        out.extend_from_slice(&[VERSION, self.bound.kind(), sink_tag, 0u8]);

        match &self.bound {
            ReadBound::None => {}
            ReadBound::PkRange(desc) => out.extend_from_slice(&desc.encode()),
            ReadBound::IndexRange { idx_cols, desc } => {
                out.extend_from_slice(&idx_cols.to_le_bytes());
                out.extend_from_slice(&desc.encode());
            }
            ReadBound::PkSet(keys) => {
                out.extend_from_slice(&(keys.len() as u32).to_le_bytes());
                for k in keys {
                    out.extend_from_slice(&k.to_le_bytes());
                }
            }
        }

        put_bytes32(&mut out, &self.predicate);

        match &self.sink {
            ReadSink::Rows {
                projection,
                order,
                limit_k,
            } => {
                out.push(order.len() as u8);
                out.extend_from_slice(&limit_k.to_le_bytes());
                for key in order {
                    out.extend_from_slice(&key.col.to_le_bytes());
                    let mut flags = 0u8;
                    if key.desc {
                        flags |= ORDER_DESC;
                    }
                    if key.nulls_first {
                        flags |= ORDER_NULLS_FIRST;
                    }
                    out.push(flags);
                    out.push(0u8); // reserved
                }
                put_bytes32(&mut out, projection);
            }
            ReadSink::Fold(agg) => {
                out.extend_from_slice(&(agg.group_cols.len() as u16).to_le_bytes());
                for &c in &agg.group_cols {
                    out.extend_from_slice(&c.to_le_bytes());
                }
                out.push(agg.aggs.len() as u8);
                for item in &agg.aggs {
                    out.push(item.op as u8);
                    out.extend_from_slice(&item.src_col.to_le_bytes());
                }
            }
        }
        out
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
        let mut r = Reader::new(buf);
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
                let desc = read_range_descriptor(&mut r)?;
                ReadBound::IndexRange { idx_cols, desc }
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
                let mut keys = Vec::with_capacity(count);
                for _ in 0..count {
                    keys.push(r.u128()?);
                }
                ReadBound::PkSet(keys)
            }
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

        if r.remaining() != 0 {
            return Err(format!("read_spec: {} trailing bytes", r.remaining()));
        }

        Ok(ReadSpec { bound, predicate, sink })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::range::Cut::{After, Before};

    fn sample_order() -> Vec<OrderKey> {
        vec![
            OrderKey {
                col: 3,
                desc: false,
                nulls_first: true,
            },
            OrderKey {
                col: 0,
                desc: true,
                nulls_first: false,
            },
        ]
    }

    #[test]
    fn roundtrips_every_bound() {
        let specs = [
            ReadSpec {
                bound: ReadBound::None,
                predicate: vec![1, 2, 3, 4],
                sink: ReadSink::Rows {
                    projection: vec![],
                    order: sample_order(),
                    limit_k: 42,
                },
            },
            ReadSpec {
                bound: ReadBound::PkRange(RangeDescriptor::new(&[], After(10), After(u64::MAX as u128))),
                predicate: vec![],
                sink: ReadSink::Rows {
                    projection: vec![9, 9, 9],
                    order: vec![],
                    limit_k: 0,
                },
            },
            ReadSpec {
                bound: ReadBound::IndexRange {
                    idx_cols: 0x0000_0002_0000_0001,
                    desc: RangeDescriptor::new(&[7], Before(1), Before(u128::MAX)),
                },
                predicate: vec![5, 5],
                sink: ReadSink::Rows {
                    projection: vec![6],
                    order: sample_order(),
                    limit_k: 100,
                },
            },
            ReadSpec {
                bound: ReadBound::PkSet(vec![5, 10, 3, 99]),
                predicate: vec![],
                sink: ReadSink::Rows {
                    projection: vec![],
                    order: vec![OrderKey {
                        col: 7,
                        desc: false,
                        nulls_first: false,
                    }],
                    limit_k: 8,
                },
            },
        ];
        for spec in specs {
            let bytes = spec.encode();
            assert!(bytes.len() <= MAX_READ_SPEC_BYTES);
            assert_eq!(ReadSpec::decode(&bytes), Ok(spec.clone()), "{spec:?}");
        }
    }

    #[test]
    fn accepts_unsorted_pk_set_including_widened_signed() {
        // `pk IN (-1, 5)` on a signed PK widens to `[0xFFFF…FF, 5]` — typed order
        // is increasing but raw-u128 order is DECREASING. The decoder must accept
        // this without a sortedness check.
        let spec = ReadSpec {
            bound: ReadBound::PkSet(vec![u64::MAX as u128, 5]),
            predicate: vec![],
            sink: ReadSink::all_rows(),
        };
        let bytes = spec.encode();
        assert_eq!(ReadSpec::decode(&bytes), Ok(spec));
    }

    fn empty_spec() -> ReadSpec {
        ReadSpec {
            bound: ReadBound::None,
            predicate: vec![],
            sink: ReadSink::all_rows(),
        }
    }

    #[test]
    fn decode_rejects_bad_version() {
        let mut bytes = empty_spec().encode();
        bytes[0] = 99;
        assert!(ReadSpec::decode(&bytes).unwrap_err().contains("version"));
    }

    #[test]
    fn decode_rejects_unknown_kind() {
        let mut bytes = empty_spec().encode();
        bytes[1] = 9;
        assert!(ReadSpec::decode(&bytes).unwrap_err().contains("bound kind"));
    }

    #[test]
    fn decode_rejects_unknown_sink_tag() {
        let mut bytes = empty_spec().encode();
        bytes[2] = 9;
        assert!(ReadSpec::decode(&bytes).unwrap_err().contains("sink tag"));
    }

    #[test]
    fn decode_rejects_order_key_over_cap() {
        // Layout of the empty Rows spec: 4-byte header | u32 predicate len |
        // n_order at offset 8.
        let mut bytes = empty_spec().encode();
        bytes[8] = (MAX_ORDER_KEYS + 1) as u8;
        assert!(ReadSpec::decode(&bytes).unwrap_err().contains("order keys exceeds cap"));
    }

    #[test]
    fn decode_rejects_pk_set_over_cap() {
        // Hand-build a header + PkSet count exceeding the cap (the count is read
        // before any per-key bytes, so no keys are needed to trip it).
        let mut bytes = vec![VERSION, BOUND_PK_SET, SINK_ROWS, 0];
        bytes.extend_from_slice(&((MAX_PK_SET_KEYS + 1) as u32).to_le_bytes());
        assert!(ReadSpec::decode(&bytes).unwrap_err().contains("PkSet count"));
    }

    /// Duplicates round-trip: rejecting them needs the PK width, which this
    /// decoder does not have — the worker's OPK-sorted gather rejects them.
    #[test]
    fn decode_keeps_duplicate_pk_set_keys() {
        let spec = ReadSpec {
            bound: ReadBound::PkSet(vec![5, 5]),
            predicate: vec![],
            sink: ReadSink::all_rows(),
        };
        assert_eq!(ReadSpec::decode(&spec.encode()).unwrap(), spec);
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let mut bytes = empty_spec().encode();
        bytes.push(0);
        assert!(ReadSpec::decode(&bytes).unwrap_err().contains("trailing"));
    }

    #[test]
    fn decode_rejects_truncation() {
        let bytes = ReadSpec {
            bound: ReadBound::PkSet(vec![1, 2, 3]),
            predicate: vec![],
            sink: ReadSink::all_rows(),
        }
        .encode();
        // Chop the last key's bytes off.
        assert!(ReadSpec::decode(&bytes[..bytes.len() - 4]).is_err());
    }

    #[test]
    fn decode_rejects_oversized_blob() {
        let big = vec![0u8; MAX_READ_SPEC_BYTES + 1];
        assert!(ReadSpec::decode(&big).unwrap_err().contains("exceeds cap"));
    }

    #[test]
    fn scan_spec_extra_roundtrips() {
        let spec = vec![1u8, 2, 3, 4, 5];
        let block = vec![9u8; 40];
        let packed = pack_scan_spec_extra(&spec, &block);
        assert_eq!(unpack_scan_spec_extra(&packed), Ok((spec.as_slice(), block.as_slice())));
        // Empty block is valid (identity projection carries a real block anyway,
        // but the codec must not choke on either side being empty).
        let packed = pack_scan_spec_extra(&[], &block);
        assert_eq!(unpack_scan_spec_extra(&packed), Ok(([].as_slice(), block.as_slice())));
    }

    #[test]
    fn scan_spec_extra_rejects_truncation_and_trailing() {
        let packed = pack_scan_spec_extra(&[1, 2, 3], &[4, 5]);
        assert!(unpack_scan_spec_extra(&packed[..packed.len() - 1]).is_err());
        let mut long = packed.clone();
        long.push(0);
        assert!(unpack_scan_spec_extra(&long).is_err());
        assert!(unpack_scan_spec_extra(&[]).is_err());
    }

    #[test]
    fn roundtrips_fold_sink() {
        let specs = [
            // Grouped aggregate: two group cols, COUNT(*) + SUM(col 2).
            ReadSpec {
                bound: ReadBound::None,
                predicate: vec![1, 2, 3],
                sink: ReadSink::Fold(AggReadSpec {
                    group_cols: vec![0, 3],
                    aggs: vec![
                        AggReadItem {
                            op: AggFunc::Count,
                            src_col: 0,
                        },
                        AggReadItem {
                            op: AggFunc::Sum,
                            src_col: 2,
                        },
                    ],
                }),
            },
            // Global aggregate over a PK range: no group cols, MIN/MAX.
            ReadSpec {
                bound: ReadBound::PkRange(RangeDescriptor::new(&[], After(1), After(9))),
                predicate: vec![],
                sink: ReadSink::Fold(AggReadSpec {
                    group_cols: vec![],
                    aggs: vec![
                        AggReadItem {
                            op: AggFunc::Min,
                            src_col: 4,
                        },
                        AggReadItem {
                            op: AggFunc::Max,
                            src_col: 4,
                        },
                    ],
                }),
            },
            // DISTINCT: group cols, no aggs.
            ReadSpec {
                bound: ReadBound::None,
                predicate: vec![],
                sink: ReadSink::Fold(AggReadSpec {
                    group_cols: vec![1, 2, 5],
                    aggs: vec![],
                }),
            },
        ];
        for spec in specs {
            let bytes = spec.encode();
            assert!(bytes.len() <= MAX_READ_SPEC_BYTES);
            assert_eq!(ReadSpec::decode(&bytes), Ok(spec.clone()), "{spec:?}");
        }
    }

    /// Header + empty predicate of a hand-built fold-sink blob.
    fn fold_header() -> Vec<u8> {
        let mut bytes = vec![VERSION, BOUND_NONE, SINK_FOLD, 0];
        bytes.extend_from_slice(&0u32.to_le_bytes()); // predicate len
        bytes
    }

    #[test]
    fn fold_rejects_bad_op_code() {
        let mut bytes = fold_header();
        bytes.extend_from_slice(&0u16.to_le_bytes()); // n_group_cols = 0
        bytes.push(1); // n_aggs = 1
        bytes.push(7); // no such AggFunc
        bytes.extend_from_slice(&0u16.to_le_bytes()); // src_col
        assert!(ReadSpec::decode(&bytes).unwrap_err().contains("unknown aggregate op"));
    }

    #[test]
    fn fold_rejects_over_cap() {
        // Group-col count over the cap (read before per-col bytes, so no col
        // bytes are needed to trip).
        let mut bytes = fold_header();
        bytes.extend_from_slice(&((MAX_COLUMNS + 1) as u16).to_le_bytes());
        assert!(ReadSpec::decode(&bytes).unwrap_err().contains("group cols exceeds cap"));

        // And an over-cap agg-item count (zero group cols first).
        let mut bytes = fold_header();
        bytes.extend_from_slice(&0u16.to_le_bytes()); // n_group_cols = 0
        bytes.push((MAX_COLUMNS + 1) as u8);
        assert!(ReadSpec::decode(&bytes).unwrap_err().contains("agg items exceeds cap"));
    }
}
