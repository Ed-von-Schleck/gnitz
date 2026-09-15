// ---------------------------------------------------------------------------
// ReadSpec — an ad-hoc bounded read: a bound, a predicate, then a sink (rows or a
// fold). The master routes a request blob by `peek_bound`; the worker's `decode`
// is the trust boundary.
// ---------------------------------------------------------------------------

use crate::circuit::{read_aggs, read_cols, read_compute_map, write_aggs, write_cols, write_compute_map};
use crate::circuit::{read_order_keys, write_order_keys, AggDescriptor, ComputeMap};
use crate::codec::{Reader, Writer};
use crate::range::{read_index_bound, read_range_descriptor, write_index_bound, write_range_descriptor};
use crate::range::{IndexBound, IndexWalk, RangeDescriptor};
use crate::MAX_PK_BYTES;

/// ORDER BY keys apply in sequence; a spec carries at most this many.
pub const MAX_ORDER_KEYS: usize = 16;
/// Decode-side ceiling on one `pk IN (…)` gather. A longer list is served as
/// several gathers or as an ordinary predicate scan, never rejected.
pub const MAX_PK_SET_KEYS: usize = 65_536;
/// Decode-side ceiling on an encoded `ReadSpec`, the reply block excluded.
pub(crate) const MAX_READ_SPEC_BYTES: usize = 2 << 20;
/// The key bytes one request may carry: half the spec cap, the rest left to the
/// predicate and the map.
const MAX_PK_SET_BYTES: usize = MAX_READ_SPEC_BYTES / 2;

const BOUND_NONE: u8 = 0;
const BOUND_PK_RANGE: u8 = 1;
const BOUND_INDEX_RANGE: u8 = 2;
const BOUND_PK_SET: u8 = 3;

const SINK_ROWS: u8 = 0;
const SINK_FOLD: u8 = 1;

/// One ORDER BY key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderKey {
    /// A full reply-schema column index (not a dense payload index).
    pub col: u16,
    pub desc: bool,
    pub nulls_first: bool,
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
    /// no more than `MAX_PK_SET_BYTES` of them.
    pub const fn max_per_request(stride: usize) -> usize {
        let by_bytes = MAX_PK_SET_BYTES / stride;
        if by_bytes < MAX_PK_SET_KEYS {
            by_bytes
        } else {
            MAX_PK_SET_KEYS
        }
    }

    /// Whether this list goes out as one request.
    pub fn fits_one_request(&self) -> bool {
        self.len() <= Self::max_per_request(self.stride())
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

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
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
    /// Secondary-index range walk. An index holds no row with a NULL in any
    /// indexed column, so neither does the walk.
    IndexRange { bound: IndexBound, walk: IndexWalk },
    /// An exact key set, at any PK arity — a `pk IN (…)` gather or one fully
    /// pinned key.
    PkSet(PkKeys),
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

impl ReadSpec {
    /// Every row `bound` walks, unfiltered and unmapped.
    pub fn all_rows(bound: ReadBound) -> Self {
        ReadSpec {
            bound,
            predicate: Vec::new(),
            sink: ReadSink::all_rows(),
        }
    }

    /// The SCAN_SPEC request blob: `reply_block`, then this spec. The block leads so
    /// `decode` caps the spec alone; a schema's column names are unbounded.
    pub fn encode(&self, reply_block: &[u8]) -> Vec<u8> {
        let keys = match &self.bound {
            ReadBound::PkSet(k) => k.as_bytes().len(),
            _ => 0,
        };
        let map = self.sink.map.as_ref().map_or(0, |m| m.program.len());
        let mut w = Writer::with_capacity(64 + reply_block.len() + self.predicate.len() + keys + map);
        w.bytes32(reply_block);

        write_read_bound(&mut w, &self.bound);

        w.bytes32(&self.predicate);

        match &self.sink.map {
            Some(m) => {
                w.u8(1);
                write_compute_map(&mut w, &m.out_cols, &m.program);
            }
            None => {
                w.u8(0);
            }
        }

        match &self.sink.kind {
            SinkKind::Rows { order, limit_k } => {
                w.u8(SINK_ROWS).u64(*limit_k);
                write_order_keys(&mut w, order);
            }
            SinkKind::Fold(agg) => {
                w.u8(SINK_FOLD);
                write_cols(&mut w, &agg.group_cols);
                write_aggs(&mut w, &agg.aggs);
            }
        }
        w.into_vec()
    }

    /// Decode at the trust boundary: the spec, and the reply block it carries.
    /// A malformed frame is an `Err`.
    pub fn decode(buf: &[u8]) -> Result<(ReadSpec, &[u8]), String> {
        let mut r = Reader::new(buf, "read_spec");
        let block = r.bytes32()?;
        if r.remaining() > MAX_READ_SPEC_BYTES {
            return Err(format!(
                "read_spec: {} bytes exceeds cap {MAX_READ_SPEC_BYTES}",
                r.remaining()
            ));
        }

        let bound = read_read_bound(&mut r)?;

        let predicate = r.bytes32()?.to_vec();

        let map = match r.u8()? {
            0 => None,
            1 => Some(read_compute_map(&mut r).map_err(|e| format!("read_spec: {e}"))?),
            other => return Err(format!("read_spec: map presence byte {other} is not 0 or 1")),
        };

        let kind = match r.u8()? {
            SINK_ROWS => {
                let limit_k = r.u64()?;
                let order = read_order_keys(&mut r).map_err(|e| format!("read_spec: {e}"))?;
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

        Ok((
            ReadSpec {
                bound,
                predicate,
                sink: ReadSink { map, kind },
            },
            block,
        ))
    }
}

/// Splice a [`ReadBound`] into a larger blob: its kind tag, then that kind's
/// layout. [`peek_bound`] reads the same layout.
pub(crate) fn write_read_bound(w: &mut Writer, b: &ReadBound) {
    match b {
        ReadBound::None => {
            w.u8(BOUND_NONE);
        }
        ReadBound::PkRange(desc) => {
            w.u8(BOUND_PK_RANGE);
            write_range_descriptor(w, desc);
        }
        ReadBound::IndexRange { bound, walk } => {
            w.u8(BOUND_INDEX_RANGE);
            write_index_bound(w, bound);
            w.u8(match walk {
                IndexWalk::Optional => 0,
                IndexWalk::Required => 1,
            });
        }
        ReadBound::PkSet(keys) => {
            w.u8(BOUND_PK_SET)
                .u8(keys.stride)
                .u32(keys.len() as u32)
                .raw(keys.as_bytes());
        }
    }
}

/// Read a [`ReadBound`] at the trust boundary — the reader dual of
/// [`write_read_bound`]. A malformed bound is an `Err`.
pub(crate) fn read_read_bound(r: &mut Reader) -> Result<ReadBound, String> {
    Ok(match r.u8()? {
        BOUND_NONE => ReadBound::None,
        BOUND_PK_RANGE => ReadBound::PkRange(read_range_descriptor(r)?),
        BOUND_INDEX_RANGE => {
            let bound = read_index_bound(r).map_err(|e| format!("read_spec: {e}"))?;
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
            if !bytes.chunks_exact(stride).is_sorted_by(|a, b| a < b) {
                return Err("read_spec: PkSet keys are not strictly ascending".to_string());
            }
            ReadBound::PkSet(PkKeys {
                stride: stride as u8,
                bytes: bytes.to_vec(),
            })
        }
        other => return Err(format!("read_spec: unknown bound kind {other}")),
    })
}

/// The bound of an encoded request that routing reads, without decoding the
/// rest. [`ReadSpec::decode`] on the worker stays the trust boundary.
pub enum BoundPeek<'a> {
    PkRange(RangeDescriptor),
    PkSet(PkSetPeek<'a>),
}

/// A request's `PkSet` key list, borrowed in place from its blob.
pub struct PkSetPeek<'a> {
    blob: &'a [u8],
    /// Offset of the list's `u32` key count inside `blob`.
    count_at: usize,
    pub stride: usize,
    pub keys: &'a [u8],
}

impl PkSetPeek<'_> {
    /// The blob with its key list replaced by `keys`, a subsequence of
    /// [`Self::keys`] — so still strictly ascending — and every other byte
    /// copied verbatim: it decodes to the same spec over those keys.
    pub fn with_keys(&self, keys: &[u8]) -> Vec<u8> {
        let tail = &self.blob[self.count_at + 4 + self.keys.len()..];
        let mut w = Writer::with_capacity(self.count_at + 4 + keys.len() + tail.len());
        w.raw(&self.blob[..self.count_at])
            .u32((keys.len() / self.stride) as u32)
            .raw(keys)
            .raw(tail);
        w.into_vec()
    }
}

/// The routing bound of a SCAN_SPEC request blob. `None` for any other bound or
/// a truncated prefix.
pub fn peek_bound(blob: &[u8]) -> Option<BoundPeek<'_>> {
    let mut r = Reader::new(blob, "read_spec");
    r.bytes32().ok()?;
    match r.u8().ok()? {
        BOUND_PK_RANGE => read_range_descriptor(&mut r).ok().map(BoundPeek::PkRange),
        BOUND_PK_SET => {
            let stride = r.u8().ok()? as usize;
            let count_at = blob.len() - r.remaining();
            let count = r.u32().ok()? as usize;
            let keys = r.take(count.checked_mul(stride)?).ok()?;
            Some(BoundPeek::PkSet(PkSetPeek { blob, count_at, stride, keys }))
        }
        _ => None,
    }
}

#[cfg(test)]
#[path = "tests/read_spec.rs"]
mod tests;
