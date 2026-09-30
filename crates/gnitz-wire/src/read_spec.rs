//! ReadSpec — an ad-hoc bounded read: a bound, a predicate, then a sink (rows or a
//! fold). The master routes a request blob by `peek_bound`; the worker's `decode`
//! is the trust boundary.

use crate::circuit::{read_aggs, read_cols, read_compute_map, write_aggs, write_cols, write_compute_map};
use crate::circuit::{read_order_keys, write_order_keys, AggDescriptor, ComputeMap};
use std::ops::Range;

use crate::codec::{decode_all, Reader, Writer};
use crate::range::{read_key_range, write_key_range, KeyRange};
use crate::MAX_PK_BYTES;

/// ORDER BY keys apply in sequence; a spec carries at most this many.
pub const MAX_ORDER_KEYS: usize = 16;

const BOUND_NONE: u8 = 0;
const BOUND_RANGE: u8 = 1;
const BOUND_PK_SET: u8 = 2;

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
/// not the SELECT list. Its reply is a `Reduce`'s output over the same group set.
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
        /// the reply layout). `len ≤ MAX_ORDER_KEYS`; empty unless `limit_k > 0`.
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
/// sweeps. Any number of keys: the request frame is the only bound on a list.
/// Fields private: every constructor sorts and dedups, or validates.
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

    /// Keys of `stride` already concatenated in strictly ascending order.
    pub fn from_sorted(stride: usize, bytes: Vec<u8>) -> Self {
        assert!((1..=MAX_PK_BYTES).contains(&stride), "PkKeys: stride {stride}");
        assert_eq!(bytes.len() % stride, 0, "PkKeys: key width");
        assert!(
            strictly_ascending(&bytes, stride),
            "PkKeys: keys are not strictly ascending"
        );
        PkKeys { stride: stride as u8, bytes }
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

    /// The first and last key, or `None` for an empty list.
    pub fn bounds(&self) -> Option<(&[u8], &[u8])> {
        let first = self.iter().next()?;
        Some((first, &self.bytes[self.bytes.len() - self.stride()..]))
    }
}

/// Whether `bytes`, read as keys of `stride`, strictly ascend.
fn strictly_ascending(bytes: &[u8], stride: usize) -> bool {
    bytes.chunks_exact(stride).is_sorted_by(|a, b| a < b)
}

/// The bound a `ReadSpec` walks before the predicate and the sink. A range carries
/// key images; a `PkSet` carries OPK keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadBound {
    /// Full merged cursor — a plain scan with a server-side predicate.
    None,
    /// Exact, and holds no row with a NULL in a listed column.
    Range(KeyRange),
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

    /// The SCAN_SPEC request blob.
    pub fn encode(&self) -> Vec<u8> {
        let keys = match &self.bound {
            ReadBound::PkSet(k) => k.as_bytes().len(),
            _ => 0,
        };
        let map = self.sink.map.as_ref().map_or(0, |m| m.program.len());
        let mut w = Writer::with_capacity(64 + self.predicate.len() + keys + map);

        write_read_bound(&mut w, &self.bound);

        w.bytes32(&self.predicate);

        match &self.sink.map {
            Some(m) => {
                w.bool(true);
                write_compute_map(&mut w, m);
            }
            None => {
                w.bool(false);
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

    /// Decode at the trust boundary. A malformed frame is an `Err`.
    pub fn decode(buf: &[u8]) -> Result<ReadSpec, String> {
        decode_all(buf, "read_spec", |r| {
            let bound = read_read_bound(r)?;

            let predicate = r.bytes32()?.to_vec();

            let map = if r.bool()? { Some(read_compute_map(r)?) } else { None };

            let kind = match r.u8()? {
                SINK_ROWS => {
                    let limit_k = r.u64()?;
                    let order = read_order_keys(r)?;
                    if limit_k == 0 && !order.is_empty() {
                        return Err("an order key without a cut".into());
                    }
                    SinkKind::Rows { order, limit_k }
                }
                SINK_FOLD => {
                    // Both counted sections take the circuit codec's caps and
                    // domain checks.
                    let group_cols = read_cols(r)?;
                    let aggs = read_aggs(r)?;
                    SinkKind::Fold(AggReadSpec { group_cols, aggs })
                }
                other => return Err(format!("unknown sink tag {other}")),
            };

            Ok(ReadSpec {
                bound,
                predicate,
                sink: ReadSink { map, kind },
            })
        })
    }
}

/// The one `PkSet` layout: stride, key count, then the keys.
fn write_pk_set(w: &mut Writer, stride: usize, keys: &[u8]) {
    w.u8(stride as u8).u32((keys.len() / stride) as u32).raw(keys);
}

/// [`write_pk_set`]'s inverse: the stride and the keys, not checked for order.
fn read_pk_set<'a>(r: &mut Reader<'a>) -> Result<(usize, &'a [u8]), String> {
    let stride = r.u8()? as usize;
    if !(1..=MAX_PK_BYTES).contains(&stride) {
        return Err(format!("PkSet stride {stride} outside 1..={MAX_PK_BYTES}"));
    }
    let count = r.u32()? as usize;
    let keys = r.take(count * stride)?;
    Ok((stride, keys))
}

/// Splice a [`ReadBound`] into a larger blob: its kind tag, then that kind's
/// layout. [`peek_bound`] reads the same layout.
pub(crate) fn write_read_bound(w: &mut Writer, b: &ReadBound) {
    match b {
        ReadBound::None => {
            w.u8(BOUND_NONE);
        }
        ReadBound::Range(range) => {
            w.u8(BOUND_RANGE);
            write_key_range(w, range);
        }
        ReadBound::PkSet(keys) => {
            w.u8(BOUND_PK_SET);
            write_pk_set(w, keys.stride(), keys.as_bytes());
        }
    }
}

/// Read a [`ReadBound`] at the trust boundary — the reader dual of
/// [`write_read_bound`]. A malformed bound is an `Err`.
pub(crate) fn read_read_bound(r: &mut Reader) -> Result<ReadBound, String> {
    Ok(match r.u8()? {
        BOUND_NONE => ReadBound::None,
        BOUND_RANGE => ReadBound::Range(read_key_range(r)?),
        BOUND_PK_SET => {
            let (stride, bytes) = read_pk_set(r)?;
            if !strictly_ascending(bytes, stride) {
                return Err("PkSet keys are not strictly ascending".to_string());
            }
            ReadBound::PkSet(PkKeys {
                stride: stride as u8,
                bytes: bytes.to_vec(),
            })
        }
        other => return Err(format!("unknown bound kind {other}")),
    })
}

/// The bound of an encoded request that routing reads, without decoding the
/// rest. [`ReadSpec::decode`] on the worker stays the trust boundary.
pub enum BoundPeek<'a> {
    Range(KeyRange),
    PkSet(PkSetPeek<'a>),
}

/// A request's `PkSet` key list, borrowed in place from its blob.
pub struct PkSetPeek<'a> {
    blob: &'a [u8],
    /// The list's whole encoding inside `blob`: stride, count and keys.
    span: Range<usize>,
    pub stride: usize,
    pub keys: &'a [u8],
}

impl PkSetPeek<'_> {
    /// The blob with its key list replaced by `keys`, a subsequence of
    /// [`Self::keys`] — so still strictly ascending — and every other byte
    /// copied verbatim: it decodes to the same spec over those keys.
    pub fn with_keys(&self, keys: &[u8]) -> Vec<u8> {
        debug_assert!(
            strictly_ascending(keys, self.stride),
            "a subsequence of the peeked keys"
        );
        let mut w = Writer::with_capacity(self.blob.len() - self.keys.len() + keys.len());
        w.raw(&self.blob[..self.span.start]);
        write_pk_set(&mut w, self.stride, keys);
        w.raw(&self.blob[self.span.end..]);
        w.into_vec()
    }
}

/// The routing bound of a SCAN_SPEC request blob. `None` for any other bound or
/// a malformed prefix.
pub fn peek_bound(blob: &[u8]) -> Option<BoundPeek<'_>> {
    let mut r = Reader::new(blob);
    match r.u8().ok()? {
        BOUND_RANGE => read_key_range(&mut r).ok().map(BoundPeek::Range),
        BOUND_PK_SET => {
            let start = r.pos();
            let (stride, keys) = read_pk_set(&mut r).ok()?;
            Some(BoundPeek::PkSet(PkSetPeek { blob, span: start..r.pos(), stride, keys }))
        }
        _ => None,
    }
}

#[cfg(test)]
#[path = "tests/read_spec.rs"]
mod tests;
