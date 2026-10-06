//! ReadSpec — an ad-hoc bounded read: a bound, a predicate, then a sink (rows or a
//! fold). The master routes a request blob by `peek_bound`; the worker's `decode`
//! is the trust boundary.

use crate::circuit::{AggDescriptor, ComputeMap};
use std::num::NonZeroU64;
use std::ops::Range;

use crate::codec::{decode_all, Reader, Wire, Writer};
use crate::range::KeyRange;
use crate::{MAX_COLUMNS, MAX_PK_BYTES};

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

const ORDER_DESC: u8 = 1 << 0;
const ORDER_NULLS_FIRST: u8 = 1 << 1;

/// The column, then a flag byte; an unknown flag bit is a refusal.
impl Wire for OrderKey {
    fn write(&self, w: &mut Writer) {
        let mut flags = 0u8;
        if self.desc {
            flags |= ORDER_DESC;
        }
        if self.nulls_first {
            flags |= ORDER_NULLS_FIRST;
        }
        w.u16(self.col).u8(flags);
    }

    fn read(r: &mut Reader) -> Result<Self, String> {
        let col = r.u16()?;
        let flags = r.flags(ORDER_DESC | ORDER_NULLS_FIRST)?;
        Ok(OrderKey {
            col,
            desc: flags & ORDER_DESC != 0,
            nulls_first: flags & ORDER_NULLS_FIRST != 0,
        })
    }
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

/// A rows sink's cut: stop once the forwarded rows' summed weight reaches `k`,
/// taking the `order`-smallest rows when `order` is non-empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowsCut {
    /// OFFSET + LIMIT in logical rows (summed weight).
    pub k: NonZeroU64,
    /// ORDER BY keys applied in sequence; `col` indexes the sink input (= the
    /// reply layout). `len ≤ MAX_ORDER_KEYS`; empty = the first rows the walk
    /// yields.
    pub order: Vec<OrderKey>,
}

/// The two sinks. A fold carries no cut: all SQL-level finishing on an
/// aggregate result is client-side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkKind {
    /// Forward rows, every one or the ones `cut` keeps.
    Rows { cut: Option<RowsCut> },
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
            kind: SinkKind::Rows { cut: None },
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
        Self::checked(stride, bytes).expect("PkKeys")
    }

    /// [`Self::from_sorted`] for keys off the wire: `Err` where they do not
    /// strictly ascend.
    pub fn checked(stride: usize, bytes: Vec<u8>) -> Result<Self, String> {
        match strictly_ascending(&bytes, stride) {
            true => Ok(PkKeys { stride: stride as u8, bytes }),
            false => Err("keys are not strictly ascending".to_string()),
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

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub fn iter(&self) -> std::slice::ChunksExact<'_, u8> {
        self.bytes.chunks_exact(self.stride())
    }

    /// Whether `key`, of this list's stride, is one of its keys.
    pub fn contains(&self, key: &[u8]) -> bool {
        debug_assert_eq!(key.len(), self.stride());
        let (mut lo, mut hi) = (0, self.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.bytes[mid * self.stride()..][..self.stride()].cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return true,
            }
        }
        false
    }

    /// The first and last key, or `None` for an empty list.
    pub fn bounds(&self) -> Option<(&[u8], &[u8])> {
        let first = self.iter().next()?;
        Some((first, &self.bytes[self.bytes.len() - self.stride()..]))
    }
}

/// Whether `bytes`, read as keys of `stride`, strictly ascend.
pub fn strictly_ascending(bytes: &[u8], stride: usize) -> bool {
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

    /// Whether this is the relation whole: nothing bounded, filtered, mapped,
    /// cut or folded.
    pub fn is_whole(&self) -> bool {
        matches!(self.bound, ReadBound::None)
            && self.predicate.is_empty()
            && matches!(
                self.sink,
                ReadSink {
                    map: None,
                    kind: SinkKind::Rows { cut: None }
                }
            )
    }

    /// The SCAN_SPEC request blob.
    pub fn encode(&self) -> Vec<u8> {
        let keys = match &self.bound {
            ReadBound::PkSet(k) => k.as_bytes().len(),
            _ => 0,
        };
        let map = self.sink.map.as_ref().map_or(0, |m| m.program.len());
        let mut w = Writer::with_capacity(64 + self.predicate.len() + keys + map);

        w.put(&self.bound).bytes32(&self.predicate);

        match &self.sink.map {
            Some(m) => w.bool(true).put(m),
            None => w.bool(false),
        };

        match &self.sink.kind {
            // `k`, or `0` for no cut; only a cut carries an order list.
            SinkKind::Rows { cut: None } => w.u8(SINK_ROWS).u64(0),
            SinkKind::Rows { cut: Some(cut) } => w.u8(SINK_ROWS).u64(cut.k.get()).list(&cut.order),
            SinkKind::Fold(agg) => w.u8(SINK_FOLD).list(&agg.group_cols).list(&agg.aggs),
        };
        w.into_vec()
    }

    /// Decode at the trust boundary. A malformed frame is an `Err`.
    pub fn decode(buf: &[u8]) -> Result<ReadSpec, String> {
        decode_all(buf, "read_spec", |r| {
            let bound = r.get()?;

            let predicate = r.bytes32()?.to_vec();

            let map = if r.bool()? { Some(r.get()?) } else { None };

            let kind = match r.u8()? {
                SINK_ROWS => {
                    let cut = match NonZeroU64::new(r.u64()?) {
                        None => None,
                        Some(k) => Some(RowsCut {
                            k,
                            order: r.list("order keys", MAX_ORDER_KEYS)?,
                        }),
                    };
                    SinkKind::Rows { cut }
                }
                SINK_FOLD => {
                    let group_cols = r.list("column list", MAX_COLUMNS)?;
                    let aggs = r.list("aggregate list", MAX_COLUMNS)?;
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

/// The kind tag, then that kind's layout. [`peek_bound`] reads the same layout.
impl Wire for ReadBound {
    fn write(&self, w: &mut Writer) {
        match self {
            ReadBound::None => {
                w.u8(BOUND_NONE);
            }
            ReadBound::Range(range) => {
                w.u8(BOUND_RANGE).put(range);
            }
            ReadBound::PkSet(keys) => {
                w.u8(BOUND_PK_SET);
                write_pk_set(w, keys.stride(), keys.as_bytes());
            }
        }
    }

    /// At the trust boundary: a malformed bound is an `Err`.
    fn read(r: &mut Reader) -> Result<Self, String> {
        Ok(match r.u8()? {
            BOUND_NONE => ReadBound::None,
            BOUND_RANGE => ReadBound::Range(r.get()?),
            BOUND_PK_SET => {
                let (stride, bytes) = read_pk_set(r)?;
                ReadBound::PkSet(PkKeys::checked(stride, bytes.to_vec()).map_err(|e| format!("PkSet {e}"))?)
            }
            other => return Err(format!("unknown bound kind {other}")),
        })
    }
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
        BOUND_RANGE => r.get().ok().map(BoundPeek::Range),
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
