//! ReadSpec — an ad-hoc bounded read: a bound, a predicate, then a sink (rows or a
//! fold). The master routes a request blob by `peek_bound`; the worker's `decode`
//! is the trust boundary.

use std::num::NonZeroU64;
use std::ops::Range;

use crate::codec::{decode_all, Reader, Wire, Writer};
use crate::operator::{order_keys, ComputeMap, GroupAggs, OrderKey};
use crate::pk::PkKeys;
use crate::range::KeyRange;
use crate::MAX_PK_BYTES;

wire_enum! {
    /// The tag a [`ReadBound`] leads with.
    pub(crate) enum BoundTag: u8 {
        None = 0,
        Range = 1,
        PkSet = 2,
    }
}

wire_enum! {
    /// The tag a [`SinkKind`] leads with.
    enum SinkTag: u8 {
        Rows = 0,
        Fold = 1,
    }
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
    /// ORDER BY keys applied in sequence; `col` numbers the sink input: the
    /// map's output (source PK first, then the declared slots), or the source
    /// itself with no map. `len ≤ MAX_ORDER_KEYS`; empty = the first rows the
    /// walk yields.
    pub order: Vec<OrderKey>,
}

/// The two sinks. A fold carries no cut: all SQL-level finishing on an
/// aggregate result is client-side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkKind {
    /// Forward rows, every one or the ones `cut` keeps.
    Rows { cut: Option<RowsCut> },
    /// Fold rows into per-group accumulators (GROUP BY / global aggregate /
    /// DISTINCT) on each worker and emit partial reduce-output rows: a `Reduce`'s
    /// output over the same group set. `group_cols` and `aggs[].col_idx` index the
    /// sink input, and `aggs` is the physical reduce layout, not the SELECT list.
    Fold(GroupAggs),
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
        let map = self.sink.map.as_ref().map_or(0, |m| m.program.len());
        let mut w = Writer::with_capacity(64 + self.predicate.len() + self.bound.key_bytes() + map);

        w.put(&self.bound).bytes32(&self.predicate).put(&self.sink.map);

        match &self.sink.kind {
            // `k`, or `0` for no cut; only a cut carries an order list.
            SinkKind::Rows { cut: None } => w.put(&SinkTag::Rows).u64(0),
            SinkKind::Rows { cut: Some(cut) } => w.put(&SinkTag::Rows).u64(cut.k.get()).list(&cut.order),
            SinkKind::Fold(g) => w.put(&SinkTag::Fold).put(g),
        };
        w.into_vec()
    }

    /// A malformed frame is an `Err`.
    pub fn decode(buf: &[u8]) -> Result<ReadSpec, String> {
        decode_all(buf, "read_spec", |r| {
            let bound = r.get()?;

            let predicate = r.bytes32()?.to_vec();

            let map = r.get()?;

            let kind = match r.get()? {
                SinkTag::Rows => {
                    let cut = match NonZeroU64::new(r.u64()?) {
                        None => None,
                        Some(k) => Some(RowsCut { k, order: order_keys(r)? }),
                    };
                    SinkKind::Rows { cut }
                }
                SinkTag::Fold => SinkKind::Fold(r.get()?),
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
    write_pk_set_head(w, stride, keys.len() / stride);
    w.raw(keys);
}

/// [`write_pk_set`]'s stride and key count, which the keys follow.
fn write_pk_set_head(w: &mut Writer, stride: usize, count: usize) {
    w.u8(stride as u8).u32(count as u32);
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

impl ReadBound {
    /// The range a `Range` bound walks; `None` for any other bound.
    pub fn range(&self) -> Option<&KeyRange> {
        match self {
            ReadBound::Range(r) => Some(r),
            ReadBound::None | ReadBound::PkSet(_) => None,
        }
    }

    /// The bytes of the keys a `PkSet` carries; `0` for any other bound.
    fn key_bytes(&self) -> usize {
        match self {
            ReadBound::PkSet(k) => k.as_bytes().len(),
            _ => 0,
        }
    }
}

impl PkKeys {
    /// A `HasPk` probe's blob: the keys as a `PkSet` bound, the layout
    /// [`peek_bound`] routes.
    pub fn encode_probe(&self) -> Vec<u8> {
        // The tag, the stride byte, the `u32` count, then the keys.
        let mut w = Writer::with_capacity(1 + 1 + 4 + self.as_bytes().len());
        w.put(&BoundTag::PkSet).put(self);
        w.into_vec()
    }

    /// [`Self::encode_probe`]'s inverse.
    pub fn decode_probe(buf: &[u8]) -> Result<PkKeys, String> {
        decode_all(buf, "pk_probe", |r| match r.get()? {
            ReadBound::PkSet(keys) => Ok(keys),
            ReadBound::None | ReadBound::Range(_) => Err("a probe carries a PkSet bound".into()),
        })
    }
}

/// The kind tag, then that kind's layout. [`peek_bound`] reads the same layout.
impl Wire for ReadBound {
    fn write(&self, w: &mut Writer) {
        match self {
            ReadBound::None => {
                w.put(&BoundTag::None);
            }
            ReadBound::Range(range) => {
                w.put(&BoundTag::Range).put(range);
            }
            ReadBound::PkSet(keys) => {
                w.put(&BoundTag::PkSet).put(keys);
            }
        }
    }

    /// A malformed bound is an `Err`.
    fn read(r: &mut Reader) -> Result<Self, String> {
        Ok(match r.get()? {
            BoundTag::None => ReadBound::None,
            BoundTag::Range => ReadBound::Range(r.get()?),
            BoundTag::PkSet => ReadBound::PkSet(r.get()?),
        })
    }
}

/// The stride, the count, then the keys; [`peek_bound`] reads the same layout.
impl Wire for PkKeys {
    fn write(&self, w: &mut Writer) {
        write_pk_set(w, self.stride(), self.as_bytes());
    }

    /// Keys out of order are an `Err`.
    fn read(r: &mut Reader) -> Result<Self, String> {
        let (stride, bytes) = read_pk_set(r)?;
        PkKeys::checked(stride, bytes).map_err(|e| format!("PkSet {e}"))
    }
}

/// The bound of an encoded request that routing reads, without decoding the
/// rest.
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
    /// The blob once per worker: worker `w`'s copy carries the `counts[w]` keys
    /// `owner` maps to it — a subsequence of [`Self::keys`], so still strictly
    /// ascending — and every other byte verbatim, so it decodes to the same spec
    /// over those keys. A worker whose count is 0 gets an empty `Vec`. `counts`
    /// must be the number of keys `owner` maps to each worker.
    pub fn split(&self, counts: &[u32], owner: impl Fn(&[u8]) -> usize) -> Vec<Vec<u8>> {
        let (head, tail) = (&self.blob[..self.span.start], &self.blob[self.span.end..]);
        // Every byte but the keys: head, stride, count and tail.
        let fixed = self.blob.len() - self.keys.len();
        let mut out: Vec<Vec<u8>> = counts
            .iter()
            .map(|&n| {
                if n == 0 {
                    return Vec::new();
                }
                let mut w = Writer::with_capacity(fixed + n as usize * self.stride);
                w.raw(head);
                write_pk_set_head(&mut w, self.stride, n as usize);
                w.into_vec()
            })
            .collect();
        for k in self.keys.chunks_exact(self.stride) {
            out[owner(k)].extend_from_slice(k);
        }
        for v in out.iter_mut().filter(|v| !v.is_empty()) {
            v.extend_from_slice(tail);
        }
        // `Vec::with_capacity` is exact, so a count off from what `owner` maps
        // leaves a blob short of or past its capacity.
        debug_assert!(
            out.iter().all(|v| v.len() == v.capacity()),
            "each blob sized by its count"
        );
        out
    }
}

/// The routing bound of a SCAN_SPEC request blob. `None` for any other bound or
/// a malformed prefix.
pub fn peek_bound(blob: &[u8]) -> Option<BoundPeek<'_>> {
    let mut r = Reader::new(blob);
    match BoundTag::from_wire(r.u8().ok()?)? {
        BoundTag::Range => r.get().ok().map(BoundPeek::Range),
        BoundTag::PkSet => {
            let start = r.pos();
            let (stride, keys) = read_pk_set(&mut r).ok()?;
            Some(BoundPeek::PkSet(PkSetPeek { blob, span: start..r.pos(), stride, keys }))
        }
        BoundTag::None => None,
    }
}

#[cfg(test)]
#[path = "tests/read_spec.rs"]
mod tests;
