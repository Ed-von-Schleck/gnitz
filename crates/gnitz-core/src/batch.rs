//! The client batch: the engine's [`Batch`] under a client [`Schema`], its
//! builders, and its WAL-block decode.

use std::borrow::Cow;
use std::ops::Range;
use std::sync::Arc;

use gnitz_expr::SchemaDescriptor;
use gnitz_wire::{PkKeys, Regions};
use gnitz_zset::repr::{Batch, BatchBuilder, CertifiedBlock, DirectWriter, MemBatch, Pick, materialize_carrying};

use crate::{ProtocolError, Schema};

/// A Z-set batch as the engine keeps one, in the canonical region order, under the client
/// [`Schema`] it is rows of: every payload slot is its own region, and a STRING/BLOB slot
/// holds 16-byte German cells against the heap. It is a reply decoded, or rows a client
/// builds to push.
///
/// A reply's lone block is read where it arrived, as the engine reads a block it is sent:
/// through a [`MemBatch`] over the frame. The first write makes the rows a batch of their
/// own.
#[derive(Clone)]
pub struct ZSetBatch {
    rows: Rows,
    schema: Arc<Schema>,
}

#[derive(Clone)]
enum Rows {
    /// Boxed, so a move copies a pointer, not the batch's inline schema descriptor.
    Owned(Box<Batch>),
    /// A WAL block of `frame`, certified under the batch's schema.
    Block { frame: Vec<u8>, block: CertifiedBlock },
}

impl ZSetBatch {
    pub fn new(schema: &Arc<Schema>) -> Self {
        Self::of(schema, Batch::empty_with_schema(schema.layout()))
    }

    /// An empty batch with room for `n` rows.
    pub fn with_capacity(schema: &Arc<Schema>, n: usize) -> Self {
        Self::of(schema, Batch::with_capacity(schema.layout(), n))
    }

    /// The rows of `batch` as rows of `schema`, whose region types they are in: the rows
    /// are relabelled to `schema`'s layout, so they read as the batch's schema states.
    pub fn of(schema: &Arc<Schema>, mut batch: Batch) -> Self {
        batch.set_schema(schema.layout());
        ZSetBatch {
            rows: Rows::Owned(Box::new(batch)),
            schema: Arc::clone(schema),
        }
    }

    /// The rows of `batch`, under this batch's schema.
    fn again(&self, batch: Batch) -> Self {
        Self::of(&self.schema, batch)
    }

    /// The schema the rows are rows of.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// The same rows under `schema`, which names the columns of this batch's region types
    /// afresh.
    pub fn with_schema(mut self, schema: Arc<Schema>) -> Self {
        assert!(
            self.layout().same_region_types(schema.layout()),
            "with_schema: the schema's region types are not the batch's"
        );
        if let Rows::Owned(b) = &mut self.rows {
            b.set_schema(schema.layout());
        }
        self.schema = schema;
        self
    }

    /// The rows, read in place.
    #[inline(always)]
    pub fn view(&self) -> MemBatch<'_> {
        match &self.rows {
            Rows::Owned(b) => b.as_mem_batch(),
            Rows::Block { frame, block } => block.view(frame, self.schema.layout()),
        }
    }

    /// The rows as a batch of their own, to write: a received block is copied out first.
    fn owned(&mut self) -> &mut Batch {
        if let Rows::Block { .. } = self.rows {
            self.rows = Rows::Owned(Box::new(self.view().to_batch()));
        }
        match &mut self.rows {
            Rows::Owned(b) => b,
            Rows::Block { .. } => unreachable!("the block was just copied out"),
        }
    }

    /// The rows as an engine batch.
    pub fn into_batch(self) -> Batch {
        match self.rows {
            Rows::Owned(b) => *b,
            Rows::Block { .. } => self.view().to_batch(),
        }
    }

    /// The layout the rows are in.
    fn layout(&self) -> &SchemaDescriptor {
        self.schema.layout()
    }

    /// One row per key of `keys`, keys of `schema` as OPK bytes back to back, each at
    /// `weight`, its payload columns inert filler: a `-1` retraction, which the server's
    /// unique-PK rule applies by PK alone, or a keys-only read's rows.
    pub fn key_rows(schema: &Arc<Schema>, keys: &[u8], weight: i64) -> Self {
        assert_eq!(
            keys.len() % schema.layout().pk_stride(),
            0,
            "key_rows: key width mismatch"
        );
        Self::of(schema, Batch::key_rows(schema.layout(), keys, weight))
    }

    /// Room for `n` more rows.
    pub fn reserve(&mut self, n: usize) {
        self.owned().reserve_rows(n);
    }

    // ── Reads ───────────────────────────────────────────────────────────────

    pub fn len(&self) -> usize {
        self.view().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Row `row`'s key as its native value. Defined only for a single-column key; a compound
    /// one is read as bytes.
    pub fn pk(&self, row: usize) -> u128 {
        let layout = self.layout();
        let ci = layout
            .lone_pk_col()
            .expect("ZSetBatch::pk: a compound key has no scalar form");
        gnitz_wire::decode_opk_u128(self.view().get_pk_bytes(row), layout.columns()[ci].type_code)
    }

    /// The keys as a read's key set.
    pub fn pk_keys(&self) -> PkKeys {
        let stride = self.layout().pk_stride();
        PkKeys::from_keys(stride, self.view().pk().chunks_exact(stride))
    }

    // ── Writes ──────────────────────────────────────────────────────────────

    /// Rewrite the rows in place through `fill`, a writer over every row that writes
    /// whole whatever it overwrites.
    pub fn rewrite<R>(&mut self, fill: impl FnOnce(&mut DirectWriter<'_>) -> R) -> R {
        self.owned().rewrite(fill)
    }

    /// Every row at weight `w`.
    pub fn fill_weights(&mut self, w: i64) {
        self.owned().map_weights(|_| w);
    }

    /// A builder appending rows onto this batch, one checked value per payload column.
    pub fn builder(&mut self) -> BatchBuilder<&mut Batch> {
        BatchBuilder::onto(self.owned())
    }

    // ── Rows from other batches ─────────────────────────────────────────────

    /// Append row `i` of `src` to `self` at `weight`, verbatim — so a `-1`
    /// reproduces the stored row rather than relying on a decoder and an encoder
    /// agreeing. `src` and `self` must share a layout.
    pub fn copy_row_at(&mut self, src: &ZSetBatch, i: usize, weight: i64) {
        debug_assert!(self.same_layout(src), "copy_row_at: layout mismatch");
        self.owned().append_row_from_source(weight, &src.view(), i, 0, None);
    }

    /// Same key column types and the same payload type list.
    fn same_layout(&self, other: &ZSetBatch) -> bool {
        self.layout().same_region_types(other.layout())
    }

    /// Append all rows of `other`.
    pub(crate) fn extend_from(&mut self, other: &ZSetBatch) {
        assert!(self.same_layout(other), "extend_from: layout mismatch");
        let from = other.view();
        if let Rows::Block { .. } = self.rows {
            // Copied out and grown at once, rather than copied out to its own size first.
            let joined = Batch::concat(self.layout(), [self.view(), from].into_iter());
            *self = self.again(joined);
        } else {
            self.owned().append_ranges(&from, &[(0, from.len())]);
        }
    }

    /// `extend_from`, taking `other` whole when `self` is empty.
    pub fn extend_from_owned(&mut self, other: ZSetBatch) {
        if self.is_empty() {
            *self = other.with_schema(Arc::clone(&self.schema));
        } else {
            self.extend_from(&other);
        }
    }

    /// The batch's current extent, for [`Self::truncate_to`].
    pub fn mark(&self) -> RowMark {
        let v = self.view();
        RowMark { rows: v.len(), heap: v.blob().len() }
    }

    /// Drop every row and heap byte appended since `mark`. Valid while no cell of a row
    /// before the mark points into the heap past it.
    pub fn truncate_to(&mut self, mark: RowMark) {
        self.owned().truncate(mark.rows, mark.heap);
    }

    /// Keep only the rows in `ranges` (ascending, disjoint, `end` exclusive), in order.
    pub fn retain_ranges(&mut self, ranges: &[(usize, usize)]) {
        if ranges != [(0, self.len())] {
            let mut kept = Batch::empty_with_schema(self.layout());
            kept.append_ranges(&self.view(), ranges);
            *self = self.again(kept);
        }
    }

    /// The rows `rows` names, in order, each at most once. A gather naming every row in
    /// place is the batch itself; otherwise the heap is carried whole where that beats
    /// moving each kept string.
    pub fn gather(self, rows: &[u32]) -> ZSetBatch {
        if rows.len() == self.len() && rows.iter().enumerate().all(|(i, &r)| r as usize == i) {
            return self;
        }
        let v = self.view();
        if v.schema().string_payload_slots() == 0 {
            return self.gathered(rows);
        }
        let picks: Vec<Pick> = rows.iter().map(|&r| (0, r, v.get_weight(r as usize))).collect();
        self.again(materialize_carrying(&[v], v.schema(), &picks))
    }

    /// The rows `rows` names, in order, each at most once, onto a fresh heap holding only
    /// their strings; `self` is left as it was.
    pub fn gathered(&self, rows: &[u32]) -> ZSetBatch {
        self.again(self.view().indexed_rows(rows))
    }

    /// This batch as its canonical region list.
    pub(crate) fn wire_regions(&self) -> Regions<'_> {
        self.view().wire_regions()
    }
}

/// Equal layouts' types, rows and heap bytes.
impl PartialEq for ZSetBatch {
    fn eq(&self, o: &Self) -> bool {
        self.same_layout(o) && self.wire_regions() == o.wire_regions()
    }
}

impl Eq for ZSetBatch {}

impl std::fmt::Debug for ZSetBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let v = self.view();
        let cols: Vec<&[u8]> = (0..v.schema().num_payload_cols()).map(|pi| v.col_data(pi)).collect();
        f.debug_struct("ZSetBatch")
            .field("pks", &v.pk())
            .field("weights", &v.weights().collect::<Vec<_>>())
            .field("nulls", &v.null_words().collect::<Vec<_>>())
            .field("payload", &cols)
            .field("heap", &v.blob())
            .finish()
    }
}

/// A [`ZSetBatch`]'s extent at one moment, taken by [`ZSetBatch::mark`].
#[derive(Clone, Copy)]
pub struct RowMark {
    rows: usize,
    heap: usize,
}

/// The rows of the WAL block `data` a server wrote, under `schema`, read in place and
/// validated as the engine validates a block a peer wrote.
fn checked_wal_block<'a>(data: &'a [u8], schema: &'a Schema) -> Result<MemBatch<'a>, ProtocolError> {
    MemBatch::of_foreign_wal_block(data, schema.layout()).map_err(wal_error)
}

/// A WAL block's validation failure as a reply's decode error.
fn wal_error(e: &str) -> ProtocolError {
    ProtocolError::DecodeError(format!("WAL {e}"))
}

/// One WAL block of a reply and the frame it lies in at `block`: owned where the transport
/// handed the frame over whole, borrowed from its read window otherwise.
#[derive(Debug)]
pub(crate) struct FrameBlock<'a> {
    pub(crate) frame: Cow<'a, [u8]>,
    pub(crate) block: Range<usize>,
}

impl FrameBlock<'_> {
    /// The block's bytes.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.frame[self.block.clone()]
    }
}

/// Decode a reply's WAL blocks under `schema` into one batch, in order: the blocks `held`
/// back from earlier frames, then `last`. Every block is checked before any is copied. A
/// lone block is read where it lies, in its frame when the transport handed that over whole
/// and in a copy of the block alone otherwise; several are concatenated into one batch.
pub(crate) fn decode_wal_blocks(
    mut held: Vec<FrameBlock<'static>>,
    last: Option<FrameBlock<'_>>,
    schema: &Arc<Schema>,
) -> Result<ZSetBatch, ProtocolError> {
    let lone = match (held.len(), last) {
        (0, None) => return Ok(ZSetBatch::new(schema)),
        (0, Some(last)) => last,
        (1, None) => held.pop().expect("one held block"),
        (_, last) => {
            let views = (held.iter().chain(last.as_ref()))
                .map(|b| checked_wal_block(b.bytes(), schema))
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(ZSetBatch::of(
                schema,
                Batch::concat(schema.layout(), views.iter().copied()),
            ));
        }
    };
    let certify = |data: &[u8], range| CertifiedBlock::certify(data, range, schema.layout()).map_err(wal_error);
    let (frame, block) = match lone.frame {
        Cow::Owned(frame) => {
            let block = certify(&frame, lone.block)?;
            (frame, block)
        }
        Cow::Borrowed(frame) => {
            let bytes = &frame[lone.block];
            let block = certify(bytes, 0..bytes.len())?;
            (bytes.to_vec(), block)
        }
    };
    Ok(ZSetBatch {
        rows: Rows::Block { frame, block },
        schema: Arc::clone(schema),
    })
}

#[cfg(test)]
#[path = "tests/batch.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/batch.rs"]
mod bench;
