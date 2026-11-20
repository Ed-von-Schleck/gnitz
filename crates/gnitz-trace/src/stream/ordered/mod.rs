//! The operators that read their groups in ORDER BY order — the top-N (`topn`)
//! and the window (`window`) — and the order index they are maintained through:
//! every input row, keyed so that a prefix walk of one group visits its rows in
//! that order. It shares its group prefix and its order images with the
//! reduce's aggregate-value index.
//!
//! ```text
//! PK      = group prefix ‖ rank_0 ‖ image_0, a string's cut to its slot
//! payload = image_0 when a string's ‖ rank_i ‖ image_i for i ≥ 1   (BLOB each)
//!         ‖ the output row's payload columns, in output order
//!         ‖ each other column the operator reads
//! ```
//!
//! An image is the column's order image, byte-complemented for DESC. A rank
//! byte, over a nullable key only, places NULLs; a NULL has no image. Images are
//! prefix-free, and a BLOB payload compares by content, so the trace's own `(PK,
//! payload)` order *is* the ORDER BY order. A fixed-width `image_0` sits whole in
//! the PK, so only rows equal on the first key share a PK and fall to the
//! merge's row-by-row equal-PK arm; a string's does past its slot.

use std::cmp::Ordering;
use std::ops::Range;

use crate::runs::{CursorRow, ReadCursor, Run};
use crate::stream::OpenAt;
use gnitz_wire::RowSource;
use gnitz_wire::{OrderKey, WeightSlots};
use gnitz_zset::algebra::{GroupOutKey, GroupPrefix};
use gnitz_zset::repr::{Batch, MemBatch, pk_group_end};
use gnitz_zset::schema::{DerivedSchema, Generic, PayloadOrder, SchemaColumn, SchemaDescriptor, compare_pk_ordering};

use gnitz_expr::{ColCopy, ColumnLocator};
use gnitz_zset::algebra::{ColMoves, IMAGE_COL, OrderCol, copy_columns};

use window::{Running, Window};

mod topn;
mod window;

/// The most rows an epoch's output batch reserves up front; it grows past that.
const MAX_OUT_CAP_HINT: usize = 1 << 16;

/// The baked plan of an operator maintained through an order index of every
/// input row: a top-N or a window.
pub struct OrderedPlan {
    index: OrderIndex,
    /// The group key region a reduce over the same group set would have, every
    /// input column not in that region, in input schema order, then the columns
    /// the operator adds.
    output_schema: SchemaDescriptor,
    kind: OrderedKind,
}

enum OrderedKind {
    /// The slots each touched group starts from; `keep ≥ 1`.
    TopN(WeightSlots),
    Window(Window),
}

impl OrderedPlan {
    /// The layout of the index.
    pub fn index_schema(&self) -> &SchemaDescriptor {
        &self.index.schema
    }

    /// The layout of δ_out.
    pub fn output_schema(&self) -> &SchemaDescriptor {
        &self.output_schema
    }

    /// One epoch over `delta`. `index` opens the order index, read without the
    /// epoch's entries.
    pub fn epoch(&self, delta: &Batch, index: OpenAt<'_>) -> OrderedEpoch {
        match &self.kind {
            OrderedKind::TopN(slots) => {
                self.walk_groups(delta, index, |out, out_pk, walk| topn::group(*slots, out, out_pk, walk))
            }
            OrderedKind::Window(window) => {
                let mut running = Running::new(window);
                self.walk_groups(delta, index, |out, out_pk, walk| running.group(out, out_pk, walk))
            }
        }
    }

    /// One epoch over `delta`: `per_group` once per group the delta touches, in
    /// group order, handed δ_out, the group's output PK and its walk. `open`
    /// opens the index, read without the epoch's entries.
    #[inline]
    fn walk_groups(
        &self,
        delta: &Batch,
        open: OpenAt<'_>,
        mut per_group: impl FnMut(&mut Batch, &[u8], &mut Walk<'_>),
    ) -> OrderedEpoch {
        let index = &self.index;
        let mut folded = index.entries(delta);
        debug_assert_eq!(folded.len(), delta.len());
        let origins = folded.consolidate_recording();
        let entries = &folded.as_mem_batch();
        let n = entries.len();
        if n == 0 {
            return OrderedEpoch {
                out: Batch::empty_with_schema(&self.output_schema),
                index_entries: folded,
            };
        }
        // Entries of one group, and no others, agree on the group prefix leading their PK.
        let width = index.prefix.width();
        let prefix = |row: usize| &entries.get_pk_bytes(row)[..width];
        let mut history = open(prefix(0), prefix(n - 1));

        let mb = delta.as_mem_batch();
        let mut out = Batch::with_capacity(&self.output_schema, n.min(MAX_OUT_CAP_HINT));
        let mut walk = Walk {
            index,
            history: &mut history,
            entries,
            group: prefix(0),
            next: 0,
            end: 0,
        };
        while walk.end < n {
            walk.next = walk.end;
            walk.end = pk_group_end(entries, walk.next, width);
            let delta_row = origins.source_row(walk.next);
            let out_pk = index.key.out_pk(&mb, delta_row);
            walk.group = prefix(walk.next);
            walk.history.seek_pk_group_ascending(walk.group);
            per_group(&mut out, out_pk.bytes(), &mut walk);
        }
        OrderedEpoch { out, index_entries: folded }
    }
}

/// One epoch of an operator maintained through the order index.
pub struct OrderedEpoch {
    /// δ_out.
    pub out: Batch,
    /// What the epoch adds to the index, which the operator read without it.
    pub index_entries: Batch,
}

/// A row of the group a [`Walk`] is in: a stored one, by its cursor handle, or
/// one of the epoch's entries.
#[derive(Clone, Copy)]
enum At {
    Stored(CursorRow),
    Entry(u32),
}

/// `$body` over the row `$at` names in `$walk`, as a `(source, row)` pair.
macro_rules! with_row {
    ($walk:expr, $at:expr, |$src:ident, $row:ident| $body:expr) => {
        match $at {
            $crate::stream::ordered::At::Stored(pos) => {
                let ($src, $row) = $walk.history.row_at(pos);
                $body
            }
            $crate::stream::ordered::At::Entry($row) => {
                let ($src, $row) = ($walk.entries, $row as usize);
                $body
            }
        }
    };
}
use with_row;

/// An element of a group: a stored row, an entry, or both as one.
#[derive(Clone, Copy)]
struct Element {
    /// Its row: the stored one, where it has one.
    at: At,
    /// Its weight before the epoch, and after it.
    old: i64,
    new: i64,
    /// Whether the epoch touches it.
    entry: bool,
}

/// One touched group of an epoch: its stored rows and the epoch's entries for
/// it, read together in index order.
struct Walk<'a> {
    index: &'a OrderIndex,
    history: &'a mut ReadCursor,
    entries: &'a MemBatch<'a>,
    /// The key every row of the group leads with.
    group: &'a [u8],
    /// The group's entries not yet read.
    next: usize,
    end: usize,
}

impl Walk<'_> {
    /// Whether an entry of the group is still to be read.
    #[inline]
    fn pending(&self) -> bool {
        self.next < self.end
    }

    /// The group's next element, `None` past its last.
    #[inline(always)]
    fn peek(&self) -> Option<Element> {
        let ord = match (self.history.is_valid(), self.pending()) {
            // No entry bounds the stored rows, so the group's own key does.
            (stored, false) => match stored && self.history.current_pk_bytes().starts_with(self.group) {
                true => Ordering::Less,
                false => return None,
            },
            (false, true) => Ordering::Greater,
            // The cursor stands at or past the group's first row, and an entry's
            // PK leads with the group: a row at or before the entry is the group's.
            (true, true) => {
                let (src, at) = self.history.current_row_source();
                Generic.compare_full(&self.index.schema, src, at, self.entries, self.next)
            }
        };
        let (stored, entry) = (ord.is_le(), ord.is_ge());
        let (at, old) = match stored {
            true => (
                At::Stored(self.history.current_position()),
                self.history.current_weight(),
            ),
            false => (At::Entry(self.next as u32), 0),
        };
        let new = old.wrapping_add(if entry { self.entries.get_weight(self.next) } else { 0 });
        Some(Element { at, old, new, entry })
    }

    #[inline(always)]
    fn step(&mut self, element: &Element) {
        if matches!(element.at, At::Stored(..)) {
            self.history.advance();
        }
        if element.entry {
            self.next += 1;
        }
    }

    /// Take the group's next element in index order, `None` past its last.
    #[inline(always)]
    fn next(&mut self) -> Option<Element> {
        let element = self.peek()?;
        self.step(&element);
        Some(element)
    }

    /// [`Self::next`] where the element is a peer of the row `of`.
    #[inline(always)]
    fn next_peer(&mut self, of: At) -> Option<Element> {
        let element = self.peek()?;
        if !self.peers(of, element.at) {
            return None;
        }
        self.step(&element);
        Some(element)
    }

    /// Take the stored row the cursor stands on, with its weight, where it sorts
    /// before the next entry's peers. An entry is pending.
    #[inline(always)]
    fn next_stored_before_entry(&mut self) -> Option<(At, i64)> {
        debug_assert!(self.pending());
        if !self.history.is_valid() {
            return None;
        }
        // As in `peek`: a row before the entry is the group's.
        let (src, row) = self.history.current_row_source();
        if !self.index.cmp_keys(src, row, self.entries, self.next).is_lt() {
            return None;
        }
        let at = At::Stored(self.history.current_position());
        let weight = self.history.current_weight();
        self.history.advance();
        Some((at, weight))
    }

    /// Pass over the stored rows whose PK is below the next entry's, handing
    /// `fold` each run with rows of it, unmerged: a run's rows at their own
    /// weights, and — as a one-row range at `Some` net weight — the row the
    /// cursor stood on. They sort before the entry's peers, so they are the
    /// group's. An entry is pending.
    #[inline]
    fn skip_stored_below_entry_pk(&mut self, mut fold: impl FnMut(&Run, Range<usize>, Option<i64>)) {
        debug_assert!(self.pending());
        let key = self.entries.get_pk_bytes(self.next);
        if !self.history.is_valid() || compare_pk_ordering(self.history.current_pk_bytes(), key).is_ge() {
            return;
        }
        let (src, row) = self.history.row_at(self.history.current_position());
        fold(src, row..row + 1, Some(self.history.current_weight()));
        self.history.skip_below(key, |src, rows| fold(src, rows, None));
    }

    /// Leave the group's remaining entries unread.
    #[inline]
    fn skip_entries(&mut self) {
        self.next = self.end;
    }

    /// Whether two rows of the group are peers: equal on every ORDER BY key.
    #[inline(always)]
    fn peers(&self, a: At, b: At) -> bool {
        with_row!(self, a, |a, a_row| with_row!(self, b, |b, b_row| self
            .index
            .cmp_keys(a, a_row, b, b_row)
            .is_eq()))
    }

    /// Append the output row's carried cells of the row `at` to the row `out`
    /// has open.
    #[inline(always)]
    fn append_row(&self, out: &mut Batch, at: At) {
        with_row!(self, at, |src, row| out.append_cells_from(&self.index.row, src, row));
    }
}

/// The index resources a plan carries, baked once at compile time.
struct OrderIndex {
    /// How a row's group is keyed in the output.
    key: GroupOutKey,
    /// The group prefix leading an entry's PK.
    prefix: GroupPrefix,
    schema: SchemaDescriptor,
    /// The first ORDER BY key, whose image leads the PK.
    lead: OrderCol,
    rest: Vec<OrderCol>,
    /// The columns an entry carries, each from the input into its payload slot:
    /// the output row's payload, then every other column asked for.
    moves: ColMoves,
    /// The output row's payload columns, as an entry holds them.
    row: Vec<ColumnLocator>,
    /// The image payload columns: with the PK, a row's whole ORDER BY key.
    images: Vec<ColumnLocator>,
}

impl OrderIndex {
    /// The index of `input` grouped by `group_cols` in `order`, and the output's
    /// leading columns: the group key region a reduce over the same group set
    /// would have, then every input column not in that region, in input schema
    /// order. An entry carries those columns and each of `also`.
    fn new(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        order: &[OrderKey],
        also: impl IntoIterator<Item = u32>,
    ) -> Result<(Self, DerivedSchema), String> {
        let mut order = order
            .iter()
            .map(|key| OrderCol::resolve(input, key).map_err(|e| format!("order index: {e}")));
        // An index is of an order: the first key's image leads its PK.
        let lead = order.next().ok_or_else(|| "order index: no order keys".to_string())??;
        let rest = order.collect::<Result<Vec<_>, String>>()?;
        let suffix: Vec<SchemaColumn> = lead.lead_cols().collect();
        let (key, row) = GroupOutKey::new(input, group_cols, 0..input.num_columns() as u32)?;
        let (prefix, mut b) = key.index_prefix(&suffix);
        let images = rest.len() + usize::from(!lead.image.fits_slot());
        for _ in 0..images {
            b.push(IMAGE_COL);
        }
        row.payload().iter().for_each(|&c| b.push(c));
        let mut carried = key.carried().to_vec();
        let row_cols = carried.len();
        for c in also {
            let (col, loc) = input.wire_col("order index: carried column", c)?;
            if !carried.contains(&loc) {
                carried.push(loc);
                b.push(col);
            }
        }
        let schema = b.finish().map_err(|e| format!("order index: {e}"))?;
        let first = schema.num_columns() - carried.len();
        let copies: Vec<ColCopy> = carried
            .iter()
            .zip(images..)
            .map(|(&src, slot)| ColCopy { src, slot, width: src.size() })
            .collect();
        let index = OrderIndex {
            key,
            prefix,
            lead,
            rest,
            row: (first..first + row_cols).map(|c| schema.locate(c)).collect(),
            moves: ColMoves::new(copies, input),
            images: (first - images..first).map(|c| schema.locate(c)).collect(),
            schema,
        };
        Ok((index, row))
    }

    /// The index column carrying the input column at `loc`.
    fn column_of(&self, loc: ColumnLocator) -> Option<usize> {
        let at = self.moves.copies().iter().position(|c| c.src == loc)?;
        Some(self.schema.num_columns() - self.moves.copies().len() + at)
    }

    /// Two entries by group, then by every ORDER BY key: the index order, less
    /// the carried columns that break its ties.
    #[inline(always)]
    fn cmp_keys(&self, a: &impl RowSource, a_row: usize, b: &impl RowSource, b_row: usize) -> Ordering {
        compare_pk_ordering(a.get_pk_bytes(a_row), b.get_pk_bytes(b_row)).then_with(|| {
            self.images
                .iter()
                .map(|loc| loc.content(a, a_row).cmp(&loc.content(b, b_row)))
                .find(|ord| ord.is_ne())
                .unwrap_or(Ordering::Equal)
        })
    }

    /// The index entries `delta` contributes, entry `i` delta row `i`'s at its
    /// weight, unsorted: a region at a time.
    fn entries(&self, delta: &Batch) -> Batch {
        let mb = delta.as_mem_batch();
        let n = delta.len();
        let mut out = Batch::with_capacity(&self.schema, n);
        // No carried string reads the delta's heap.
        let heap_at = match self.moves.string_slots() {
            0 => None,
            strings => out.carry_heap(&mb, strings, &[(0, n)], 0),
        };
        let (stride, group) = (self.schema.pk_stride(), self.prefix.width());
        let mut image = Vec::new();
        out.write_rows(n, |w| {
            self.key
                .write_prefixes(&self.prefix, &mb, &[(0, n)], w.pk_mut(), stride);
            let (keys, cells, blob) = w.pk_and_payload_mut(0..self.images.len());
            let mut cells: Vec<_> = cells.collect();
            // The image columns: a string lead's whole image, then the rest's.
            let (whole, rest) = cells.split_at_mut(usize::from(!self.lead.image.fits_slot()));
            // A scalar image is written a column at a time; a wide one takes the
            // row loop below, which appends any heap image in row order.
            let lead = &self.lead;
            let lead_wide = lead.image.is_wide();
            if !lead_wide {
                lead.write_leads(&mb, keys, stride, group);
            }
            for (spec, col) in self.rest.iter().zip(rest.iter_mut()) {
                if !spec.image.is_wide() {
                    spec.write_inline_images(&mb, col);
                }
            }
            if lead_wide || self.rest.iter().any(|spec| spec.image.is_wide()) {
                for (row, key) in keys.chunks_exact_mut(stride).enumerate() {
                    if lead_wide {
                        image.clear();
                        lead.write_lead(&mb, row, &mut key[group..], &mut image);
                        if let Some(whole) = whole.first_mut() {
                            whole[row * 16..][..16].copy_from_slice(&gnitz_wire::encode_german_string(&image, blob));
                        }
                    }
                    for (spec, col) in self.rest.iter().zip(rest.iter_mut()) {
                        if spec.image.is_wide() {
                            image.clear();
                            spec.append_image(&mb, row, &mut image);
                            col[row * 16..][..16].copy_from_slice(&gnitz_wire::encode_german_string(&image, blob));
                        }
                    }
                }
            }
            w.weight_mut().copy_from_slice(delta.weight());
            copy_columns(&mb, w, &self.moves, n, heap_at);
        });
        out
    }
}

#[cfg(test)]
#[path = "tests/ordered.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/ordered.rs"]
mod bench;
