//! The order index: every input row of an operator that reads its groups in
//! ORDER BY order — the top-N and the window — keyed so that a prefix walk of
//! one group visits its rows in that order. It shares its group-key packer and
//! its order images with the reduce's aggregate-value index.
//!
//! ```text
//! PK      = group key ‖ rank_0 ‖ image_0, a string's cut to its slot
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

use crate::algebra::{GroupOutKey, ReindexPacker};
use crate::repr::{pk_prefix_group_end, Batch, MemBatch, ReadCursor};
use crate::schema::payload_order::compare_full_rows;
use crate::schema::{ColumnLocator, DerivedSchema, SchemaColumn, SchemaDescriptor, TypeCode};
use crate::stream::topn::{op_topn, TopN};
use crate::stream::window::{op_window, Window};
use crate::stream::OpenAt;
use gnitz_wire::OrderKey;
use gnitz_wire::RowSource;

use crate::algebra::{
    copied_string_slots, copy_column, image_slot_col, rebase_copied_strings, ImageCol, RowWindow, IMAGE_COL,
};
use gnitz_expr::{ColCopy, NullPerm};

/// The most rows an epoch's output batch reserves up front; it grows past that.
const MAX_OUT_CAP_HINT: usize = 1 << 16;

/// The rank byte leading a nullable key's image.
const RANK_COL: SchemaColumn = SchemaColumn::new(TypeCode::U8, false);

/// One ORDER BY key, resolved against the input: its image, complemented for
/// DESC, and where a NULL — which has no image — stands.
struct OrderSpec {
    image: ImageCol,
    nulls_first: bool,
    nullable: bool,
}

impl OrderSpec {
    #[inline]
    fn is_null(&self, src: &impl RowSource, row: usize) -> bool {
        self.nullable && self.image.loc.is_null(src, row)
    }

    /// The rank byte that places a row of a nullable key. `desc` does not flip
    /// it, exactly as [`gnitz_expr::cmp_order_keys`] orders NULLs.
    #[inline]
    fn rank(&self, is_null: bool) -> u8 {
        (is_null != self.nulls_first) as u8
    }

    /// Append the key's image for `row`: the rank byte of a nullable key, then —
    /// non-NULL only — the order image.
    #[inline]
    fn append_image(&self, src: &impl RowSource, row: usize, out: &mut Vec<u8>) {
        let is_null = self.is_null(src, row);
        if self.nullable {
            out.push(self.rank(is_null));
        }
        if !is_null {
            self.image.append(src, row, out);
        }
    }

    /// Write the key's lead for `row` into `lead`, [`Self::lead_cols`] wide: the
    /// rank byte of a nullable key, then the image, zero-padded or cut to its
    /// slot. A string's whole image is appended to `image`.
    #[inline]
    fn write_lead(&self, src: &impl RowSource, row: usize, lead: &mut [u8], image: &mut Vec<u8>) {
        let is_null = self.is_null(src, row);
        let slot = match self.nullable {
            true => {
                lead[0] = self.rank(is_null);
                &mut lead[1..]
            }
            false => lead,
        };
        match is_null {
            true => slot.fill(0),
            false => self.image.write_slot(src, row, slot, image),
        }
    }

    /// The PK columns holding the image's lead: the rank byte of a nullable
    /// key, then the image slot.
    fn lead_cols(&self) -> impl Iterator<Item = SchemaColumn> {
        let slot = image_slot_col(self.image.is_wide());
        self.nullable.then_some(RANK_COL).into_iter().chain([slot])
    }
}

/// The baked plan of an operator maintained through an order index of every
/// input row: a top-N or a window.
pub struct OrderedPlan {
    pub(in crate::stream) index: OrderIndex,
    /// The group key region a reduce over the same group set would have, every
    /// input column not in that region, in input schema order, then the columns
    /// the operator adds.
    pub(in crate::stream) output_schema: SchemaDescriptor,
    pub(in crate::stream) kind: OrderedKind,
}

pub(in crate::stream) enum OrderedKind {
    TopN(TopN),
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

    /// One epoch over `delta`. `history` opens the index, read without the
    /// epoch's entries.
    pub fn epoch(&self, delta: &Batch, history: OpenAt<'_>) -> OrderedEpoch {
        match &self.kind {
            OrderedKind::TopN(top) => op_topn(delta, history, self, top),
            OrderedKind::Window(window) => op_window(delta, history, self, window),
        }
    }
}

/// One epoch of an operator maintained through an [`OrderIndex`].
pub struct OrderedEpoch {
    /// δ_out.
    pub out: Batch,
    /// What the epoch adds to the index, which the operator read without it.
    pub index_entries: Batch,
}

/// A row of the group a [`Walk`] is in: a stored one, by its cursor position, or
/// one of the epoch's entries.
#[derive(Clone, Copy)]
pub(in crate::stream) enum At {
    Stored(u32, u32),
    Entry(u32),
}

/// `$body` over the row `$at` names in `$walk`, as a `(source, row)` pair.
macro_rules! with_row {
    ($walk:expr, $at:expr, |$src:ident, $row:ident| $body:expr) => {
        match $at {
            $crate::stream::order_index::At::Stored(source, $row) => {
                let ($src, $row) = ($walk.history.source_at(source as usize), $row as usize);
                $body
            }
            $crate::stream::order_index::At::Entry($row) => {
                let ($src, $row) = ($walk.entries, $row as usize);
                $body
            }
        }
    };
}
pub(in crate::stream) use with_row;

/// An element of a group: a stored row, an entry, or both as one.
#[derive(Clone, Copy)]
pub(in crate::stream) struct Element {
    /// Its row: the stored one, where it has one.
    pub(in crate::stream) at: At,
    /// Its weight before the epoch, and after it.
    pub(in crate::stream) old: i64,
    pub(in crate::stream) new: i64,
    /// Whether the epoch touches it.
    pub(in crate::stream) entry: bool,
}

/// One touched group of an epoch: its stored rows and the epoch's entries for
/// it, read together in index order.
pub(in crate::stream) struct Walk<'a> {
    index: &'a OrderIndex,
    pub(in crate::stream) history: &'a mut ReadCursor,
    pub(in crate::stream) entries: &'a MemBatch<'a>,
    /// The key every row of the group leads with.
    group: &'a [u8],
    /// The group's entries not yet read.
    next: usize,
    end: usize,
}

impl Walk<'_> {
    /// Whether an entry of the group is still to be read.
    #[inline]
    pub(in crate::stream) fn pending(&self) -> bool {
        self.next < self.end
    }

    /// The group's next element, `None` past its last.
    #[inline(always)]
    fn peek(&self) -> Option<Element> {
        let ord = match (self.history.valid, self.pending()) {
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
                compare_full_rows(&self.index.schema, src, at, self.entries, self.next)
            }
        };
        let (stored, entry) = (ord.is_le(), ord.is_ge());
        let (at, old) = match stored {
            true => {
                let (source, row) = self.history.current_position();
                (At::Stored(source as u32, row as u32), self.history.current_weight)
            }
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
    pub(in crate::stream) fn next(&mut self) -> Option<Element> {
        let element = self.peek()?;
        self.step(&element);
        Some(element)
    }

    /// [`Self::next`] where the element is a peer of the row `of`, or `of` is `None`.
    #[inline(always)]
    pub(in crate::stream) fn next_peer(&mut self, of: Option<At>) -> Option<Element> {
        let element = self.peek()?;
        if of.is_some_and(|of| !self.peers(of, element.at)) {
            return None;
        }
        self.step(&element);
        Some(element)
    }

    /// Take the stored row the cursor stands on, where it sorts before the next
    /// entry's peers. An entry is pending.
    #[inline(always)]
    pub(in crate::stream) fn next_stored_before_entry(&mut self) -> Option<Element> {
        debug_assert!(self.pending());
        if !self.history.valid {
            return None;
        }
        // As in `peek`: a row before the entry is the group's.
        let (src, row) = self.history.current_row_source();
        if !self.index.cmp_keys(src, row, self.entries, self.next).is_lt() {
            return None;
        }
        let (source, row) = self.history.current_position();
        let weight = self.history.current_weight;
        self.history.advance();
        Some(Element {
            at: At::Stored(source as u32, row as u32),
            old: weight,
            new: weight,
            entry: false,
        })
    }

    /// Leave the group's remaining entries unread.
    #[inline]
    pub(in crate::stream) fn skip_entries(&mut self) {
        self.next = self.end;
    }

    /// Whether two rows of the group are peers: equal on every ORDER BY key.
    #[inline(always)]
    pub(in crate::stream) fn peers(&self, a: At, b: At) -> bool {
        with_row!(self, a, |a, a_row| with_row!(self, b, |b, b_row| self
            .index
            .cmp_keys(a, a_row, b, b_row)
            .is_eq()))
    }

    /// Append the output row's carried cells of the row `at` to the row `out`
    /// has open.
    #[inline(always)]
    pub(in crate::stream) fn append_row(&self, out: &mut Batch, at: At) {
        with_row!(self, at, |src, row| out.append_cells_from(0, &self.index.row, src, row));
    }
}

/// The index resources a plan carries, baked once at compile time.
pub(in crate::stream) struct OrderIndex {
    key_packer: ReindexPacker,
    /// How a row's group is keyed in the output.
    key: GroupOutKey,
    pub(in crate::stream) schema: SchemaDescriptor,
    /// The first ORDER BY key, whose image leads the PK.
    lead: OrderSpec,
    rest: Vec<OrderSpec>,
    /// The columns an entry carries, each from the input into its payload slot:
    /// the output row's payload, then every other column asked for.
    copies: Vec<ColCopy>,
    /// How their null bits move from an input row's null word to an entry's.
    nulls: NullPerm,
    /// The input's string slots among them.
    strings: u64,
    /// The output row's payload columns, as an entry holds them.
    pub(in crate::stream) row: Vec<ColumnLocator>,
    /// The image payload columns: with the PK, a row's whole ORDER BY key.
    images: Vec<ColumnLocator>,
}

impl OrderIndex {
    /// The index of `input` grouped by `group_cols` in `order`, with how a row's
    /// group is keyed in the output and the output's leading columns: the group
    /// key region a reduce over the same group set would have, then every input
    /// column not in that region, in input schema order. An entry carries those
    /// columns and each of `also`.
    pub(in crate::stream) fn new(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        order: &[OrderKey],
        also: impl IntoIterator<Item = u32>,
    ) -> Result<(Self, DerivedSchema), String> {
        let mut order = order.iter().map(|key| {
            let (col, loc) = input.wire_col("order index: order column", key.col as u32)?;
            Ok(OrderSpec {
                image: ImageCol::new(loc, key.desc),
                nulls_first: key.nulls_first,
                nullable: col.nullable,
            })
        });
        // An index is of an order: the first key's image leads its PK.
        let lead = order.next().ok_or_else(|| "order index: no order keys".to_string())??;
        let rest = order.collect::<Result<Vec<_>, String>>()?;
        let suffix: Vec<SchemaColumn> = lead.lead_cols().collect();
        let (key_packer, mut b) = ReindexPacker::new_group_key(input, group_cols, &suffix)?;
        let images = rest.len() + usize::from(!lead.image.fits_slot());
        for _ in 0..images {
            b.push(IMAGE_COL);
        }
        let (key, row) = GroupOutKey::new(input, group_cols, 0..input.num_columns() as u32)?;
        b.push_payload_of(&row.finish().map_err(|e| format!("order index: row {e}"))?);
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
            key_packer,
            key,
            lead,
            rest,
            row: (first..first + row_cols).map(|c| schema.locate(c)).collect(),
            nulls: NullPerm::new(&copies, input.nullable_payload_slots()),
            strings: copied_string_slots(&copies),
            copies,
            images: (first - images..first).map(|c| schema.locate(c)).collect(),
            schema,
        };
        Ok((index, row))
    }

    /// The index column carrying the input column at `loc`.
    pub(in crate::stream) fn column_of(&self, loc: ColumnLocator) -> Option<usize> {
        let at = self.copies.iter().position(|c| c.src == loc)?;
        Some(self.schema.num_columns() - self.copies.len() + at)
    }

    /// Two entries by group, then by every ORDER BY key: the index order, less
    /// the carried columns that break its ties.
    #[inline(always)]
    fn cmp_keys(&self, a: &impl RowSource, a_row: usize, b: &impl RowSource, b_row: usize) -> Ordering {
        a.get_pk_bytes(a_row).cmp(b.get_pk_bytes(b_row)).then_with(|| {
            self.images
                .iter()
                .map(|loc| loc.content(a, a_row).cmp(loc.content(b, b_row)))
                .find(|ord| ord.is_ne())
                .unwrap_or(Ordering::Equal)
        })
    }

    /// The index entries `delta` contributes, entry `i` delta row `i`'s at its
    /// weight, unsorted: a region at a time.
    pub(in crate::stream) fn batch(&self, delta: &Batch) -> Batch {
        let mb = delta.as_mem_batch();
        let n = delta.count;
        let mut out = Batch::with_capacity(&self.schema, n.max(1));
        // No carried string reads the delta's heap.
        let heap_at = match self.strings {
            0 => None,
            strings => out.carry_heap(&mb, strings, &[(0, n)]),
        };
        let (stride, group) = (self.schema.pk_stride(), self.key_packer.out_stride);
        let mut image = Vec::new();
        out.append_session(n).write(n, |w| {
            self.key_packer.pack_rows(w.pk_mut(), stride, &mb, &[(0, n)]);
            let (mut regions, blob, _) = w.split_mut();
            let keys = regions.next().expect("the PK region");
            // Past the weights and the null words: the image columns' cells.
            let mut cells: Vec<_> = regions.skip(2).take(self.images.len()).collect();
            for (row, key) in keys.chunks_exact_mut(stride).enumerate() {
                let mut cells = cells.iter_mut().map(|col| &mut col[row * 16..][..16]);
                let mut put = |image: &[u8]| {
                    let cell = cells.next().expect("a cell per image column");
                    cell.copy_from_slice(&gnitz_wire::encode_german_string(image, blob));
                };
                image.clear();
                self.lead.write_lead(&mb, row, &mut key[group..], &mut image);
                if !self.lead.image.fits_slot() {
                    put(&image);
                }
                for spec in &self.rest {
                    image.clear();
                    spec.append_image(&mb, row, &mut image);
                    put(&image);
                }
            }
            w.weight_mut().copy_from_slice(delta.weight_data());
            self.nulls.write_rows(mb.null_bmp(), 0, w.null_bmp_mut(), 0, n);
            for c in &self.copies {
                copy_column(delta, w, c, RowWindow { src: 0, dst: 0, n });
            }
            rebase_copied_strings(&self.copies, w, delta.blob(), heap_at);
        });
        out
    }

    /// One epoch over `delta`: `group` once per group the delta touches, in
    /// group order, handed δ_out, the group's output PK and its walk. The history
    /// is read without the epoch's entries.
    #[inline]
    pub(in crate::stream) fn epoch(
        &self,
        delta: &Batch,
        history: OpenAt<'_>,
        output: &SchemaDescriptor,
        mut group: impl FnMut(&mut Batch, &[u8], &mut Walk<'_>),
    ) -> OrderedEpoch {
        let mut folded = self.batch(delta);
        debug_assert_eq!(folded.count, delta.count);
        // Entry `i` is delta row `i`'s, unless a fold moved the entries: then
        // `moved` records every entry's.
        let mut moved: Vec<(u32, u32, i64)> = Vec::new();
        match folded.stands_consolidated() {
            true => folded.certify_consolidated(),
            false => {
                moved.reserve_exact(folded.count);
                folded = Batch::consolidated_from(&folded, &mut moved);
            }
        }
        let entries = &folded.as_mem_batch();
        let n = entries.count;
        if n == 0 {
            return OrderedEpoch {
                out: Batch::empty_with_schema(output),
                index_entries: folded,
            };
        }
        // Entries of one group, and no others, agree on the group key leading their PK.
        let width = self.key_packer.out_stride;
        let prefix = |row: usize| &entries.get_pk_bytes(row)[..width];
        let mut history = history(prefix(0), prefix(n - 1));

        let mb = delta.as_mem_batch();
        let mut out = Batch::with_capacity(output, n.min(MAX_OUT_CAP_HINT));
        let mut walk = Walk {
            index: self,
            history: &mut history,
            entries,
            group: prefix(0),
            next: 0,
            end: 0,
        };
        while walk.end < n {
            walk.next = walk.end;
            walk.end = pk_prefix_group_end(entries, walk.next, width);
            let delta_row = moved.get(walk.next).map_or(walk.next, |m| m.1 as usize);
            let out_pk = self.key.out_pk(&mb, delta_row);
            walk.group = prefix(walk.next);
            walk.history.seek_pk_group_ascending(walk.group);
            group(&mut out, out_pk.bytes(), &mut walk);
        }
        OrderedEpoch { out, index_entries: folded }
    }
}

#[cfg(test)]
#[path = "tests/order_index.rs"]
pub(in crate::stream) mod tests;

#[cfg(test)]
#[path = "benches/order_index.rs"]
mod bench;
