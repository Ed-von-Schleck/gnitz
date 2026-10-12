//! The order index: every input row of an operator that reads its groups in
//! ORDER BY order — the top-N and the window — keyed so that a prefix walk of
//! one group visits its rows in that order. The reduce's aggregate-value index
//! with the row attached — the same group-key packer, the same order images.
//!
//! ```text
//! PK      = group key (the AVI packer) ‖ rank_0 ‖ image_0, a string's cut to its slot
//! payload = image_0 when a string's ‖ rank_i ‖ image_i for i ≥ 1   (BLOB each)
//!         ‖ the output row's payload columns, in output order
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
use crate::stream::OpenAt;
use gnitz_wire::OrderKey;
use gnitz_wire::RowSource;

use crate::algebra::{image_slot_col, ImageCol, IMAGE_COL};

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
    /// Over a nullable column, the rank byte that places `row` and whether the
    /// row is NULL. `desc` does not flip the rank, exactly as
    /// [`gnitz_expr::cmp_order_keys`] orders NULLs.
    #[inline]
    fn rank(&self, src: &impl RowSource, row: usize) -> Option<(u8, bool)> {
        self.nullable.then(|| {
            let is_null = self.image.loc.is_null(src, row);
            ((is_null != self.nulls_first) as u8, is_null)
        })
    }

    /// Append the key's image for `row`: the rank byte of a nullable key, then —
    /// non-NULL only — the order image.
    #[inline]
    fn append_image(&self, src: &impl RowSource, row: usize, out: &mut Vec<u8>) {
        let rank = self.rank(src, row);
        out.extend(rank.map(|(rank, _)| rank));
        if !matches!(rank, Some((_, true))) {
            self.image.append(src, row, out);
        }
    }

    /// Write the key's lead for `row` into `lead`, [`Self::lead_cols`] wide: the
    /// rank byte of a nullable key, then the image, zero-padded or cut to its
    /// slot. A string's whole image is appended to `image`.
    #[inline]
    fn write_lead(&self, src: &impl RowSource, row: usize, lead: &mut [u8], image: &mut Vec<u8>) {
        let slot = match self.rank(src, row) {
            None => lead,
            Some((rank, is_null)) => {
                let (first, slot) = lead.split_at_mut(1);
                first[0] = rank;
                if is_null {
                    slot.fill(0);
                    return;
                }
                slot
            }
        };
        self.image.write_slot(src, row, slot, image);
    }

    /// The PK columns holding the image's lead: the rank byte of a nullable
    /// key, then the image slot.
    fn lead_cols(&self) -> impl Iterator<Item = SchemaColumn> {
        let slot = image_slot_col(self.image.is_wide());
        self.nullable.then_some(RANK_COL).into_iter().chain([slot])
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
    Stored(usize, usize),
    Entry(usize),
}

/// `$body` over the row `$at` names in `$walk`, as a `(source, row)` pair.
macro_rules! with_row {
    ($walk:expr, $at:expr, |$src:ident, $row:ident| $body:expr) => {
        match $at {
            $crate::stream::order_index::At::Stored(source, $row) => {
                let $src = $walk.history.source_at(source);
                $body
            }
            $crate::stream::order_index::At::Entry($row) => {
                let $src = &$walk.entries;
                $body
            }
        }
    };
}
pub(in crate::stream) use with_row;

/// The next element of a group in index order: a stored row, an entry, or both
/// as one.
#[derive(Clone, Copy)]
pub(in crate::stream) struct Element {
    /// Its weight before the epoch, and after it.
    pub(in crate::stream) old: i64,
    pub(in crate::stream) new: i64,
    stored: bool,
    /// Whether the epoch touches it.
    pub(in crate::stream) entry: bool,
}

/// One touched group of an epoch: its stored rows and the epoch's entries for
/// it, read together in index order.
pub(in crate::stream) struct Walk<'a> {
    index: &'a OrderIndex,
    pub(in crate::stream) history: &'a mut ReadCursor,
    pub(in crate::stream) entries: MemBatch<'a>,
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
    pub(in crate::stream) fn peek(&self) -> Option<Element> {
        let stored = self.history.valid && self.history.current_pk_bytes().starts_with(self.group);
        let ord = match (stored, self.pending()) {
            (false, false) => return None,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (true, true) => {
                let (src, at) = self.history.current_row_source();
                compare_full_rows(&self.index.schema, src, at, &self.entries, self.next)
            }
        };
        let (stored, entry) = (ord.is_le(), ord.is_ge());
        let old = if stored { self.history.current_weight } else { 0 };
        let new = old.wrapping_add(if entry { self.entries.get_weight(self.next) } else { 0 });
        Some(Element { old, new, stored, entry })
    }

    /// Where `element` is, the one [`Self::peek`] or [`Self::stored_before_entry`]
    /// last returned.
    #[inline(always)]
    pub(in crate::stream) fn at(&self, element: &Element) -> At {
        match element.stored {
            true => {
                let (source, row) = self.history.current_position();
                At::Stored(source, row)
            }
            false => At::Entry(self.next),
        }
    }

    /// The stored row the cursor stands on, where it sorts before the next
    /// entry's peers. An entry is pending.
    #[inline(always)]
    pub(in crate::stream) fn stored_before_entry(&self) -> Option<Element> {
        debug_assert!(self.pending());
        if !self.history.valid {
            return None;
        }
        // The cursor stands at or past the group's first row, and an entry's PK
        // leads with the group: a row before the entry is the group's.
        let (src, row) = self.history.current_row_source();
        let before = self.index.cmp_keys(src, row, &self.entries, self.next).is_lt();
        let weight = self.history.current_weight;
        before.then_some(Element {
            old: weight,
            new: weight,
            stored: true,
            entry: false,
        })
    }

    /// Step past `element`, the one [`Self::peek`] or [`Self::stored_before_entry`]
    /// last returned.
    #[inline(always)]
    pub(in crate::stream) fn advance(&mut self, element: &Element) {
        if element.stored {
            self.history.advance();
        }
        if element.entry {
            self.next += 1;
        }
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
        let cells = &self.index.carried_in_index[..self.index.row_cols];
        with_row!(self, at, |src, row| out.append_cells_from(0, cells, src, row));
    }
}

/// The index resources a plan carries, baked once at compile time.
pub(in crate::stream) struct OrderIndex {
    key_packer: ReindexPacker,
    pub(in crate::stream) schema: SchemaDescriptor,
    /// The first ORDER BY key, whose image leads the PK.
    lead: OrderSpec,
    rest: Vec<OrderSpec>,
    /// The columns an entry carries, as the input locates them: the output
    /// row's payload, then every other column asked for.
    carried: Vec<ColumnLocator>,
    /// The same columns, as an entry holds them.
    carried_in_index: Vec<ColumnLocator>,
    /// How many of them are the output row's.
    row_cols: usize,
    /// The image payload columns: with the PK, a row's whole ORDER BY key.
    images: Vec<ColumnLocator>,
    /// Width of [`OrderSpec::lead_cols`].
    lead_bytes: usize,
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
    ) -> Result<(Self, GroupOutKey, DerivedSchema), String> {
        let mut order = order.iter().map(|key| {
            let (col, loc) = input.wire_col("order index: order column", key.col as u32)?;
            Ok(OrderSpec {
                image: ImageCol::new(loc, key.desc),
                nulls_first: key.nulls_first,
                nullable: col.nullable,
            })
        });
        // Without a key the index orders a group's rows arbitrarily, so what an
        // operator reads off that order would not be a function of the Z-set.
        let lead = order.next().ok_or_else(|| "order index: no order keys".to_string())??;
        let rest = order.collect::<Result<Vec<_>, String>>()?;
        let suffix: Vec<SchemaColumn> = lead.lead_cols().collect();
        let (key_packer, mut b) = ReindexPacker::new_group_key(input, group_cols, &suffix)?;
        for _ in 0..rest.len() + usize::from(!lead.image.fits_slot()) {
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
        let tail = schema.num_columns() - carried.len();
        let index = OrderIndex {
            key_packer,
            lead,
            rest,
            carried_in_index: (tail..schema.num_columns()).map(|c| schema.locate(c)).collect(),
            carried,
            row_cols,
            images: (schema.pk_cols().len()..tail).map(|c| schema.locate(c)).collect(),
            schema,
            lead_bytes: suffix.iter().map(|c| c.size()).sum(),
        };
        Ok((index, key, row))
    }

    /// The index column carrying the input column at `loc`.
    pub(in crate::stream) fn column_of(&self, loc: ColumnLocator) -> Option<usize> {
        let at = self.carried.iter().position(|&c| c == loc)?;
        Some(self.schema.num_columns() - self.carried.len() + at)
    }

    /// Width of the group key leading an entry's PK: entries of one group, and
    /// no others, agree on these bytes.
    #[inline]
    pub(in crate::stream) fn group_bytes(&self) -> usize {
        self.key_packer.out_stride
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
    /// weight, unsorted.
    pub(in crate::stream) fn batch(&self, delta: &Batch) -> Batch {
        let mb = delta.as_mem_batch();
        // One entry per row, and every carried string plus every wide image lands
        // in this heap — so the source's own heap is the presize the crate's
        // string-emitting operators all take.
        let mut out = Batch::with_capacity_blob(&self.schema, delta.count.max(1), delta.blob().len());
        let stride = self.key_packer.out_stride;
        let mut image = Vec::new();
        self.key_packer.for_each_key(&mb, stride + self.lead_bytes, |row, key| {
            let weight = mb.get_weight(row);
            image.clear();
            self.lead
                .write_lead(&mb, row, &mut key[stride..stride + self.lead_bytes], &mut image);
            out.begin_row(key, weight);
            let mut col = 0;
            if !self.lead.image.fits_slot() {
                out.extend_col_blob(col, &image);
                col += 1;
            }
            for spec in &self.rest {
                image.clear();
                spec.append_image(&mb, row, &mut image);
                out.extend_col_blob(col, &image);
                col += 1;
            }
            out.append_cells_from(col, &self.carried, &mb, row);
            out.commit_row();
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
        key: &GroupOutKey,
        history: OpenAt<'_>,
        output: &SchemaDescriptor,
        mut group: impl FnMut(&mut Batch, &[u8], &mut Walk<'_>),
    ) -> OrderedEpoch {
        // Entry `i` is delta row `i`'s, until a fold moves it to where `moved` records.
        let mut folded = self.batch(delta);
        debug_assert_eq!(folded.count, delta.count);
        let mut moved: Vec<(u32, u32, i64)> = Vec::new();
        let folds = !folded.stands_consolidated();
        match folds {
            true => folded = Batch::consolidated_from(&folded, &mut moved),
            false => folded.certify_consolidated(),
        }
        let delta_row = |entry: usize| if folds { moved[entry].1 as usize } else { entry };
        let entries = folded.as_mem_batch();
        let n = entries.count;
        if n == 0 {
            return OrderedEpoch {
                out: Batch::empty_with_schema(output),
                index_entries: folded,
            };
        }
        let width = self.group_bytes();
        let prefix = |row: usize| &entries.get_pk_bytes(row)[..width];
        let mut history = history(prefix(0), prefix(n - 1));
        let mut walk = Walk {
            index: self,
            history: &mut history,
            group: prefix(0),
            entries: entries.clone(),
            next: 0,
            end: 0,
        };

        let mb = delta.as_mem_batch();
        let mut out = Batch::with_capacity(output, n.min(MAX_OUT_CAP_HINT));
        while walk.end < n {
            walk.next = walk.end;
            walk.end = pk_prefix_group_end(&walk.entries, walk.next, width);
            let out_pk = key.out_pk(&mb, delta_row(walk.next));
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
