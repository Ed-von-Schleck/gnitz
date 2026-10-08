//! The schema descriptor: a column table and PK list with every layout fact
//! derived from them — the *schema* peer of [`crate::BatchView`]'s *access*
//! contract, linked by the engine and the client alike.

use gnitz_wire::schema_block::SchemaBlockCol;
use gnitz_wire::{ColType, PkListRole, PkRule, TypeCode, MAX_COLUMNS, MAX_PK_BYTES, MAX_PK_COLUMNS};

use crate::ColumnLocator;

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SchemaColumn {
    pub type_code: TypeCode,
    size: u8,
    pub nullable: bool,
    is_signed: u8,
}

// `SchemaDescriptor` holds `MAX_COLUMNS` of these by value.
const _: () = assert!(std::mem::size_of::<SchemaColumn>() == 4);

impl SchemaColumn {
    /// The filler of a schema's unused column slots.
    const EMPTY: SchemaColumn = SchemaColumn {
        type_code: TypeCode::U8,
        size: 0,
        nullable: false,
        is_signed: 0,
    };

    pub const fn new(type_code: TypeCode, nullable: bool) -> Self {
        SchemaColumn {
            type_code,
            size: type_code.wire_stride() as u8,
            nullable,
            is_signed: type_code.is_signed_int() as u8,
        }
    }

    /// On-disk byte width of one cell of this column.
    #[inline(always)]
    pub const fn size(&self) -> usize {
        self.size as usize
    }

    /// True iff this column's type is [`TypeCode::is_signed_int`].
    #[inline(always)]
    pub const fn is_signed(&self) -> bool {
        self.is_signed != 0
    }

    /// This column's type as a ≤ 8-byte integer, or `None` for any other type —
    /// the one rule the shard writer packs FoR by and the reader accepts it by.
    #[inline]
    pub fn fixed_int(&self) -> Option<gnitz_wire::FixedInt> {
        gnitz_wire::FixedInt::from_type_code(self.type_code)
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct SchemaDescriptor {
    /// Bit `pi` set iff payload slot `pi` is a German string.
    string_slots: u64,
    /// Bit `pi` set iff payload slot `pi` admits NULL.
    nullable_slots: u64,
    /// Region `r`'s byte offset within one row's fixed bytes, in canonical
    /// region order; entry `num_regions()` is the row width.
    region_off: [u16; gnitz_wire::MAX_WIRE_REGIONS],
    num_columns: u32,
    pk_count: u32,
    pk_indices: [u32; MAX_PK_COLUMNS],
    /// Total bytes per row of the PK region.
    pk_stride: u8,
    /// Payload slot → logical column index.
    payload_to_ci: [u8; MAX_COLUMNS],
    /// Every payload column is a non-nullable integer of at most 8 bytes.
    fixed_int_nonnull: bool,
    /// One less than the row-count multiple an arena of eight rows or more
    /// rounds its capacity up to, so that every region starts 8-aligned.
    cap_mask: u8,
    columns: [SchemaColumn; MAX_COLUMNS],
}

// Every `Batch` embeds a `SchemaDescriptor` by value, so a field added here is
// paid for at every batch copy.
const _: () = assert!(std::mem::size_of::<SchemaDescriptor>() <= 512);

// No column is wider than 16 bytes, so a PK within `MAX_PK_COLUMNS` is within
// `MAX_PK_BYTES` — what every `[0u8; MAX_PK_BYTES]` scratch key relies on — and
// its stride fits the `u8` field.
const _: () = {
    let mut i = 0;
    while i < TypeCode::ALL.len() {
        assert!(TypeCode::ALL[i].wire_stride() <= 16);
        i += 1;
    }
    assert!(MAX_PK_COLUMNS * 16 <= MAX_PK_BYTES && MAX_PK_BYTES <= u8::MAX as usize);
};

/// Why a column list is no schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaRefusal {
    TooManyColumns(usize),
    Pk(PkRule),
}

impl std::fmt::Display for SchemaRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaRefusal::TooManyColumns(n) => write!(f, "column count {n} exceeds MAX_COLUMNS ({MAX_COLUMNS})"),
            SchemaRefusal::Pk(rule) => f.write_str(&rule.for_role(PkListRole::PrimaryKey)),
        }
    }
}

impl From<SchemaRefusal> for String {
    fn from(refusal: SchemaRefusal) -> String {
        refusal.to_string()
    }
}

impl SchemaDescriptor {
    /// The schema of `cols` keyed by `pk_indices`. `Err` for a list over the
    /// column limit and for a PK list that breaks the PK admission rules.
    pub fn try_new(cols: &[SchemaColumn], pk_indices: &[u32]) -> Result<Self, SchemaRefusal> {
        if cols.len() > MAX_COLUMNS {
            return Err(SchemaRefusal::TooManyColumns(cols.len()));
        }
        gnitz_wire::validate_pk_tuple(pk_indices, cols.len(), MAX_PK_COLUMNS, |c| {
            let col = &cols[c as usize];
            (col.type_code, col.nullable)
        })
        .map_err(SchemaRefusal::Pk)?;
        let pk_stride: usize = pk_indices.iter().map(|&c| cols[c as usize].size()).sum();
        let mut columns = [SchemaColumn::EMPTY; MAX_COLUMNS];
        columns[..cols.len()].copy_from_slice(cols);
        let mut pk = [0u32; MAX_PK_COLUMNS];
        pk[..pk_indices.len()].copy_from_slice(pk_indices);
        let mut payload_to_ci = [0u8; MAX_COLUMNS];
        for ci in 0..cols.len() {
            if let Some(pi) = gnitz_wire::payload_slot(pk_indices, ci) {
                payload_to_ci[pi] = ci as u8;
            }
        }
        let num_payload = cols.len() - pk_indices.len();
        let payload = payload_to_ci[..num_payload].iter().map(|&ci| cols[ci as usize]);
        let mut region_off = [0u16; gnitz_wire::MAX_WIRE_REGIONS];
        let (mut string_slots, mut nullable_slots) = (0u64, 0u64);
        let mut off = pk_stride + 16;
        region_off[gnitz_wire::REG_WEIGHT] = pk_stride as u16;
        region_off[gnitz_wire::REG_NULL_BMP] = pk_stride as u16 + 8;
        for (pi, col) in payload.clone().enumerate() {
            region_off[gnitz_wire::REG_PAYLOAD_START + pi] = off as u16;
            off += col.size();
            string_slots |= u64::from(col.type_code.is_german_string()) << pi;
            nullable_slots |= u64::from(col.nullable) << pi;
        }
        region_off[gnitz_wire::REG_PAYLOAD_START + num_payload] = off as u16;
        // The smallest power of two `q <= 8` with `q * region_off[r]` a multiple
        // of 8 for every region.
        let low = region_off[..gnitz_wire::REG_PAYLOAD_START + num_payload]
            .iter()
            .fold(8u16, |g, &o| g | o);
        let cap_mask = (8u8 >> low.trailing_zeros().min(3)) - 1;
        Ok(SchemaDescriptor {
            string_slots,
            nullable_slots,
            region_off,
            num_columns: cols.len() as u32,
            pk_count: pk_indices.len() as u32,
            pk_indices: pk,
            pk_stride: pk_stride as u8,
            payload_to_ci,
            fixed_int_nonnull: payload.clone().all(|c| !c.nullable && c.type_code.is_fixed_int()),
            cap_mask,
            columns,
        })
    }

    /// [`Self::try_new`] for a column list the caller has already admitted.
    #[track_caller]
    pub fn new(cols: &[SchemaColumn], pk_indices: &[u32]) -> Self {
        match Self::try_new(cols, pk_indices) {
            Ok(schema) => schema,
            Err(e) => panic!("SchemaDescriptor::new: {e}"),
        }
    }

    /// Number of logical columns in this schema (PK + payload).
    #[inline]
    pub const fn num_columns(&self) -> usize {
        self.num_columns as usize
    }

    /// True when `self` is `prev` with zero or more columns appended — every
    /// column `prev` had keeping its position, `type_code` and PK membership.
    /// What leaves a baked span-encode plan's offsets and payload slots valid.
    pub fn is_trailing_append_of(&self, prev: &SchemaDescriptor) -> bool {
        self.pk_cols() == prev.pk_cols()
            && self.num_columns() >= prev.num_columns()
            && (0..prev.num_columns()).all(|i| self.columns[i].type_code == prev.columns[i].type_code)
    }

    /// The PK columns in PK-list order, as `(col_idx, &SchemaColumn)`.
    #[inline]
    pub fn pk_columns(&self) -> impl Iterator<Item = (usize, &SchemaColumn)> {
        self.pk_cols()
            .iter()
            .map(move |&ci| (ci as usize, &self.columns[ci as usize]))
    }

    /// Total bytes per row of the PK region.
    #[inline]
    pub const fn pk_stride(&self) -> usize {
        self.pk_stride as usize
    }

    /// Number of non-PK ("payload") columns. `#[inline(always)]`: `Batch` derives
    /// its region count from this, so at `-O0` a plain `#[inline]` puts a real
    /// call on the per-row appenders that read it as a loop bound.
    #[inline(always)]
    pub const fn num_payload_cols(&self) -> usize {
        self.num_columns as usize - self.pk_count as usize
    }

    /// The non-PK ("payload") columns in schema order, as `(payload_idx,
    /// &SchemaColumn)`: `payload_idx` is the dense index of the column's batch
    /// region and null-bitmap bit.
    #[inline]
    pub fn payload_columns(&self) -> impl Iterator<Item = (usize, &SchemaColumn)> {
        (0..self.num_payload_cols()).map(move |pi| (pi, &self.columns[self.payload_to_ci[pi] as usize]))
    }

    /// Whether the schema carries a STRING/BLOB (German-string) column, and so
    /// whether a batch over it has a live blob region.
    #[inline]
    pub fn has_german_string(&self) -> bool {
        self.string_slots != 0
    }

    /// Bit `pi` set iff payload slot `pi`'s column is a German string (STRING or BLOB).
    #[inline]
    pub fn string_payload_slots(&self) -> u64 {
        self.string_slots
    }

    /// Bit `pi` set iff payload slot `pi`'s column admits NULL.
    #[inline]
    pub fn nullable_payload_slots(&self) -> u64 {
        self.nullable_slots
    }

    /// The null bits a conforming batch never sets: every bit but the nullable
    /// slots', those past the last payload slot included.
    #[inline]
    pub fn not_null_payload_slots(&self) -> u64 {
        !self.nullable_slots
    }

    /// Fixed regions of a batch: PK, weight, null words, one per payload column.
    #[inline(always)]
    pub const fn num_regions(&self) -> usize {
        gnitz_wire::REG_PAYLOAD_START + self.num_payload_cols()
    }

    /// One row's bytes across the fixed regions.
    #[inline(always)]
    pub const fn row_width(&self) -> usize {
        self.region_off[self.num_regions()] as usize
    }

    /// Where region `r` starts in an arena of `cap` rows: the regions lie back to
    /// back, `cap` rows each.
    #[inline(always)]
    pub const fn region_start(&self, r: usize, cap: usize) -> usize {
        cap * self.region_off[r] as usize
    }

    /// Bytes of one cell of region `r`.
    #[inline(always)]
    pub const fn region_stride(&self, r: usize) -> usize {
        (self.region_off[r + 1] - self.region_off[r]) as usize
    }

    /// Whether `other` lays a batch's regions out as this schema does.
    pub fn same_regions(&self, other: &SchemaDescriptor) -> bool {
        self.region_off[..=self.num_regions()] == other.region_off[..=other.num_regions()]
    }

    /// The capacity an arena for `rows` rows is allocated at: `rows` itself below
    /// eight, where no pass over a region is long enough for its alignment to
    /// matter, and else the next count that starts every region 8-aligned.
    #[inline]
    pub const fn arena_rows(&self, rows: usize) -> usize {
        let mask = self.cap_mask as usize;
        if rows < 8 {
            rows
        } else {
            (rows + mask) & !mask
        }
    }

    /// The live columns, in column order.
    #[inline]
    pub fn columns(&self) -> &[SchemaColumn] {
        &self.columns[..self.num_columns as usize]
    }

    /// Column `c` of a wire-supplied list, `what` naming the list in the refusal.
    pub fn wire_col(&self, what: impl std::fmt::Display, c: u32) -> Result<(SchemaColumn, ColumnLocator), String> {
        match self.try_locate(c as usize) {
            Some(loc) => Ok((self.columns[c as usize], loc)),
            None => Err(format!("{what} {c} out of range ({} cols)", self.num_columns())),
        }
    }

    /// True when `cols` holds every PK column. The span such a list encodes is
    /// a fixed-width OPK concatenation and widening is injective, so the span
    /// determines the row's PK: a unique index on `cols` cannot collide.
    pub fn covers_pk(&self, cols: &[u32]) -> bool {
        self.pk_cols().iter().all(|p| cols.contains(p))
    }

    /// This schema's PK columns alone, in PK-list order.
    pub fn pk_only(&self) -> SchemaDescriptor {
        let cols: Vec<SchemaColumn> = self.pk_columns().map(|(_, c)| *c).collect();
        let pk: Vec<u32> = (0..cols.len() as u32).collect();
        SchemaDescriptor::new(&cols, &pk)
    }

    /// The column index of payload slot `pi`, which must be below `num_payload_cols()`.
    #[inline]
    pub fn payload_col_idx(&self, pi: usize) -> usize {
        debug_assert!(pi < self.num_payload_cols(), "payload_col_idx: pi {pi} out of range");
        self.payload_to_ci[pi] as usize
    }

    /// The PK columns in PK-list order, which is the order they pack into the
    /// OPK region.
    #[inline]
    pub fn pk_cols(&self) -> &[u32] {
        &self.pk_indices[..self.pk_count as usize]
    }

    /// Every payload column is a non-nullable integer of at most 8 bytes;
    /// vacuously true with no payload column.
    #[inline(always)]
    pub fn payload_is_fixed_int_nonnull(&self) -> bool {
        self.fixed_int_nonnull
    }

    /// Bytes of the leading `k` PK columns; the whole PK stride at
    /// `k == pk_cols().len()`.
    pub fn pk_prefix_stride(&self, k: usize) -> usize {
        self.pk_cols()[..k]
            .iter()
            .map(|&c| self.columns[c as usize].size())
            .sum()
    }

    /// Where column `ci`'s value physically lives, or `None` when `ci` is out of
    /// range.
    pub fn try_locate(&self, ci: usize) -> Option<ColumnLocator> {
        if ci >= self.num_columns() {
            return None;
        }
        let SchemaColumn { type_code, size, .. } = self.columns[ci];
        Some(match gnitz_wire::payload_slot(self.pk_cols(), ci) {
            Some(slot) => ColumnLocator::Payload { slot: slot as u8, size, type_code },
            None => {
                let k = self
                    .pk_cols()
                    .iter()
                    .position(|&p| p as usize == ci)
                    .expect("a PK column");
                ColumnLocator::Pk {
                    byte_off: self.pk_prefix_stride(k) as u8,
                    size,
                    type_code,
                }
            }
        })
    }

    /// [`Self::try_locate`] for a column that must exist. Panics on an
    /// out-of-range `ci`.
    pub fn locate(&self, ci: usize) -> ColumnLocator {
        self.try_locate(ci).unwrap_or_else(|| {
            panic!(
                "locate: col_idx {ci} out of bounds (num_columns = {})",
                self.num_columns()
            )
        })
    }

    /// True iff column `ci` is a PK column; false for an out-of-range `ci`.
    pub fn is_pk_col(&self, ci: usize) -> bool {
        self.pk_cols().iter().any(|&p| p as usize == ci)
    }

    /// Dense payload slot of `ci`, or `None` for a PK column or an out-of-range
    /// `ci`.
    pub fn payload_slot(&self, ci: usize) -> Option<usize> {
        self.try_locate(ci)?.payload_slot()
    }

    /// Every payload column's locator, in slot order.
    pub fn payload_locators(&self) -> Vec<ColumnLocator> {
        self.payload_columns()
            .map(|(pi, c)| ColumnLocator::Payload {
                slot: pi as u8,
                size: c.size,
                type_code: c.type_code,
            })
            .collect()
    }

    /// The PK column when the PK is exactly one column.
    pub fn lone_pk_col(&self) -> Option<usize> {
        match self.pk_cols() {
            [c] => Some(*c as usize),
            _ => None,
        }
    }

    /// The types of a batch's regions: the PK columns in PK-list order, then the
    /// payload columns in slot order.
    fn region_types(&self) -> impl Iterator<Item = TypeCode> + '_ {
        let pk = self.pk_columns().map(|(_, c)| c.type_code);
        pk.chain(self.payload_columns().map(|(_, c)| c.type_code))
    }

    /// The digest a read request names its reply layout by: its regions' types.
    /// Column numbering and nullability are not part of it.
    pub fn layout_digest(&self) -> u64 {
        gnitz_wire::layout_digest(self.pk_cols().len(), self.region_types())
    }

    /// Same PK list and per-column type codes: the same regions under the same
    /// column numbering. Nullability is not compared.
    pub fn same_layout(&self, other: &SchemaDescriptor) -> bool {
        self.num_columns() == other.num_columns() && self.is_trailing_append_of(other)
    }

    /// Whether a batch of `self` and one of `other` have the same regions — what
    /// [`Self::layout_digest`] digests. Column numbering and nullability are
    /// labels over those bytes.
    pub fn same_region_types(&self, other: &SchemaDescriptor) -> bool {
        self.pk_cols().len() == other.pk_cols().len() && self.region_types().eq(other.region_types())
    }

    /// The output-key kind a reduce grouped by `group` over this schema warrants.
    pub fn reduce_out_key(&self, group: &[u32]) -> gnitz_wire::ReduceOutKey {
        gnitz_wire::ReduceOutKey::for_group_cols(self.pk_cols(), group, |c| {
            let col = self.columns[c as usize];
            (col.type_code, col.nullable)
        })
    }

    /// One row's PK as OPK bytes, from one native value per PK column in PK-list
    /// order. A signed value is passed sign-extended (`v as u128`).
    pub fn opk_key_cols(&self, natives: &[u128]) -> gnitz_wire::PkBuf {
        debug_assert_eq!(
            natives.len(),
            self.pk_cols().len(),
            "opk_key_cols: one native value per PK column",
        );
        let mut key = gnitz_wire::PkBuf::zeroed(0);
        for ((_, col), &v) in self.pk_columns().zip(natives) {
            let (tc, w) = (col.type_code, col.size());
            debug_assert!(
                {
                    let dropped = v.checked_shr(w as u32 * 8).unwrap_or(0);
                    dropped == 0 || dropped == u128::MAX >> (w * 8)
                },
                "opk_key_cols: {v:#x} does not fit a {tc:?} column",
            );
            key.push(w, v, tc.is_signed_int());
        }
        key
    }
}

impl std::fmt::Debug for SchemaDescriptor {
    // The fixed-size `columns` / `pk_indices` arrays make a derive useless (it
    // would dump all MAX_COLUMNS slots). Print only the live columns — their
    // type, a `?` for nullable, and `pk` for PK columns — plus the PK index list.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SchemaDescriptor {{ columns: [")?;
        for ci in 0..self.num_columns() {
            if ci > 0 {
                write!(f, ", ")?;
            }
            let col = self.columns[ci];
            write!(f, "{:?}", col.type_code)?;
            if col.nullable {
                write!(f, "?")?;
            }
            if self.is_pk_col(ci) {
                write!(f, " pk")?;
            }
        }
        write!(f, "], pk_indices: {:?} }}", self.pk_cols())
    }
}

impl PartialEq for SchemaDescriptor {
    fn eq(&self, other: &Self) -> bool {
        if self.num_columns() != other.num_columns() || self.pk_cols() != other.pk_cols() {
            return false;
        }
        self.columns[..self.num_columns()] == other.columns[..other.num_columns()]
    }
}

impl Eq for SchemaDescriptor {}

/// Rebuild a [`SchemaDescriptor`] from a meta-schema record. Column names are
/// carried on the wire but nothing engine-side reads one.
pub fn decode_schema_block(data: &[u8]) -> Result<SchemaDescriptor, String> {
    let mut cols = [SchemaColumn::EMPTY; MAX_COLUMNS];
    let mut n = 0;
    let pk = gnitz_wire::schema_block::decode(data, |c| {
        cols[n] = SchemaColumn::new(c.ty.tc, c.nullable);
        n += 1;
        Ok(())
    })?;
    Ok(SchemaDescriptor::try_new(&cols[..n], pk.as_slice())?)
}

/// Encode `schema`'s physical column shape — the inverse of
/// [`decode_schema_block`]: no names, no hidden flag, every scale zero.
pub fn encode_schema_block(schema: &SchemaDescriptor) -> Vec<u8> {
    let cols = schema.columns[..schema.num_columns()].iter().map(|c| SchemaBlockCol {
        ty: ColType::of(c.type_code),
        nullable: c.nullable,
        hidden: false,
        name: b"",
    });
    gnitz_wire::schema_block::encode(cols, schema.pk_cols())
}

#[cfg(test)]
#[path = "tests/schema.rs"]
mod tests;
