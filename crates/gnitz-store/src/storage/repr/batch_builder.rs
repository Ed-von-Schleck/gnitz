//! [`BatchBuilder`] — the row-at-a-time writer the system-table mutations build
//! their rows with. It owns its `Batch` and grows it; `merge::DirectWriter` is
//! the same shape over a pre-carved arena.

use super::batch::Batch;
use crate::schema::{SchemaDescriptor, SchemaFacts};

/// Lightweight row-by-row builder for constructing Batch in Rust.
/// Operates on Batch directly; the schema lives on the batch itself.
pub struct BatchBuilder {
    batch: Batch,
    // per-row state
    curr_null_word: u64,
    curr_col: usize,
}

/// The engine half of the shared catalog row codecs: the sink
/// `gnitz_wire::sys_rows` writes a system-table row into.
impl gnitz_wire::sys_rows::SysRowSink for BatchBuilder {
    fn begin_row(&mut self, pk: &[u128], weight: i64) {
        BatchBuilder::begin_row_opk(self, pk, weight);
    }
    fn put_u64(&mut self, v: u64) {
        BatchBuilder::put_u64(self, v);
    }
    fn put_string(&mut self, s: &str) {
        BatchBuilder::put_string(self, s);
    }
    fn put_bytes(&mut self, b: &[u8]) {
        BatchBuilder::put_blob(self, b);
    }
    fn put_null(&mut self) {
        BatchBuilder::put_null(self);
    }
    fn end_row(&mut self) {
        BatchBuilder::end_row(self);
    }
}

impl BatchBuilder {
    pub fn new(schema: SchemaDescriptor) -> Self {
        BatchBuilder {
            // Uninitialized, like every batch arena: every row writes every
            // column (`put_null` zero-fills rather than skipping).
            batch: Batch::with_capacity(&schema, 8),
            curr_null_word: 0,
            curr_col: 0,
        }
    }

    /// Begin a new row with the given single-column PK and weight: `pk` is the
    /// column's native value, as [`Self::begin_row_opk`] takes it.
    pub fn begin_row(&mut self, pk: u128, weight: i64) {
        self.begin_row_opk(&[pk], weight);
    }

    /// [`Self::begin_row`] for a **compound** PK: `natives` are the PK columns'
    /// native values in PK-list order, OPK-encoded into the packed PK region.
    pub fn begin_row_opk(&mut self, natives: &[u128], weight: i64) {
        let key = self.schema().opk_key_cols(natives);
        self.begin_row_bytes(key.pk_bytes(), weight);
    }

    /// [`Self::begin_row`] for a PK already in its at-rest OPK image: `pk` is
    /// written verbatim and must be exactly `pk_stride` bytes.
    pub fn begin_row_bytes(&mut self, pk: &[u8], weight: i64) {
        self.batch.begin_row(pk, weight);
        self.curr_null_word = 0;
        self.curr_col = 0;
    }

    /// Put an integer value for the current payload column, in that column's own
    /// width — so a value and its column cannot be mismatched, the shape
    /// [`Self::put_null`] below already has.
    ///
    /// Signed values are passed as `v as u128`: sign-extending to 128 bits leaves
    /// the low `size()` bytes exactly the two's-complement image the column
    /// holds. That is the same native convention [`Self::begin_row_opk`] takes
    /// for PK columns.
    pub fn put_int(&mut self, val: u128) {
        let col_size = self.schema().columns[self.physical_col_idx()].size() as usize;
        debug_assert!(
            col_size == 16 || {
                // The bytes about to be dropped must carry no information: all
                // zero for an unsigned value, all one for a sign-extended
                // negative. Anything else is a value too wide for its column.
                let dropped = val >> (col_size * 8);
                dropped == 0 || dropped == u128::MAX >> (col_size * 8)
            },
            "put_int: {val:#x} does not fit the column's {col_size} bytes",
        );
        self.batch.extend_col(self.curr_col, &val.to_le_bytes()[..col_size]);
        self.curr_col += 1;
    }

    /// [`Self::put_int`] under the name `SysRowSink` requires; every system-table
    /// payload column is a U64.
    pub fn put_u64(&mut self, val: u64) {
        self.put_int(val as u128);
    }

    /// Put raw bytes for the current STRING/BLOB payload column;
    /// `payload_bytes` is the read-back twin.
    pub fn put_blob(&mut self, b: &[u8]) {
        self.batch.extend_col_blob(self.curr_col, b);
        self.curr_col += 1;
    }

    /// Put a string value for the current payload column.
    pub fn put_string(&mut self, s: &str) {
        self.put_blob(s.as_bytes());
    }

    /// Put a NULL value for the current payload column.
    pub fn put_null(&mut self) {
        self.batch.fill_col_zero(self.curr_col);
        gnitz_wire::null_word_set(&mut self.curr_null_word, self.curr_col, true);
        self.curr_col += 1;
    }

    /// Put a float for the current payload column, narrowed to that column's own
    /// width: an F32 column stores the `as f32` narrowing.
    #[cfg(test)]
    pub(crate) fn put_float(&mut self, val: f64) {
        let col_size = self.schema().columns[self.physical_col_idx()].size() as usize;
        match col_size {
            4 => self.batch.extend_col(self.curr_col, &(val as f32).to_le_bytes()),
            _ => self.batch.extend_col(self.curr_col, &val.to_le_bytes()),
        }
        self.curr_col += 1;
    }

    /// [`Self::put_int`], or [`Self::put_null`] for `None`.
    #[cfg(test)]
    pub(crate) fn put_opt_int(&mut self, val: Option<u128>) {
        match val {
            Some(v) => self.put_int(v),
            None => self.put_null(),
        }
    }

    /// Finish the current row: the accumulated null word, through the shared
    /// `Batch::commit_row`.
    pub fn end_row(&mut self) {
        // Nothing else notices a row that skipped a column: the count still
        // advances and the short region keeps whatever bytes were there.
        debug_assert_eq!(
            self.curr_col,
            self.schema().num_payload_cols(),
            "BatchBuilder row got {} of {} payload columns",
            self.curr_col,
            self.schema().num_payload_cols(),
        );
        self.batch.commit_row(self.curr_null_word);
    }

    /// Consume the builder, returning the built batch.
    pub fn finish(self) -> Batch {
        self.batch
    }

    fn schema(&self) -> &SchemaDescriptor {
        self.batch.schema()
    }

    fn physical_col_idx(&self) -> usize {
        self.schema().payload_col_idx(self.curr_col)
    }
}

#[cfg(test)]
#[path = "tests/batch_builder.rs"]
mod tests;
