//! [`BatchBuilder`] — the row-at-a-time writer of a batch: the catalog's seed rows and
//! the sequence delta, a client's catalog rows and pushed rows, and the test fixtures.
//! It writes a batch of its own or appends onto one it borrows.

use std::borrow::BorrowMut;

use super::batch::Batch;
use crate::schema::SchemaDescriptor;
use gnitz_wire::TypeCode;

/// Row-by-row builder of a `Batch`, whose schema lives on the batch itself: `B` is the
/// batch, owned, or `&mut Batch` to append onto.
///
/// A row is opened with its key and weight, then takes one value per payload column in
/// slot order, each checked against its column: a native against the column's width, a string or blob against a string column, a float against a float column. The
/// row joins the batch when its last value lands; opening the next row or
/// [`Self::end_row`] before then panics, as a short row would leave cells unwritten.
pub struct BatchBuilder<B: BorrowMut<Batch> = Batch> {
    batch: B,
    /// The open row's next payload column; the payload count while no row is open.
    curr_col: usize,
}

/// The sink a `gnitz_wire` system row writes itself into: the shared catalog row codecs.
impl<B: BorrowMut<Batch>> gnitz_wire::SysRowSink for BatchBuilder<B> {
    fn begin_row(&mut self, pk: &[u128], weight: i64) {
        BatchBuilder::begin_row_natives(self, pk, weight);
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
    fn end_row(&mut self) {
        BatchBuilder::end_row(self);
    }
}

impl BatchBuilder {
    /// A builder of a new batch under `schema`.
    pub fn new(schema: &SchemaDescriptor) -> Self {
        BatchBuilder::onto(Batch::empty_with_schema(schema))
    }

    /// Consume the builder, returning the built batch.
    pub fn finish(self) -> Batch {
        self.check_row_closed();
        self.batch
    }
}

impl<B: BorrowMut<Batch>> BatchBuilder<B> {
    /// A builder appending rows onto `batch`, under its schema.
    pub fn onto(batch: B) -> Self {
        let curr_col = batch.borrow().schema().num_payload_cols();
        BatchBuilder { batch, curr_col }
    }

    /// Begin a new row with the given single-column PK and weight: `pk` is the
    /// column's native value, as [`Self::begin_row_natives`] takes it.
    pub fn begin_row(&mut self, pk: u128, weight: i64) -> &mut Self {
        self.begin_row_natives(&[pk], weight)
    }

    /// [`Self::begin_row`] for a **compound** PK: `natives` are the PK columns'
    /// native values in PK-list order, OPK-encoded into the packed PK region.
    pub fn begin_row_natives(&mut self, natives: &[u128], weight: i64) -> &mut Self {
        // Hard, not debug-only: a short list would encode a short key, and the next
        // column's bytes would land in its place.
        assert_eq!(
            natives.len(),
            self.schema().pk_cols().len(),
            "BatchBuilder: one native value per key column"
        );
        let key = self.schema().opk_key_cols(natives);
        self.begin_row_bytes(key.pk_bytes(), weight)
    }

    /// [`Self::begin_row`] for a PK already in its at-rest OPK image: `pk` is
    /// written verbatim and must be exactly `pk_stride` bytes.
    pub fn begin_row_bytes(&mut self, pk: &[u8], weight: i64) -> &mut Self {
        self.check_row_closed();
        self.batch.borrow_mut().begin_row(pk, weight);
        self.curr_col = 0;
        self.close_if_full();
        self
    }

    /// Put a native for the current fixed-width payload column, in that column's own width:
    /// an integer, or a float's bits, no wider than the column.
    ///
    /// Signed values are passed as `v as u128`: sign-extending to 128 bits leaves
    /// the low `size()` bytes exactly the two's-complement image the column
    /// holds. That is the same native convention [`Self::begin_row_natives`] takes
    /// for PK columns.
    pub fn put_int(&mut self, val: u128) -> &mut Self {
        let (pi, tc) = self.next_col();
        assert!(
            !tc.is_german_string(),
            "BatchBuilder: an integer cannot be written to the {tc:?} column at payload slot {pi}"
        );
        assert!(
            gnitz_wire::native_fits(val, tc.wire_stride()),
            "BatchBuilder: {val:#x} is wider than the {tc:?} column at payload slot {pi}"
        );
        let width = tc.wire_stride();
        self.batch.borrow_mut().extend_col(pi, &val.to_le_bytes()[..width]);
        self.advance()
    }

    /// [`Self::put_int`] for a u64 value.
    pub fn put_u64(&mut self, val: u64) -> &mut Self {
        self.put_int(val as u128)
    }

    /// Put raw bytes for the current STRING or BLOB payload column; `payload_bytes` is
    /// the read-back twin. A STRING column's bytes are UTF-8 only where a server's
    /// ingest checks them.
    pub fn put_blob(&mut self, b: &[u8]) -> &mut Self {
        self.put_german(b, "Blob")
    }

    /// Put a string value for the current STRING or BLOB payload column.
    pub fn put_string(&mut self, s: &str) -> &mut Self {
        self.put_german(s.as_bytes(), "String")
    }

    /// One German-string cell of a `what` value, its long value in the batch's heap.
    fn put_german(&mut self, b: &[u8], what: &str) -> &mut Self {
        let (pi, tc) = self.next_col();
        assert!(
            tc.is_german_string(),
            "BatchBuilder: a {what} value cannot be written to the {tc:?} column at payload slot {pi}"
        );
        self.batch.borrow_mut().extend_col_blob(pi, b);
        self.advance()
    }

    /// Put a NULL value for the current payload column, whatever its type: a zeroed
    /// cell under its bitmap bit.
    pub fn put_null(&mut self) -> &mut Self {
        let (pi, _) = self.next_col();
        self.batch.borrow_mut().put_null(pi);
        self.advance()
    }

    /// Put a float for the current float payload column, narrowed to that column's own
    /// width: an F32 column stores the `as f32` narrowing.
    pub fn put_float(&mut self, val: f64) -> &mut Self {
        let (pi, tc) = self.next_col();
        let batch = self.batch.borrow_mut();
        match tc {
            TypeCode::F32 => batch.extend_col(pi, &(val as f32).to_le_bytes()),
            TypeCode::F64 => batch.extend_col(pi, &val.to_le_bytes()),
            _ => panic!("BatchBuilder: a float cannot be written to the {tc:?} column at payload slot {pi}"),
        }
        self.advance()
    }

    /// Put a zeroed cell that is not NULL for the current payload column, whatever its
    /// type: a filler no reader reads.
    pub fn put_zero(&mut self) -> &mut Self {
        let (pi, _) = self.next_col();
        self.batch.borrow_mut().open_cell_mut(pi).0.fill(0);
        self.advance()
    }

    /// Write the current payload column's cell through `fill`, for a caller that encodes
    /// the value itself: the cell, its type's wire stride wide, to write whole, and the heap
    /// a German cell's long value lands in. A `fill` that fails leaves the column unwritten
    /// and the row open, so no row with a half-written cell joins the batch.
    pub fn put_cell<E>(&mut self, fill: impl FnOnce(&mut [u8], &mut Vec<u8>) -> Result<(), E>) -> Result<&mut Self, E> {
        let (pi, _) = self.next_col();
        let (cell, heap) = self.batch.borrow_mut().open_cell_mut(pi);
        fill(cell, heap)?;
        Ok(self.advance())
    }

    /// [`Self::put_int`], or [`Self::put_null`] for `None`.
    pub fn put_opt_int(&mut self, val: Option<u128>) -> &mut Self {
        match val {
            Some(v) => self.put_int(v),
            None => self.put_null(),
        }
    }

    /// Close the current row: every payload column got its value, which closed it.
    pub fn end_row(&mut self) {
        self.check_row_closed();
    }

    fn schema(&self) -> &SchemaDescriptor {
        self.batch.borrow().schema()
    }

    /// The open row's next payload column and its type.
    fn next_col(&self) -> (usize, TypeCode) {
        let n = self.schema().num_payload_cols();
        assert!(
            self.curr_col < n,
            "BatchBuilder: no payload column open (column {} of {n}): a value before begin_row or past the row's last column",
            self.curr_col,
        );
        let schema = self.schema();
        (
            self.curr_col,
            schema.columns()[schema.payload_col_idx(self.curr_col)].type_code,
        )
    }

    /// Past the column just written; the row joins the batch once its last one is.
    fn advance(&mut self) -> &mut Self {
        self.curr_col += 1;
        self.close_if_full();
        self
    }

    fn close_if_full(&mut self) {
        if self.curr_col == self.schema().num_payload_cols() {
            self.batch.borrow_mut().commit_row();
        }
    }

    /// No row is open: the last one got a value per payload column.
    fn check_row_closed(&self) {
        let n = self.schema().num_payload_cols();
        assert!(
            self.curr_col == n,
            "BatchBuilder: row got {} of {n} payload columns",
            self.curr_col,
        );
    }
}

#[cfg(test)]
#[path = "tests/batch_builder.rs"]
mod tests;
