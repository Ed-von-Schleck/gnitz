//! The symbol table a store codes its shard heaps under (FSST), as an encoder
//! and a decoder of one table.
//!
//! A shard written under a store's codebook copies the codes of every value
//! already coded under the same table, and codes only the rest. The encoder a
//! shard's table is rebuilt into codes every value as the one trained did, so a
//! value has the same codes in every shard of one table.

use std::rc::Rc;

use gnitz_wire::{HeapValue, MAX_SYMBOL_LEN, MAX_SYMBOLS, SymbolTable, push_coded, push_codes};

use super::batch::Batch;

const _: () = assert!(
    fsst::ESCAPE_CODE == gnitz_wire::SYMBOL_ESCAPE,
    "the heap decoder reads the encoder's escape code"
);

/// Long-value bytes a heap must hold before a table is trained over it: below
/// four full tables, the table alone takes a quarter of them.
const MIN_CODED_HEAP: usize = 4 * super::shard::symbols_head_len(MAX_SYMBOLS);

/// Plain bytes of the values sampled to price a table against a batch.
const PROBE_BYTES: usize = 16 << 10;

/// How much worse than at its training, in sixteenths of the plain bytes, a
/// table may code a batch before one trained on that batch replaces it.
const DRIFT_SIXTEENTHS: usize = 1;

/// One symbol table, as the encoder a writer codes values with and the table a
/// reader decodes them through.
pub struct Codebook {
    encoder: fsst::Compressor,
    table: Box<SymbolTable>,
    /// What the table codes a probe of its training values to, in sixteenths
    /// of their plain bytes.
    trained_sixteenths: usize,
}

impl Codebook {
    /// The codebook of a table read back, its symbols and their lengths, which
    /// coded its training values to `trained_sixteenths`.
    pub(crate) fn rebuilt<'s>(
        symbols: impl Iterator<Item = (&'s [u8; MAX_SYMBOL_LEN], u8)>,
        trained_sixteenths: usize,
    ) -> Self {
        let (symbols, lens): (Vec<fsst::Symbol>, Vec<u8>) =
            symbols.map(|(sym, len)| (fsst::Symbol::from_slice(sym), len)).unzip();
        Self::of_encoder(fsst::Compressor::rebuild_from(symbols, lens), trained_sixteenths)
    }

    fn of_encoder(encoder: fsst::Compressor, trained_sixteenths: usize) -> Self {
        let symbols: Vec<[u8; MAX_SYMBOL_LEN]> = encoder
            .symbol_table()
            .iter()
            .map(|s| s.to_u64().to_le_bytes())
            .collect();
        let table = SymbolTable::new(&symbols, encoder.symbol_lengths()).expect("a trained table");
        Codebook { encoder, table, trained_sixteenths }
    }

    /// The table, as a decoder reads it.
    pub(crate) fn table(&self) -> &SymbolTable {
        &self.table
    }

    /// The table's [`SymbolTable::digest`].
    pub fn digest(&self) -> u64 {
        self.table.digest()
    }

    /// How the table coded a probe of its training values, in sixteenths of
    /// their plain bytes.
    pub(crate) fn trained_sixteenths(&self) -> usize {
        self.trained_sixteenths
    }

    /// Append `value`'s codes to `dst`.
    pub(crate) fn encode_into(&self, value: &[u8], dst: &mut Vec<u8>) {
        let at = dst.len();
        // Every byte an escape, at worst: two code bytes each.
        dst.reserve(2 * value.len());
        // SAFETY: the spare capacity holds two bytes per input byte, the most
        // `compress_into` writes, and it reports how many it wrote.
        unsafe {
            let n = self.encoder.compress_into(value, dst.spare_capacity_mut());
            dst.set_len(at + n);
        }
    }

    /// Append `value` to `dst`, a heap coded under this table: the codes it is
    /// stored as, where it is coded under this table, else its bytes coded.
    pub(crate) fn append(&self, value: HeapValue<'_>, dst: &mut Vec<u8>) {
        match value.table_digest() == Some(self.table.digest()) {
            true => push_codes(dst, value.stored()),
            false => push_coded(dst, |dst| self.encode_into(&value.bytes(), dst)),
        }
    }

    /// What the table codes values spread evenly over `values`, about
    /// `PROBE_BYTES` of them, to, in sixteenths of their plain bytes.
    fn probe_sixteenths(&self, values: &[&[u8]]) -> usize {
        let total: usize = values.iter().map(|v| v.len()).sum();
        let stride = (total / PROBE_BYTES).max(1);
        let (mut plain, mut codes) = (0, Vec::new());
        for v in values.iter().step_by(stride) {
            plain += v.len();
            self.encode_into(v, &mut codes);
        }
        (16 * codes.len()).div_ceil(plain.max(1))
    }

    /// The codebook a store holding `current` codes `batch`'s heap under:
    /// `current` while it codes the batch's long values within
    /// `DRIFT_SIXTEENTHS` of how it coded its own training values, else one
    /// trained on them; `None` while they are too few to pay for a table.
    pub fn for_batch(current: Option<&Rc<Codebook>>, batch: &Batch) -> Option<Rc<Codebook>> {
        let blob = batch.blob();
        // Each long value once per cell naming it.
        let values: Vec<&[u8]> = gnitz_wire::BitIter(batch.schema().string_payload_slots())
            .flat_map(|pi| batch.col_data(pi).as_chunks::<16>().0)
            .filter_map(|cell| gnitz_wire::german_string_heap(cell, blob.len()).map(|r| &blob[r]))
            .collect();
        if values.iter().map(|v| v.len()).sum::<usize>() < MIN_CODED_HEAP {
            return None;
        }
        match current {
            Some(book) if book.probe_sixteenths(&values) <= book.trained_sixteenths + DRIFT_SIXTEENTHS => {
                Some(Rc::clone(book))
            }
            _ => {
                // A table trained on `values`.
                let mut book = Self::of_encoder(fsst::Compressor::train(&values), 0);
                book.trained_sixteenths = book.probe_sixteenths(&values);
                Some(Rc::new(book))
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/codebook.rs"]
mod tests;
