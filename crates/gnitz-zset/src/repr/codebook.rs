//! The symbol table a store codes its shard heaps under (FSST), as an encoder
//! and a decoder of one table.
//!
//! A value coded under one table is the same codes whoever codes it — the
//! encoder a shard's table is rebuilt into codes every value as the one trained
//! did — so a shard written under a store's codebook copies the codes of every
//! value it reads from a shard of the same table, and codes only the rest.

use std::rc::Rc;

use gnitz_wire::{push_coded, push_codes, HeapValue, SymbolTable, MAX_SYMBOL_LEN};

use super::batch::Batch;
use super::shard_reader::MappedShard;

const _: () = assert!(
    fsst::ESCAPE_CODE == gnitz_wire::SYMBOL_ESCAPE,
    "the heap decoder reads the encoder's escape code"
);

/// Long-value bytes a heap must hold before a table is trained over it: below
/// four full tables, the table alone takes a quarter of them.
const MIN_CODED_HEAP: usize = 4 * super::encoding::symbols_head_len(gnitz_wire::MAX_SYMBOLS);

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
    /// The codebook of `shard`'s table; `None` for a plain heap, or a table no
    /// trained encoder could have written.
    pub fn of_shard(shard: &MappedShard) -> Option<Self> {
        let image = shard.coded_heap()?;
        let table = &image.table;
        let (symbols, lens): (Vec<fsst::Symbol>, Vec<u8>) = table
            .symbols()
            .map(|(sym, len)| (fsst::Symbol::from_slice(sym), len))
            .unzip();
        // The order a trained table keeps, which the rebuild asserts: the longer
        // symbols ascending by length, then the one-byte ones.
        let multi = lens.iter().take_while(|&&n| n > 1).count();
        if !lens[..multi].is_sorted() || lens[multi..].iter().any(|&n| n != 1) {
            return None;
        }
        Some(Self::of_encoder(
            fsst::Compressor::rebuild_from(symbols, lens),
            image.trained_sixteenths,
        ))
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
        let schema = batch.schema();
        let blob = batch.blob();
        // Each long value once per cell naming it.
        let values: Vec<&[u8]> = schema
            .payload_columns()
            .filter(|(_, col)| col.type_code.is_german_string())
            .flat_map(|(pi, _)| batch.col_data(pi).as_chunks::<16>().0)
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
