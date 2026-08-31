//! Covers the duplicate-key rejection path a unique-index backfill runs over
//! each projected batch.

use super::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};

/// Build a projected-index batch whose PK region is each supplied span
/// (here the whole index PK; the dedup keys on the leading `key_size`).
fn idx_batch(spans: &[[u8; 24]]) -> Batch {
    // Three U64 PK columns → a 24-byte composite span (> 16 bytes).
    let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0); 3], &[0, 1, 2]);
    let mut b = Batch::with_capacity(schema, spans.len().max(1));
    for s in spans {
        b.ensure_row_capacity();
        b.extend_pk_bytes(s);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.count += 1;
    }
    b
}

/// A >16-byte composite span is deduped on its FULL width: two spans sharing
/// their leading 16 bytes but differing in the trailing 8 are distinct (the
/// `u128` truncation this replaced would have falsely reported a duplicate),
/// while an exact repeat is a duplicate.
#[test]
fn dup_keys_over_16_bytes_no_truncation() {
    let span = |tail: u64| {
        let mut s = [0u8; 24];
        s[..16].copy_from_slice(&[7u8; 16]); // shared leading 16 bytes
        s[16..].copy_from_slice(&tail.to_be_bytes());
        s
    };

    let mut seen = rustc_hash::FxHashSet::default();
    assert!(
        !projected_chunk_has_dup_keys(&idx_batch(&[span(1), span(2)]), 24, &mut seen),
        "distinct 24-byte spans sharing a 16-byte prefix are NOT duplicates",
    );

    let mut seen2 = rustc_hash::FxHashSet::default();
    assert!(
        projected_chunk_has_dup_keys(&idx_batch(&[span(1), span(1)]), 24, &mut seen2),
        "identical 24-byte spans are duplicates",
    );
}

/// `seen` carries cross-chunk state: a duplicate split across two chunk
/// calls is caught on the second chunk.
#[test]
fn dup_keys_cross_chunk() {
    let span = |tail: u64| {
        let mut s = [0u8; 24];
        s[16..].copy_from_slice(&tail.to_be_bytes());
        s
    };
    let mut seen = rustc_hash::FxHashSet::default();
    assert!(!projected_chunk_has_dup_keys(&idx_batch(&[span(1)]), 24, &mut seen));
    assert!(
        projected_chunk_has_dup_keys(&idx_batch(&[span(1)]), 24, &mut seen),
        "the same span in a later chunk is a cross-chunk duplicate",
    );
}
