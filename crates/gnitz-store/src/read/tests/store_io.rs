use super::*;
use crate::relation::{IndexClaim, RelationSpec, StoreConfig};
use crate::schema::{SchemaColumn, SchemaDescriptor, Slot, TypeCode};
use crate::storage::BatchBuilder;
use gnitz_wire::{key_image, Cut, PkColList};

/// The base relation every test below reads.
const TID: u64 = gnitz_wire::FIRST_USER_TABLE_ID;

const NBASE: u64 = 200;

/// A `(id U64 PK | val I64)` base of `NBASE` rows at `val = id * 10`, indexed
/// on `val`.
fn fixture(name: &str) -> RelationRegistry {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let mut registry = RelationRegistry::new(
        &crate::test_support::scratch_dir("store_io", name),
        Slot::SOLO,
        StoreConfig::default(),
    );
    registry
        .register(RelationSpec {
            id: TID,
            kind: RelationKind::BaseTable,
            schema,
        })
        .unwrap();
    registry
        .add_index(TID, IndexClaim::Index { id: TID + 1, unique: false }, &[1])
        .unwrap();
    let mut bb = BatchBuilder::new(schema);
    for i in 0..NBASE {
        bb.begin_row(i as u128, 1);
        bb.put_int(i as u128 * 10);
        bb.end_row();
    }
    registry.ingest(TID, bb.finish()).unwrap();
    registry
}

/// The bound `val ∈ [500, 600)`: ids 50..60.
fn val_50s() -> ReadBound {
    let img = |v: i64| key_image(TypeCode::I64, v as u64 as u128);
    ReadBound::Range(KeyRange::new(
        PkColList::from_slice(&[1]),
        &[],
        Cut::before(img(500)),
        Cut::before(img(600)),
    ))
}

/// Drain a cursor to `(pk, weight)` pairs, in chunks of `chunk` so a chunk
/// boundary lands mid-range.
fn drain_all(cur: &mut SourceCursor, chunk: usize) -> Vec<(u128, i64)> {
    let mut out = Vec::new();
    while let Some(b) = cur.drain_chunk(chunk) {
        assert!(!b.is_empty(), "drain_chunk yielded an empty chunk");
        for i in 0..b.len() {
            out.push((b.get_pk(i), b.get_weight(i)));
        }
    }
    out.sort_unstable();
    out
}

/// The `val` index's live key for base row `id`.
fn index_key_of(registry: &RelationRegistry, id: u64) -> Vec<u8> {
    let entry = registry.relation_or_err(TID).unwrap();
    let ic = &entry.indexes()[0];
    let src_pk_stride = entry.schema().pk_stride();
    let idx_key_size = ic.key_spec().key_size();
    let mut probe = ic.cursor();
    while probe.valid {
        let k = probe.current_pk_bytes();
        if k[idx_key_size..idx_key_size + src_pk_stride] == id.to_be_bytes()[..src_pk_stride] {
            return k.to_vec();
        }
        probe.advance();
    }
    panic!("no live index entry for id {id}");
}

/// Write `key` into the `val` index at `+1`, bypassing the base.
fn forge_index_entry(registry: &mut RelationRegistry, key: &[u8]) {
    let idx_schema = registry.relation_or_err(TID).unwrap().indexes()[0].schema();
    let mut b = Batch::with_capacity(&idx_schema, 1);
    b.push_key_row(key, 1);
    registry
        .relation_mut(TID)
        .and_then(|r| r.index_on_mut(&[1]))
        .unwrap()
        .ingest_owned_batch(b)
        .unwrap();
}

/// A live index entry whose base row is gone, in the middle of a chunk, does not
/// cut the rows after it out of that chunk.
#[test]
fn absent_pk_mid_chunk_does_not_truncate_the_gather() {
    let mut registry = fixture("midchunk");
    let schema = registry.relation(TID).map(Relation::schema).unwrap();
    let victim = 55u64;
    let victim_idx_key = index_key_of(&registry, victim);

    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(victim as u128, -1);
    bb.put_int(victim as u128 * 10);
    bb.end_row();
    registry.ingest(TID, bb.finish()).unwrap();

    forge_index_entry(&mut registry, &victim_idx_key);

    let victim_key = schema.opk_key(&(victim as u128).to_le_bytes());
    let probe_entry = registry.relation_or_err(TID).unwrap();
    let mut probe = probe_entry.cursor();
    probe.seek_bytes(victim_key.pk_bytes());
    assert!(
        !(probe.valid && probe.current_pk_eq(victim_key.pk_bytes()) && probe.current_weight > 0),
        "the store must hold no live row at the victim's key"
    );
    drop(probe);

    let (mut cur, _) = registry.open_bound(TID, val_50s()).unwrap();
    assert!(matches!(cur, SourceCursor::Bounded(_)));
    let got = drain_all(&mut cur, 64);
    let want: Vec<(u128, i64)> = (50..60u128).filter(|&i| i != victim as u128).map(|i| (i, 1)).collect();
    assert_eq!(
        got, want,
        "ids above the absent one (id {victim}) must survive the same chunk"
    );
}

/// An in-range index entry whose base row is absent does not end the walk.
#[test]
fn an_orphaned_index_entry_does_not_end_the_walk() {
    let mut registry = fixture("orphan");
    let src_pk_stride = registry.relation_or_err(TID).unwrap().schema().pk_stride();
    let idx_key_size = registry.relation_or_err(TID).unwrap().indexes()[0]
        .key_spec()
        .key_size();

    // val 550's entry, re-pointed at id 9999, which no base row carries.
    let mut orphan_key = index_key_of(&registry, 55);
    orphan_key[idx_key_size..idx_key_size + src_pk_stride].copy_from_slice(&9999u64.to_be_bytes()[8 - src_pk_stride..]);
    forge_index_entry(&mut registry, &orphan_key);

    // A chunk of 1 gives the orphan an index window of its own.
    let (mut cur, _) = registry.open_bound(TID, val_50s()).unwrap();
    assert!(matches!(cur, SourceCursor::Bounded(_)));
    let got = drain_all(&mut cur, 1);
    let want: Vec<(u128, i64)> = (50..60u128).map(|i| (i, 1)).collect();
    assert_eq!(
        got, want,
        "the orphan must be skipped and the walk continue to the range end"
    );
}

/// A point seek landing on an orphaned index entry terminates, finding nothing.
#[test]
fn test_seek_by_index_orphan_entry_terminates() {
    let mut registry = fixture("seek_orphan");
    let schema = registry.relation(TID).map(Relation::schema).unwrap();

    // Row 12345 at val 777, no other row's val, goes in and out again; its
    // index entry then goes back alone.
    let row = |w: i64| {
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(12345, w);
        bb.put_int(777);
        bb.end_row();
        bb.finish()
    };
    registry.ingest(TID, row(1)).unwrap();
    let orphan_key = index_key_of(&registry, 12345);
    registry.ingest(TID, row(-1)).unwrap();
    forge_index_entry(&mut registry, &orphan_key);

    let img = key_image(TypeCode::I64, 777);
    let range = KeyRange::new(PkColList::from_slice(&[1]), &[], Cut::before(img), Cut::after(img));
    let bound = ReadBound::Range(range);
    assert!(matches!(
        registry.open_bound(TID, bound.clone()).unwrap().0,
        SourceCursor::Bounded(_)
    ));
    let spec = gnitz_wire::ReadSpec::all_rows(bound);
    let rows = registry.scan_spec(TID, spec, schema.layout_digest(), None).unwrap();
    assert!(rows.is_empty(), "orphan index entry must resolve to no source row");
}
