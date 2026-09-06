use super::*;
use gnitz_store::schema::SchemaColumn;
use gnitz_wire::type_code;

/// Base table `(label STRING, id U64 PRIMARY KEY)` — PK at column 1. The FK
/// parent fast-path encodes against a base-table schema whose lone PK may be
/// declared at any column position; resolving the leading key column from
/// `columns[0]` (the STRING) would encode the probe at the wrong type and
/// width, and mangle the existence check.
///
/// The probe batch itself is built against `probe_schema`, where the PK is
/// renumbered to column 0 — so this exercises `enc_key`'s general contract, the
/// one that keeps that renumbering an optimisation rather than a premise.
#[test]
fn enc_key_encodes_a_nonleading_pk_at_its_own_column() {
    let cols = vec![
        SchemaColumn::new(type_code::STRING, 0),
        SchemaColumn::new(type_code::U64, 0),
    ];
    let schema = SchemaDescriptor::new(&cols, &[1]);

    let key = enc_key(&schema, 42u128, type_code::U64);

    let mut expected = [0u8; 8];
    gnitz_wire::encode_pk_column(&42u64.to_le_bytes(), type_code::U64, &mut expected);
    assert_eq!(
        key.pk_bytes(),
        &expected[..],
        "probe key must equal the stored OPK PK for a non-leading PK column"
    );
}

/// A probe batch carries the target's KEY and nothing else, whatever the
/// table's payload is: `handle_has_pk` reads back `get_pk_bytes` alone, so a
/// payload region would be SAL bytes and a zero-fill per column per row for
/// data no one reads. `push_key_row` asserts the schema is payload-free, so
/// this pins the projection that makes it so.
#[test]
fn probe_schema_keeps_the_key_and_drops_every_payload_column() {
    let cols = vec![
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::I64, 0),
        SchemaColumn::new(type_code::STRING, 1),
    ];
    let schema = SchemaDescriptor::new(&cols, &[0]);
    let pk_only = probe_schema(&schema);

    assert_eq!(pk_only.num_payload_cols(), 0);
    assert_eq!(pk_only.pk_stride(), schema.pk_stride());
    assert_eq!(pk_only.pk_indices(), &[0]);

    let batch = build_check_batch(&pk_only, &[42u128], type_code::U64);
    assert_eq!(batch.len(), 1);
    let mut expected = [0u8; 8];
    gnitz_wire::encode_pk_column(&42u64.to_le_bytes(), type_code::U64, &mut expected);
    assert_eq!(batch.get_pk_bytes(0), &expected[..]);
}

/// The placement re-stamp. `project_schema` finishes through
/// `SchemaDescriptor::new`, which stamps `KEYED_DEFAULT`; on a `CLUSTER BY`
/// table that resolves to a different router width, so probe rows would
/// scatter to workers that do not store the key and every present key would
/// read as absent. `PartialEq for SchemaDescriptor` ignores `placement`, so no
/// schema guard downstream can catch it.
#[test]
fn probe_schema_carries_the_source_placement() {
    let cols = vec![
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::I64, 0),
    ];
    let clustered =
        SchemaDescriptor::new(&cols, &[0, 1]).with_placement(gnitz_store::schema::Placement::Keyed { prefix_len: 1 });

    let pk_only = probe_schema(&clustered);
    assert_eq!(pk_only.placement(), clustered.placement());
    assert_eq!(
        pk_only.dist_stride(),
        clustered.dist_stride(),
        "the router width must survive the projection"
    );
    assert_ne!(
        pk_only.dist_stride(),
        pk_only.pk_stride() as u8,
        "the fixture must actually be CLUSTER BY, or the test proves nothing"
    );
}

/// The check-batch allocation path with `BUF_POOL` as its only cache — what is
/// left after the per-(target, key columns) batch pool in front of it was
/// deleted. `Batch::drop` returns its arena at full capacity and
/// `with_capacity` pops it straight back, so a steady cycle allocates nothing
/// after the first round.
///
/// `cd crates && cargo test -p gnitz-server --release check_batch_build_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore]
fn check_batch_build_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    // A realistic bulk probe: ~20k keys is the band the deleted pool's
    // `MAX_RETAIN_BYTES` refused and its `POOL_BYPASS_BYTES` floor still served,
    // i.e. exactly where the two caches could have differed.
    const ROWS: usize = 20_000;
    const ROUNDS: usize = 2_000;

    let cols = vec![
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::I64, 0),
    ];
    let schema = probe_schema(&SchemaDescriptor::new(&cols, &[0]));
    let keys: Vec<[u8; 8]> = (0..ROWS as u64).map(|i| i.to_be_bytes()).collect();

    // One warm round, so the arena `BUF_POOL` hands out is already the right
    // size and the timed loop measures the steady state.
    drop(build_check_batch_pk_bytes(&schema, keys.iter().map(|k| &k[..])));

    let t = Instant::now();
    for _ in 0..ROUNDS {
        let b = build_check_batch_pk_bytes(&schema, keys.iter().map(|k| &k[..]));
        black_box(b.len());
    }
    let per_row = t.elapsed().as_nanos() as f64 / (ROUNDS * ROWS) as f64;
    println!("check_batch_build: {ROWS} rows x {ROUNDS} rounds, {per_row:.2} ns/row");
}
