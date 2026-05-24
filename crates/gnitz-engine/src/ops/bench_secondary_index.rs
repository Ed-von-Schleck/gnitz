//! Microbenchmarks for the reduce-time secondary-index population hot path
//! (GroupIndex + AggValueIndex). Ignored by default; run with:
//!
//! ```text
//! cargo test -p gnitz-engine --release secondary_index_bench -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Lives inside `ops` (not the crate root) so it can call the real internal
//! key-composition helpers (`IndexColExtractor`, `GroupKeyExtractor`,
//! `encode_ordered`) and decompose the per-row population cost into four
//! cleanly-attributed layers:
//!
//!   compose : build the index key into a stack buffer, discard
//!   assembly: the `extend_*` calls that turn keys into a `Batch` (= build - compose)
//!   sort    : `into_consolidated` (argsort + dedup of the unsorted batch)
//!   upsert  : `ingest_owned_batch_memonly` of an *already-consolidated* batch
//!             (its internal `into_consolidated` short-circuits, so this times
//!             only the memtable upsert: per-row bloom population + run push)
//!
//! All four are measured directly (no full-minus-X subtraction). Throughput is
//! computed at runtime; nothing here is a hard-coded result.

use std::time::{Duration, Instant};

use super::index::{AviDesc, GiDesc, GI_GC_BYTES, make_avi_schema, make_gi_schema, op_integrate_with_indexes};
use super::util::{AVI_AV_BYTES, GroupKeyExtractor, IndexColExtractor, encode_ordered};
use crate::schema::{MAX_PK_BYTES, SchemaColumn, SchemaDescriptor, TypeCode, type_code};
use crate::storage::{Batch, Table};

const N_ROWS: usize = 500_000;
const N_GROUPS: u64 = 10_000;
const ITERS: usize = 40;

/// Memtable arena size. Default 1 GiB isolates the in-memory cost (no flush
/// during the timed region). Override with `GNITZ_BENCH_ARENA_KB` to measure
/// the production-representative path, where small arenas (index tables use
/// 256 KiB–1 MiB) flush shards to disk mid-population.
fn arena() -> u64 {
    match std::env::var("GNITZ_BENCH_ARENA_KB") {
        Ok(kb) => kb.parse::<u64>().expect("GNITZ_BENCH_ARENA_KB must be an integer") * 1024,
        Err(_) => 1 << 30,
    }
}

/// Source schema: U64 pk (col 0) | U32 grp (col 1) | I64 val (col 2).
/// GI groups by col 1; AVI groups by col 1 and aggregates MIN over col 2.
fn src_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

fn build_input(schema: &SchemaDescriptor) -> Batch {
    let mut b = Batch::with_schema(*schema, N_ROWS);
    for row in 0..N_ROWS as u64 {
        b.extend_pk(row as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &((row % N_GROUPS) as u32).to_le_bytes());
        b.extend_col(1, &((row.wrapping_mul(2654435761)) as i64).to_le_bytes());
        b.count += 1;
    }
    b.sorted = false;
    b.consolidated = false;
    b
}

fn ns_per_row(elapsed: Duration) -> f64 {
    elapsed.as_nanos() as f64 / (N_ROWS * ITERS) as f64
}

/// Run `f` for `ITERS` iterations (plus one warmup), returning total elapsed.
fn time<F: FnMut()>(mut f: F) -> Duration {
    f(); // warmup
    let start = Instant::now();
    for _ in 0..ITERS {
        f();
    }
    start.elapsed()
}

fn report(index: &str, compose: Duration, build: Duration, sort: Duration, upsert: Duration) {
    let (c, b, s, u) =
        (ns_per_row(compose), ns_per_row(build), ns_per_row(sort), ns_per_row(upsert));
    let assembly = b - c;
    let total = c + assembly + s + u;
    println!("\n{index} population — per-row cost decomposition ({ITERS}x{N_ROWS} rows):");
    println!("  compose (key only)       {c:7.2} ns/row   {:5.1}%", 100.0 * c / total);
    println!("  assembly (extend_*)      {assembly:7.2} ns/row   {:5.1}%", 100.0 * assembly / total);
    println!("  sort (into_consolidated) {s:7.2} ns/row   {:5.1}%", 100.0 * s / total);
    println!("  upsert (memtable+bloom)  {u:7.2} ns/row   {:5.1}%", 100.0 * u / total);
    println!("  -----");
    println!("  sum                      {total:7.2} ns/row   ({:.2} Mrows/s)", 1000.0 / total);
}

/// Time `ingest_owned_batch_memonly` alone: rebuild + pre-consolidate the index
/// batch *outside* the timed region (so `into_consolidated` short-circuits
/// inside ingest), and create the destination table outside it too.
fn time_upsert(tmp: &std::path::Path, schema: SchemaDescriptor, base_id: u32, mut build_pre: impl FnMut() -> Batch) -> Duration {
    let mut total = Duration::ZERO;
    for i in 0..=ITERS as u32 {
        let pre = build_pre();
        let mut t = Table::new(tmp.to_str().unwrap(), "u", schema, base_id + i, arena(), false).unwrap();
        let start = Instant::now();
        t.ingest_owned_batch_memonly(pre).unwrap();
        if i > 0 {
            total += start.elapsed();
        }
        std::hint::black_box(&t);
    }
    total
}

#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn secondary_index_bench_gi_decomposition() {
    let schema = src_schema();
    let gi_schema = make_gi_schema(&schema);
    let input = build_input(&schema);
    let mb = input.as_mem_batch();
    let tmp = tempfile::tempdir().unwrap();
    let gc = IndexColExtractor::new(&schema, 1);

    // GI key = gc(8) ++ source PK bytes.
    let gi_key = |key: &mut [u8], row: usize| -> usize {
        let gc_u64 = gc.extract(&mb, row);
        key[..GI_GC_BYTES].copy_from_slice(&gc_u64.to_le_bytes());
        let src_pk = mb.get_pk_bytes(row);
        key[GI_GC_BYTES..GI_GC_BYTES + src_pk.len()].copy_from_slice(src_pk);
        GI_GC_BYTES + src_pk.len()
    };
    let build_batch = || {
        let mut out = Batch::with_schema(gi_schema, N_ROWS);
        out.sorted = false;
        out.consolidated = false;
        let mut key = [0u8; MAX_PK_BYTES];
        for row in 0..N_ROWS {
            let klen = gi_key(&mut key, row);
            out.extend_pk_bytes(&key[..klen]);
            out.extend_weight(&mb.get_weight(row).to_le_bytes());
            out.extend_null_bmp(&0u64.to_le_bytes());
            out.count += 1;
        }
        out
    };

    let compose = time(|| {
        let mut key = [0u8; MAX_PK_BYTES];
        for row in 0..N_ROWS {
            let klen = gi_key(&mut key, row);
            std::hint::black_box(&key[..klen]);
        }
    });
    let build = time(|| {
        std::hint::black_box(build_batch());
    });
    let sort = time(|| {
        std::hint::black_box(build_batch().into_consolidated(&gi_schema));
    });
    let upsert = time_upsert(tmp.path(), gi_schema, 100, || {
        build_batch().into_consolidated(&gi_schema).into_inner()
    });

    // Cross-check the layered sum against the real public entry point.
    let mut id = 1000u32;
    let full = time(|| {
        let mut t = Table::new(tmp.path().to_str().unwrap(), "gi", gi_schema, id, arena(), false).unwrap();
        id += 1;
        let gi = GiDesc { table: &mut t as *mut Table, col_idx: 1 };
        op_integrate_with_indexes(&input, None, &schema, Some(&gi), None).unwrap();
        std::hint::black_box(&t);
    });

    report("GI (U64 pk)", compose, build, sort, upsert);
    println!("  (op_integrate full path: {:.2} ns/row)", ns_per_row(full));
}

/// Incremental lifecycle bench: one long-lived GI table fed many small
/// per-epoch deltas, each epoch reading back (compacting cursor + prefix
/// walk over the touched groups) exactly as `op_reduce` does. This is the
/// production shape the single-batch decomposition tests above cannot see:
/// a tiny arena (index tables ship 256 KiB–1 MiB) flushes frequently, and
/// since the read path opens `create_cursor_compacting`, every read past
/// `L0_COMPACT_THRESHOLD` shards triggers a compaction — LSM write
/// amplification driven purely by memtable size. Sweep `GNITZ_BENCH_ARENA_KB`
/// (e.g. 1024 = shipped GI arena vs. 65536 = working-set-sized) to price it.
#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn secondary_index_bench_gi_incremental() {
    let delta: usize = std::env::var("GNITZ_BENCH_DELTA")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let epochs: usize = N_ROWS / delta;

    let schema = src_schema();
    let gi_schema = make_gi_schema(&schema);
    let tmp = tempfile::tempdir().unwrap();
    let mut t =
        Table::new(tmp.path().to_str().unwrap(), "gi_inc", gi_schema, 0, arena(), false).unwrap();

    let mut pk: u64 = 0;
    let mut write_t = Duration::ZERO;
    let mut open_t = Duration::ZERO;
    let mut seek_t = Duration::ZERO;
    let start = Instant::now();
    for _ in 0..epochs {
        let mut batch = Batch::with_schema(schema, delta);
        batch.sorted = false;
        batch.consolidated = false;
        let mut touched: Vec<u64> = Vec::with_capacity(delta);
        for _ in 0..delta {
            let g = pk % N_GROUPS;
            batch.extend_pk(pk as u128);
            batch.extend_weight(&1i64.to_le_bytes());
            batch.extend_null_bmp(&0u64.to_le_bytes());
            batch.extend_col(0, &(g as u32).to_le_bytes());
            batch.extend_col(1, &((pk.wrapping_mul(2654435761)) as i64).to_le_bytes());
            batch.count += 1;
            touched.push(g);
            pk += 1;
        }

        let gi = GiDesc { table: &mut t as *mut Table, col_idx: 1 };
        let w0 = Instant::now();
        op_integrate_with_indexes(&batch, None, &schema, Some(&gi), None).unwrap();
        write_t += w0.elapsed();

        // Read-back: compacting cursor, then walk each touched group's prefix
        // (the GI group-column value widens to the u64 gc prefix).
        touched.sort_unstable();
        touched.dedup();
        let o0 = Instant::now();
        // Mirrors op_reduce's GI read path (vm.rs), which compacts on read.
        #[allow(clippy::disallowed_methods)]
        let mut handle = t.create_cursor_compacting().unwrap();
        open_t += o0.elapsed();
        let c = handle.cursor_mut();
        let s0 = Instant::now();
        for g in &touched {
            let prefix = (*g).to_le_bytes();
            let mut hit = c.seek_first_positive_with_prefix(&prefix);
            while hit {
                std::hint::black_box(c.current_pk_bytes());
                c.advance();
                hit = c.walk_to_positive_with_prefix(&prefix);
            }
        }
        seek_t += s0.elapsed();
    }
    let elapsed = start.elapsed();
    let rows = (epochs * delta) as f64;
    println!(
        "\nGI incremental — {epochs} epochs x {delta} rows, arena {} KiB:",
        arena() / 1024
    );
    println!(
        "  {:.1} ns/row   ({:.2} Mrows/s)   total {:.3}s",
        elapsed.as_nanos() as f64 / rows,
        rows / elapsed.as_secs_f64() / 1e6,
        elapsed.as_secs_f64(),
    );
    let pct = |d: Duration| 100.0 * d.as_secs_f64() / elapsed.as_secs_f64();
    let nspr = |d: Duration| d.as_nanos() as f64 / rows;
    println!("    write (integrate)   {:7.1} ns/row   {:5.1}%", nspr(write_t), pct(write_t));
    println!("    read: cursor open   {:7.1} ns/row   {:5.1}%", nspr(open_t), pct(open_t));
    println!("    read: prefix seeks  {:7.1} ns/row   {:5.1}%", nspr(seek_t), pct(seek_t));
}

#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn secondary_index_bench_avi_decomposition() {
    let schema = src_schema();
    let group_by_cols = vec![1u32];
    let avi_schema = make_avi_schema(&schema, &group_by_cols);
    let input = build_input(&schema);
    let mb = input.as_mem_batch();
    let tmp = tempfile::tempdir().unwrap();
    let extractor = GroupKeyExtractor::new(&schema, &group_by_cols);
    let n = extractor.stride;
    let avi_pi = schema.payload_idx(2);
    let tc = type_code::I64;

    // AVI key = group_key_bytes ++ av_encoded(8).
    let avi_key = |key: &mut [u8], row: usize| -> usize {
        extractor.gather(&mb, row, key);
        let av_bytes = mb.get_col_ptr(row, avi_pi, 8);
        let av = encode_ordered(av_bytes, tc, false);
        key[n..n + AVI_AV_BYTES].copy_from_slice(&av.to_le_bytes());
        n + AVI_AV_BYTES
    };
    let build_batch = || {
        let mut out = Batch::with_schema(avi_schema, N_ROWS);
        out.sorted = false;
        out.consolidated = false;
        let mut key = [0u8; MAX_PK_BYTES];
        for row in 0..N_ROWS {
            let klen = avi_key(&mut key, row);
            out.extend_pk_bytes(&key[..klen]);
            out.extend_weight(&mb.get_weight(row).to_le_bytes());
            out.extend_null_bmp(&0u64.to_le_bytes());
            out.count += 1;
        }
        out
    };

    let compose = time(|| {
        let mut key = [0u8; MAX_PK_BYTES];
        for row in 0..N_ROWS {
            let klen = avi_key(&mut key, row);
            std::hint::black_box(&key[..klen]);
        }
    });
    let build = time(|| {
        std::hint::black_box(build_batch());
    });
    let sort = time(|| {
        std::hint::black_box(build_batch().into_consolidated(&avi_schema));
    });
    let upsert = time_upsert(tmp.path(), avi_schema, 200, || {
        build_batch().into_consolidated(&avi_schema).into_inner()
    });

    let mut id = 2000u32;
    let full = time(|| {
        let mut t = Table::new(tmp.path().to_str().unwrap(), "avi", avi_schema, id, arena(), false).unwrap();
        id += 1;
        let avi = AviDesc {
            table: &mut t as *mut Table,
            for_max: false,
            agg_col_type_code: TypeCode::I64 as u8,
            group_by_cols: group_by_cols.clone(),
            agg_col_idx: 2,
        };
        op_integrate_with_indexes(&input, None, &schema, None, Some(&avi)).unwrap();
        std::hint::black_box(&t);
    });

    report("AVI (U32 grp)", compose, build, sort, upsert);
    println!("  (op_integrate full path: {:.2} ns/row)", ns_per_row(full));
}

/// Time the `into_consolidated` sort layer for a single-column PK schema
/// (`[pk, I64 val]`). `pk_bytes_for(row)` yields the 8-byte LE PK; payload is a
/// scrambled I64 so the payload tiebreak is exercised. Hashed PKs keep the input
/// unsorted (real sort work) with occasional folds.
fn bench_single_pk_sort(
    label: &str,
    pk_schema: SchemaDescriptor,
    pk_bytes_for: impl Fn(usize) -> [u8; 8],
) {
    let build = || {
        let mut out = Batch::with_schema(pk_schema, N_ROWS);
        out.sorted = false;
        out.consolidated = false;
        for row in 0..N_ROWS {
            out.extend_pk_bytes(&pk_bytes_for(row));
            out.extend_weight(&1i64.to_le_bytes());
            out.extend_null_bmp(&0u64.to_le_bytes());
            out.extend_col(0, &((row as i64).wrapping_mul(2654435761)).to_le_bytes());
            out.count += 1;
        }
        out
    };
    let sort = time(|| {
        std::hint::black_box(build().into_consolidated(&pk_schema));
    });
    let s = ns_per_row(sort);
    println!(
        "\n{label} — sort (into_consolidated): {s:7.2} ns/row   ({:.2} Mrows/s)",
        1000.0 / s,
    );
}

/// Single-U64 PK: the `pk_is_fast` path that must NOT regress (OPK is bypassed;
/// `pack_pk_le` output is byte-identical). Guards against accidental routing of
/// the fast path through the OPK encoder.
#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn secondary_index_bench_single_u64_pk_sort() {
    let schema = SchemaDescriptor::new(
        &[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(type_code::I64, 0)],
        &[0],
    );
    bench_single_pk_sort("single-U64 PK (fast path)", schema, |row| {
        (row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes()
    });
}

/// Single-I64 PK: the signed single-column case. Confirms the order-preserving
/// key is a net win (or at least not a regression) versus the old
/// per-comparison signed cast.
#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn secondary_index_bench_single_i64_pk_sort() {
    let schema = SchemaDescriptor::new(
        &[SchemaColumn::new(type_code::I64, 0), SchemaColumn::new(type_code::I64, 0)],
        &[0],
    );
    bench_single_pk_sort("single-I64 PK (OPK path)", schema, |row| {
        // Cast to i64 spreads the hash across negatives and positives.
        ((row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) as i64).to_le_bytes()
    });
}
