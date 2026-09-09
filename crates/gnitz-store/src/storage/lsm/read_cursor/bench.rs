//! `#[ignore]`d microbenchmarks for the read cursor.
//!
//! The crate is a binary with no `criterion` dependency, so these live as
//! ignored tests and time with `Instant`. Run in release — a debug build
//! measures nothing useful:
//!
//! ```text
//! cargo test -p gnitz-store --release _bench -- --ignored --nocapture --test-threads=1
//! ```

use super::tests::{adv_assert_cursor_oracle, adv_key, write_test_shard};
use super::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::Layout;
use crate::test_support::{make_schema_u128_i64, make_schema_u64_i64, wide_pk_3xu64_schema};
use std::rc::Rc;

/// Baseline: shard-backed merge-scan throughput. Four overlapping-key shards
/// of 256K rows each (U64 PK; I64, nullable I64, STRING payload) built via the
/// production `BatchBuilder`/`write_as_shard` region layout — the STRING values
/// exceed `SHORT_STRING_THRESHOLD` so they live in the blob heap and the drain's
/// German-string blob relocation on the scatter path is exercised; the nullable
/// I64 column is NULL on a subset. Drains a 4-source `ReadCursor` via
/// `drain_chunk`, pinning the loser-tree merge + scatter over mmap'd shards.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn shard_merge_scan_bench() {
    use crate::storage::BatchBuilder;
    use std::time::Instant;
    let dir = tempfile::tempdir().unwrap();

    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),    // PK
            SchemaColumn::new(type_code::I64, 0),    // I64 payload
            SchemaColumn::new(type_code::I64, 1),    // nullable I64
            SchemaColumn::new(type_code::STRING, 0), // long-string payload
        ],
        &[0],
    );

    const N_SHARDS: usize = 4;
    const PER_SHARD: u64 = 256 * 1024;
    // Overlapping key ranges: shard `s` spans [s·OFFSET, s·OFFSET + PER_SHARD),
    // so adjacent shards share keys and the loser tree does real cross-source
    // merge/consolidation work.
    const OFFSET: u64 = 100_000;

    // Construction + open outside the timed region (validate_checksums = false,
    // the query-time read path's flag).
    let shards: Vec<Rc<MappedShard>> = (0..N_SHARDS as u64)
        .map(|s| {
            let mut bb = BatchBuilder::new(schema);
            for i in 0..PER_SHARD {
                let key = i + s * OFFSET;
                bb.begin_row(key as u128, 1);
                bb.put_u64(key); // I64 region (bytes reused; value irrelevant)
                if i % 4 == 0 {
                    bb.put_null(); // nullable I64 NULL on a subset
                } else {
                    bb.put_u64(key.wrapping_mul(7));
                }
                bb.put_string(&format!("payload-string-value-{key}"));
                bb.end_row();
            }
            let batch = bb.finish();
            let path = dir.path().join(format!("ms_{s}.db"));
            let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            batch
                .write_as_shard(&cpath, &schema, super::super::shard_file::ShardWriteOpts::default())
                .unwrap();
            Rc::new(MappedShard::open(&cpath, &schema, false).unwrap())
        })
        .collect();

    let input_rows = N_SHARDS as u64 * PER_SHARD;

    // Build the cursor once (construction stays off the clock). One untimed
    // warm-up drain faults in the cold PK/payload pages (lazy mmap, no
    // MAP_POPULATE) so the timed region reflects merge/scatter compute.
    let mut c = create_read_cursor(&[], &shards, schema);
    std::hint::black_box(c.drain_chunk(usize::MAX));

    const ITERS: usize = 10;
    let mut sink = 0usize;
    let t = Instant::now();
    for _ in 0..ITERS {
        c.rewind();
        let out = c.drain_chunk(usize::MAX).expect("non-empty drain");
        sink = sink.wrapping_add(out.count);
        std::hint::black_box(&out);
    }
    let secs = t.elapsed().as_secs_f64();
    std::hint::black_box(sink);
    let rps = (ITERS as u64 * input_rows) as f64 / secs;
    println!("shard_merge_scan: {input_rows} input rows × {ITERS} iters in {secs:.3}s = {rps:.0} rows/s");
}

/// Baseline: shard PK point-probe throughput. One 1M-row shard (U64 PK, single
/// I64 payload), 100K shuffled `seek_bytes` probes ~50/50 present / absent (odd
/// keys landing between the sparse even shard keys). The only *timed* coverage of
/// the stateless search — every other cursor bench seeds at a live position.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn shard_point_probe_bench() {
    use std::time::Instant;
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64(); // U64 PK | I64 payload

    const N: u64 = 1_000_000;
    const PROBES: usize = 100_000;

    // Sparse (even) shard keys so odd probes resolve a lower bound *between*
    // two present keys. Construction + open outside the timed region.
    let rows: Vec<(u128, i64, i64)> = (0..N).map(|i| ((i * 2) as u128, 1, i as i64)).collect();
    let shard = write_test_shard(&dir, &schema, 0, &rows);

    // Shuffled 50/50 present (even) / absent (odd, between keys) probes.
    let mut rng = crate::test_rng::Rng::new(0xC0DE_1234_5678_9ABC);
    let seek_keys: Vec<[u8; 8]> = (0..PROBES)
        .map(|_| {
            let base = rng.gen_range(N) * 2; // an even present key
            if rng.next_u64() & 1 == 0 {
                base.to_be_bytes() // present → exact hit
            } else {
                (base + 1).to_be_bytes() // odd → lower bound between keys
            }
        })
        .collect();

    let mut c = create_read_cursor(&[], std::slice::from_ref(&shard), schema);

    // Untimed warm-up: one full sweep faults in the PK pages.
    for k in &seek_keys {
        c.seek_bytes(k);
        std::hint::black_box(c.valid);
    }

    let mut sink = 0u64;
    let t = Instant::now();
    for k in &seek_keys {
        c.seek_bytes(k);
        if c.valid {
            sink = sink.wrapping_add(c.current_key_narrow() as u64);
        }
    }
    let secs = t.elapsed().as_secs_f64();
    std::hint::black_box(sink);
    println!(
        "point_probe seek_bytes: {PROBES} probes in {secs:.4}s = {:.0} probes/s",
        PROBES as f64 / secs
    );
}

// ===========================================================================
// advance_to microbenchmark suite
//
// Three `#[ignore]`d benches isolating `advance_to` as a general-purpose sorted-
// run seek primitive, so a win generalises to every caller and schema instead of
// overfitting the one DAG (`test_view_maintenance`) that flagged it. Each spans
// the primitive's full envelope: all four `opk_width_dispatch!` arms (stride
// 8/16/24/40), gallop depths from adjacent to deep (gap 1..4096), and — the
// load-bearing axis — both cache tiers. The macro bottleneck is memory-latency-
// bound, so a cache-hot bench is a false proxy: `hot` measures the compute floor,
// `cold` (PK region ≫ L3, probed lines evicted between sweeps) is the macro proxy
// every accept/reject is gated on. Each timed region is oracle-checked against the
// stateless lower bound / a from-scratch `seek_bytes` on a sample (off the clock).
// ===========================================================================

/// Cache tier. `Hot` sizes the PK region to fit L2/L3 (the compute floor,
/// diagnostic only); `Cold` sizes the summed PK region ≫ the 16 MiB L3 so the
/// gallop's `get_pk_bytes` loads miss to DRAM — the regime the macro profile is
/// dominated by. Tune to `Cold`.
#[derive(Clone, Copy)]
enum Tier {
    Hot,
    Cold,
}

impl Tier {
    /// PK-region byte budget. `Hot` fits L2; `Cold` is ≫ L3 (the plan's ≥ 48 MiB
    /// floor). Row count for a given stride is `bytes() / stride`, holding the
    /// PK-region bytes constant across strides so a cross-stride comparison
    /// isolates decode/compare cost rather than which stride spills a cache tier.
    fn bytes(self) -> usize {
        match self {
            Tier::Hot => 1 << 20,   // 1 MiB — fits L2
            Tier::Cold => 48 << 20, // 48 MiB — ≫ L3
        }
    }

    fn tag(self) -> &'static str {
        match self {
            Tier::Hot => "hot",
            Tier::Cold => "cold",
        }
    }
}

/// The four `opk_width_dispatch!` arms: 8 → `u64`, 16 → `u128`, 24 → `[u128;2]`,
/// 40 → byte-`memcmp` fallback. Stride 8 is the profiled anchor; the other three
/// guard against a win that quietly regresses a wider PK another workload uses.
///
/// 12 and 13 sit in `pack_pk_be`'s overlapping-load band, which no whole-width
/// arm covers — the ordinary compound shapes `PRIMARY KEY (BIGINT, INT)` and the
/// AVI's `GROUP BY <INT>` key land there.
const ADV_STRIDES: [usize; 6] = [8, 12, 13, 16, 24, 40];

/// Gallop depth sweep: `O(1)` adjacency (per-probe decode dominates) through deep
/// skips (`O(log gap)` binary search dominates). The profiled workload's ~100–500
/// is the middle; the sweep extends past it in both directions on purpose.
const ADV_GAPS: [usize; 5] = [1, 16, 128, 512, 4096];

/// L3-eviction scratch buffer (2× a 16 MiB L3). Thrashed between cold probe
/// batches so the next batch faults its PK lines from DRAM.
const ADV_SCRATCH_BYTES: usize = 32 << 20;

/// 5×`U64` compound PK (stride 40 — the byte-`memcmp` dispatch arm) + I64 payload.
/// `MAX_PK_COLUMNS = 5` / `MAX_PK_BYTES = 80`-legal. The stride-40 sibling of
/// `wide_pk_3xu64_schema` (stride 24).
fn adv_schema_5xu64() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1, 2, 3, 4],
    )
}

/// Schema for a bench stride: single-column PKs for 8/16 (the profiled shape and
/// its `u128` sibling), compound all-`U64` for 24/40 (the wide dispatch arms). All
/// carry one I64 payload — a `FixedIntNonnull` schema, so a `Multi` merge's PK-tie
/// tiebreak runs `compare_rows_fixedint_nonnull` (the profile's 3.30% child).
fn adv_bench_schema(stride: usize) -> SchemaDescriptor {
    match stride {
        8 => make_schema_u64_i64(),    // U64 PK + I64
        12 => adv_schema_u64_u32(),    // (U64, U32) PK + I64
        13 => adv_schema_u64_u32_u8(), // (U64, U32, U8) PK + I64
        16 => make_schema_u128_i64(),  // U128 PK + I64
        24 => wide_pk_3xu64_schema(),  // 3×U64 PK + I64
        40 => adv_schema_5xu64(),      // 5×U64 PK + I64
        _ => unreachable!("bench stride must be one of ADV_STRIDES"),
    }
}

/// Stride-12 compound PK — `PRIMARY KEY (BIGINT, INT)`. All-unsigned, so the
/// concatenated OPK region reads as one big-endian number and `adv_key` stays
/// monotone across the column boundary.
fn adv_schema_u64_u32() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1],
    )
}

/// Stride-13 compound PK — the AVI's `GROUP BY <INT>` shape (group slot, ordinal,
/// value collapse to the same width). Same monotonicity argument as stride 12.
fn adv_schema_u64_u32_u8() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U32, 0),
            SchemaColumn::new(type_code::U8, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0, 1, 2],
    )
}

/// Timed-probe budget per (driver, tier): enough for a stable `ns/probe`. Point
/// lookups (`!monotone`) are `O(log N)` each, so they need far fewer than the
/// `O(1)`-amortised monotone skip.
fn adv_target(monotone: bool, tier: Tier) -> usize {
    match (monotone, tier) {
        (true, Tier::Hot) => 200_000,
        (true, Tier::Cold) => 50_000,
        (false, Tier::Hot) => 30_000,
        (false, Tier::Cold) => 10_000,
    }
}

/// Evict the cache by reading+writing one line per 64 bytes of an L3-sized scratch
/// buffer. Called (untimed) between cold probe batches so the batch's PK lines
/// fault from DRAM rather than a resident tier — turning the cache-regime risk
/// into an active tripwire instead of an accident of a warm working set.
fn adv_flush_cache(scratch: &mut [u8]) {
    let mut acc = 0u8;
    for chunk in scratch.chunks_mut(64) {
        chunk[0] = chunk[0].wrapping_add(1);
        acc = acc.wrapping_add(chunk[0]);
    }
    std::hint::black_box(acc);
}

/// Stream `(pk_bytes, weight, val)` regions to a freshly-written shard and open it.
/// `pks` is `count × pk_stride` packed OPK bytes (any width — the compound-PK
/// writer the single-integer `write_test_shard*` helpers lack). `pks` must be
/// OPK-sorted. `name` is a unique base (fixtures share one tmpdir, so a reused
/// name would alias a live mmap).
fn adv_write_shard(
    dir: &tempfile::TempDir,
    schema: &SchemaDescriptor,
    name: &str,
    pks: &[u8],
    weights: &[i64],
    vals: &[i64],
) -> Rc<MappedShard> {
    debug_assert_eq!(pks.len(), weights.len() * schema.pk_stride());
    debug_assert_eq!(vals.len(), weights.len());
    let rows: Vec<(Vec<u8>, i64, i64)> = pks
        .chunks_exact(schema.pk_stride())
        .zip(weights.iter().zip(vals))
        .map(|(pk, (&w, &v))| (pk.to_vec(), w, v))
        .collect();
    let cpath = super::super::shard_file::write_test_shard(
        &dir.path().join(format!("{name}.db")),
        schema,
        &rows,
        super::super::shard_file::ShardWriteOpts::default(),
    );
    Rc::new(MappedShard::open(&cpath, schema, false).unwrap())
}

/// `n` shards whose keys interleave `[0, total)` round-robin (shard `s` owns keys
/// `s, s+n, …`), so the union is dense and the summed PK region is `total × stride`
/// (the tier budget) regardless of `n`. Non-empty by construction (`total ≫ n`) —
/// the precondition for the source-count → mode mapping (`create_read_cursor`
/// drops empty sources). `n = 1` yields one dense shard.
fn adv_build_interleaved_shards(
    dir: &tempfile::TempDir,
    schema: &SchemaDescriptor,
    name: &str,
    total: usize,
    n: usize,
) -> Vec<Rc<MappedShard>> {
    let stride = schema.pk_stride();
    (0..n)
        .map(|s| {
            let keys: Vec<u64> = (s..total).step_by(n).map(|k| k as u64).collect();
            let cnt = keys.len();
            let mut pks = vec![0u8; cnt * stride];
            for (i, &k) in keys.iter().enumerate() {
                pks[i * stride..(i + 1) * stride].copy_from_slice(&adv_key(k, stride)[..stride]);
            }
            let weights = vec![1i64; cnt];
            let vals = vec![0i64; cnt];
            adv_write_shard(dir, schema, &format!("{name}_{s}"), &pks, &weights, &vals)
        })
        .collect()
}

/// One dense in-RAM `Batch` over keys `[0, count)` (row `i` = key `i`) — the RAM
/// tier of the leaf gallop, whose `get_pk_bytes` leaf differs from the shard mmap.
fn adv_build_batch_dense(schema: SchemaDescriptor, count: usize) -> Rc<Batch> {
    let stride = schema.pk_stride();
    let mut b = Batch::with_capacity(&schema, count.max(1));
    for i in 0..count {
        b.extend_pk_bytes(&adv_key(i as u64, stride)[..stride]);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &0i64.to_le_bytes());
        b.count += 1;
    }
    b.certify_layout(Layout::Consolidated);
    Rc::new(b)
}

/// Build an in-RAM `Batch` from `(pk, weight, val)` tuples (already OPK-sorted by
/// the caller), OPK-encoding each PK. The delta source of the `Multi` fixture.
fn adv_build_batch_rows(schema: SchemaDescriptor, rows: &[(u64, i64, i64)]) -> Rc<Batch> {
    let stride = schema.pk_stride();
    let mut b = Batch::with_capacity(&schema, rows.len().max(1));
    for &(pk, w, v) in rows {
        b.extend_pk_bytes(&adv_key(pk, stride)[..stride]);
        b.extend_weight(&w.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &v.to_le_bytes());
        b.count += 1;
    }
    b.certify_layout(Layout::Consolidated);
    Rc::new(b)
}

/// Pack `(pk, weight, val)` rows (OPK-sorted) into a shard.
fn adv_write_shard_rows(
    dir: &tempfile::TempDir,
    schema: &SchemaDescriptor,
    name: &str,
    rows: &[(u64, i64, i64)],
) -> Rc<MappedShard> {
    let stride = schema.pk_stride();
    let cnt = rows.len();
    let mut pks = vec![0u8; cnt * stride];
    let mut weights = Vec::with_capacity(cnt);
    let mut vals = Vec::with_capacity(cnt);
    for (i, &(pk, w, v)) in rows.iter().enumerate() {
        pks[i * stride..(i + 1) * stride].copy_from_slice(&adv_key(pk, stride)[..stride]);
        weights.push(w);
        vals.push(v);
    }
    adv_write_shard(dir, schema, name, &pks, &weights, &vals)
}

/// Merge-mode fixture: one in-RAM delta batch + `k` shards, keys interleaved over
/// `k + 1` sources (source 0 = delta), so every source overlaps the others. Each
/// key gets a base `+1` in its home source; a `retract_step`-th key also gets a
/// canceling `-1` at the SAME payload in its neighbour (a net-zero cross-source
/// ghost — the `op_reduce` retraction shape), and every 7th non-retracted key a
/// `+1` at a DIFFERING payload in its neighbour (a cross-source PK collision that
/// forces the `seek_phase` payload tiebreak). `retract_step = 0` disables the
/// canceling term (insert-only). Sources hold distinct PKs internally, so a
/// PK-ascending sort is a valid `(PK, payload)` order.
fn adv_build_multi_fixture(
    dir: &tempfile::TempDir,
    schema: &SchemaDescriptor,
    name: &str,
    total: usize,
    k: usize,
    retract_step: usize,
) -> (Rc<Batch>, Vec<Rc<MappedShard>>) {
    let nsrc = k + 1;
    let mut rows: Vec<Vec<(u64, i64, i64)>> = vec![Vec::new(); nsrc];
    for key in 0..total {
        let home = key % nsrc;
        let k64 = key as u64;
        rows[home].push((k64, 1, key as i64));
        if retract_step != 0 && key % retract_step == 0 {
            let nb = (home + 1) % nsrc;
            rows[nb].push((k64, -1, key as i64)); // canceling: same payload, -w
        } else if key % 7 == 0 {
            let nb = (home + 1) % nsrc;
            rows[nb].push((k64, 1, (key as i64) ^ 0x5555)); // duplicate: differing payload
        }
    }
    for r in rows.iter_mut() {
        r.sort_by(|a, b| a.0.cmp(&b.0).then(a.2.cmp(&b.2)));
    }
    let delta = adv_build_batch_rows(*schema, &rows[0]);
    let shards = rows[1..]
        .iter()
        .enumerate()
        .map(|(i, r)| adv_write_shard_rows(dir, schema, &format!("{name}_{i}"), r))
        .collect();
    (delta, shards)
}

/// Off-clock oracle: the galloped landing equals the stateless lower bound at both
/// a cold and a live seed, and both equal `r` since row `r` holds key `r`.
/// Independent of gap and pattern, so it runs once per fixture.
fn adv_assert_leaf_oracle(
    count: usize,
    stride: usize,
    advance: impl Fn(&[u8], usize) -> usize,
    lower_bound: impl Fn(&[u8]) -> usize,
) {
    let mut r = count / 33;
    while r < count {
        let k = adv_key(r as u64, stride);
        assert_eq!(advance(&k[..stride], 0), r, "leaf oracle hint=0 r={r}");
        assert_eq!(advance(&k[..stride], r), r, "leaf oracle hint=live r={r}");
        assert_eq!(lower_bound(&k[..stride]), r, "leaf lower_bound r={r}");
        r += count / 33;
    }
}

/// Time one leaf-gallop corner (Bench 1). `advance(key, hint)` is
/// `Batch`/`MappedShard::advance_to`. `monotone` threads the returned index back as
/// the next hint (the `CursorState::advance_to` skip pattern); `!monotone` passes
/// `hint = 0` every probe (the full `O(log N)` point-lookup worst case). Cold
/// flushes the cache before each sweep and times only the sweep (not the flush).
#[allow(clippy::too_many_arguments)]
fn adv_time_leaf(
    count: usize,
    stride: usize,
    gap: usize,
    monotone: bool,
    tier: Tier,
    scratch: &mut [u8],
    advance: impl Fn(&[u8], usize) -> usize,
) -> f64 {
    use std::time::{Duration, Instant};

    let target = adv_target(monotone, tier);
    let mut elapsed = Duration::ZERO;
    let mut done = 0usize;
    let mut sink = 0usize;

    if matches!(tier, Tier::Hot) {
        // Warm-up sweep faults + caches the region (untimed).
        let mut hint = 0;
        let mut r = gap;
        while r < count {
            let k = adv_key(r as u64, stride);
            hint = advance(&k[..stride], if monotone { hint } else { 0 });
            sink = sink.wrapping_add(hint);
            r += gap;
        }
    }

    while done < target {
        if matches!(tier, Tier::Cold) {
            adv_flush_cache(scratch);
        }
        let mut hint = 0usize;
        let mut swept = 0usize;
        let t = Instant::now();
        let mut r = gap;
        while r < count {
            let k = adv_key(r as u64, stride);
            hint = advance(std::hint::black_box(&k[..stride]), if monotone { hint } else { 0 });
            sink = sink.wrapping_add(hint);
            swept += 1;
            if done + swept >= target {
                break;
            }
            r += gap;
        }
        elapsed += t.elapsed();
        done += swept;
    }
    std::hint::black_box(sink);
    elapsed.as_nanos() as f64 / done as f64
}

/// Time one merge-mode `seek_phase` corner (Bench 2): a monotone ascending
/// probe sweep at mean gap `gap`. `rewind` between *sweeps* only (never between
/// probes — that would restart every gallop from 0 and measure re-galloping, not
/// the monotone skip). Cold flushes between sweeps and times only the sweep.
fn adv_time_multi(c: &mut ReadCursor, stride: usize, total: usize, gap: usize, tier: Tier, scratch: &mut [u8]) -> f64 {
    use std::time::{Duration, Instant};
    let target = adv_target(true, tier);
    let mut elapsed = Duration::ZERO;
    let mut done = 0usize;
    let mut sink = 0u64;

    if matches!(tier, Tier::Hot) {
        c.rewind();
        let mut v = gap;
        while v < total {
            let k = adv_key(v as u64, stride);
            c.advance_to(&k[..stride]);
            sink ^= c.current_weight as u64;
            v += gap;
        }
    }

    while done < target {
        if matches!(tier, Tier::Cold) {
            adv_flush_cache(scratch);
        }
        c.rewind();
        let mut swept = 0usize;
        let t = Instant::now();
        let mut v = gap;
        while v < total {
            let k = adv_key(v as u64, stride);
            c.advance_to(std::hint::black_box(&k[..stride]));
            sink ^= std::hint::black_box(c.current_weight) as u64;
            swept += 1;
            if done + swept >= target {
                break;
            }
            v += gap;
        }
        elapsed += t.elapsed();
        done += swept;
    }
    std::hint::black_box(sink);
    elapsed.as_nanos() as f64 / done as f64
}

/// Time a cursor `advance_to` sweep through its non-fast-path (Bench 3): a
/// `Single` cursor always takes the absolute-reposition path, so an
/// ascending sweep (`ascending`) measures the forward slow path, and a descending
/// sweep the bounded-backward `[0, hint)` leaf gallop (the range-`Lt` reset shape).
/// `rewind` between sweeps; cold flushes and times only the sweep.
fn adv_time_cursor_sweep(
    c: &mut ReadCursor,
    stride: usize,
    span: usize,
    gap: usize,
    ascending: bool,
    tier: Tier,
    scratch: &mut [u8],
) -> f64 {
    use std::time::{Duration, Instant};
    let mut values: Vec<usize> = Vec::new();
    let mut v = gap;
    while v < span {
        values.push(v);
        v += gap;
    }
    if !ascending {
        values.reverse();
    }
    let target = adv_target(true, tier);
    let mut elapsed = Duration::ZERO;
    let mut done = 0usize;
    let mut sink = 0u64;

    if matches!(tier, Tier::Hot) {
        c.rewind();
        for &val in &values {
            let k = adv_key(val as u64, stride);
            c.advance_to(&k[..stride]);
            sink ^= c.current_weight as u64;
        }
    }

    while done < target {
        if matches!(tier, Tier::Cold) {
            adv_flush_cache(scratch);
        }
        c.rewind();
        let mut swept = 0usize;
        let t = Instant::now();
        for &val in &values {
            let k = adv_key(val as u64, stride);
            c.advance_to(std::hint::black_box(&k[..stride]));
            sink ^= std::hint::black_box(c.current_weight) as u64;
            swept += 1;
            if done + swept >= target {
                break;
            }
        }
        elapsed += t.elapsed();
        done += swept;
    }
    std::hint::black_box(sink);
    elapsed.as_nanos() as f64 / done as f64
}

/// Time the merge mode's `key == current_pk` replay path (Bench 3, arm B): the
/// probe is not strictly-forward, so `advance_to` replays the tournament instead
/// of galloping it. Stationary, so the leaf resolves in `O(1)` and only the
/// replay's `O(n)` compares are left. It allocates nothing — hot and cold alike.
fn adv_time_cursor_stationary(c: &mut ReadCursor, tier: Tier, scratch: &mut [u8]) -> f64 {
    use std::time::Instant;
    let iters = if matches!(tier, Tier::Hot) {
        200_000usize
    } else {
        50_000
    };
    c.rewind();
    assert!(c.valid, "stationary bench needs a live first row");
    let mut buf = [0u8; 40];
    let mut sink = 0u64;

    if matches!(tier, Tier::Cold) {
        adv_flush_cache(scratch);
    } else {
        for _ in 0..1000 {
            let s = c.current_pk_bytes().len();
            buf[..s].copy_from_slice(c.current_pk_bytes());
            c.advance_to(&buf[..s]);
            sink ^= c.current_weight as u64;
        }
    }

    let t = Instant::now();
    for _ in 0..iters {
        let s = c.current_pk_bytes().len();
        buf[..s].copy_from_slice(c.current_pk_bytes());
        c.advance_to(std::hint::black_box(&buf[..s]));
        sink ^= std::hint::black_box(c.current_weight) as u64;
    }
    let secs = t.elapsed();
    std::hint::black_box(sink);
    secs.as_nanos() as f64 / iters as f64
}

/// Bench 1 — leaf `gallop_opk` in isolation (no cursor, no loser tree): call
/// `Batch`/`MappedShard::advance_to` directly. Reproduces the leaf gallop work the
/// profile splits across `Run::advance_to` self-time (8.77%) and
/// `lower_bound_by` (2.04%), plus — cold — the scattered `get_pk_bytes` mmap-load
/// latency that dominates it. Both the RAM (`Batch`) and mmap (`Shard`) tiers,
/// monotone (position-seeded skip) and point-lookup (`hint = 0`) drivers.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn advance_to_leaf_gallop_bench() {
    let dir = tempfile::tempdir().unwrap();
    let mut scratch = vec![0u8; ADV_SCRATCH_BYTES];

    for &tier in &[Tier::Hot, Tier::Cold] {
        for &stride in &ADV_STRIDES {
            let schema = adv_bench_schema(stride);
            assert_eq!(schema.pk_stride(), stride, "schema stride mismatch");
            let count = tier.bytes() / stride;
            let shards = adv_build_interleaved_shards(&dir, &schema, &format!("b1_{}_{stride}", tier.tag()), count, 1);
            let shard = &shards[0];
            let batch = adv_build_batch_dense(schema, count);
            adv_assert_leaf_oracle(
                count,
                stride,
                |k, h| shard.advance_to(k, h),
                |k| shard.find_lower_bound_bytes(k),
            );
            adv_assert_leaf_oracle(
                count,
                stride,
                |k, h| batch.advance_to(k, h),
                |k| batch.find_lower_bound_bytes(k),
            );

            for &gap in &ADV_GAPS {
                for &monotone in &[true, false] {
                    let pat = if monotone { "monotone" } else { "point" };
                    let ns_s = adv_time_leaf(count, stride, gap, monotone, tier, &mut scratch, |k, h| {
                        shard.advance_to(k, h)
                    });
                    let ns_b = adv_time_leaf(count, stride, gap, monotone, tier, &mut scratch, |k, h| {
                        batch.advance_to(k, h)
                    });
                    println!(
                        "leaf {:>4} shard stride={stride:>2} gap={gap:>4} {pat:>8}: {ns_s:7.1} ns/probe  {:>11.0} probes/s",
                        tier.tag(),
                        1e9 / ns_s
                    );
                    println!(
                        "leaf {:>4} batch stride={stride:>2} gap={gap:>4} {pat:>8}: {ns_b:7.1} ns/probe  {:>11.0} probes/s",
                        tier.tag(),
                        1e9 / ns_b
                    );
                }
            }
        }
    }
}

/// Bench 2 — the merge fast path (`ReadCursor::advance_to` → `seek_phase`), the
/// exact reduce/distinct/join trace-probe shape. A merge-mode cursor (delta batch +
/// `K` overlapping shards, with cross-shard duplicate PKs at differing payloads
/// and a swept fraction of canceling weights) is driven by a monotone ascending
/// probe sweep. Sweeping `K` separates the leaf-gallop cost from the `Θ(log K)`
/// loser-tree maintenance; the retract axis matches the retraction-heavy
/// `op_reduce` caller (the 3.95% callee), not just the all-`+1` insert case.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn read_cursor_advance_to_multi_bench() {
    let dir = tempfile::tempdir().unwrap();
    let mut scratch = vec![0u8; ADV_SCRATCH_BYTES];

    for &tier in &[Tier::Hot, Tier::Cold] {
        for &stride in &ADV_STRIDES {
            let schema = adv_bench_schema(stride);
            let total = tier.bytes() / stride;
            for &k in &[2usize, 4, 8, 16] {
                for &retract_step in &[0usize, 2] {
                    let rlabel = if retract_step == 0 { "0.0" } else { "0.5" };
                    let name = format!("b2_{}_{stride}_{k}_{retract_step}", tier.tag());
                    let (delta, shards) = adv_build_multi_fixture(&dir, &schema, &name, total, k, retract_step);
                    let mut c = create_read_cursor(std::slice::from_ref(&delta), &shards, schema);
                    assert!(c.mode.is_none(), "bench must drive the merge fast path (k={k})");

                    // Oracle (off-clock): ascending sample matches from-scratch seeks.
                    let sample: Vec<u64> = (1..=16).map(|i| (i * total / 17) as u64).collect();
                    adv_assert_cursor_oracle(
                        || create_read_cursor(std::slice::from_ref(&delta), &shards, schema),
                        stride,
                        &sample,
                    );

                    for &gap in &ADV_GAPS {
                        let ns = adv_time_multi(&mut c, stride, total, gap, tier, &mut scratch);
                        println!(
                            "multi {:>4} K={k:>2} retract={rlabel} stride={stride:>2} gap={gap:>4}: {ns:7.1} ns/probe  {:>11.0} probes/s",
                            tier.tag(),
                            1e9 / ns
                        );
                    }
                }
            }
        }
    }
}

/// Bench 3 — the slow path the fast path avoids, each arm mapped to a real caller.
/// `fwd-single`/`fwd-pair`: the absolute-reposition drive a low-run / freshly-
/// compacted table takes. `eq-multi`: the `key == current_pk` tournament replay.
/// `bwd-single`: the bounded-backward `[0, hint)` leaf gallop — the range join's
/// per-group reset, which at `eq_size > 0` seeks to a cut that can sort below the
/// cursor. A guardrail: a fast-path win must not silently regress these.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn read_cursor_advance_to_rebuild_bench() {
    let dir = tempfile::tempdir().unwrap();
    let mut scratch = vec![0u8; ADV_SCRATCH_BYTES];

    for &tier in &[Tier::Hot, Tier::Cold] {
        for &stride in &ADV_STRIDES {
            let schema = adv_bench_schema(stride);
            let count = tier.bytes() / stride;
            // Single + backward share one full dense shard; Multi splits the
            // same budget across 2/4 interleaved shards.
            let single = adv_build_interleaved_shards(&dir, &schema, &format!("b3s_{}_{stride}", tier.tag()), count, 1);
            let pair = adv_build_interleaved_shards(&dir, &schema, &format!("b3p_{}_{stride}", tier.tag()), count, 2);
            let multi = adv_build_interleaved_shards(&dir, &schema, &format!("b3m_{}_{stride}", tier.tag()), count, 4);
            let sample: Vec<u64> = (1..=16).map(|i| (i * count / 17) as u64).collect();

            {
                let mut c = create_read_cursor(&[], &single, schema);
                assert!(c.mode.is_some());
                adv_assert_cursor_oracle(|| create_read_cursor(&[], &single, schema), stride, &sample);
                let ns = adv_time_cursor_sweep(&mut c, stride, count, 128, true, tier, &mut scratch);
                println!(
                    "rebuild {:>4} fwd-single stride={stride:>2}: {ns:7.1} ns/probe",
                    tier.tag()
                );
            }
            {
                let mut c = create_read_cursor(&[], &pair, schema);
                assert!(c.mode.is_none());
                adv_assert_cursor_oracle(|| create_read_cursor(&[], &pair, schema), stride, &sample);
                let ns = adv_time_cursor_sweep(&mut c, stride, count, 128, true, tier, &mut scratch);
                println!(
                    "rebuild {:>4} fwd-pair   stride={stride:>2}: {ns:7.1} ns/probe",
                    tier.tag()
                );
            }
            {
                let mut c = create_read_cursor(&[], &multi, schema);
                assert!(c.mode.is_none());
                adv_assert_cursor_oracle(|| create_read_cursor(&[], &multi, schema), stride, &sample);
                let ns = adv_time_cursor_stationary(&mut c, tier, &mut scratch);
                println!(
                    "rebuild {:>4} eq-multi   stride={stride:>2}: {ns:7.1} ns/probe",
                    tier.tag()
                );
            }
            {
                let mut c = create_read_cursor(&[], &single, schema);
                let ns = adv_time_cursor_sweep(&mut c, stride, count, 128, false, tier, &mut scratch);
                println!(
                    "rebuild {:>4} bwd-single stride={stride:>2}: {ns:7.1} ns/probe",
                    tier.tag()
                );
            }
        }
    }
}
