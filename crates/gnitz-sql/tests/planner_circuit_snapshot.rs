#![cfg(feature = "integration")]

//! Circuit-shape pinning suite.
//!
//! Each `sentinel_*` test freezes the **structure** a representative view
//! compiles to — opcodes, full wiring (ports + children), scan provenance, and
//! every `CIRCUIT_NODE_COLUMNS` param — against the *current* compiler, so a
//! later behavior-preserving refactor that re-emits the same graph in a
//! different order must reproduce it byte-for-byte. The dump is
//! emission-order-independent by construction (`canonical_circuit_dump`,
//! `common/mod.rs`): nodes are labelled by DFS-from-sink position (`#N`), base
//! scans by name, hidden segments by structural index.
//!
//! One sentinel per distinct **primitive family / reduce-selector arm /
//! multi-segment composition** — a shared-primitive regression surfaces in *any*
//! shape using that primitive, so the variants are covered by weight pins, not
//! more frozen dumps.
//!
//! **Expr programs are deliberately NOT in the dump**: a `Filter` encodes
//! to zero node-columns, so this mechanism is structurally blind to predicate
//! *content* by design. A green structural sentinel therefore does **not** mean
//! predicate/expr coverage — that is the job of the weight pins on the
//! predicate-bearing shapes (`planner_weight_pins.rs`). Do not delete a backing weight pin as
//! "redundant with a sentinel".
//!
//! Authoring loop: run one test with `GNITZ_DUMP_CIRCUIT=1` (the helper
//! `eprintln!`s the dump), paste it as `EXPECTED`, re-run without the env var.

use gnitz_core::{CircuitBuilder, GnitzClient};
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

/// Resolve `view` + the named base tables and return its canonical circuit dump.
/// `bases` are the **base tables** only — hidden segments resolve structurally.
fn dump_for(client: &mut GnitzClient, sn: &str, label: &str, view: &str, bases: &[&str]) -> String {
    let final_vid = client.resolve_table_or_view_id(sn, view).unwrap().0;
    let mut base_tables: Vec<(&str, u64)> = Vec::new();
    for b in bases {
        let id = client.resolve_table_or_view_id(sn, b).unwrap().0;
        base_tables.push((*b, id));
    }
    if std::env::var_os("GNITZ_DUMP_CIRCUIT").is_some() {
        eprintln!("\n########## SENTINEL {label} ##########");
    }
    canonical_circuit_dump(client, final_vid, &base_tables)
}

// ═════════════════════════════════════════════════════════════════════════════
// Order-independence unit test (mandatory).
//
// The 3×-determinism authoring check proves *stability*, not *order-
// independence*. This builds the *same* logical circuit twice via a
// `CircuitBuilder` with node-creation calls in two different orders, materializes
// both through `Circuit::into_rows()`, and asserts the dump is byte-identical —
// the property the whole migration leans on. No server needed.
// ═════════════════════════════════════════════════════════════════════════════

/// A union of two independent single-column-projection branches over two base
/// sources — the smallest graph whose build order can be permuted without
/// changing structure. `swapped` builds the B-branch first; the logical wiring
/// (`union(a_branch, b_branch)`, a on `PORT_IN_A`) is identical either way.
fn build_union(swapped: bool) -> gnitz_core::CircuitRows {
    let mut cb = CircuitBuilder::new(999, 100);
    let (a_branch, b_branch) = if !swapped {
        let a = cb.input_delta(); // primary source 100
        let ma = cb.map(a, &[0]);
        let b = cb.input_delta_tagged(200);
        let mb = cb.map(b, &[0]);
        (ma, mb)
    } else {
        let b = cb.input_delta_tagged(200);
        let mb = cb.map(b, &[0]);
        let a = cb.input_delta(); // primary source 100
        let ma = cb.map(a, &[0]);
        (ma, mb)
    };
    let u = cb.union(a_branch, b_branch);
    cb.sink(u);
    cb.build().into_rows()
}

#[test]
fn dump_is_emission_order_independent() {
    let bases = [("a", 100u64), ("b", 200u64)];
    let d0 = canonical_circuit_dump_from_rows(999, &build_union(false), &bases);
    let d1 = canonical_circuit_dump_from_rows(999, &build_union(true), &bases);
    assert_eq!(
        d0, d1,
        "the dump must depend only on structure, not node-id assignment order"
    );
    // Guard against a degenerate all-empty render masking the equality.
    assert!(
        d0.contains("UNION") && d0.contains("base:a") && d0.contains("base:b"),
        "sanity: {d0}"
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// Structural sentinels (~17 dumps, one per primitive family / arm / composition).
// EXPECTED authored against the current compiler via GNITZ_DUMP_CIRCUIT=1.
// ═════════════════════════════════════════════════════════════════════════════

// #1 — linear (+WHERE): filter/map, plus a `WHERE ind = 5` scan-bound over an
// indexed column (the canonicalizer bootstrap). Predicate *content* is not in
// the dump — it is weight-pinned (Part B.4).
#[test]
fn sentinel_01_linear_where_bound() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, \
         ind BIGINT NOT NULL, other BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "CREATE INDEX ON t(ind)");
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT g, other FROM t WHERE ind = 5",
    );
    let dump = dump_for(&mut client, &sn, "01_linear_where_bound", "v", &["t"]);
    assert_eq!(dump, EXPECTED_01);
}
// Plain reproduction pin. Predicate *content* (`ind = 5`) is NOT captured here
// (expr excluded) — it is weight-pinned in `planner_weight_pins.rs`.
const EXPECTED_01: &str = r#"seg 0:
#0 INTEGRATE_SINK <- (#1@0)
#1 MAP_PROJ params:[(PROJ,0,1,0);(PROJ,1,3,0)] <- (#2@0)
#2 FILTER <- (#3@0)
#3 SCAN_DELTA params:[(SCAN_BOUND,0,2,0)] src:base:t
"#;

// #2 — equi join inner: `emit_equi_join_terms`.
#[test]
fn sentinel_02_equi_join_inner() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE ers_a (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE ers_b (id BIGINT NOT NULL PRIMARY KEY, bv BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW ers_i AS SELECT ers_b.bv AS bv FROM ers_a JOIN ers_b ON ers_a.fk = ers_b.id",
    );
    let dump = dump_for(&mut client, &sn, "02_equi_join_inner", "ers_i", &["ers_a", "ers_b"]);
    assert_eq!(dump, EXPECTED_02);
}
const EXPECTED_02: &str = r#"seg 0:
#0 INTEGRATE_SINK <- (#1@0)
#1 MAP_PROJ params:[(PROJ,0,2,0)] <- (#2@0)
#2 UNION <- (#3@0,#10@1)
#3 MAP_PROJ params:[(PROJ,0,1,0);(PROJ,1,2,0)] <- (#4@0)
#4 JOIN_DELTA_TRACE <- (#5@0,#7@1)
#5 MAP_EXPR params:[(REINDEX,0,1,0);(ROUTE_KEY,0,1,0)] <- (#6@0)
#6 SCAN_DELTA src:base:ers_a
#7 INTEGRATE_TRACE <- (#8@0)
#8 MAP_EXPR params:[(REINDEX,0,0,0);(ROUTE_KEY,0,1,0)] <- (#9@0)
#9 SCAN_DELTA src:base:ers_b
#10 MAP_PROJ params:[(PROJ,0,2,0);(PROJ,1,1,0)] <- (#11@0)
#11 JOIN_DELTA_TRACE <- (#8@0,#12@1)
#12 INTEGRATE_TRACE <- (#5@0)
"#;

// #3 — equi join LEFT: `null_extend` + `positive_diff` ν (equi/band realization).
#[test]
fn sentinel_03_equi_join_left() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW vl AS SELECT * FROM l LEFT JOIN r ON l.fk = r.k",
    );
    let dump = dump_for(&mut client, &sn, "03_equi_join_left", "vl", &["l", "r"]);
    assert_eq!(dump, EXPECTED_03);
}
const EXPECTED_03: &str = r#"seg 0:
#0 INTEGRATE_SINK <- (#1@0)
#1 UNION <- (#2@0,#7@1)
#2 NULL_EXTEND params:[(NULL_EXT,0,9,0);(NULL_EXT,1,9,0)] <- (#3@0)
#3 POSITIVE_PART <- (#4@0)
#4 UNION <- (#5@0,#10@1)
#5 NEGATE <- (#6@0)
#6 MAP_PROJ params:[(PROJ,0,1,0);(PROJ,1,2,0)] <- (#7@0)
#7 UNION <- (#8@0,#15@1)
#8 MAP_PROJ params:[(PROJ,0,1,0);(PROJ,1,2,0);(PROJ,2,3,0);(PROJ,3,4,0)] <- (#9@0)
#9 JOIN_DELTA_TRACE <- (#10@0,#12@1)
#10 MAP_EXPR params:[(REINDEX,0,1,0);(ROUTE_KEY,0,1,0)] <- (#11@0)
#11 SCAN_DELTA src:base:l
#12 INTEGRATE_TRACE <- (#13@0)
#13 MAP_EXPR params:[(REINDEX,0,0,0);(ROUTE_KEY,0,1,0)] <- (#14@0)
#14 SCAN_DELTA src:base:r
#15 MAP_PROJ params:[(PROJ,0,3,0);(PROJ,1,4,0);(PROJ,2,1,0);(PROJ,3,2,0)] <- (#16@0)
#16 JOIN_DELTA_TRACE <- (#13@0,#17@1)
#17 INTEGRATE_TRACE <- (#10@0)
"#;

// #4 — band LEFT: `emit_range_join` band mechanics + ν.
#[test]
fn sentinel_04_band_left() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE bls_a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE bls_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW bls_left AS SELECT bls_a.id AS aid, bls_b.id AS bid \
         FROM bls_a LEFT JOIN bls_b ON bls_a.k = bls_b.k AND bls_a.lo <= bls_b.t",
    );
    let dump = dump_for(&mut client, &sn, "04_band_left", "bls_left", &["bls_a", "bls_b"]);
    assert_eq!(dump, EXPECTED_04);
}
const EXPECTED_04: &str = r#"seg 0:
#0 INTEGRATE_SINK <- (#1@0)
#1 EXCHANGE_SHARD params:[(SHARD,0,0,0);(SHARD,1,1,0)] <- (#2@0)
#2 MAP_PROJ params:[(PROJ,0,2,0);(PROJ,1,5,0)] <- (#3@0)
#3 UNION <- (#4@0,#24@1)
#4 MAP_PROJ params:[(PROJ,0,2,0);(PROJ,1,3,0);(PROJ,2,4,0);(PROJ,3,5,0);(PROJ,4,6,0);(PROJ,5,7,0)] <- (#5@0)
#5 MAP_EXPR params:[(REINDEX,0,0,0);(REINDEX,1,4,0);(ROUTE_KEY,0,0,0)] <- (#6@0)
#6 NULL_EXTEND params:[(NULL_EXT,0,9,0);(NULL_EXT,1,9,0);(NULL_EXT,2,9,0)] <- (#7@0)
#7 POSITIVE_PART <- (#8@0)
#8 UNION <- (#9@0,#23@1)
#9 NEGATE <- (#10@0)
#10 MAP_PROJ params:[(PROJ,0,3,0);(PROJ,1,4,0);(PROJ,2,5,0)] <- (#11@0)
#11 MAP_EXPR params:[(REINDEX,0,2,0);(ROUTE_KEY,0,0,0)] <- (#12@0)
#12 UNION <- (#13@0,#20@1)
#13 MAP_PROJ params:[(PROJ,0,2,0);(PROJ,1,3,0);(PROJ,2,4,0);(PROJ,3,5,0);(PROJ,4,6,0);(PROJ,5,7,0)] <- (#14@0)
#14 JOIN_DELTA_TRACE_RANGE params:[(RANGE_JOIN,0,1,3)] <- (#15@0,#17@1)
#15 MAP_EXPR params:[(REINDEX,0,1,0);(REINDEX,1,2,0);(ROUTE_KEY,0,1,0)] <- (#16@0)
#16 SCAN_DELTA src:base:bls_a
#17 INTEGRATE_TRACE <- (#18@0)
#18 MAP_EXPR params:[(REINDEX,0,1,0);(REINDEX,1,2,0);(ROUTE_KEY,0,1,0)] <- (#19@0)
#19 SCAN_DELTA src:base:bls_b
#20 MAP_PROJ params:[(PROJ,0,5,0);(PROJ,1,6,0);(PROJ,2,7,0);(PROJ,3,2,0);(PROJ,4,3,0);(PROJ,5,4,0)] <- (#21@0)
#21 JOIN_DELTA_TRACE_RANGE params:[(RANGE_JOIN,0,1,1)] <- (#18@0,#22@1)
#22 INTEGRATE_TRACE <- (#15@0)
#23 MAP_EXPR params:[(REINDEX,0,0,0);(ROUTE_KEY,0,0,0)] <- (#16@0)
#24 MAP_PROJ params:[(PROJ,0,4,0);(PROJ,1,5,0);(PROJ,2,6,0);(PROJ,3,7,0);(PROJ,4,8,0);(PROJ,5,9,0)] <- (#25@0)
#25 MAP_EXPR params:[(REINDEX,0,2,0);(REINDEX,1,5,0);(ROUTE_KEY,0,0,0)] <- (#12@0)
"#;

// #5 — pure-range LEFT: `build_pure_range_threshold` ν (the only shape using it).
#[test]
fn sentinel_05_pure_range_left() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE pls_a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE pls_b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW pls_left AS SELECT pls_a.id AS aid, pls_b.id AS bid \
         FROM pls_a LEFT JOIN pls_b ON pls_a.x < pls_b.y",
    );
    let dump = dump_for(&mut client, &sn, "05_pure_range_left", "pls_left", &["pls_a", "pls_b"]);
    assert_eq!(dump, EXPECTED_05);
}
const EXPECTED_05: &str = r#"seg 0:
#0 INTEGRATE_SINK <- (#1@0)
#1 EXCHANGE_SHARD params:[(SHARD,0,0,0);(SHARD,1,1,0)] <- (#2@0)
#2 MAP_PROJ params:[(PROJ,0,2,0);(PROJ,1,4,0)] <- (#3@0)
#3 UNION <- (#4@0,#28@1)
#4 MAP_PROJ params:[(PROJ,0,2,0);(PROJ,1,3,0);(PROJ,2,4,0);(PROJ,3,5,0)] <- (#5@0)
#5 MAP_EXPR params:[(REINDEX,0,0,0);(REINDEX,1,3,0);(ROUTE_KEY,0,0,0)] <- (#6@0)
#6 NULL_EXTEND params:[(NULL_EXT,0,9,0);(NULL_EXT,1,9,0)] <- (#7@0)
#7 UNION <- (#8@0,#13@1)
#8 MAP_PROJ params:[(PROJ,0,2,0);(PROJ,1,3,0)] <- (#9@0)
#9 MAP_EXPR params:[(REINDEX,0,1,0);(ROUTE_KEY,0,0,0)] <- (#10@0)
#10 WORKER_FILTER <- (#11@0)
#11 MAP_EXPR params:[(REINDEX,0,1,0);(ROUTE_KEY,0,1,0)] <- (#12@0)
#12 SCAN_DELTA src:base:pls_a
#13 NEGATE <- (#14@0)
#14 MAP_PROJ params:[(PROJ,0,2,0);(PROJ,1,3,0)] <- (#15@0)
#15 MAP_EXPR params:[(REINDEX,0,1,0);(ROUTE_KEY,0,0,0)] <- (#16@0)
#16 UNION <- (#17@0,#25@1)
#17 MAP_PROJ params:[(PROJ,0,1,0);(PROJ,1,2,0)] <- (#18@0)
#18 JOIN_DELTA_TRACE_RANGE params:[(RANGE_JOIN,0,0,2)] <- (#10@0,#19@1)
#19 INTEGRATE_TRACE <- (#20@0)
#20 MAP_EXPR params:[(REINDEX,0,1,0);(ROUTE_KEY,0,0,0)] <- (#21@0)
#21 REDUCE params:[(AGG_SPEC,0,4,1)] <- (#22@0)
#22 MAP_HASH_ROW params:[(PROJ,0,0,0);(BRANCH_ID,0,0,0)] <- (#23@0)
#23 MAP_EXPR params:[(REINDEX,0,1,0);(ROUTE_KEY,0,1,0)] <- (#24@0)
#24 SCAN_DELTA src:base:pls_b
#25 MAP_PROJ params:[(PROJ,0,3,0);(PROJ,1,4,0)] <- (#26@0)
#26 JOIN_DELTA_TRACE_RANGE params:[(RANGE_JOIN,0,0,0)] <- (#20@0,#27@1)
#27 INTEGRATE_TRACE <- (#10@0)
#28 MAP_PROJ params:[(PROJ,0,3,0);(PROJ,1,4,0);(PROJ,2,5,0);(PROJ,3,6,0)] <- (#29@0)
#29 MAP_EXPR params:[(REINDEX,0,1,0);(REINDEX,1,3,0);(ROUTE_KEY,0,0,0)] <- (#30@0)
#30 UNION <- (#31@0,#35@1)
#31 MAP_PROJ params:[(PROJ,0,1,0);(PROJ,1,2,0);(PROJ,2,3,0);(PROJ,3,4,0)] <- (#32@0)
#32 JOIN_DELTA_TRACE_RANGE params:[(RANGE_JOIN,0,0,2)] <- (#11@0,#33@1)
#33 INTEGRATE_TRACE <- (#34@0)
#34 WORKER_FILTER <- (#23@0)
#35 MAP_PROJ params:[(PROJ,0,3,0);(PROJ,1,4,0);(PROJ,2,1,0);(PROJ,3,2,0)] <- (#36@0)
#36 JOIN_DELTA_TRACE_RANGE params:[(RANGE_JOIN,0,0,0)] <- (#23@0,#27@1)
"#;

// #6a — semi join (EXISTS): `semi_and_anti` + `positive_part`.
#[test]
fn sentinel_06a_equi_semi() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE eq_a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE eq_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW eq_semi AS SELECT eq_a.v FROM eq_a WHERE EXISTS (SELECT 1 FROM eq_b WHERE eq_b.k = eq_a.k)",
    );
    let dump = dump_for(&mut client, &sn, "06a_equi_semi", "eq_semi", &["eq_a", "eq_b"]);
    assert_eq!(dump, EXPECTED_06A);
}
const EXPECTED_06A: &str = r#"seg 0:
#0 INTEGRATE_SINK <- (#1@0)
#1 MAP_PROJ params:[(PROJ,0,3,0)] <- (#2@0)
#2 UNION <- (#3@0,#10@1)
#3 NEGATE <- (#4@0)
#4 POSITIVE_PART <- (#5@0)
#5 UNION <- (#6@0,#10@1)
#6 NEGATE <- (#7@0)
#7 UNION <- (#8@0,#15@1)
#8 MAP_PROJ params:[(PROJ,0,1,0);(PROJ,1,2,0);(PROJ,2,3,0)] <- (#9@0)
#9 JOIN_DELTA_TRACE <- (#10@0,#12@1)
#10 MAP_EXPR params:[(REINDEX,0,1,0);(ROUTE_KEY,0,1,0)] <- (#11@0)
#11 SCAN_DELTA src:base:eq_a
#12 INTEGRATE_TRACE <- (#13@0)
#13 MAP_EXPR params:[(REINDEX,0,1,0);(ROUTE_KEY,0,1,0)] <- (#14@0)
#14 SCAN_DELTA src:base:eq_b
#15 MAP_PROJ params:[(PROJ,0,3,0);(PROJ,1,4,0);(PROJ,2,5,0)] <- (#16@0)
#16 JOIN_DELTA_TRACE <- (#13@0,#17@1)
#17 INTEGRATE_TRACE <- (#10@0)
"#;

// #6b — anti join (NOT EXISTS): the anti arm of `semi_and_anti`.
#[test]
fn sentinel_06b_equi_anti() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE eq_a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE eq_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW eq_anti AS SELECT eq_a.v FROM eq_a WHERE NOT EXISTS (SELECT 1 FROM eq_b WHERE eq_b.k = eq_a.k)",
    );
    let dump = dump_for(&mut client, &sn, "06b_equi_anti", "eq_anti", &["eq_a", "eq_b"]);
    assert_eq!(dump, EXPECTED_06B);
}
const EXPECTED_06B: &str = r#"seg 0:
#0 INTEGRATE_SINK <- (#1@0)
#1 MAP_PROJ params:[(PROJ,0,3,0)] <- (#2@0)
#2 POSITIVE_PART <- (#3@0)
#3 UNION <- (#4@0,#8@1)
#4 NEGATE <- (#5@0)
#5 UNION <- (#6@0,#13@1)
#6 MAP_PROJ params:[(PROJ,0,1,0);(PROJ,1,2,0);(PROJ,2,3,0)] <- (#7@0)
#7 JOIN_DELTA_TRACE <- (#8@0,#10@1)
#8 MAP_EXPR params:[(REINDEX,0,1,0);(ROUTE_KEY,0,1,0)] <- (#9@0)
#9 SCAN_DELTA src:base:eq_a
#10 INTEGRATE_TRACE <- (#11@0)
#11 MAP_EXPR params:[(REINDEX,0,1,0);(ROUTE_KEY,0,1,0)] <- (#12@0)
#12 SCAN_DELTA src:base:eq_b
#13 MAP_PROJ params:[(PROJ,0,3,0);(PROJ,1,4,0);(PROJ,2,5,0)] <- (#14@0)
#14 JOIN_DELTA_TRACE <- (#11@0,#15@1)
#15 INTEGRATE_TRACE <- (#8@0)
"#;

// #7 — mark (projected EXISTS): the mark two-branch structure + `MAP_HASH_ROW`.
#[test]
fn sentinel_07_mark() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW mk AS SELECT id, EXISTS (SELECT 1 FROM b WHERE b.k = a.k) AS flag FROM a",
    );
    let dump = dump_for(&mut client, &sn, "07_mark", "mk", &["a", "b"]);
    assert_eq!(dump, EXPECTED_07);
}
// The two mark branches are always emitted and unioned (matched/unmatched), each
// with its own finalize MAP_EXPR (#2 / #19) — never collapsed to one filter over
// a runtime mark column. Predicate/branch *content* is weight-pinned (Part B.4).
const EXPECTED_07: &str = r#"seg 0:
#0 INTEGRATE_SINK <- (#1@0)
#1 UNION <- (#2@0,#19@1)
#2 MAP_EXPR params:[(MAP_OUT_COLS,0,9,0);(MAP_OUT_COLS,1,9,1)] <- (#3@0)
#3 UNION <- (#4@0,#11@1)
#4 NEGATE <- (#5@0)
#5 POSITIVE_PART <- (#6@0)
#6 UNION <- (#7@0,#11@1)
#7 NEGATE <- (#8@0)
#8 UNION <- (#9@0,#16@1)
#9 MAP_PROJ params:[(PROJ,0,1,0);(PROJ,1,2,0);(PROJ,2,3,0)] <- (#10@0)
#10 JOIN_DELTA_TRACE <- (#11@0,#13@1)
#11 MAP_EXPR params:[(REINDEX,0,1,0);(ROUTE_KEY,0,1,0)] <- (#12@0)
#12 SCAN_DELTA src:base:a
#13 INTEGRATE_TRACE <- (#14@0)
#14 MAP_EXPR params:[(REINDEX,0,1,0);(ROUTE_KEY,0,1,0)] <- (#15@0)
#15 SCAN_DELTA src:base:b
#16 MAP_PROJ params:[(PROJ,0,3,0);(PROJ,1,4,0);(PROJ,2,5,0)] <- (#17@0)
#17 JOIN_DELTA_TRACE <- (#14@0,#18@1)
#18 INTEGRATE_TRACE <- (#11@0)
#19 MAP_EXPR params:[(MAP_OUT_COLS,0,9,0);(MAP_OUT_COLS,1,9,1)] <- (#5@0)
"#;

// #8 — GROUP BY grouped: the reduce grouped arm.
#[test]
fn sentinel_08_group_by_grouped() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT NOT NULL, PRIMARY KEY (a, b))",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW g_full AS SELECT a AS ka, b AS kb, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY a, b",
    );
    let dump = dump_for(&mut client, &sn, "08_group_by_grouped", "g_full", &["t"]);
    assert_eq!(dump, EXPECTED_08);
}
// The positional col-index params (GROUP/AGG_SPEC) shift under checkpoint-8
// column pruning — a one-time reviewed golden update there, cross-checked
// against the grouped weight pins (existing e2e).
const EXPECTED_08: &str = r#"seg 0:
#0 INTEGRATE_SINK <- (#1@0)
#1 MAP_EXPR params:[(MAP_OUT_COLS,0,9,0);(MAP_OUT_COLS,1,9,0)] <- (#2@0)
#2 REDUCE params:[(GROUP,0,0,0);(GROUP,1,1,0);(AGG_SPEC,0,1,0);(AGG_SPEC,1,2,2);(REDUCE_OUT_KEY,0,1,0)] <- (#3@0)
#3 EXCHANGE_SHARD params:[(SHARD,0,0,0);(SHARD,1,1,0)] <- (#4@0)
#4 SCAN_DELTA src:base:t
"#;

// #9 — global-funnel agg (non-linear): the reduce funnel arm (`reduce_multi`),
// driven by MIN/MAX (non-linear ⇒ not two-phase).
#[test]
fn sentinel_09_global_funnel() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE nn_t (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, x BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW nn_global AS SELECT SUM(x) AS sx, MIN(x) AS mnx, MAX(x) AS mxx FROM nn_t",
    );
    let dump = dump_for(&mut client, &sn, "09_global_funnel", "nn_global", &["nn_t"]);
    assert_eq!(dump, EXPECTED_09);
}
const EXPECTED_09: &str = r#"seg 0:
#0 INTEGRATE_SINK <- (#1@0)
#1 MAP_EXPR params:[(MAP_OUT_COLS,0,9,1);(MAP_OUT_COLS,1,9,1);(MAP_OUT_COLS,2,9,1)] <- (#2@0)
#2 REDUCE params:[(AGG_SPEC,0,2,2);(AGG_SPEC,1,3,2);(AGG_SPEC,2,4,2);(AGG_SPEC,3,1,0);(GLOBAL_GROUND,0,1,0)] <- (#3@0)
#3 EXCHANGE_SHARD <- (#4@0)
#4 SCAN_DELTA src:base:nn_t
"#;

// #10 — two-phase all-linear global agg: the reduce two-phase arm (SUM/COUNT over
// a partitioned integer source ⇒ `two_phase = true`, `group_by.rs`).
#[test]
fn sentinel_10_two_phase_global() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t2p (id BIGINT PRIMARY KEY, x BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT SUM(x) AS s, COUNT(*) AS c FROM t2p",
    );
    let dump = dump_for(&mut client, &sn, "10_two_phase_global", "v", &["t2p"]);
    assert_eq!(dump, EXPECTED_10);
}
// Two-phase: local partial REDUCE (#4, no exchange) → the single ExchangeShard(∅)
// (#3) → combine REDUCE (#2). The combine sums each partial with the per-op merge
// func (SUM_ZERO=6 for the COUNT partial) plus the COUNT-of-partials gate.
const EXPECTED_10: &str = r#"seg 0:
#0 INTEGRATE_SINK <- (#1@0)
#1 MAP_EXPR params:[(MAP_OUT_COLS,0,9,1);(MAP_OUT_COLS,1,9,0)] <- (#2@0)
#2 REDUCE params:[(AGG_SPEC,0,2,1);(AGG_SPEC,1,6,2);(AGG_SPEC,2,1,0);(GLOBAL_GROUND,0,1,0)] <- (#3@0)
#3 EXCHANGE_SHARD <- (#4@0)
#4 REDUCE params:[(AGG_SPEC,0,2,1);(AGG_SPEC,1,1,0)] <- (#5@0)
#5 SCAN_DELTA src:base:t2p
"#;

// #11 — replicated-table reduce: the `reduce_multi_local` arm (shard-free).
#[test]
fn sentinel_11_replicated_reduce() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE rt (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, x BIGINT NOT NULL) WITH (replicated = true)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT g, SUM(x) AS s FROM rt GROUP BY g",
    );
    let dump = dump_for(&mut client, &sn, "11_replicated_reduce", "v", &["rt"]);
    assert_eq!(dump, EXPECTED_11);
}
// Replicated source: shard-free `reduce_multi_local` — the REDUCE reads the
// SCAN_DELTA directly, with NO ExchangeShard (contrast #8's grouped arm). The
// N-fold-multiply hazard this arm avoids is weight-pinned at W>1 (Part B.4).
const EXPECTED_11: &str = r#"seg 0:
#0 INTEGRATE_SINK <- (#1@0)
#1 MAP_EXPR params:[(MAP_OUT_COLS,0,9,0);(MAP_OUT_COLS,1,9,0)] <- (#2@0)
#2 REDUCE params:[(GROUP,0,1,0);(AGG_SPEC,0,2,2);(AGG_SPEC,1,1,0)] <- (#3@0)
#3 SCAN_DELTA src:base:rt
"#;

// #12 — DISTINCT: the weight-clamp.
#[test]
fn sentinel_12_distinct() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (a BIGINT, b BIGINT, c1 BIGINT NOT NULL, PRIMARY KEY (a, b))",
    );
    exec(&mut client, &sn, "CREATE VIEW v AS SELECT DISTINCT a FROM t");
    let dump = dump_for(&mut client, &sn, "12_distinct", "v", &["t"]);
    assert_eq!(dump, EXPECTED_12);
}
const EXPECTED_12: &str = r#"seg 0:
#0 INTEGRATE_SINK <- (#1@0)
#1 DISTINCT <- (#2@0)
#2 EXCHANGE_SHARD params:[(SHARD,0,0,0)] <- (#3@0)
#3 MAP_HASH_ROW params:[(PROJ,0,0,0);(BRANCH_ID,0,0,0)] <- (#4@0)
#4 SCAN_DELTA src:base:t
"#;

// #13a — set-op EXCEPT (distinct, `positive_part`).
#[test]
fn sentinel_13a_except() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t1 (a BIGINT, b BIGINT, val BIGINT NOT NULL, PRIMARY KEY (a, b))",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t2 (a BIGINT, b BIGINT, val BIGINT NOT NULL, PRIMARY KEY (a, b))",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v_except AS SELECT val FROM t1 EXCEPT SELECT val FROM t2",
    );
    let dump = dump_for(&mut client, &sn, "13a_except", "v_except", &["t1", "t2"]);
    assert_eq!(dump, EXPECTED_13A);
}
// EXCEPT DISTINCT = positive_part(distinct(A) − distinct(B)); the two content-hash
// leaves (#4/#8) are structurally identical. The quantifier variants (ALL, and
// INTERSECT/UNION DISTINCT) differ by one leaf clamp — weight-pinned by the
// existing `test_set_ops.py` per-op oracle, not frozen as separate dumps.
const EXPECTED_13A: &str = r#"seg 0:
#0 INTEGRATE_SINK <- (#1@0)
#1 POSITIVE_PART <- (#2@0)
#2 UNION <- (#3@0,#8@1)
#3 NEGATE <- (#4@0)
#4 DISTINCT <- (#5@0)
#5 EXCHANGE_SHARD params:[(SHARD,0,0,0)] <- (#6@0)
#6 MAP_HASH_ROW params:[(PROJ,0,2,0);(BRANCH_ID,0,0,0)] <- (#7@0)
#7 SCAN_DELTA src:base:t2
#8 DISTINCT <- (#9@0)
#9 EXCHANGE_SHARD params:[(SHARD,0,0,0)] <- (#10@0)
#10 MAP_HASH_ROW params:[(PROJ,0,2,0);(BRANCH_ID,0,0,0)] <- (#11@0)
#11 SCAN_DELTA src:base:t1
"#;

// #13b — set-op UNION ALL (pure Z-set addition), the second structurally distinct
// set-op form.
#[test]
fn sentinel_13b_union_all() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t1 (a BIGINT, b BIGINT, val BIGINT NOT NULL, PRIMARY KEY (a, b))",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t2 (a BIGINT, b BIGINT, val BIGINT NOT NULL, PRIMARY KEY (a, b))",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v_union_all AS SELECT val FROM t1 UNION ALL SELECT val FROM t2",
    );
    let dump = dump_for(&mut client, &sn, "13b_union_all", "v_union_all", &["t1", "t2"]);
    assert_eq!(dump, EXPECTED_13B);
}
// UNION ALL = pure Z-set addition: no distinct, the two sides differ only in
// their MAP_HASH_ROW BRANCH_ID (0 vs 1) so identical rows keep both copies.
const EXPECTED_13B: &str = r#"seg 0:
#0 INTEGRATE_SINK <- (#1@0)
#1 UNION <- (#2@0,#5@1)
#2 EXCHANGE_SHARD params:[(SHARD,0,0,0)] <- (#3@0)
#3 MAP_HASH_ROW params:[(PROJ,0,2,0);(BRANCH_ID,0,0,0)] <- (#4@0)
#4 SCAN_DELTA src:base:t1
#5 EXCHANGE_SHARD params:[(SHARD,0,0,0)] <- (#6@0)
#6 MAP_HASH_ROW params:[(PROJ,0,2,0);(BRANCH_ID,0,1,0)] <- (#7@0)
#7 SCAN_DELTA src:base:t2
"#;

// #14a — correlated scalar subquery (multi-segment: generic cut + composition).
#[test]
fn sentinel_14a_scalar_correlated() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, w BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k = a.k) AS c FROM a",
    );
    let dump = dump_for(&mut client, &sn, "14a_scalar_correlated", "v", &["a", "b"]);
    assert_eq!(dump, EXPECTED_14A);
}
// Multi-segment (generic cut + composition): seg 2 = the correlated COUNT reduce
// (grouped by the correlation key), seg 1 = the LEFT join of the outer against it
// (the scalar-decorrelation form), seg 0 = the finalize over the join output. The
// HIR path prunes the join payload to the live columns (the outer PK + the agg),
// so the join carries fewer columns than the old AST H-materialization did.
// COUNT's `coalesce_zero` finalize (0 over an empty group) is weight-pinned by the
// existing test_scalar_subquery.py — not captured here (expr excluded).
const EXPECTED_14A: &str = r#"seg 0:
#0 INTEGRATE_SINK <- (#1@0)
#1 MAP_EXPR params:[(MAP_OUT_COLS,0,9,0);(MAP_OUT_COLS,1,9,1)] <- (#2@0)
#2 SCAN_DELTA src:seg:1

seg 1:
#0 INTEGRATE_SINK <- (#1@0)
#1 UNION <- (#2@0,#7@1)
#2 NULL_EXTEND params:[(NULL_EXT,0,9,0)] <- (#3@0)
#3 POSITIVE_PART <- (#4@0)
#4 UNION <- (#5@0,#10@1)
#5 NEGATE <- (#6@0)
#6 MAP_PROJ params:[(PROJ,0,1,0)] <- (#7@0)
#7 UNION <- (#8@0,#15@1)
#8 MAP_PROJ params:[(PROJ,0,1,0);(PROJ,1,2,0)] <- (#9@0)
#9 JOIN_DELTA_TRACE <- (#10@0,#12@1)
#10 MAP_EXPR params:[(REINDEX,0,1,0);(ROUTE_KEY,0,1,0)] <- (#11@0)
#11 SCAN_DELTA src:base:a
#12 INTEGRATE_TRACE <- (#13@0)
#13 MAP_EXPR params:[(REINDEX,0,1,0);(ROUTE_KEY,0,1,0)] <- (#14@0)
#14 SCAN_DELTA src:seg:2
#15 MAP_PROJ params:[(PROJ,0,2,0);(PROJ,1,1,0)] <- (#16@0)
#16 JOIN_DELTA_TRACE <- (#13@0,#17@1)
#17 INTEGRATE_TRACE <- (#10@0)

seg 2:
#0 INTEGRATE_SINK <- (#1@0)
#1 MAP_EXPR params:[(MAP_OUT_COLS,0,9,0);(MAP_OUT_COLS,1,9,0)] <- (#2@0)
#2 REDUCE params:[(GROUP,0,1,0);(AGG_SPEC,0,1,0)] <- (#3@0)
#3 EXCHANGE_SHARD params:[(SHARD,0,1,0)] <- (#4@0)
#4 SCAN_DELTA src:base:b
"#;

// #14b — uncorrelated scalar subquery (top-level WHERE comparison arm).
#[test]
fn sentinel_14b_scalar_uncorrelated() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, w BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT a.id FROM a WHERE a.v < (SELECT MAX(w) FROM b)",
    );
    let dump = dump_for(&mut client, &sn, "14b_scalar_uncorrelated", "v", &["a", "b"]);
    assert_eq!(dump, EXPECTED_14B);
}
// Two segments: seg 1 = the uncorrelated global MAX reduce (one-row output),
// seg 0 = the pure-range INNER join of the outer against that value (the aggregate
// value is the join key) — the bare `a.id` projection fuses into the join's own
// output, so the HIR path needs no separate finalize segment. `#12 FILTER` is the
// null gate on that key: the global MAX renders NULL over an empty `b`, and
// without the gate `map_reindex` would OPK-encode it as the real key `0`. The
// `< NULL ⇒ UNKNOWN` semantics are weight-pinned by test_scalar_subquery.py.
const EXPECTED_14B: &str = r#"seg 0:
#0 INTEGRATE_SINK <- (#1@0)
#1 EXCHANGE_SHARD params:[(SHARD,0,0,0);(SHARD,1,1,0)] <- (#2@0)
#2 MAP_PROJ params:[(PROJ,0,3,0)] <- (#3@0)
#3 MAP_EXPR params:[(REINDEX,0,1,0);(REINDEX,1,4,0);(ROUTE_KEY,0,0,0)] <- (#4@0)
#4 UNION <- (#5@0,#14@1)
#5 MAP_PROJ params:[(PROJ,0,1,0);(PROJ,1,2,0);(PROJ,2,3,0);(PROJ,3,4,0);(PROJ,4,5,0)] <- (#6@0)
#6 JOIN_DELTA_TRACE_RANGE params:[(RANGE_JOIN,0,0,2)] <- (#7@0,#9@1)
#7 MAP_EXPR params:[(REINDEX,0,2,0);(ROUTE_KEY,0,1,0)] <- (#8@0)
#8 SCAN_DELTA src:base:a
#9 INTEGRATE_TRACE <- (#10@0)
#10 WORKER_FILTER <- (#11@0)
#11 MAP_EXPR params:[(REINDEX,0,1,0);(ROUTE_KEY,0,1,0)] <- (#12@0)
#12 FILTER <- (#13@0)
#13 SCAN_DELTA src:seg:1
#14 MAP_PROJ params:[(PROJ,0,3,0);(PROJ,1,4,0);(PROJ,2,5,0);(PROJ,3,1,0);(PROJ,4,2,0)] <- (#15@0)
#15 JOIN_DELTA_TRACE_RANGE params:[(RANGE_JOIN,0,0,0)] <- (#11@0,#16@1)
#16 INTEGRATE_TRACE <- (#17@0)
#17 WORKER_FILTER <- (#7@0)

seg 1:
#0 INTEGRATE_SINK <- (#1@0)
#1 MAP_EXPR params:[(MAP_OUT_COLS,0,9,1)] <- (#2@0)
#2 REDUCE params:[(AGG_SPEC,0,4,2);(AGG_SPEC,1,1,0);(GLOBAL_GROUND,0,1,0)] <- (#3@0)
#3 EXCHANGE_SHARD <- (#4@0)
#4 SCAN_DELTA src:base:b
"#;

// #15 — join-over-grouped-CTE (multi-segment: cross-segment wiring).
#[test]
fn sentinel_15_join_over_grouped_cte() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, cid BIGINT NOT NULL, amt BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS WITH agg AS (SELECT cid, SUM(amt) AS total FROM orders GROUP BY cid) \
         SELECT c.name AS nm, agg.total AS tot FROM agg JOIN customers c ON agg.cid = c.id",
    );
    let dump = dump_for(
        &mut client,
        &sn,
        "15_join_over_grouped_cte",
        "v",
        &["orders", "customers"],
    );
    assert_eq!(dump, EXPECTED_15);
}
// Multi-segment (cross-segment wiring): seg 1 = the SUM-per-group reduce over
// `orders`, seg 0 = the equi join of that hidden segment (`src:seg:1`) against
// `customers`. Confirms the cut rule + the join reading a segment trace.
const EXPECTED_15: &str = r#"seg 0:
#0 INTEGRATE_SINK <- (#1@0)
#1 MAP_PROJ params:[(PROJ,0,2,0);(PROJ,1,1,0)] <- (#2@0)
#2 UNION <- (#3@0,#10@1)
#3 MAP_PROJ params:[(PROJ,0,1,0);(PROJ,1,2,0)] <- (#4@0)
#4 JOIN_DELTA_TRACE <- (#5@0,#7@1)
#5 MAP_EXPR params:[(REINDEX,0,1,0);(ROUTE_KEY,0,1,0)] <- (#6@0)
#6 SCAN_DELTA src:seg:1
#7 INTEGRATE_TRACE <- (#8@0)
#8 MAP_EXPR params:[(REINDEX,0,0,0);(ROUTE_KEY,0,1,0)] <- (#9@0)
#9 SCAN_DELTA src:base:customers
#10 MAP_PROJ params:[(PROJ,0,2,0);(PROJ,1,1,0)] <- (#11@0)
#11 JOIN_DELTA_TRACE <- (#8@0,#12@1)
#12 INTEGRATE_TRACE <- (#5@0)

seg 1:
#0 INTEGRATE_SINK <- (#1@0)
#1 MAP_EXPR params:[(MAP_OUT_COLS,0,9,0);(MAP_OUT_COLS,1,9,0)] <- (#2@0)
#2 REDUCE params:[(GROUP,0,1,0);(AGG_SPEC,0,2,2);(AGG_SPEC,1,1,0)] <- (#3@0)
#3 EXCHANGE_SHARD params:[(SHARD,0,1,0)] <- (#4@0)
#4 SCAN_DELTA src:base:orders
"#;
