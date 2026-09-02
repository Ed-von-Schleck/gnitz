use super::*;
use crate::exec::batch::{project, resolve_projection};
use crate::test_support::{col_def, parse_query};
use gnitz_core::null_word_get;
use sqlparser::ast::{SelectItem, SetExpr};

// The per-type window comparison itself (`cmp_typed_le`) is pinned by
// gnitz-wire's own tests; the tests here cover the sink built on it.

// ---- paginate (multiplicity walk) ----

#[test]
fn paginate_all_weight_one_is_entry_counting() {
    let w = vec![1i64; 5];
    // LIMIT 3.
    assert_eq!(paginate(&w, 0, 3), vec![(0, 1), (1, 1), (2, 1)]);
    // OFFSET 2 LIMIT 2.
    assert_eq!(paginate(&w, 2, 4), vec![(2, 1), (3, 1)]);
}

#[test]
fn paginate_boundary_entry_keeps_reduced_weight() {
    // Entries [2,3,2]; LIMIT 3 lands inside entry 1 (weight 3 → 1 survives).
    let w = vec![2i64, 3, 2];
    assert_eq!(paginate(&w, 0, 3), vec![(0, 2), (1, 1)]);
    // The surviving logical-row count is exactly 3 (bag semantics).
    assert_eq!(paginate(&w, 0, 3).iter().map(|(_, x)| *x).sum::<i64>(), 3);
}

#[test]
fn paginate_offset_and_limit_inside_one_entry() {
    // Weight-5 entry, OFFSET 1 LIMIT 1 → the overlap formula yields weight 1
    // (a two-independent-passes impl gets this wrong).
    let w = vec![5i64];
    assert_eq!(paginate(&w, 1, 2), vec![(0, 1)]);
}

#[test]
fn paginate_weight3_straddles_cut() {
    // A weight-3 entry straddling the window keeps only the overlapping part.
    let w = vec![3i64, 3, 3];
    // window [0,4): entry0 whole (3), entry1 clipped to 1.
    assert_eq!(paginate(&w, 0, 4), vec![(0, 3), (1, 1)]);
    // window [4,7): entry1 clipped low (2), entry2 clipped high (1).
    assert_eq!(paginate(&w, 4, 7), vec![(1, 2), (2, 1)]);
}

// ---- sink integration ----

/// `(id U64 pk, v I64 nullable, s STRING nullable)`.
fn kv_schema() -> Schema {
    Schema {
        columns: vec![
            col_def("id", TypeCode::U64, false),
            col_def("v", TypeCode::I64, true),
            col_def("s", TypeCode::String, true),
        ],
        pk_cols: vec![0],
    }
}

/// Push one row (`pk`, optional `v`, optional `s`, weight) into a `kv_schema` batch.
fn push_kv(b: &mut ZSetBatch, pk: u64, v: Option<i64>, s: Option<&str>, weight: i64) {
    b.pks.push_u128(pk as u128);
    b.weights.push(weight);
    let mut null_word = 0u64;
    if v.is_none() {
        null_word |= 1 << 0; // payload idx 0 = v
    }
    if s.is_none() {
        null_word |= 1 << 1; // payload idx 1 = s
    }
    b.nulls.push(null_word);
    if let ColData::Fixed(buf) = &mut b.columns[1] {
        buf.extend_from_slice(&v.unwrap_or(0).to_le_bytes());
    }
    if let ColData::Strings(vs) = &mut b.columns[2] {
        vs.push(Some(s.unwrap_or("").to_string()));
    }
}

/// The two halves the fold tail runs, composed: resolve the ORDER BY over the
/// result's own schema, then sort and window by those keys.
fn passthrough(
    schema: Schema,
    batch: ZSetBatch,
    order_by: Option<&OrderBy>,
    offset: usize,
    limit: Option<usize>,
) -> Result<(Schema, ZSetBatch), GnitzSqlError> {
    let keys = resolve_out_schema_order(order_by, &schema)?;
    Ok(read_spec_finish(schema, batch, &keys, offset, limit))
}

/// Test-only sort → window → project: the production sinks window an
/// already-projected result or a ScanSpec reply, both through
/// [`read_spec_finish`]; this wrapper windows over the full source schema and
/// projects afterwards so the tests can order by non-projected columns.
fn order_limit_project(
    projection: &[SelectItem],
    actual_schema: &Schema,
    full_batch: Option<ZSetBatch>,
    order_by: Option<&OrderBy>,
    offset: usize,
    limit: Option<usize>,
) -> Result<(Schema, ZSetBatch), GnitzSqlError> {
    let resolved = resolve_projection(projection, actual_schema)?;
    let full = full_batch.unwrap_or_else(|| ZSetBatch::new(actual_schema));
    let (_, windowed) = passthrough(actual_schema.clone(), full, order_by, offset, limit)?;
    Ok(project(resolved, actual_schema, Some(windowed)))
}

/// Run the sink for `sql` over `batch`, returning the ordered `(id, v)` pairs.
fn run(sql: &str, schema: &Schema, batch: ZSetBatch) -> Result<Vec<(u64, Option<i64>)>, GnitzSqlError> {
    let q = parse_query(sql);
    let select = match q.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => panic!("not a select"),
    };
    // The test SQL only ever uses `LIMIT <n>`; the deeper `Expr::Value` is
    // folded into the outer pattern so no re-match of the same binding.
    let limit = match &q.limit_clause {
        Some(sqlparser::ast::LimitClause::LimitOffset {
            limit: Some(Expr::Value(vws)),
            ..
        }) => match &vws.value {
            sqlparser::ast::Value::Number(n, _) => n.parse::<usize>().ok(),
            _ => None,
        },
        _ => None,
    };
    let (out_schema, out) =
        order_limit_project(&select.projection, schema, Some(batch), q.order_by.as_ref(), 0, limit)?;
    let id_ci = out_schema.pk_cols[0];
    let v_ci = out_schema.columns.iter().position(|c| c.name == "v");
    let mut rows = Vec::new();
    for i in 0..out.len() {
        let id = out.pks.get(i) as u64;
        let v = v_ci.and_then(|ci| {
            if null_word_get(out.nulls[i], out_schema.payload_idx(ci)) {
                None
            } else if let ColData::Fixed(buf) = &out.columns[ci] {
                Some(i64::from_le_bytes(buf[i * 8..i * 8 + 8].try_into().unwrap()))
            } else {
                None
            }
        });
        let _ = id_ci;
        rows.push((id, v));
    }
    Ok(rows)
}

#[test]
fn sink_order_by_asc_desc_and_source_col() {
    let schema = kv_schema();
    let mut b = ZSetBatch::new(&schema);
    push_kv(&mut b, 1, Some(30), Some("a"), 1);
    push_kv(&mut b, 2, Some(10), Some("b"), 1);
    push_kv(&mut b, 3, Some(20), Some("c"), 1);

    // ORDER BY a non-projected source column (`v`), ascending.
    let got = run("SELECT id FROM t ORDER BY v", &schema, b.clone()).unwrap();
    assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![2, 3, 1]);
    // DESC reverses.
    let got = run("SELECT id FROM t ORDER BY v DESC", &schema, b).unwrap();
    assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1, 3, 2]);
}

#[test]
fn sink_order_by_alias_and_positional() {
    // The passthrough sink sorts an already-projected result, so a SELECT
    // alias is simply the result schema's column name — pin that both a
    // name and a 1-based position resolve against it.
    let schema = Schema {
        columns: vec![col_def("id", TypeCode::U64, false), col_def("val", TypeCode::I64, true)],
        pk_cols: vec![0],
    };
    let mut b = ZSetBatch::new(&schema);
    let mut push = |pk: u64, v: i64| {
        b.pks.push_u128(pk as u128);
        b.weights.push(1);
        b.nulls.push(0);
        if let ColData::Fixed(buf) = &mut b.columns[1] {
            buf.extend_from_slice(&v.to_le_bytes());
        }
    };
    push(1, 30);
    push(2, 10);
    push(3, 20);
    let ids = |out: &ZSetBatch| (0..out.len()).map(|i| out.pks.get(i) as u64).collect::<Vec<_>>();

    let q = parse_query("SELECT * FROM t ORDER BY val");
    let (_, out) = passthrough(schema.clone(), b.clone(), q.order_by.as_ref(), 0, None).unwrap();
    assert_eq!(ids(&out), vec![2, 3, 1]);
    // Positional: the 2nd visible column is `val`.
    let q = parse_query("SELECT * FROM t ORDER BY 2 DESC");
    let (_, out) = passthrough(schema, b, q.order_by.as_ref(), 0, None).unwrap();
    assert_eq!(ids(&out), vec![1, 3, 2]);
}

#[test]
fn sink_nulls_default_and_explicit_absolute() {
    let schema = kv_schema();
    let mut b = ZSetBatch::new(&schema);
    push_kv(&mut b, 1, Some(10), None, 1);
    push_kv(&mut b, 2, None, None, 1);
    push_kv(&mut b, 3, Some(20), None, 1);

    // ASC → NULLS LAST by default.
    let got = run("SELECT id, v FROM t ORDER BY v", &schema, b.clone()).unwrap();
    assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1, 3, 2]);
    // DESC → NULLS FIRST by default (placement is absolute, not value-flipped).
    let got = run("SELECT id, v FROM t ORDER BY v DESC", &schema, b.clone()).unwrap();
    assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![2, 3, 1]);
    // Explicit NULLS FIRST on ASC overrides the default.
    let got = run("SELECT id, v FROM t ORDER BY v ASC NULLS FIRST", &schema, b).unwrap();
    assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![2, 1, 3]);
}

#[test]
fn sink_string_order_and_multikey() {
    let schema = kv_schema();
    let mut b = ZSetBatch::new(&schema);
    push_kv(&mut b, 1, Some(1), Some("banana"), 1);
    push_kv(&mut b, 2, Some(1), Some("apple"), 1);
    push_kv(&mut b, 3, Some(2), Some("apple"), 1);

    // ORDER BY v, s: (1,apple)=2, (1,banana)=1, (2,apple)=3.
    let got = run("SELECT id, v, s FROM t ORDER BY v, s", &schema, b).unwrap();
    assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![2, 1, 3]);
}

#[test]
fn sink_limit_counts_multiplicity() {
    let schema = kv_schema();
    let mut b = ZSetBatch::new(&schema);
    // A weight-3 entry sorts first, then a weight-1 entry.
    push_kv(&mut b, 1, Some(10), Some("x"), 3);
    push_kv(&mut b, 2, Some(20), Some("y"), 1);

    // LIMIT 2 must clip the weight-3 entry to weight 2 (2 logical rows), not
    // return 2 entries (= 4 logical rows).
    let q = parse_query("SELECT id, v FROM t ORDER BY v LIMIT 2");
    let select = match q.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => unreachable!(),
    };
    let (out_schema, out) =
        order_limit_project(&select.projection, &schema, Some(b), q.order_by.as_ref(), 0, Some(2)).unwrap();
    assert_eq!(out.len(), 1, "one entry survives");
    assert_eq!(out.weights[0], 2, "clipped to weight 2");
    let _ = out_schema;
}

/// Expand a windowed `kv_schema` batch into its logical rows — each entry
/// repeated by its weight. The bag is the only thing a cut is a function of;
/// *which* of several entries sharing an identity survives is not.
fn expand_kv(schema: &Schema, out: &ZSetBatch) -> Vec<(u64, Option<i64>)> {
    let v_ci = schema.columns.iter().position(|c| c.name == "v").unwrap();
    let mut rows = Vec::new();
    for i in 0..out.len() {
        let v = if null_word_get(out.nulls[i], schema.payload_idx(v_ci)) {
            None
        } else if let ColData::Fixed(buf) = &out.columns[v_ci] {
            Some(i64::from_le_bytes(buf[i * 8..i * 8 + 8].try_into().unwrap()))
        } else {
            None
        };
        for _ in 0..out.weights[i] {
            rows.push((out.pks.get(i) as u64, v));
        }
    }
    rows
}

#[test]
fn cut_splitting_a_tie_group_matches_the_consolidated_input() {
    // Three entries share one identity (weights 1, 1, 3) plus one larger row:
    // n = 4, k = 2, so the partial selection keeps two of the three tied
    // entries and splits the tie group. The result must be the same bag the
    // consolidated batch (one weight-5 entry) yields — duplicate
    // (PK, payload) is not a precondition the cut may assume.
    let schema = kv_schema();
    let q = parse_query("SELECT id, v FROM t ORDER BY v LIMIT 2");

    let mut dup = ZSetBatch::new(&schema);
    for w in [1i64, 1, 3] {
        push_kv(&mut dup, 1, Some(10), None, w);
    }
    push_kv(&mut dup, 2, Some(20), None, 1);

    let mut folded = ZSetBatch::new(&schema);
    push_kv(&mut folded, 1, Some(10), None, 5);
    push_kv(&mut folded, 2, Some(20), None, 1);

    let (s_dup, out_dup) = passthrough(schema.clone(), dup, q.order_by.as_ref(), 0, Some(2)).unwrap();
    let (s_folded, out_folded) = passthrough(schema, folded, q.order_by.as_ref(), 0, Some(2)).unwrap();

    assert_eq!(expand_kv(&s_dup, &out_dup), vec![(1, Some(10)); 2]);
    assert_eq!(expand_kv(&s_dup, &out_dup), expand_kv(&s_folded, &out_folded));
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "non-positive weight violates the bag invariant")]
fn cut_names_a_ghost_it_would_discard_unseen() {
    // n = 3, k = 2 — the ghost sorts last, so the truncation drops it and the
    // multiplicity walk never reaches it. Only the whole-batch check sees it.
    let schema = kv_schema();
    let q = parse_query("SELECT id, v FROM t ORDER BY v LIMIT 2");
    let mut b = ZSetBatch::new(&schema);
    push_kv(&mut b, 1, Some(10), None, 1);
    push_kv(&mut b, 2, Some(20), None, 1);
    push_kv(&mut b, 3, Some(30), None, 0);
    let _ = passthrough(schema, b, q.order_by.as_ref(), 0, Some(2));
}

#[test]
fn sink_positional_over_hidden_view_schema() {
    // A view result carries a hidden key at physical index 0; position 1 must
    // name the first VISIBLE column, not the hidden key.
    let schema = Schema {
        columns: vec![
            col_def("_group_pk", TypeCode::U128, false).hidden(),
            col_def("city", TypeCode::U64, false),
            col_def("cnt", TypeCode::I64, false),
        ],
        pk_cols: vec![0],
    };
    let mut b = ZSetBatch::new(&schema);
    let mut push = |pk: u128, city: u64, cnt: i64| {
        b.pks.push_u128(pk);
        b.weights.push(1);
        b.nulls.push(0);
        if let ColData::Fixed(buf) = &mut b.columns[1] {
            buf.extend_from_slice(&city.to_le_bytes());
        }
        if let ColData::Fixed(buf) = &mut b.columns[2] {
            buf.extend_from_slice(&cnt.to_le_bytes());
        }
    };
    push(100, 30, 1);
    push(200, 10, 1);
    push(300, 20, 1);

    // ORDER BY 1 → the first visible column `city`, ascending.
    let q = parse_query("SELECT * FROM v ORDER BY 1");
    let select = match q.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => unreachable!(),
    };
    let (out_schema, out) =
        order_limit_project(&select.projection, &schema, Some(b), q.order_by.as_ref(), 0, None).unwrap();
    // Read `city` (physical col 1) in output order.
    let city_ci = out_schema.columns.iter().position(|c| c.name == "city").unwrap();
    let cities: Vec<u64> = (0..out.len())
        .map(|i| {
            let ColData::Fixed(buf) = &out.columns[city_ci] else {
                unreachable!()
            };
            u64::from_le_bytes(buf[i * 8..i * 8 + 8].try_into().unwrap())
        })
        .collect();
    assert_eq!(cities, vec![10, 20, 30]);
}

#[test]
fn sink_null_detected_via_bitmap_in_u128_column() {
    // A NULL U128 payload is zero-filled filler; NULL must be read from the
    // bitmap, not an is_none() on the value.
    let schema = Schema {
        columns: vec![col_def("id", TypeCode::U64, false), col_def("u", TypeCode::U128, true)],
        pk_cols: vec![0],
    };
    let mut b = ZSetBatch::new(&schema);
    let mut push = |pk: u128, u: Option<u128>| {
        b.pks.push_u128(pk);
        b.weights.push(1);
        b.nulls.push(if u.is_none() { 1 } else { 0 });
        if let ColData::Fixed(v) = &mut b.columns[1] {
            v.extend_from_slice(&u.unwrap_or(0).to_le_bytes());
        }
    };
    push(1, Some(5));
    push(2, None);
    push(3, Some(1));

    // ASC NULLS LAST default: 1(=1), 5, NULL → ids 3, 1, 2.
    let q = parse_query("SELECT id, u FROM t ORDER BY u");
    let select = match q.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => unreachable!(),
    };
    let (out_schema, out) =
        order_limit_project(&select.projection, &schema, Some(b), q.order_by.as_ref(), 0, None).unwrap();
    let ids: Vec<u64> = (0..out.len()).map(|i| out.pks.get(i) as u64).collect();
    assert_eq!(ids, vec![3, 1, 2]);
    let _ = out_schema;
}

#[test]
fn sink_compound_pk_key_orders_by_typed_value() {
    // Compound PK (a U16, b I16); sort by the SECOND PK column `b` which holds
    // a negative — pins that the Bytes accessor + typed compare handle a
    // multi-byte LE value (256 vs 1) and a non-leading signed column.
    let schema = Schema {
        columns: vec![
            col_def("a", TypeCode::U16, false),
            col_def("b", TypeCode::I16, false),
            col_def("w", TypeCode::I64, true),
        ],
        pk_cols: vec![0, 1],
    };
    let mut b = ZSetBatch::new(&schema);
    let mut push = |a: u16, bb: i16| {
        let mut pk = [0u8; 4];
        pk[..2].copy_from_slice(&a.to_le_bytes());
        pk[2..].copy_from_slice(&bb.to_le_bytes());
        b.pks.push_bytes(&pk);
        b.weights.push(1);
        b.nulls.push(1); // w NULL (payload idx 0)
        if let ColData::Fixed(buf) = &mut b.columns[2] {
            buf.extend_from_slice(&0i64.to_le_bytes());
        }
    };
    // rows: b ∈ {1, -5, 256}; ascending signed order is -5, 1, 256.
    push(1, 1);
    push(2, -5);
    push(3, 256);

    let q = parse_query("SELECT a, b FROM t ORDER BY b");
    let select = match q.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => unreachable!(),
    };
    let (out_schema, out) =
        order_limit_project(&select.projection, &schema, Some(b), q.order_by.as_ref(), 0, None).unwrap();
    // Read `b` from the compound PK region (physical col 1), addressed the
    // way the comparator addresses it rather than by re-deriving the offset.
    let key = SortKey::new(&out_schema, 1, true, false);
    let bs: Vec<i16> = (0..out.len())
        .map(|i| {
            let w = out.pks.col_window(i, key.pk_offset.unwrap(), key.stride);
            i16::from_le_bytes(w.try_into().unwrap())
        })
        .collect();
    assert_eq!(bs, vec![-5, 1, 256]);
}

#[test]
fn sink_zero_copy_passthrough() {
    // No ORDER BY, no OFFSET, no LIMIT → the source batch flows straight
    // through (identity projection), unchanged.
    let schema = kv_schema();
    let mut b = ZSetBatch::new(&schema);
    push_kv(&mut b, 3, Some(30), Some("c"), 1);
    push_kv(&mut b, 1, Some(10), Some("a"), 1);
    let clone = b.clone();
    let q = parse_query("SELECT * FROM t");
    let select = match q.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => unreachable!(),
    };
    let (_, out) = order_limit_project(&select.projection, &schema, Some(b), q.order_by.as_ref(), 0, None).unwrap();
    assert_eq!(out, clone, "passthrough must not touch the batch (order preserved)");
}

#[test]
fn sink_rejects_unsupported_order_by_forms() {
    let schema = kv_schema();
    let mk = |sql: &str| {
        let q = parse_query(sql);
        let select = match q.body.as_ref() {
            SetExpr::Select(s) => s.clone(),
            _ => unreachable!(),
        };
        order_limit_project(
            &select.projection,
            &schema,
            Some(ZSetBatch::new(&schema)),
            q.order_by.as_ref(),
            0,
            None,
        )
        .map(|_| ())
    };
    // A non-column-ref expression key → Unsupported.
    assert!(matches!(
        mk("SELECT id, v FROM t ORDER BY v + 1"),
        Err(GnitzSqlError::Unsupported(_))
    ));
    // Positional out of range → error (position 9 > 2 output cols; position 0).
    assert!(mk("SELECT id, v FROM t ORDER BY 9").is_err());
    assert!(mk("SELECT id, v FROM t ORDER BY 0").is_err());
}

#[test]
fn resolve_order_by_rejects_clickhouse_duckdb_extensions() {
    // `ORDER BY ALL` / `WITH FILL` / `INTERPOLATE` only reach `resolve_order_by`
    // as their typed AST forms under a dialect that parses them (GenericDialect
    // parses `ALL` as an identifier), so drive the arms directly.
    use sqlparser::ast::{Ident, Interpolate, OrderByExpr, OrderByOptions, WithFill};
    let ident_key = || OrderByExpr {
        expr: Expr::Identifier(Ident::new("v")),
        options: OrderByOptions::default(),
        with_fill: None,
    };

    let all = OrderBy {
        kind: OrderByKind::All(OrderByOptions::default()),
        interpolate: None,
    };
    assert!(matches!(resolve_order_by(&all), Err(GnitzSqlError::Unsupported(_))));

    let interpolate = OrderBy {
        kind: OrderByKind::Expressions(vec![ident_key()]),
        interpolate: Some(Interpolate { exprs: None }),
    };
    assert!(matches!(
        resolve_order_by(&interpolate),
        Err(GnitzSqlError::Unsupported(_))
    ));

    let with_fill = OrderBy {
        kind: OrderByKind::Expressions(vec![OrderByExpr {
            expr: Expr::Identifier(Ident::new("v")),
            options: OrderByOptions::default(),
            with_fill: Some(WithFill {
                from: None,
                to: None,
                step: None,
            }),
        }]),
        interpolate: None,
    };
    assert!(matches!(
        resolve_order_by(&with_fill),
        Err(GnitzSqlError::Unsupported(_))
    ));
}

/// The worker's ORDER BY comparator and this sink's are two *decodes* of one
/// order — OPK through [`ColumnLocator`] there, native LE through `SortKey`
/// here — and a disagreement drops a row the final LIMIT window wanted, since
/// the worker's top-k discards before the client sees it. Its own function lives
/// in gnitz-store, which this crate does not link, so what runs here is the
/// per-key rule it is built out of.
#[test]
fn both_order_by_comparators_agree_on_every_pair() {
    use gnitz_expr::RowSource;

    /// One key of the worker-side comparator, over a locator and a view.
    fn engine_cmp_key(
        view: &gnitz_core::ZSetBatchView<'_>,
        loc: &ColumnLocator,
        desc: bool,
        nulls_first: bool,
        ra: usize,
        rb: usize,
    ) -> Ordering {
        match (
            loc.is_null_word(view.get_null_word(ra)),
            loc.is_null_word(view.get_null_word(rb)),
        ) {
            (true, true) => return Ordering::Equal,
            // NULL placement is absolute: `nulls_first` decides it, `desc` does
            // not flip it.
            (true, false) => return if nulls_first { Ordering::Less } else { Ordering::Greater },
            (false, true) => return if nulls_first { Ordering::Greater } else { Ordering::Less },
            (false, false) => {}
        }
        let ord = loc.cmp_non_null(view, ra, view, rb);
        if desc {
            ord.reverse()
        } else {
            ord
        }
    }

    // One column of each shape the two decodes can differ on: a U64 PK, a signed
    // narrow PK column, a U64 payload past 2^63 (which a bit-reinterpreting
    // decode sorts first), a nullable signed payload, and a string.
    let schema = Schema {
        columns: vec![
            col_def("pk_u", TypeCode::U64, false),
            col_def("pk_i", TypeCode::I16, false),
            col_def("u", TypeCode::U64, true),
            col_def("i", TypeCode::I32, true),
            col_def("s", TypeCode::String, true),
        ],
        pk_cols: vec![0, 1],
    };
    // Repeats, ties, both NULL polarities, and values straddling every sign
    // boundary the two decodes would disagree on if either were wrong.
    type Row = (u64, i16, Option<u64>, Option<i32>, Option<&'static str>);
    let rows: &[Row] = &[
        (0, 0, Some(0), Some(0), Some("")),
        (1, -1, Some(u64::MAX), Some(-1), Some("a")),
        (1, 1, Some(1 << 63), Some(i32::MIN), Some("ab")),
        (u64::MAX, i16::MIN, None, Some(i32::MAX), None),
        (
            7,
            i16::MAX,
            Some(9),
            None,
            Some("a longer value past the inline boundary"),
        ),
        (
            7,
            i16::MAX,
            Some(9),
            None,
            Some("a longer value past the inline boundary"),
        ),
        (2, 3, Some(u64::MAX - 1), Some(5), Some("A")),
    ];
    let mut batch = ZSetBatch::new(&schema);
    for &(pk_u, pk_i, u, i, s) in rows {
        let mut pk = [0u8; 10];
        pk[..8].copy_from_slice(&pk_u.to_le_bytes());
        pk[8..].copy_from_slice(&pk_i.to_le_bytes());
        batch.pks.push_bytes(&pk);
        batch.weights.push(1);
        let mut null_word = 0u64;
        for (slot, is_null) in [u.is_none(), i.is_none(), s.is_none()].into_iter().enumerate() {
            if is_null {
                null_word |= 1 << slot;
            }
        }
        batch.nulls.push(null_word);
        if let ColData::Fixed(buf) = &mut batch.columns[2] {
            buf.extend_from_slice(&u.unwrap_or(0).to_le_bytes());
        }
        if let ColData::Fixed(buf) = &mut batch.columns[3] {
            buf.extend_from_slice(&i.unwrap_or(0).to_le_bytes());
        }
        if let ColData::Strings(v) = &mut batch.columns[4] {
            v.push(s.map(str::to_string));
        }
    }
    batch.validate(&schema).expect("the fixture batch must be well-formed");

    let mut bufs = gnitz_core::ViewBuffers::default();
    let view = bufs.view(&batch, &schema);
    let n = rows.len();
    for ci in 0..schema.columns.len() {
        let loc = SchemaFacts::locate(&schema, ci);
        for asc in [true, false] {
            for nulls_first in [true, false] {
                let key = SortKey::new(&schema, ci, asc, nulls_first);
                for ra in 0..n {
                    for rb in 0..n {
                        assert_eq!(
                            cmp_key(&batch, &key, ra, rb),
                            engine_cmp_key(&view, &loc, !asc, nulls_first, ra, rb),
                            "column {ci}, asc={asc}, nulls_first={nulls_first}, rows {ra}/{rb}",
                        );
                    }
                }
            }
        }
    }
}
