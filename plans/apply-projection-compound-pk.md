# apply_projection: compound-PK source

## Symptom

`SELECT a, b, payload FROM t` on a compound-PK table (`PRIMARY KEY (a, b)`)
returns a result batch whose payload columns are empty buffers. Iterating
the rows from Python panics with `range end index 8 out of range for slice
of length 0` reading `columns[ci]`.

`SELECT *` works (the wildcard branch returns the source batch unchanged).

## Root cause

`crates/gnitz-sql/src/dml.rs:1137-1138`:

```rust
let new_pk_idx = col_indices.iter().position(|&i| schema.is_pk_col(i)).unwrap_or(0);
let new_schema = Schema { columns: new_cols, pk_cols: vec![new_pk_idx] };
```

`apply_projection` rebuilds the projected schema with a single-PK
`pk_cols: vec![new_pk_idx]`. Three latent failures fall out of that
assumption:

1. **Source PK columns silently dropped.** The copy loop at line 1147
   skips every column that was PK in the *source* (`if schema.is_pk_col(old_ci) { continue; }`).
   Only the first projected source-PK survives as new PK; the rest sit as
   empty `Fixed` payload buffers.

2. **`new_batch.pks = pks` is unconditional.** For a compound source the
   `pks` variant is `PkColumn::Bytes { stride, buf }`; for a single-PK
   destination `ZSetBatch::new(&new_schema)` allocates `U64s`/`U128s`. The
   assignment overwrites the variant. Downstream `PkColumn::get()` /
   `to_vec_u128()` panic on `Bytes`.

3. **`new_batch.nulls = nulls` is unconditional.** Null bits are indexed
   by `payload_idx`, which shifts whenever payload columns are reordered
   or dropped — the wire decoder reads garbage null bits.

4. **`unwrap_or(0)` fallback corrupts further.** If the projection drops
   *all* PK columns (`SELECT non_pk FROM t`), `new_pk_idx = 0` promotes a
   payload column to PK. The planner forbids String/Blob in a PK
   (`planner.rs:245`), so `PkColumn::empty_for_schema` and the wire
   encoder can hit unreachable variants.

## Fix

Replace `apply_projection`'s body after the wildcard branch and
`col_indices` dedup with the routine below. Invariants used:

- All projected source-PK columns become new PK columns (in projection
  order). No source-PK→new-payload case exists.
- PK columns are strictly numeric/UUID (`planner.rs:239-251`); they map
  to `ColData::Fixed` or `ColData::U128s`, never `Strings`/`Bytes`.
- For a compound source (`schema.pk_count() >= 2`), the source `pks`
  variant is `PkColumn::Bytes`. For a single source PK it is
  `U64s`/`U128s`. A single-PK source therefore never reaches the
  non-preserved rebuild branch: its lone PK must be projected (the
  0-PK guard rejects the alternative), so `pk_preserved` is true.
- The null bitmap is one `u64` per row (`MAX_COLUMNS = 65` in
  `gnitz-wire/src/lib.rs`), so `new_pi ≤ 63` and `1u64 << new_pi`
  never overflows.

```rust
    // 1. Build new PK column set; every projected source-PK becomes a new PK.
    let new_pk_cols: Vec<usize> = col_indices.iter().enumerate()
        .filter(|(_, &old_ci)| schema.is_pk_col(old_ci))
        .map(|(new_ci, _)| new_ci)
        .collect();

    if new_pk_cols.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "SELECT must project at least one PRIMARY KEY column".to_string()
        ));
    }

    let new_cols: Vec<gnitz_core::ColumnDef> = col_indices.iter()
        .map(|&i| schema.columns[i].clone())
        .collect();
    let new_schema = Schema { columns: new_cols, pk_cols: new_pk_cols };

    let src_batch = batch.unwrap_or_else(|| ZSetBatch::new(schema));

    // 2. Identity fast-path: every source column projected in source order.
    let is_identity = col_indices.len() == schema.columns.len()
        && col_indices.iter().enumerate().all(|(i, &ci)| ci == i);
    if is_identity {
        return Ok((new_schema, src_batch));
    }

    let row_count = src_batch.len();
    let ZSetBatch { pks: src_pks, weights, nulls: src_nulls, columns: mut src_columns } = src_batch;
    let mut new_batch = ZSetBatch::new(&new_schema);
    new_batch.weights = weights;

    // 3. PK region: move when layout is byte-identical; else rebuild from
    //    the packed source bytes. `pk_preserved` covers compound→compound
    //    when columns and order match; the single-PK source can only land
    //    here (see invariants above).
    let pk_preserved = new_schema.pk_cols.len() == schema.pk_cols.len()
        && new_schema.pk_cols.iter().enumerate()
            .all(|(i, &new_pk_ci)| col_indices[new_pk_ci] == schema.pk_cols[i]);

    if pk_preserved {
        new_batch.pks = src_pks;
    } else {
        let src_bytes = match &src_pks {
            PkColumn::Bytes { buf, .. } => buf.as_slice(),
            _ => unreachable!("non-preserved PK rebuild requires compound source"),
        };
        let src_stride = schema.pk_stride();

        // Per-PK (col_off, stride) is invariant across rows — hoist.
        let pk_mappings: Vec<(usize, usize)> = new_schema.pk_cols.iter()
            .map(|&new_pk_ci| {
                let old_ci = col_indices[new_pk_ci];
                (schema.pk_byte_offset(old_ci), schema.columns[old_ci].type_code.wire_stride())
            })
            .collect();

        match &mut new_batch.pks {
            PkColumn::Bytes { buf, .. } => {
                buf.reserve(row_count * new_schema.pk_stride());
                for i in 0..row_count {
                    let row_off = i * src_stride;
                    for &(col_off, stride) in &pk_mappings {
                        buf.extend_from_slice(&src_bytes[row_off + col_off..row_off + col_off + stride]);
                    }
                }
            }
            PkColumn::U64s(v) => {
                // Destination is single-PK → exactly one mapping.
                let (col_off, stride) = pk_mappings[0];
                v.reserve(row_count);
                for i in 0..row_count {
                    let row_off = i * src_stride;
                    let mut b = [0u8; 8];
                    b[..stride].copy_from_slice(&src_bytes[row_off + col_off..row_off + col_off + stride]);
                    v.push(u64::from_le_bytes(b));
                }
            }
            PkColumn::U128s(v) => {
                let (col_off, stride) = pk_mappings[0];
                v.reserve(row_count);
                for i in 0..row_count {
                    let row_off = i * src_stride;
                    let mut b = [0u8; 16];
                    b[..stride].copy_from_slice(&src_bytes[row_off + col_off..row_off + col_off + stride]);
                    v.push(u128::from_le_bytes(b));
                }
            }
        }
    }

    // 4. Null bitmap: move when payload layout matches the source's;
    //    else rebuild bit-by-bit using a hoisted (new_pi, old_pi) mapping.
    let payload_preserved = new_schema.num_payload_cols() == schema.num_payload_cols()
        && new_schema.payload_columns().zip(schema.payload_columns())
            .all(|((_, new_ci, _), (_, old_ci, _))| col_indices[new_ci] == old_ci);

    if payload_preserved {
        new_batch.nulls = src_nulls;
    } else {
        // Projected PKs become new PKs (not payload), so old_ci is a
        // source payload column by construction — payload_idx is safe.
        let pi_mappings: Vec<(usize, usize)> = new_schema.payload_columns()
            .map(|(new_pi, new_ci, _)| (new_pi, schema.payload_idx(col_indices[new_ci])))
            .collect();

        new_batch.nulls.reserve(row_count);
        for &old_word in &src_nulls {
            let mut new_word = 0u64;
            for &(new_pi, old_pi) in &pi_mappings {
                if (old_word >> old_pi) & 1 == 1 {
                    new_word |= 1u64 << new_pi;
                }
            }
            new_batch.nulls.push(new_word);
        }
    }

    // 5. Payload columns: strictly payload→payload. `src_columns` is owned,
    //    so swap whole vectors instead of cloning per element — avoids
    //    per-row Option<String>/Option<Vec<u8>> allocations on string/blob
    //    result sets.
    for (_, new_ci, _) in new_schema.payload_columns() {
        let old_ci = col_indices[new_ci];
        let src_col = std::mem::replace(&mut src_columns[old_ci], ColData::Fixed(Vec::new()));
        match (src_col, &mut new_batch.columns[new_ci]) {
            (ColData::Fixed(s),   ColData::Fixed(d))   => *d = s,
            (ColData::Strings(s), ColData::Strings(d)) => *d = s,
            (ColData::Bytes(s),   ColData::Bytes(d))   => *d = s,
            (ColData::U128s(s),   ColData::U128s(d))   => *d = s,
            _ => unreachable!("mismatched ColData variants for column {new_ci}"),
        }
    }

    Ok((new_schema, new_batch))
```

## Verification

Add the following tests to `crates/gnitz-py/tests/test_compound_pk.py`.
They cover full-PK projection (symptom case), PK subset, PK reorder,
payload reorder with nulls, and the no-PK rejection. Existing
`SELECT *` tests still cover the wildcard fast-path.

```python
def test_compound_pk_full_projection(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "t")
        client.execute_sql(
            "INSERT INTO t (a, b, payload) VALUES (1, 2, 100), (3, 4, 200)",
            schema_name=sn,
        )
        results = client.execute_sql("SELECT a, b, payload FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.payload) for row in rows_result["rows"])
        assert seen == [(1, 2, 100), (3, 4, 200)]
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_subset_projection(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "t")
        client.execute_sql(
            "INSERT INTO t (a, b, payload) VALUES (1, 2, 100), (3, 4, 200)",
            schema_name=sn,
        )
        results = client.execute_sql("SELECT a, payload FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.payload) for row in rows_result["rows"])
        assert seen == [(1, 100), (3, 200)]
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_reorder_projection(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "t")
        client.execute_sql(
            "INSERT INTO t (a, b, payload) VALUES (1, 2, 100), (3, 4, 200)",
            schema_name=sn,
        )
        results = client.execute_sql("SELECT b, a, payload FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.b, row.a, row.payload) for row in rows_result["rows"])
        assert seen == [(2, 1, 100), (4, 3, 200)]
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_payload_reorder_with_nulls(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, "
            "p1 BIGINT, p2 BIGINT, PRIMARY KEY (a, b))",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t (a, b, p1, p2) VALUES (1, 2, NULL, 100), (3, 4, 200, NULL)",
            schema_name=sn,
        )
        results = client.execute_sql("SELECT a, b, p2, p1 FROM t", schema_name=sn)
        rows_result = next(r for r in results if r["type"] == "Rows")
        seen = sorted((row.a, row.b, row.p2, row.p1) for row in rows_result["rows"])
        assert seen == [(1, 2, 100, None), (3, 4, None, 200)]
    finally:
        _cleanup(client, sn, "t")


def test_compound_pk_no_pk_projection_rejected(client):
    sn = "cpk" + _uid()
    client.create_schema(sn)
    try:
        _make_compound_table(client, sn, "t")
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql("SELECT payload FROM t", schema_name=sn)
        assert "must project at least one PRIMARY KEY column" in str(exc.value)
    finally:
        _cleanup(client, sn, "t")
```

## Out of scope

- ORDER BY (rejected separately in `execute_select`).
- Group/aggregate projections (handled in views, not direct SELECT).
- Any planner-side rewrites of PK columns.
