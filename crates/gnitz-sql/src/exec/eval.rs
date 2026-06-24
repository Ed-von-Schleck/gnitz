use crate::codec::nullmap::null_word_get;
use crate::error::GnitzSqlError;
use crate::ir::{BinOp, BoundExpr, UnaryOp};
use crate::lower::{lower_bound_expr, BoundExprBackend};
use gnitz_core::{ColData, FixedInt, PkColumn, Schema, TypeCode, ZSetBatch};

/// Decode the native LE bytes of a PK column into an i64. U128/UUID don't fit
/// through the i64 return shape and are rejected.
fn decode_pk_bytes_to_i64(tc: TypeCode, slice: &[u8]) -> Result<i64, GnitzSqlError> {
    FixedInt::from_type_code(tc)
        .map(|ft| ft.decode_le_i64(slice))
        .ok_or_else(|| {
            GnitzSqlError::Unsupported(format!(
                "residual filter on PK column of type {tc:?} not supported; \
             use `pk = literal` to seek instead"
            ))
        })
}

/// Client-side interpreter backend: evaluates a single batch row into an
/// `Option<i64>` (`None` = SQL NULL). The structural walk is shared with the
/// opcode lowerer via [`BoundExprBackend`]. Integer columns decode through
/// `FixedInt::decode_le_i64`: narrow unsigned columns (U8/U16/U32) are
/// zero-extended (`200u8` → `200`, never a sign-extended negative); a full-width
/// U64 is bit-reinterpreted (`U64::MAX` → `-1i64`); and every non-≤8-byte-integer
/// type (U128, UUID, I128, F32/F64, STRING, BLOB) is rejected via
/// `from_type_code → None` rather than mis-decoded. See the `*_not_sign_extended`
/// and `eval_expr_u64_high_bit_decodes_correctly` tests.
struct InterpBackend<'a> {
    batch: &'a ZSetBatch,
    row: usize,
    schema: &'a Schema,
}

impl BoundExprBackend for InterpBackend<'_> {
    type Out = Option<i64>;

    fn col_ref(&mut self, c: usize) -> Result<Self::Out, GnitzSqlError> {
        let (batch, i, schema) = (self.batch, self.row, self.schema);
        let col_def = &schema.columns[c];
        // The PK column's slot in `batch.columns` is `Fixed(vec![])`
        // (placeholder set by ZSetBatch::new). The PK value lives in
        // `batch.pks`; decode the bytes for column `c` under the
        // column's declared signedness. U128/UUID don't fit through the
        // i64 return shape — reject explicitly.
        if schema.is_pk_col(c) {
            let stride = col_def.type_code.wire_stride();
            match &batch.pks {
                PkColumn::Bytes { stride: s, buf } => {
                    let s = *s as usize;
                    let off = schema.pk_byte_offset(c);
                    let slice = &buf[i * s + off..i * s + off + stride];
                    return decode_pk_bytes_to_i64(col_def.type_code, slice).map(Some);
                }
                _ => {
                    let bytes = batch.pks.get(i).to_le_bytes();
                    return decode_pk_bytes_to_i64(col_def.type_code, &bytes[..stride]).map(Some);
                }
            }
        }
        // Payload column: check the null bitmap first.
        let payload_idx = schema.payload_idx(c);
        if null_word_get(batch.nulls[i], payload_idx) {
            return Ok(None);
        }
        match &batch.columns[c] {
            ColData::Fixed(buf) => match FixedInt::from_type_code(col_def.type_code) {
                Some(ft) => {
                    let stride = ft.width();
                    let start = i * stride;
                    Ok(Some(ft.decode_le_i64(&buf[start..start + stride])))
                }
                None => Err(GnitzSqlError::Unsupported(format!(
                    "residual filter on {:?} column not supported",
                    col_def.type_code
                ))),
            },
            ColData::Strings(_) => Err(GnitzSqlError::Unsupported(
                "residual filter on string column not supported".to_string(),
            )),
            ColData::Bytes(_) => Err(GnitzSqlError::Unsupported(
                "residual filter on blob column not supported".to_string(),
            )),
            ColData::U128s(_) => {
                let type_name = if schema.columns[c].type_code == TypeCode::UUID {
                    "UUID"
                } else {
                    "U128"
                };
                Err(GnitzSqlError::Unsupported(format!(
                    "residual filter on {type_name} column not supported; \
                     use a primary-key seek or CREATE INDEX for equality lookups"
                )))
            }
        }
    }

    fn lit_int(&mut self, v: i64) -> Result<Self::Out, GnitzSqlError> {
        Ok(Some(v))
    }

    fn lit_float(&mut self, _v: f64) -> Result<Self::Out, GnitzSqlError> {
        Err(GnitzSqlError::Unsupported(
            "float literals in residual filter not supported".to_string(),
        ))
    }

    fn lit_str(&mut self, _s: &str) -> Result<Self::Out, GnitzSqlError> {
        Err(GnitzSqlError::Unsupported(
            "string literals in WHERE predicate not supported; \
             use CREATE INDEX or CREATE VIEW"
                .to_string(),
        ))
    }

    fn binop(&mut self, l: &BoundExpr, op: BinOp, r: &BoundExpr) -> Result<Self::Out, GnitzSqlError> {
        // SQL 3VL: short-circuit before propagating NULL.
        // TRUE OR any = TRUE; FALSE AND any = FALSE — regardless of NULL.
        match op {
            BinOp::Or => {
                let lv = lower_bound_expr(l, self)?;
                if lv.is_some_and(|v| v != 0) {
                    return Ok(Some(1));
                }
                let rv = lower_bound_expr(r, self)?;
                if rv.is_some_and(|v| v != 0) {
                    return Ok(Some(1));
                }
                return Ok(if lv.is_some() && rv.is_some() { Some(0) } else { None });
            }
            BinOp::And => {
                let lv = lower_bound_expr(l, self)?;
                if lv == Some(0) {
                    return Ok(Some(0));
                }
                let rv = lower_bound_expr(r, self)?;
                if rv == Some(0) {
                    return Ok(Some(0));
                }
                return Ok(if lv.is_some() && rv.is_some() { Some(1) } else { None });
            }
            _ => {}
        }
        let lv = match lower_bound_expr(l, self)? {
            None => return Ok(None),
            Some(v) => v,
        };
        let rv = match lower_bound_expr(r, self)? {
            None => return Ok(None),
            Some(v) => v,
        };
        Ok(Some(match op {
            BinOp::Add => lv.wrapping_add(rv),
            BinOp::Sub => lv.wrapping_sub(rv),
            BinOp::Mul => lv.wrapping_mul(rv),
            // wrapping_div/rem prevent the i64::MIN / -1 overflow panic.
            BinOp::Div => {
                if rv == 0 {
                    0
                } else {
                    lv.wrapping_div(rv)
                }
            }
            BinOp::Mod => {
                if rv == 0 {
                    0
                } else {
                    lv.wrapping_rem(rv)
                }
            }
            BinOp::Eq => (lv == rv) as i64,
            BinOp::Ne => (lv != rv) as i64,
            BinOp::Gt => (lv > rv) as i64,
            BinOp::Ge => (lv >= rv) as i64,
            BinOp::Lt => (lv < rv) as i64,
            BinOp::Le => (lv <= rv) as i64,
            BinOp::And | BinOp::Or => unreachable!(),
        }))
    }

    fn unop(&mut self, op: UnaryOp, e: &BoundExpr) -> Result<Self::Out, GnitzSqlError> {
        let v = match lower_bound_expr(e, self)? {
            None => return Ok(None),
            Some(v) => v,
        };
        Ok(Some(match op {
            UnaryOp::Neg => v.wrapping_neg(),
            UnaryOp::Not => (v == 0) as i64,
        }))
    }

    fn null_test(&mut self, c: usize, want_null: bool) -> Result<Self::Out, GnitzSqlError> {
        if self.schema.is_pk_col(c) {
            // PK is never null → IS NULL is always false, IS NOT NULL always true.
            return Ok(Some(if want_null { 0 } else { 1 }));
        }
        let payload_idx = self.schema.payload_idx(c);
        let is_null = null_word_get(self.batch.nulls[self.row], payload_idx);
        Ok(Some(if want_null { is_null as i64 } else { !is_null as i64 }))
    }

    fn agg_call(&mut self) -> Result<Self::Out, GnitzSqlError> {
        Err(GnitzSqlError::Unsupported(
            "aggregate functions not allowed in this context".to_string(),
        ))
    }
}

/// Evaluate a bound expression against a single row.
///
/// Returns `None` when the result is SQL NULL. A NULL column operand propagates
/// through all arithmetic and comparison operators. Callers that need a boolean
/// predicate should treat `None` as `false` (UNKNOWN → row excluded).
pub(crate) fn eval_expr(
    expr: &BoundExpr,
    batch: &ZSetBatch,
    i: usize,
    schema: &Schema,
) -> Result<Option<i64>, GnitzSqlError> {
    let mut backend = InterpBackend { batch, row: i, schema };
    lower_bound_expr(expr, &mut backend)
}

pub(crate) fn eval_pred_row(
    pred: &BoundExpr,
    batch: &ZSetBatch,
    i: usize,
    schema: &Schema,
) -> Result<bool, GnitzSqlError> {
    // SQL NULL in a predicate position is UNKNOWN → treated as false (row excluded).
    Ok(eval_expr(pred, batch, i, schema)?.is_some_and(|v| v != 0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{batch_2col, col_def, compound_schema_u64_u64, pk_schema, two_col, uuid_schema_payload};
    use gnitz_core::{ColData, Schema, TypeCode, ZSetBatch};

    // ------------------------------------------------------------------
    // Bug #4 — unsigned integers must not be sign-extended
    // ------------------------------------------------------------------

    #[test]
    fn test_u8_200_not_sign_extended() {
        let schema = two_col(TypeCode::U8);
        let batch = batch_2col(vec![200u8], TypeCode::U8, 0);
        assert_eq!(eval_expr(&BoundExpr::ColRef(1), &batch, 0, &schema).unwrap(), Some(200));
    }

    #[test]
    fn test_u16_60000_not_sign_extended() {
        let schema = two_col(TypeCode::U16);
        let batch = batch_2col(60000u16.to_le_bytes().to_vec(), TypeCode::U16, 0);
        assert_eq!(
            eval_expr(&BoundExpr::ColRef(1), &batch, 0, &schema).unwrap(),
            Some(60000)
        );
    }

    #[test]
    fn test_u32_3b_not_sign_extended() {
        let schema = two_col(TypeCode::U32);
        let batch = batch_2col(3_000_000_000u32.to_le_bytes().to_vec(), TypeCode::U32, 0);
        assert_eq!(
            eval_expr(&BoundExpr::ColRef(1), &batch, 0, &schema).unwrap(),
            Some(3_000_000_000)
        );
    }

    // ------------------------------------------------------------------
    // Bug #5 — NULL must propagate through predicates
    // ------------------------------------------------------------------

    #[test]
    fn test_null_col_returns_none() {
        let schema = two_col(TypeCode::I64);
        let batch = batch_2col(vec![0u8; 8], TypeCode::I64, 0b1); // bit 0 set → val is NULL
        assert_eq!(eval_expr(&BoundExpr::ColRef(1), &batch, 0, &schema).unwrap(), None);
    }

    #[test]
    fn test_null_col_eq_zero_predicate_is_false() {
        // WHERE val = 0 must NOT match a NULL row (SQL: NULL = 0 → UNKNOWN → false)
        let schema = two_col(TypeCode::I64);
        let batch = batch_2col(vec![0u8; 8], TypeCode::I64, 0b1);
        let pred = BoundExpr::BinOp(
            Box::new(BoundExpr::ColRef(1)),
            BinOp::Eq,
            Box::new(BoundExpr::LitInt(0)),
        );
        assert!(!eval_pred_row(&pred, &batch, 0, &schema).unwrap());
    }

    // ------------------------------------------------------------------
    // Bug #6 — IS NULL / IS NOT NULL on the PK column must not panic
    // ------------------------------------------------------------------

    #[test]
    fn test_isnull_on_pk_is_false_no_panic() {
        let schema = two_col(TypeCode::I64);
        let batch = batch_2col(vec![0u8; 8], TypeCode::I64, 0);
        // pk_index = 0; previously `*c - 1` would underflow when *c == 0
        assert_eq!(eval_expr(&BoundExpr::IsNull(0), &batch, 0, &schema).unwrap(), Some(0));
    }

    #[test]
    fn test_isnotnull_on_pk_is_true_no_panic() {
        let schema = two_col(TypeCode::I64);
        let batch = batch_2col(vec![0u8; 8], TypeCode::I64, 0);
        assert_eq!(
            eval_expr(&BoundExpr::IsNotNull(0), &batch, 0, &schema).unwrap(),
            Some(1)
        );
    }

    // ------------------------------------------------------------------
    // Bug #7 — i64::MIN / -1 and i64::MIN % -1 must not panic
    // ------------------------------------------------------------------

    #[test]
    fn test_div_i64_min_by_neg1_no_panic() {
        let schema = two_col(TypeCode::I64);
        let batch = batch_2col(i64::MIN.to_le_bytes().to_vec(), TypeCode::I64, 0);
        let expr = BoundExpr::BinOp(
            Box::new(BoundExpr::ColRef(1)),
            BinOp::Div,
            Box::new(BoundExpr::LitInt(-1)),
        );
        assert!(eval_expr(&expr, &batch, 0, &schema).is_ok());
    }

    #[test]
    fn test_mod_i64_min_by_neg1_no_panic() {
        let schema = two_col(TypeCode::I64);
        let batch = batch_2col(i64::MIN.to_le_bytes().to_vec(), TypeCode::I64, 0);
        let expr = BoundExpr::BinOp(
            Box::new(BoundExpr::ColRef(1)),
            BinOp::Mod,
            Box::new(BoundExpr::LitInt(-1)),
        );
        assert!(eval_expr(&expr, &batch, 0, &schema).is_ok());
    }

    // ------------------------------------------------------------------
    // eval_expr — reading PK / payload columns of various widths
    // ------------------------------------------------------------------

    fn float_col_schema(tc: TypeCode) -> Schema {
        Schema {
            columns: vec![col_def("pk", TypeCode::U64, false), col_def("v", tc, false)],
            pk_cols: vec![0],
        }
    }

    #[test]
    fn test_uuid_residual_filter_error_names_uuid() {
        let schema = uuid_schema_payload();
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(1);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::U128s(v) = &mut batch.columns[1] {
            v.push(0);
        }
        let pred = BoundExpr::ColRef(1);
        let err = eval_expr(&pred, &batch, 0, &schema).unwrap_err();
        assert!(err.to_string().contains("UUID"), "error should mention UUID: {err}");
    }

    #[test]
    fn eval_expr_pk_i64_negative() {
        let schema = pk_schema(TypeCode::I64);
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(((-1i64) as u64) as u128);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&0i64.to_le_bytes());
        }
        let got = eval_expr(&BoundExpr::ColRef(0), &batch, 0, &schema).unwrap();
        assert_eq!(got, Some(-1i64));
    }

    #[test]
    fn eval_expr_pk_u32_max() {
        // U32 PK = u32::MAX; zero-extended into u128 by extract_pk_value.
        let schema = pk_schema(TypeCode::U32);
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(u32::MAX as u128);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&0i64.to_le_bytes());
        }
        let got = eval_expr(&BoundExpr::ColRef(0), &batch, 0, &schema).unwrap();
        assert_eq!(got, Some(u32::MAX as i64));
    }

    #[test]
    fn eval_expr_pk_u128_unsupported() {
        // U128 PK doesn't fit through eval_expr's i64 projection; must error,
        // not panic.
        let schema = pk_schema(TypeCode::U128);
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(u128::MAX);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&0i64.to_le_bytes());
        }
        let err = eval_expr(&BoundExpr::ColRef(0), &batch, 0, &schema).expect_err("U128 PK must return Unsupported");
        assert!(err.to_string().contains("residual filter on PK"), "error: {err}");
    }

    #[test]
    fn compound_pk_eval_expr_pk_colref_reads_byte_region() {
        let schema = compound_schema_u64_u64();
        // Build a one-row Bytes batch: pk = (a=7, b=9), v = 42.
        let mut batch = ZSetBatch::new(&schema);
        let mut pk_bytes = [0u8; 16];
        pk_bytes[..8].copy_from_slice(&7u64.to_le_bytes());
        pk_bytes[8..16].copy_from_slice(&9u64.to_le_bytes());
        batch.pks.push_bytes(&pk_bytes);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[2] {
            buf.extend_from_slice(&42i64.to_le_bytes());
        }
        // Read column 0 (a) and column 1 (b) through eval_expr.
        let a = eval_expr(&BoundExpr::ColRef(0), &batch, 0, &schema).unwrap();
        assert_eq!(a, Some(7));
        let b = eval_expr(&BoundExpr::ColRef(1), &batch, 0, &schema).unwrap();
        assert_eq!(b, Some(9));
        // Payload column still works.
        let v = eval_expr(&BoundExpr::ColRef(2), &batch, 0, &schema).unwrap();
        assert_eq!(v, Some(42));
    }

    #[test]
    fn eval_expr_f32_payload_returns_unsupported() {
        let schema = float_col_schema(TypeCode::F32);
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(1);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&1.0f32.to_le_bytes());
        }
        let err = eval_expr(&BoundExpr::ColRef(1), &batch, 0, &schema).unwrap_err();
        assert!(err.to_string().to_lowercase().contains("not supported"), "got: {err}");
    }

    #[test]
    fn eval_expr_f64_payload_returns_unsupported() {
        let schema = float_col_schema(TypeCode::F64);
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(1);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&1.0f64.to_le_bytes());
        }
        let err = eval_expr(&BoundExpr::ColRef(1), &batch, 0, &schema).unwrap_err();
        assert!(err.to_string().to_lowercase().contains("not supported"), "got: {err}");
    }

    #[test]
    fn eval_expr_u64_high_bit_decodes_correctly() {
        // U64 with the high bit set: the new path must produce the same bitcast
        // as the old path. In i64, this is -1.
        let schema = float_col_schema(TypeCode::U64);
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(1);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&u64::MAX.to_le_bytes());
        }
        let got = eval_expr(&BoundExpr::ColRef(1), &batch, 0, &schema).unwrap();
        assert_eq!(got, Some(-1i64), "U64::MAX must bitcast to -1i64 via new path");
    }
}
