use super::*;
use crate::codec::project_schema::{reply_program, ProjItem};
use crate::test_support::{col, ncol};
use gnitz_wire::TypeCode;

/// `(k I64 PK, v I64 NULL)`.
fn source() -> Schema {
    Schema {
        columns: vec![col("k", TypeCode::I64), ncol("v", TypeCode::I64)],
        pk_cols: vec![0],
    }
}

/// `SELECT v, k`'s reply map over [`source`]: the hidden key, then `v`, then `k`.
fn v_then_k(src: &Schema) -> ClientMap {
    let items = vec![
        ProjItem::PassThrough { src_col: 1 },
        ProjItem::PassThrough { src_col: 0 },
    ];
    let cols = vec![src.columns[1].clone(), src.columns[0].clone()];
    let (out, program) = reply_program(&items, cols, src).unwrap();
    ClientMap::new(program, src, Arc::new(out)).unwrap()
}

/// A PK copy is the key's native value — the OPK sign flip undone — a payload copy
/// moves with its NULLs, and every row keeps its key and weight.
#[test]
fn a_key_copy_decodes_and_a_payload_copy_moves() {
    let src = source();
    let mut b = ZSetBatch::new(&src);
    for (k, v, w) in [(-5i64, Some(7i64), 1i64), (3, None, 2)] {
        b.pks.push_natives(&src, &[k as u128]);
        b.weights.push(w);
        b.nulls.push(v.is_none() as u64);
        b.payload[0].bytes.extend_from_slice(&v.unwrap_or(0).to_le_bytes());
    }
    let pks = b.pks.region().to_vec();
    let mut map = v_then_k(&src);
    let out = map.apply(b);
    assert_eq!(out.pks.region(), &pks[..]);
    assert_eq!(out.weights, vec![1, 2]);
    let cell = |pi: usize, r: usize| i64::from_le_bytes(out.payload[pi].bytes[r * 8..r * 8 + 8].try_into().unwrap());
    assert_eq!((cell(0, 0), cell(1, 0)), (7, -5));
    assert_eq!(cell(1, 1), 3);
    assert_eq!(
        out.nulls,
        vec![0, 1],
        "the NULL `v` stays NULL; a key copy is never NULL"
    );
}
