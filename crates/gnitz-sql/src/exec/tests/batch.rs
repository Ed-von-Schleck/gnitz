use super::*;
use crate::test_support::compound_schema_u64_u64;

#[test]
fn compound_pk_row_gather_preserves_compound_pk() {
    let schema = compound_schema_u64_u64();
    let mut src = ZSetBatch::new(&schema);
    let mut pk_bytes = [0u8; 16];
    pk_bytes[..8].copy_from_slice(&11u64.to_le_bytes());
    pk_bytes[8..16].copy_from_slice(&22u64.to_le_bytes());
    src.pks.push_bytes(&schema, &pk_bytes);
    src.weights.push(1);
    src.nulls.push(0);
    {
        let buf = &mut src.columns[2];
        buf.extend_from_slice(&5i64.to_le_bytes());
    }

    let mut dst = ZSetBatch::new(&schema);
    RowGather::new(&schema).copy(&src, 0, &mut dst);
    assert_eq!(dst.pks.len(), 1);
    assert_eq!(dst.pks.region(), src.pks.region());
}
