//! Secondary index integration: GiDesc, AviDesc, op_integrate_with_indexes.

use crate::schema::{SchemaDescriptor, SchemaColumn, type_code};
use crate::storage::{Batch, MemBatch};

// ---------------------------------------------------------------------------
// Column promotion helpers
// ---------------------------------------------------------------------------

/// Promote a column value to u64 for GroupIndex composite keys.
///
/// Caller is responsible for resolving `pi`; this helper is schema-agnostic
/// so PK and payload reads can dispatch externally.
fn promote_col_to_u64(mb: &MemBatch, row: usize, pi: usize, col_type_code: u8) -> u64 {
    let cs = match col_type_code {
        type_code::U8 | type_code::I8 => 1,
        type_code::U16 | type_code::I16 => 2,
        type_code::U32 | type_code::I32 | type_code::F32 => 4,
        _ => 8, // U64, I64, F64; U128 excluded by caller
    };
    let ptr = mb.get_col_ptr(row, pi, cs);
    let mut buf = [0u8; 8];
    buf[..cs].copy_from_slice(ptr);
    u64::from_le_bytes(buf)
}

/// Promote an aggregate column value to an order-preserving u64 for AVI keys.
fn promote_agg_col_to_u64_ordered(
    mb: &MemBatch,
    row: usize,
    pi: usize,
    col_type_code: u8,
    for_max: bool,
) -> u64 {
    if col_type_code == type_code::F32 {
        let ptr = mb.get_col_ptr(row, pi, 4);
        let raw32 = u32::from_le_bytes(ptr.try_into().unwrap());
        let val = ieee_order_bits_f32(raw32);
        return if for_max { !val } else { val };
    }

    let cs = crate::schema::type_size(col_type_code) as usize;
    let ptr = mb.get_col_ptr(row, pi, cs);
    let mut buf = [0u8; 8];
    buf[..cs].copy_from_slice(ptr);
    let raw = u64::from_le_bytes(buf);

    let val = match col_type_code {
        type_code::U8 | type_code::U16 | type_code::U32 | type_code::U64 => raw,
        type_code::I8 | type_code::I16 | type_code::I32 | type_code::I64 => {
            // Sign-extend then offset-binary
            let signed = crate::schema::read_signed(&buf[..cs], cs);
            (signed as u64).wrapping_add(1u64 << 63)
        }
        type_code::F64 => ieee_order_bits(raw),
        _ => raw,
    };

    if for_max { !val } else { val }
}

fn ieee_order_bits(raw_bits: u64) -> u64 {
    if raw_bits >> 63 != 0 {
        !raw_bits
    } else {
        raw_bits ^ (1u64 << 63)
    }
}

fn ieee_order_bits_f32(raw_bits: u32) -> u64 {
    (if raw_bits >> 31 != 0 { !raw_bits } else { raw_bits ^ (1u32 << 31) }) as u64
}

// ---------------------------------------------------------------------------
// Public descriptor types
// ---------------------------------------------------------------------------

/// GI/AVI descriptor for integrate_with_indexes.
pub struct GiDesc {
    pub table: *mut crate::storage::Table,
    pub col_idx: u32,
    pub col_type_code: u8,
}

pub struct AviDesc {
    pub table: *mut crate::storage::Table,
    pub for_max: bool,
    pub agg_col_type_code: u8,
    pub group_by_cols: Vec<u32>,
    pub input_schema: SchemaDescriptor,
    pub agg_col_idx: u32,
}

/// GI schema: U128 PK + I64 payload (spk_hi).
pub(crate) fn make_gi_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// AVI schema: U128 PK only, no payload.
pub(crate) fn make_avi_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(&[SchemaColumn::new(type_code::U128, 0)], &[0])
}

// ---------------------------------------------------------------------------
// op_integrate_with_indexes
// ---------------------------------------------------------------------------

/// Integrate a delta batch into a target table, optionally populating
/// GroupIndex and AggValueIndex secondary indexes.
///
/// The Rust Table handles memtable capacity internally (flush-on-overflow).
pub fn op_integrate_with_indexes(
    batch: &Batch,
    target_table: Option<&mut crate::storage::Table>,
    input_schema: &SchemaDescriptor,
    gi: Option<&GiDesc>,
    avi: Option<&AviDesc>,
) -> Result<(), crate::storage::StorageError> {
    if batch.count == 0 {
        return Ok(());
    }

    // `reduce` for non-group-by-PK aggregates may emit retract+insert pairs
    // that are physically unsorted while claiming `consolidated = true`.
    if let Some(table) = target_table {
        let mut cloned = batch.clone_batch();
        cloned.sorted = false;
        cloned.consolidated = false;
        table.ingest_owned_batch_memonly(cloned)?;
    }

    let mb = batch.as_mem_batch();

    // GroupIndex population
    if let Some(gi_desc) = gi {
        let gi_schema = make_gi_schema();
        let mut gi_batch = Batch::with_schema(gi_schema, batch.count);
        gi_batch.sorted = false;
        gi_batch.consolidated = false;

        let gi_col = gi_desc.col_idx as usize;
        let gi_is_pk = input_schema.is_pk_col(gi_col);
        // PK has no payload-index slot; the null-bit probe is skipped below,
        // so a dummy 0 is safe.
        let gi_pi = if gi_is_pk { 0 } else { input_schema.payload_idx(gi_col) };

        for row in 0..batch.count {
            // PK is never null (catalog rule: pk-not-null.md), so its dense
            // bit position is undefined and the null probe is skipped.
            if !gi_is_pk && (mb.get_null_word(row) >> gi_pi) & 1 != 0 {
                continue;
            }

            let source_pk = mb.get_pk(row);
            let gc_u64 = if gi_is_pk {
                source_pk as u64
            } else {
                promote_col_to_u64(&mb, row, gi_pi, gi_desc.col_type_code)
            };
            let source_pk_lo = source_pk as u64;
            let source_pk_hi = (source_pk >> 64) as u64;
            let weight = mb.get_weight(row);

            // Composite key: ck_lo = source_pk_lo, ck_hi = gc_u64
            gi_batch.extend_pk(((gc_u64 as u128) << 64) | (source_pk_lo as u128));
            gi_batch.extend_weight(&weight.to_le_bytes());
            gi_batch.extend_null_bmp(&0u64.to_le_bytes());
            // Payload: spk_hi (source pk high 64 bits) as I64
            gi_batch.extend_col(0, &(source_pk_hi as i64).to_le_bytes());
            gi_batch.count += 1;
        }

        if gi_batch.count > 0 {
            let gi_table = unsafe { &mut *gi_desc.table };
            let _ = gi_table.ingest_owned_batch_memonly(gi_batch);
        }
    }

    // AggValueIndex population
    if let Some(avi_desc) = avi {
        let avi_schema = make_avi_schema();
        let mut avi_batch = Batch::with_schema(avi_schema, batch.count);
        avi_batch.sorted = false;
        avi_batch.consolidated = false;

        let avi_col = avi_desc.agg_col_idx as usize;
        let avi_is_pk = input_schema.is_pk_col(avi_col);
        let avi_pi = if avi_is_pk { 0 } else { input_schema.payload_idx(avi_col) };

        for row in 0..batch.count {
            // PK is never null, so its dense bit position is undefined.
            if !avi_is_pk && (mb.get_null_word(row) >> avi_pi) & 1 != 0 {
                continue;
            }

            let gc_u64 = super::util::extract_gc_u64(
                &mb, row, &avi_desc.input_schema, &avi_desc.group_by_cols,
            );
            let av_u64 = if avi_is_pk {
                let val = mb.get_pk(row) as u64;
                if avi_desc.for_max { !val } else { val }
            } else {
                promote_agg_col_to_u64_ordered(
                    &mb, row, avi_pi,
                    avi_desc.agg_col_type_code, avi_desc.for_max,
                )
            };
            let weight = mb.get_weight(row);

            // Composite key: ck_lo = av_u64, ck_hi = gc_u64
            avi_batch.extend_pk(((gc_u64 as u128) << 64) | (av_u64 as u128));
            avi_batch.extend_weight(&weight.to_le_bytes());
            avi_batch.extend_null_bmp(&0u64.to_le_bytes());
            // No payload columns (AVI schema is U128 PK only)
            avi_batch.count += 1;
        }

        if avi_batch.count > 0 {
            gnitz_debug!("integrate_avi: ingesting {} rows, for_max={}, agg_col_idx={}, agg_type={}",
                avi_batch.count, avi_desc.for_max, avi_desc.agg_col_idx, avi_desc.agg_col_type_code);
            for i in 0..avi_batch.count {
                let pk = avi_batch.get_pk(i);
                let w = i64::from_le_bytes(avi_batch.weight_data()[i*8..(i+1)*8].try_into().unwrap());
                gnitz_debug!("  avi[{}]: pk={:#034x} w={}", i, pk, w);
            }
            let avi_table = unsafe { &mut *avi_desc.table };
            let _ = avi_table.ingest_owned_batch_memonly(avi_batch);
        }
    }

    Ok(())
}
