mod ddl_tests;
mod fk_tests;
mod uuid_tests;
mod view_tests;
mod index_tests;
mod engine_tests;

use super::*;
use super::sys_tables::*;

use std::fs;

fn temp_dir(name: &str) -> String {
    crate::util::raise_fd_limit_for_tests();
    let path = format!("/tmp/gnitz_catalog_test_{}", name);
    let _ = fs::remove_dir_all(&path);
    path
}

fn u64_col_def(name: &str) -> ColumnDef {
    ColumnDef { name: name.into(), type_code: type_code::U64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
}
fn i64_col_def(name: &str) -> ColumnDef {
    ColumnDef { name: name.into(), type_code: type_code::I64, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
}
fn u8_col_def(name: &str) -> ColumnDef {
    ColumnDef { name: name.into(), type_code: type_code::U8, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
}
fn u16_col_def(name: &str) -> ColumnDef {
    ColumnDef { name: name.into(), type_code: type_code::U16, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
}
fn i32_col_def(name: &str) -> ColumnDef {
    ColumnDef { name: name.into(), type_code: type_code::I32, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
}
fn str_col_def(name: &str) -> ColumnDef {
    ColumnDef { name: name.into(), type_code: type_code::STRING, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
}
fn u128_col_def(name: &str) -> ColumnDef {
    ColumnDef { name: name.into(), type_code: type_code::U128, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 }
}

fn count_records(table: &mut Table) -> usize {
    let mut count = 0;
    if let Ok(cursor) = table.create_cursor() {
        let mut c = cursor;
        while c.cursor.valid {
            if c.cursor.current_weight > 0 { count += 1; }
            c.cursor.advance();
        }
    }
    count
}

