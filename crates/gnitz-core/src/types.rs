use crate::protocol::{ColumnDef, Schema};
use std::sync::OnceLock;

/// The client `Schema` for system table `tid`. Every system-table schema is
/// derived from the shared `gnitz-wire` family table, so the client and engine
/// cannot drift on a table's shape — and a caller holding a table id cannot pair
/// it with another family's schema, because there is nothing to pair.
///
/// Panics on an id that is not a system family: every call site names a
/// `gnitz_wire::*_TAB` constant.
pub fn sys_schema(tid: u64) -> &'static Schema {
    static INSTANCE: OnceLock<Vec<Schema>> = OnceLock::new();
    let schemas = INSTANCE.get_or_init(|| {
        gnitz_wire::SYS_FAMILIES
            .iter()
            .map(|f| schema_from_wire_cols(f.cols, f.pk_cols))
            .collect()
    });
    let idx = gnitz_wire::sys_family_index(tid).unwrap_or_else(|| panic!("not a system table id: {tid}"));
    &schemas[idx]
}

pub(crate) fn schema_from_wire_cols(cols: &[gnitz_wire::WireSysCol], pk_cols: &[u32]) -> Schema {
    Schema {
        columns: cols
            .iter()
            .map(|c| ColumnDef::new(c.name, c.type_code, c.nullable))
            .collect(),
        pk_cols: pk_cols.to_vec(),
    }
}
