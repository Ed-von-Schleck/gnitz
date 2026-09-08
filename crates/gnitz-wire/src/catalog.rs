//! System-catalog wire layout: the shared `WireSysCol` descriptor, system
//! table column lists and IDs, schema sizing caps, and the compound-PK
//! column-list codec for the persisted `TABLE_TAB.pk_col_idx` u64.

use crate::TypeCode;

// ---------------------------------------------------------------------------
// System table column descriptors — shared single source of truth
// ---------------------------------------------------------------------------

pub struct WireSysCol {
    pub name: &'static str,
    pub type_code: TypeCode,
    pub nullable: bool,
}

/// Terse `WireSysCol` constructor so the column tables read as one line per
/// column. `pub(crate)` — internal to the wire crate (also used by `control.rs`);
/// not part of the public surface. `const` so it is callable in the `pub const`
/// table initializers (visibility does not affect const-eval).
pub(crate) const fn col(name: &'static str, type_code: TypeCode, nullable: bool) -> WireSysCol {
    WireSysCol { name, type_code, nullable }
}

/// Index of the column named `name` in `cols`, resolved at compile time.
/// Panics (const-eval failure) if absent, so a renamed/removed column fails the
/// build rather than silently mis-indexing. `==` on `&str` is not const-stable,
/// hence the manual byte compare.
pub const fn col_index_in(cols: &[WireSysCol], name: &str) -> usize {
    let mut i = 0;
    while i < cols.len() {
        let a = cols[i].name.as_bytes();
        let b = name.as_bytes();
        if a.len() == b.len() {
            let mut j = 0;
            let mut matched = true;
            while j < a.len() {
                if a[j] != b[j] {
                    matched = false;
                    break;
                }
                j += 1;
            }
            if matched {
                return i;
            }
        }
        i += 1;
    }
    panic!("column not found")
}

/// Dense **payload** index of the column named `name` in `cols` — its position
/// among the non-PK columns, which is the slot the null bitmap and the payload
/// region directory address it by.
///
/// Valid only for a column list whose primary key is the single leading column,
/// where the payload index collapses to `col_index_in(..) - 1`. Every list this
/// is called with is one (`SCHEMA_TAB`, `TABLE_TAB`, `VIEW_TAB`, `COL_TAB`,
/// `IDX_TAB`, `SEQ_TAB`, and the IPC control block); the compound-PK lists
/// (the circuit family) renumbers around *every* PK position, so the closed
/// form does not hold for them and this must not be used there. The
/// `assert!` rejects the PK column itself, and `col_index_in`'s own const-eval
/// panic covers a renamed column — both at compile time.
pub(crate) const fn pay_index_in(cols: &[WireSysCol], name: &str) -> usize {
    pay_index_in_keyed(cols, name, LEADING_COL_PK)
}

/// [`pay_index_in`] for a family whose key is its leading `pk_cols` columns —
/// the payload index is the column index less the key arity, so a compound key
/// shifts every payload slot by more than one. Takes the family's own `pk_cols`
/// rather than a count so the two cannot be stated apart.
pub(crate) const fn pay_index_in_keyed(cols: &[WireSysCol], name: &str, pk_cols: &[u32]) -> usize {
    let ci = col_index_in(cols, name);
    assert!(ci >= pk_cols.len(), "a PK column has no payload index");
    ci - pk_cols.len()
}

/// Region index of the payload column named `name`: the fixed regions,
/// then the payload columns in schema order. Same leading-PK restriction as
/// [`pay_index_in`].
pub(crate) const fn payload_region_in(cols: &[WireSysCol], name: &str) -> usize {
    crate::REG_PAYLOAD_START + pay_index_in(cols, name)
}

// Every system table's column shape is defined once, here, and reached through
// `SYS_FAMILIES` (by `sys_family_index(id)`), which pairs each shape with the
// key that goes with it: the engine builds its `SchemaDescriptor`s and the COL_TAB
// self-description rows from a family, the client builds its `Schema`s. The two
// must agree on both halves, and a disagreement on the key is a `pk_stride`
// mismatch the wire decode rejects — so neither half is offered separately for a
// consumer to pair up itself.

/// The primary key of every system table whose key is its single leading column.
pub(crate) const LEADING_COL_PK: &[u32] = &[0];

pub(crate) const SCHEMA_TAB_COLS: &[WireSysCol] = &[
    col("schema_id", TypeCode::U64, false),
    col("name", TypeCode::String, false),
];

pub(crate) const TABLE_TAB_COLS: &[WireSysCol] = &[
    col("table_id", TypeCode::U64, false),
    col("schema_id", TypeCode::U64, false),
    col("name", TypeCode::String, false),
    // Packed PK column list (`pack_pk_cols`).
    col("pk_col_idx", TypeCode::U64, false),
    // See `TableProps::pack` for the bit layout.
    col("flags", TypeCode::U64, false),
];

pub(crate) const VIEW_TAB_COLS: &[WireSysCol] = &[
    col("view_id", TypeCode::U64, false),
    col("schema_id", TypeCode::U64, false),
    col("name", TypeCode::String, false),
    // Packed view-PK column list (`pack_pk_cols`).
    col("pk_col_idx", TypeCode::U64, false),
    // `CREATE VIEW … WITH (capacity = …)`, in bytes; `0` is unbounded. Presence
    // of a capacity *is* the bounded classification — a second flag word could
    // only ever disagree with it. The engine reads it at boot from the replayed
    // rows; nothing else records it.
    col("capacity_bytes", TypeCode::U64, false),
    // `CREATE VIEW … WITH (delta = …)`, in bytes; `0` is no feed. Presence of a
    // budget *is* the fed classification, exactly as `capacity_bytes` above.
    // Orthogonal to it: the two are refused together, but by a rule, not by the
    // encoding. The engine reads it at boot from the replayed rows; nothing else
    // records it.
    col("delta_bytes", TypeCode::U64, false),
    // The user view this row is an internal chain segment of; `0` is a user
    // view. A column rather than a name prefix, because the precheck can
    // validate it against a forger. Appended, not inserted: the `RELTAB_*`
    // constants below pin `name` to the same slot in TABLE_TAB and VIEW_TAB.
    col("owner_view_id", TypeCode::U64, false),
];

pub(crate) const COL_TAB_COLS: &[WireSysCol] = &[
    col("column_id", TypeCode::U64, false),
    col("owner_id", TypeCode::U64, false),
    col("owner_kind", TypeCode::U64, false),
    col("col_idx", TypeCode::U64, false),
    col("name", TypeCode::String, false),
    col("type_code", TypeCode::U64, false),
    col("is_nullable", TypeCode::U64, false),
    col("fk_table_id", TypeCode::U64, false),
    col("fk_col_idx", TypeCode::U64, false),
    // is_serial marker: 1 for a SERIAL PK column, else 0. Lets a connection
    // that only fetched the schema distinguish an auto-assigned SERIAL PK from
    // a user-supplied non-null integer PK. Stored verbatim by the engine.
    col("is_serial", TypeCode::U64, false),
    // is_hidden marker: 1 for a hidden key slot (synthetic view keys and
    // unprojected passthrough PKs), else 0. Echoed into reply schema blocks as
    // META_FLAG_HIDDEN; the engine never branches on it.
    col("is_hidden", TypeCode::U64, false),
];

pub(crate) const IDX_TAB_COLS: &[WireSysCol] = &[
    col("index_id", TypeCode::U64, false),
    col("owner_id", TypeCode::U64, false),
    // Holds `pack_pk_cols(&col_indices)` for every row (single- and
    // multi-column indexes alike); decoded via `unpack_pk_cols`.
    col("source_col_idx", TypeCode::U64, false),
    col("name", TypeCode::String, false),
    // See `IndexProps::pack` for the bit layout. One word rather than a bool
    // column per property: the family is scanned whole by `DROP INDEX` and the
    // planner's name/column probe, so a second `U64` would widen every row for
    // one bit.
    col("flags", TypeCode::U64, false),
];

/// The reply **schema block**'s column shape. Not a system table — it is the
/// per-message block describing a reply's columns — but it is a wire schema both
/// ends exchange, so it belongs with them. Its codec is
/// [`crate::schema_block`], which is what both ends actually run; the `flags`
/// word is packed by [`crate::pack_col_meta_flags`].
pub(crate) const META_SCHEMA_COLS: &[WireSysCol] = &[
    col("col_idx", TypeCode::U64, false),
    col("type_code", TypeCode::U64, false),
    col("flags", TypeCode::U64, false),
    col("name", TypeCode::String, false),
];

pub(crate) const SEQ_TAB_COLS: &[WireSysCol] = &[
    col("seq_id", TypeCode::U64, false),
    col("next_val", TypeCode::U64, false),
];

// The circuit table uses a real compound primary key `(view_id, node_id)`
// instead of hand-packing both halves into one U128 column. PK = columns [0, 1].
//
// `opcode` and `source_table` stay scannable columns so `for_each_scan_edge` can
// rebuild the dependency map without the `params` codec. `input_0`/`input_1` are
// port-indexed, so the column name *is* the port; keeping them out of `params`
// leaves a parameterless node with no blob cell at all.
pub const CIRCUIT_NODES_COLS: &[WireSysCol] = &[
    col("view_id", TypeCode::U64, false),
    col("node_id", TypeCode::U64, false),
    col("opcode", TypeCode::U64, false),
    col("source_table", TypeCode::U64, true),
    col("input_0", TypeCode::U64, true),
    col("input_1", TypeCode::U64, true),
    // The node's per-opcode parameters (`encode_op_node`); NULL for an operator
    // that carries none.
    col("params", TypeCode::Blob, true),
];

// ---------------------------------------------------------------------------
// Catalog column positions
// ---------------------------------------------------------------------------
//
// Resolved by name from the lists above, so a dropped or renamed column fails
// the build instead of shifting a literal. Both the engine and the client read
// catalog batches and must agree on every position, so they are stated once,
// here, rather than derived again in each crate.
//
// `*_PAY_*` index the payload region — the space every engine-side read uses.
// `*_COL_*` index a full schema, PK slot included: the shape of the client's
// `ZSetBatch::columns[..]`, and nothing else reads it.

pub const SCHEMATAB_COL_NAME: usize = col_index_in(SCHEMA_TAB_COLS, "name");
pub const SCHEMATAB_PAY_NAME: usize = pay_index_in(SCHEMA_TAB_COLS, "name");

pub const TABTAB_COL_FLAGS: usize = col_index_in(TABLE_TAB_COLS, "flags");
pub const TABTAB_PAY_PK_COL_IDX: usize = pay_index_in(TABLE_TAB_COLS, "pk_col_idx");
pub const TABTAB_PAY_FLAGS: usize = pay_index_in(TABLE_TAB_COLS, "flags");

pub const VIEWTAB_PAY_PK_COL_IDX: usize = pay_index_in(VIEW_TAB_COLS, "pk_col_idx");
pub const VIEWTAB_PAY_CAPACITY: usize = pay_index_in(VIEW_TAB_COLS, "capacity_bytes");
pub const VIEWTAB_PAY_DELTA: usize = pay_index_in(VIEW_TAB_COLS, "delta_bytes");
pub const VIEWTAB_COL_OWNER_VIEW_ID: usize = col_index_in(VIEW_TAB_COLS, "owner_view_id");
pub const VIEWTAB_PAY_OWNER_VIEW_ID: usize = pay_index_in(VIEW_TAB_COLS, "owner_view_id");

// `RELTAB_*`: an address valid in TABLE_TAB and VIEW_TAB alike, for the readers
// that take either family through one code path. A column at the same slot in
// both but read through neither stays two constants.

/// `name`'s index in two column lists that must agree on it. Const-eval fails if
/// they do not, so the shared address cannot be claimed for a column that moved.
const fn shared_col_index(a: &[WireSysCol], b: &[WireSysCol], name: &str) -> usize {
    let i = col_index_in(a, name);
    assert!(i == col_index_in(b, name), "the two families disagree on this column");
    i
}

/// [`shared_col_index`] in the payload index space.
const fn shared_pay_index(a: &[WireSysCol], b: &[WireSysCol], name: &str) -> usize {
    let i = pay_index_in(a, name);
    assert!(i == pay_index_in(b, name), "the two families disagree on this column");
    i
}

pub const RELTAB_COL_SCHEMA_ID: usize = shared_col_index(TABLE_TAB_COLS, VIEW_TAB_COLS, "schema_id");
pub const RELTAB_COL_NAME: usize = shared_col_index(TABLE_TAB_COLS, VIEW_TAB_COLS, "name");
pub const RELTAB_PAY_SCHEMA_ID: usize = shared_pay_index(TABLE_TAB_COLS, VIEW_TAB_COLS, "schema_id");
pub const RELTAB_PAY_NAME: usize = shared_pay_index(TABLE_TAB_COLS, VIEW_TAB_COLS, "name");

pub const COLTAB_PAY_OWNER_ID: usize = pay_index_in(COL_TAB_COLS, "owner_id");
pub const COLTAB_PAY_OWNER_KIND: usize = pay_index_in(COL_TAB_COLS, "owner_kind");
pub const COLTAB_PAY_COL_IDX: usize = pay_index_in(COL_TAB_COLS, "col_idx");
pub const COLTAB_PAY_NAME: usize = pay_index_in(COL_TAB_COLS, "name");
pub const COLTAB_PAY_TYPE_CODE: usize = pay_index_in(COL_TAB_COLS, "type_code");
pub const COLTAB_PAY_IS_SERIAL: usize = pay_index_in(COL_TAB_COLS, "is_serial");
pub const COLTAB_PAY_FK_TABLE_ID: usize = pay_index_in(COL_TAB_COLS, "fk_table_id");
pub const COLTAB_PAY_FK_COL_IDX: usize = pay_index_in(COL_TAB_COLS, "fk_col_idx");
pub const COLTAB_PAY_IS_NULLABLE: usize = pay_index_in(COL_TAB_COLS, "is_nullable");
pub const COLTAB_PAY_IS_HIDDEN: usize = pay_index_in(COL_TAB_COLS, "is_hidden");

pub const CIRCNODES_PAY_OPCODE: usize = pay_index_in_keyed(CIRCUIT_NODES_COLS, "opcode", CIRCUIT_FAMILY_PK);
pub const CIRCNODES_PAY_SOURCE_TABLE: usize = pay_index_in_keyed(CIRCUIT_NODES_COLS, "source_table", CIRCUIT_FAMILY_PK);
pub const CIRCNODES_PAY_INPUT_0: usize = pay_index_in_keyed(CIRCUIT_NODES_COLS, "input_0", CIRCUIT_FAMILY_PK);
pub const CIRCNODES_PAY_INPUT_1: usize = pay_index_in_keyed(CIRCUIT_NODES_COLS, "input_1", CIRCUIT_FAMILY_PK);
pub const CIRCNODES_PAY_PARAMS: usize = pay_index_in_keyed(CIRCUIT_NODES_COLS, "params", CIRCUIT_FAMILY_PK);

pub const IDXTAB_COL_SOURCE_COLS: usize = col_index_in(IDX_TAB_COLS, "source_col_idx");
pub const IDXTAB_COL_NAME: usize = col_index_in(IDX_TAB_COLS, "name");
pub const IDXTAB_PAY_OWNER_ID: usize = pay_index_in(IDX_TAB_COLS, "owner_id");
pub const IDXTAB_PAY_SOURCE_COLS: usize = pay_index_in(IDX_TAB_COLS, "source_col_idx");
pub const IDXTAB_PAY_NAME: usize = pay_index_in(IDX_TAB_COLS, "name");
pub const IDXTAB_PAY_FLAGS: usize = pay_index_in(IDX_TAB_COLS, "flags");

pub const SEQTAB_PAY_VALUE: usize = pay_index_in(SEQ_TAB_COLS, "next_val");

// ---------------------------------------------------------------------------
// Stored-shape digest
// ---------------------------------------------------------------------------

/// FNV-1a over one family's column shape — each column's name, type and
/// nullability, then its key columns.
const fn fold_family(mut h: u64, cols: &[WireSysCol], pk: &[u32]) -> u64 {
    const PRIME: u64 = 0x100_0000_01b3;
    let mut i = 0;
    while i < cols.len() {
        let name = cols[i].name.as_bytes();
        let mut j = 0;
        while j < name.len() {
            h = (h ^ name[j] as u64).wrapping_mul(PRIME);
            j += 1;
        }
        h = (h ^ cols[i].type_code as u64).wrapping_mul(PRIME);
        h = (h ^ cols[i].nullable as u64).wrapping_mul(PRIME);
        i += 1;
    }
    let mut k = 0;
    while k < pk.len() {
        h = (h ^ pk[k] as u64).wrapping_mul(PRIME);
        k += 1;
    }
    h
}

/// Digest of every system family's stored shape, and of the blob layouts stored
/// *inside* one. Shards and SAL frames are decoded against the *live* schema, so
/// a shape change silently reinterprets an existing data directory unless a
/// format word rejects it first. Both words do, by different means: the engine
/// XORs this into its `SHARD_VERSION`, which therefore moves on its own, while
/// [`crate::wal::WAL_FORMAT_VERSION`] is hand-bumped and its test pins the pair,
/// so a change reaching only the SAL side fails until that bump happens.
///
/// The column half is folded straight over [`SYS_FAMILIES`], so a family added
/// there is covered without a second edit. The version consts cover what that
/// fold cannot see: a `Blob` column's bytes are as durable as its neighbours',
/// but its internal layout is invisible to a fold over names and types.
pub const SYS_SCHEMA_DIGEST: u64 = {
    const PRIME: u64 = 0x100_0000_01b3;
    let mut h = 0xcbf2_9ce4_8422_2325;
    let mut i = 0;
    while i < SYS_FAMILIES.len() {
        h = fold_family(h, SYS_FAMILIES[i].cols, SYS_FAMILIES[i].pk_cols);
        i += 1;
    }
    h = (h ^ crate::circuit::CIRCUIT_PARAMS_VERSION as u64).wrapping_mul(PRIME);
    h = (h ^ crate::expr::EXPR_BLOB_VERSION as u64).wrapping_mul(PRIME);
    h
};

// ---------------------------------------------------------------------------
// System table IDs
// ---------------------------------------------------------------------------

pub const SCHEMA_TAB: u64 = 1;
pub const TABLE_TAB: u64 = 2;
pub const VIEW_TAB: u64 = 3;
pub const COL_TAB: u64 = 4;
pub const IDX_TAB: u64 = 5;
pub const SEQ_TAB: u64 = 7;
pub const CIRCUIT_NODES_TAB: u64 = 11;

/// The circuit family's compound primary key: columns `(view_id, node_id)`.
pub const CIRCUIT_FAMILY_PK: &[u32] = &[0, 1];

/// One system family's wire identity: the table id both sides address it by,
/// its name, and the column shape they each build their schema type from.
/// Grouping these means a caller holding a table id can *derive* the shape
/// instead of being handed a separately-chosen one that may not match.
pub struct WireSysFamily {
    pub id: u64,
    /// The table's name, and its subdirectory under the engine's catalog root.
    pub name: &'static str,
    pub cols: &'static [WireSysCol],
    pub pk_cols: &'static [u32],
}

const fn fam(id: u64, name: &'static str, cols: &'static [WireSysCol], pk_cols: &'static [u32]) -> WireSysFamily {
    WireSysFamily { id, name, cols, pk_cols }
}

/// Every system family, in the order both sides index them by.
pub const SYS_FAMILIES: &[WireSysFamily] = &[
    fam(SCHEMA_TAB, "_schemas", SCHEMA_TAB_COLS, LEADING_COL_PK),
    fam(TABLE_TAB, "_tables", TABLE_TAB_COLS, LEADING_COL_PK),
    fam(VIEW_TAB, "_views", VIEW_TAB_COLS, LEADING_COL_PK),
    fam(COL_TAB, "_columns", COL_TAB_COLS, LEADING_COL_PK),
    fam(IDX_TAB, "_indices", IDX_TAB_COLS, LEADING_COL_PK),
    fam(SEQ_TAB, "_sequences", SEQ_TAB_COLS, LEADING_COL_PK),
    fam(
        CIRCUIT_NODES_TAB,
        "_circuit_nodes",
        CIRCUIT_NODES_COLS,
        CIRCUIT_FAMILY_PK,
    ),
];

/// Position of family `id` in [`SYS_FAMILIES`], or `None` for a non-family id.
pub const fn sys_family_index(id: u64) -> Option<usize> {
    let mut i = 0;
    while i < SYS_FAMILIES.len() {
        if SYS_FAMILIES[i].id == id {
            return Some(i);
        }
        i += 1;
    }
    None
}

pub const FIRST_USER_TABLE_ID: u64 = 16;
pub const FIRST_USER_SCHEMA_ID: u64 = 3;

/// A tripwire on durable relation-id allocation, not a live limit: reaching it
/// needs 2^31 durable CREATEs. Durable relations live in
/// `[FIRST_USER_TABLE_ID, RELATION_ID_CEILING)`, and the engine rejects any id at
/// or above it where one enters its relation DAG.
///
/// A relation id narrows to a `u32` at every physical boundary (the SAL group
/// header, a store's `table_id`, a wire batch, a shard file name). `1<<31` sits
/// short of that ceiling, so an id below it round-trips all of them exactly.
pub const RELATION_ID_CEILING: u64 = 1 << 31;

pub const OWNER_KIND_TABLE: u64 = 0;
pub const OWNER_KIND_VIEW: u64 = 1;

// ---------------------------------------------------------------------------
// COL_TAB primary-key packing
// ---------------------------------------------------------------------------

/// Bit width of the column-index field in a packed COL_TAB PK.
pub(crate) const COL_ID_IDX_BITS: u32 = 9;

/// Pack an owner (table/view) id and column index into the COL_TAB PK word:
/// `(owner_id << 9) | col_idx`. Rejects a column index that overflows its
/// 9-bit field or an owner id that overflows the remaining 55 bits — either
/// would silently alias another column's record.
pub fn pack_col_id(owner_id: u64, col_idx: u64) -> Result<u64, String> {
    if col_idx >= (1 << COL_ID_IDX_BITS) {
        return Err(format!(
            "column index {col_idx} exceeds maximum {}",
            (1u64 << COL_ID_IDX_BITS) - 1
        ));
    }
    if owner_id > (u64::MAX >> COL_ID_IDX_BITS) {
        return Err(format!(
            "owner_id {owner_id} exceeds {}-bit maximum for column ID packing",
            64 - COL_ID_IDX_BITS
        ));
    }
    Ok((owner_id << COL_ID_IDX_BITS) | col_idx)
}

/// Inverse of [`pack_col_id`]: `(owner_id, col_idx)`.
pub const fn unpack_col_id(packed: u64) -> (u64, u64) {
    (packed >> COL_ID_IDX_BITS, packed & ((1 << COL_ID_IDX_BITS) - 1))
}

// ---------------------------------------------------------------------------
// Identifier validation (shared between the SQL planner and the engine)
// ---------------------------------------------------------------------------

/// The character set every stored catalog name is drawn from. Public because the
/// engine applies the same charset at its own trust boundary without taking the
/// rest of [`validate_user_identifier`]'s client-side policy.
pub fn is_valid_ident_char(ch: u8) -> bool {
    ch.is_ascii_alphanumeric() || ch == b'_'
}

/// Reject empty names, names starting with `_` (reserved for the engine's own
/// internal relation and index names) and names outside `[A-Za-z0-9_]`.
pub fn validate_user_identifier(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Identifier cannot be empty".into());
    }
    if name.as_bytes()[0] == b'_' {
        return Err(format!(
            "User identifiers cannot start with '_' (reserved for system prefix): {name}"
        ));
    }
    for &ch in name.as_bytes() {
        if !is_valid_ident_char(ch) {
            return Err(format!("Identifier contains invalid characters: {name}"));
        }
    }
    Ok(())
}

/// [`validate_user_identifier`], then the canonical stored form: an ASCII
/// lowercase fold. Validating first is what makes the fold safe — over
/// `[A-Za-z0-9_]` it is injective on the classes SQL already considers equal,
/// which it is not over arbitrary bytes.
///
/// The single definition of catalog-name case-insensitivity: every user-supplied
/// relation, schema or index name crosses it on the way to a catalog row.
pub fn canonical_identifier(name: &str) -> Result<String, String> {
    validate_user_identifier(name)?;
    Ok(name.to_ascii_lowercase())
}

/// The canonical `"schema.relation"` key, from names **already canonical**.
///
/// It must not fold: a mixed-case name that slipped past the engine's
/// `reject_non_canonical` would then key the cache while mismatching the store,
/// turning a loud rejection into silent divergence. `.` is outside
/// [`is_valid_ident_char`], so no pair can produce another pair's key.
pub fn qualified_key(schema_name: &str, name: &str) -> String {
    let mut q = String::with_capacity(schema_name.len() + 1 + name.len());
    q.push_str(schema_name);
    q.push('.');
    q.push_str(name);
    q
}

/// One column of a view's delta-feed reply schema. The variant carries the
/// key/payload split, so neither side re-derives it from a position.
pub enum DeltaCol {
    /// The synthesized `_tick` U64 stamp — a key column, and the first.
    Tick,
    /// The view's column at this index, as a key column.
    Key(usize),
    /// The view's column at this index, as payload.
    Payload(usize),
}

/// The columns of a view's delta-feed reply schema in output order: the `_tick`
/// stamp, then the view's PK columns in PK order, then its payload columns in
/// schema order.
///
/// The permutation needs no type, name or nullability — only `(pk_indices,
/// num_columns)` — so it is stated once here and applied by the client (building
/// a `Schema`) and the engine (building a `SchemaDescriptor`) alike, each keeping
/// its own overflow behaviour.
pub fn delta_schema_order(pk_indices: &[u32], num_columns: usize) -> Vec<DeltaCol> {
    let mut out = Vec::with_capacity(num_columns + 1);
    out.push(DeltaCol::Tick);
    out.extend(pk_indices.iter().map(|&i| DeltaCol::Key(i as usize)));
    out.extend(
        (0..num_columns)
            .filter(|&i| !pk_indices.contains(&(i as u32)))
            .map(DeltaCol::Payload),
    );
    out
}

// ---------------------------------------------------------------------------
// Schema sizing caps
// ---------------------------------------------------------------------------

/// Maximum number of columns (PK + payload) in any table or view schema.
/// Capped at 65 by the row-major null bitmap: each row stores one u64 word
/// with one bit per payload column, so payload columns ≤ 64.
pub const MAX_COLUMNS: usize = 65;

/// Sizing cap for the compound-PK column list.
///
/// Set to 5 to cover the user-facing PK cap (4 columns, planner-enforced)
/// plus one indexed-column prefix used by secondary index schemas — modeled
/// as `(indexed_col, src_pk_0, …, src_pk_{k-1})`.
pub const MAX_PK_COLUMNS: usize = 5;

/// Maximum byte width of a PK region per row. Product of `MAX_PK_COLUMNS`
/// and the per-column ceiling (16 == max wire stride of any type valid as a
/// PK column — U128, UUID, I128; STRING and BLOB are rejected by schema
/// validation). Auto-tracks growth of `MAX_PK_COLUMNS`.
pub const MAX_PK_BYTES: usize = MAX_PK_COLUMNS * 16;

// ---------------------------------------------------------------------------
// Compound-PK list encoding for the persisted `TABLE_TAB.pk_col_idx` u64.
//
// The only form. A word without the tag is malformed, not a second spelling:
//        bit 63        : PK_LIST_PACKED_FLAG
//        bit 62        : reserved
//        bits [0..4)   : decoded count (1..=PK_LIST_MAX_COLS valid; larger
//                        counts are reserved for tests / malformed payloads)
//        bits [4+7i..) : i-th column index, 7 bits each
//
// Both client (gnitz-core) and engine (gnitz-server catalog) MUST share this
// encoder/decoder so they cannot drift on the encoding.
// ---------------------------------------------------------------------------

/// Capacity of the persisted PK-list codec (`pack_pk_cols` / `PkColList`):
/// the most PK columns a table or view PK may declare. The codec, both
/// validators, and the two planner admission caps all derive from this one
/// constant — raise or lower it and every dependent site follows, with the
/// static guards below catching a value the encoding or schema layout can't
/// hold.
///
/// Distinct from `MAX_PK_COLUMNS` (5), which sizes the engine's in-memory PK
/// arrays and reserves one extra slot for the secondary-index column prefix.
/// Keeping `PK_LIST_MAX_COLS < MAX_PK_COLUMNS` leaves that slot free for any
/// table.
pub const PK_LIST_MAX_COLS: usize = 4;

/// Width of the packed decoded-count field (bits `[0..4)`).
const PK_LIST_COUNT_BITS: u32 = 4;
/// Width of each packed column-index field (bits `[4 + 7i..)`), and so the
/// exclusive ceiling on a column index the list can carry.
const PK_LIST_COL_BITS: u32 = 7;
const PK_LIST_COL_MAX: u32 = (1 << PK_LIST_COL_BITS) - 1;
/// The exclusive ceiling on a column index a packed list can carry — the bound a
/// [`validate_pk_col_list`] caller passes when it holds no column count of its own.
pub const PK_LIST_COL_LIMIT: usize = 1 << PK_LIST_COL_BITS;

/// The PK-list arity rule: `1..=PK_LIST_MAX_COLS` columns. One predicate behind
/// every spelling of it — the panicking constructors, the fallible decode, and
/// the validator — so they cannot disagree on the bound.
#[inline]
pub(crate) const fn pk_list_arity_ok(n: usize) -> bool {
    n >= 1 && n <= PK_LIST_MAX_COLS
}

// The packed u64 lays the decoded count in bits [0..4) and each column index in
// a 7-bit field at bit 4 + 7*i; the packed flag occupies bit 63. Guard the
// ceilings at compile time so bumping PK_LIST_MAX_COLS past what the encoding
// (or the index-prefix reservation) can hold fails to build instead of
// silently corrupting the catalog word.
const _: () = assert!(PK_LIST_MAX_COLS >= 1);
const _: () = assert!(
    PK_LIST_MAX_COLS < (1 << PK_LIST_COUNT_BITS),
    "PK_LIST_MAX_COLS overflows the packed count field"
);
const _: () = assert!(
    PK_LIST_COUNT_BITS as usize + PK_LIST_COL_BITS as usize * PK_LIST_MAX_COLS <= 62,
    "PK_LIST_MAX_COLS overflows the packed u64 column region" // bits 62/63 are flags
);
const _: () = assert!(
    PK_LIST_MAX_COLS < MAX_PK_COLUMNS, // leave the index-prefix slot
    "PK_LIST_MAX_COLS leaves no MAX_PK_COLUMNS slot for the secondary-index prefix"
);
const _: () = assert!(
    MAX_COLUMNS <= 1 << PK_LIST_COL_BITS,
    "a schema column index no longer fits the packed 7-bit field"
);

/// Bit 63: the tag whose absence [`unpack_pk_cols`] refuses. A `seek_col_idx`
/// means different things per message kind, so the decode insists on a tag
/// rather than reading a shape.
pub const PK_LIST_PACKED_FLAG: u64 = 1 << 63;

/// The `seek_col_idx` value naming the relation's own PK store — see
/// [`probe_key_columns`].
pub const PROBE_KEYSPACE_PK: u64 = 0;

/// The keyspace a `HasPk` group's `seek_col_idx` names: `None` for the
/// relation's own PK store, `Some(packed column list)` for a secondary index.
///
/// [`pack_pk_cols`] lays a count of `1..=PK_LIST_MAX_COLS` in the low bits, so a
/// real column list is never [`PROBE_KEYSPACE_PK`]. Both ends of that hop decode
/// through here, so the sentinel has one spelling.
#[inline]
pub fn probe_key_columns(seek_col_idx: u64) -> Option<u64> {
    (seek_col_idx != PROBE_KEYSPACE_PK).then_some(seek_col_idx)
}

/// A PK column list, `1..=PK_LIST_MAX_COLS` entries, inline. Constructing one is
/// the arity check, so a holder never gates on the count. `from_slice` zero-fills
/// the tail, so the derived `Hash` and `PartialEq` see no padding and a list can
/// key a map.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct PkColList {
    cols: [u32; PK_LIST_MAX_COLS],
    len: usize,
}

impl PkColList {
    /// Construct from a column-index slice. Panics on an out-of-range length
    /// (`1..=PK_LIST_MAX_COLS`) — like `pack_pk_cols`, callers must validate the
    /// arity before constructing, because a silent clamp here would desync the
    /// list from the persisted/packed form it round-trips with. Wire input goes
    /// through [`unpack_pk_cols`], which returns that arity as an `Err`.
    pub fn from_slice(cols: &[u32]) -> Self {
        assert!(
            pk_list_arity_ok(cols.len()),
            "PkColList::from_slice: count {} out of range 1..={PK_LIST_MAX_COLS}",
            cols.len(),
        );
        let mut arr = [0u32; PK_LIST_MAX_COLS];
        arr[..cols.len()].copy_from_slice(cols);
        PkColList { cols: arr, len: cols.len() }
    }
    /// The column indices, in list order.
    pub fn as_slice(&self) -> &[u32] {
        &self.cols[..self.len]
    }
}

/// The `Err`-returning form of [`pack_pk_cols`]' panicking contract, plus the
/// no-duplicates rule both consumers (table PKs and index column lists) share:
/// count in `1..=PK_LIST_MAX_COLS`, every index below `ncols`, no repeated
/// column. Call this at user-input boundaries so `pack_pk_cols` and
/// `PkColList::from_slice` can never panic downstream.
///
/// `ncols` is the source relation's column count, or [`PK_LIST_COL_LIMIT`] — the
/// packed field's own bound — for a caller that holds no column count.
pub fn validate_pk_col_list(cols: &[u32], ncols: usize) -> Result<(), String> {
    // The rule itself is `validate_pk_indices`. Only the wording differs — these
    // lists are also secondary-index column lists, which "primary key ..." would
    // misname.
    crate::validate_pk_indices(cols, ncols).map_err(|rule| rule.for_role(crate::PkListRole::ColumnList))
}

/// Validate a `CLUSTER BY` column list against the table's PK, returning the
/// distribution prefix length `k` (`= cols.len()`) when `cols` is exactly the
/// PK's leading prefix in PK order. The distribution key is constrained to a
/// **leading PK prefix** so write-side routing is a pure byte-slice of the OPK
/// region (no gather, no separate packer). No type/null/width re-checks — those
/// are inherited from the PK validation, since `dist ⊆ pk`. Shared by the SQL
/// planner (pre-engine `CLUSTER BY` check) so the surface error names PK column
/// order.
pub fn validate_dist_prefix(pk: &[u32], cols: &[u32]) -> Result<usize, String> {
    if cols.is_empty() || cols.len() > pk.len() {
        return Err(format!(
            "CLUSTER BY expects 1..={} leading PRIMARY KEY columns, got {}",
            pk.len(),
            cols.len()
        ));
    }
    if let Some(c) = cols.iter().find(|c| !pk.contains(c)) {
        return Err(format!("CLUSTER BY column {c} is not a PRIMARY KEY column"));
    }
    if cols != &pk[..cols.len()] {
        return Err("CLUSTER BY columns must be a leading prefix of the PRIMARY KEY, \
                    in PK order; reorder the PK so the distribution column(s) lead"
            .into());
    }
    Ok(cols.len())
}

/// Pack a PK column-index list into the persisted `u64` form. Panics on a
/// violated contract because a silent truncation here corrupts the
/// catalog encoding; callers (client + engine) must reject out-of-range
/// lists before calling this (see [`validate_pk_col_list`]).
pub fn pack_pk_cols(pk_cols: &[u32]) -> u64 {
    assert!(
        pk_list_arity_ok(pk_cols.len()),
        "pack_pk_cols: count {} out of range 1..={PK_LIST_MAX_COLS}",
        pk_cols.len(),
    );
    let mut v = pk_cols.len() as u64; // the count field
    for (i, &c) in pk_cols.iter().enumerate() {
        assert!(c <= PK_LIST_COL_MAX, "pack_pk_cols: column index {c} exceeds its field");
        v |= (c as u64) << (PK_LIST_COUNT_BITS + PK_LIST_COL_BITS * i as u32);
    }
    v | PK_LIST_PACKED_FLAG
}

/// Decode the persisted `u64` PK-list form. The one crossing from wire bits into
/// [`PkColList`], and so where an untagged word or a crafted count is refused.
pub fn unpack_pk_cols(packed: u64) -> Result<PkColList, crate::PkRule> {
    if packed & PK_LIST_PACKED_FLAG == 0 {
        return Err(crate::PkRule::NotPacked);
    }
    let n = (packed & ((1 << PK_LIST_COUNT_BITS) - 1)) as usize;
    if !pk_list_arity_ok(n) {
        return Err(if n == 0 {
            crate::PkRule::Empty
        } else {
            crate::PkRule::TooManyColumns { count: n }
        });
    }
    let mut cols = [0u32; PK_LIST_MAX_COLS];
    for (i, slot) in cols.iter_mut().enumerate().take(n) {
        *slot = ((packed >> (PK_LIST_COUNT_BITS + PK_LIST_COL_BITS * i as u32)) & PK_LIST_COL_MAX as u64) as u32;
    }
    Ok(PkColList { cols, len: n })
}

/// Width of one `seek_by_index` key slot on the wire: a native `u128`, LE.
pub(crate) const INDEX_KEY_SLOT: usize = 16;

/// Pack K native index-key values into the `PkBuf` a `seek_by_index` request
/// carries — one 16-byte LE slot each, which [`crate::control::split_ctrl_key`]
/// then routes as slot 0 → `seek_pk` and slots 1..K → `seek_pk_extra`. A prefix
/// seek supplies K < the
/// index's arity. Returns the packed bytes; K is bounded by [`PK_LIST_MAX_COLS`],
/// so at most `PK_LIST_MAX_COLS * INDEX_KEY_SLOT` of the buffer is ever written.
pub fn pack_index_key_slots(key_vals: &[u128]) -> ([u8; MAX_PK_BYTES], usize) {
    assert!(
        pk_list_arity_ok(key_vals.len()),
        "pack_index_key_slots: count {} out of range 1..={PK_LIST_MAX_COLS}",
        key_vals.len(),
    );
    let mut buf = [0u8; MAX_PK_BYTES];
    for (i, &v) in key_vals.iter().enumerate() {
        buf[i * INDEX_KEY_SLOT..(i + 1) * INDEX_KEY_SLOT].copy_from_slice(&v.to_le_bytes());
    }
    (buf, key_vals.len() * INDEX_KEY_SLOT)
}

/// Recover the K key values a `seek_by_index` request packed, from the wire pair
/// `seek_pk` (slot 0) + `seek_pk_extra` (slots 1..K). Validates at the trust
/// boundary: a misaligned `extra` or a K beyond `arity` is rejected rather than
/// silently dropping trailing bytes or over-reading the index.
pub fn unpack_index_key_slots(seek_pk: u128, extra: &[u8], arity: usize) -> Result<PkKeyVals, String> {
    if !extra.len().is_multiple_of(INDEX_KEY_SLOT) {
        return Err(format!(
            "seek_by_index: key tail of {} bytes is not a multiple of {INDEX_KEY_SLOT}",
            extra.len()
        ));
    }
    let len = 1 + extra.len() / INDEX_KEY_SLOT;
    if len > arity.min(PK_LIST_MAX_COLS) {
        return Err(format!("seek_by_index: {len} key values exceed index arity {arity}"));
    }
    let mut vals = [0u128; PK_LIST_MAX_COLS];
    vals[0] = seek_pk;
    for (i, slot) in extra.as_chunks::<INDEX_KEY_SLOT>().0.iter().enumerate() {
        vals[i + 1] = u128::from_le_bytes(*slot);
    }
    Ok(PkKeyVals { vals, len })
}

/// The K native key values of a `seek_by_index` request, inline.
pub struct PkKeyVals {
    vals: [u128; PK_LIST_MAX_COLS],
    len: usize,
}

impl PkKeyVals {
    pub fn as_slice(&self) -> &[u128] {
        &self.vals[..self.len]
    }
}

// ---------------------------------------------------------------------------
// TABLE_TAB.flags layout — the single source of truth shared by the gnitz-core
// writer and the gnitz-server reader, so the bit packing cannot drift.
//
//   bit 0        replicated (TABLE_FLAG_REPLICATED) — full copy on every worker
//   bit 1        stream (TABLE_FLAG_STREAM) — storeless append-only ingestion point
//   bits [2..8)  reserved for future boolean flags
//   bits [8..16) distribution prefix length k (0 = default = full PK)
//
// `k` is byte-aligned so the boolean flag bits stay free for future flags
// without colliding with it. `replicated` and a non-default `k` are mutually
// exclusive (a CLUSTER BY prefix is meaningless when every worker holds the
// full copy); the packing cannot represent that constraint, so
// [`TableProps::validate`] carries it instead, and every layer that builds a
// `TableProps` calls it.
// ---------------------------------------------------------------------------

/// Bit 0: the table is **replicated** — every worker holds an identical full
/// copy (writes broadcast, reads single-source). Mutually exclusive with a
/// non-default `dist_prefix_len` — see [`TableProps::validate`], since this
/// packing cannot represent the constraint.
const TABLE_FLAG_REPLICATED: u64 = 1;
/// Bit 1: the table is a **stream** — a storeless, append-only ingestion point.
/// Independent of every other bit: a stream may be replicated or CLUSTER BY'd.
const TABLE_FLAG_STREAM: u64 = 1 << 1;
/// Bit position of the distribution-prefix-length byte in `TABLE_TAB.flags`.
const TABLE_FLAG_DIST_SHIFT: u32 = 8;
/// Mask for the distribution-prefix-length byte (one byte: 0..=255). An
/// explicit prefix is `1..=PK_LIST_MAX_COLS`, well within the byte; the full
/// byte is deliberate headroom.
const TABLE_FLAG_DIST_MASK: u64 = 0xFF;

/// `IDX_TAB.flags` bit 0: the index enforces uniqueness.
const INDEX_FLAG_UNIQUE: u64 = 1 << 0;
/// `IDX_TAB.flags` bit 1: the index is engine-internal — created by the FK
/// auto-index hook, not by any user statement, and undroppable through
/// `DROP INDEX`.
const INDEX_FLAG_INTERNAL: u64 = 1 << 1;

/// The logical content of `IDX_TAB.flags`. A struct rather than two bools
/// passed positionally, because a transposed call would silently make a user
/// index undroppable.
#[derive(Copy, Clone, Default, PartialEq, Eq, Debug)]
pub struct IndexProps {
    pub is_unique: bool,
    /// Engine-internal: the FK auto-index. **A client may never set this** — the
    /// catalog precheck rejects a `+1` carrying it, because an index flagged
    /// internal is one `DROP INDEX` refuses, so one frame would otherwise
    /// permanently deny a name in the index namespace.
    pub is_internal: bool,
}

impl IndexProps {
    /// Pack the persisted `IDX_TAB.flags` u64. Inverse of [`Self::from_flags`].
    #[inline]
    pub fn pack(self) -> u64 {
        (if self.is_unique { INDEX_FLAG_UNIQUE } else { 0 }) | (if self.is_internal { INDEX_FLAG_INTERNAL } else { 0 })
    }

    /// Decode a persisted `IDX_TAB.flags` u64. Reserved bits are ignored, so a
    /// word a later version widened still decodes the fields defined here.
    #[inline]
    pub fn from_flags(flags: u64) -> IndexProps {
        IndexProps {
            is_unique: flags & INDEX_FLAG_UNIQUE != 0,
            is_internal: flags & INDEX_FLAG_INTERNAL != 0,
        }
    }
}

/// The logical content of `TABLE_TAB.flags` — equivalently, the non-column
/// properties of a `CREATE TABLE`. A struct rather than positional arguments
/// because `replicated` and `stream` are independent booleans that a transposed
/// call would silently swap, and swapping them turns a durable table into one
/// whose rows a restart discards.
#[derive(Copy, Clone, Default, PartialEq, Eq, Debug)]
pub struct TableProps {
    /// Keep a full copy on every worker: writes broadcast, reads single-source.
    pub replicated: bool,
    /// A storeless, append-only ingestion point rather than a table: it holds no
    /// rows and nothing it ingests survives a restart.
    pub stream: bool,
    /// Hash-distribution prefix length `k`: rows are partitioned by the first `k`
    /// PK columns (`CLUSTER BY` the PK's leading prefix). `0` — the default —
    /// partitions by the full PK, and the schema constructor normalizes it to
    /// `k = |PK|`. The SQL planner validates `k` against the PK before packing.
    pub dist_prefix_len: usize,
}

impl TableProps {
    /// Pack the persisted `TABLE_TAB.flags` u64. Inverse of [`Self::from_flags`].
    #[inline]
    pub fn pack(self) -> u64 {
        (((self.dist_prefix_len as u64) & TABLE_FLAG_DIST_MASK) << TABLE_FLAG_DIST_SHIFT)
            | if self.replicated { TABLE_FLAG_REPLICATED } else { 0 }
            | if self.stream { TABLE_FLAG_STREAM } else { 0 }
    }

    /// Decode a persisted `TABLE_TAB.flags` u64. Reserved bits are ignored, so a
    /// word a later version widened still decodes the fields defined here.
    #[inline]
    pub fn from_flags(flags: u64) -> TableProps {
        TableProps {
            replicated: flags & TABLE_FLAG_REPLICATED != 0,
            stream: flags & TABLE_FLAG_STREAM != 0,
            dist_prefix_len: ((flags >> TABLE_FLAG_DIST_SHIFT) & TABLE_FLAG_DIST_MASK) as usize,
        }
    }

    /// The one rule the flags packing cannot make unrepresentable: `replicated`
    /// and a non-default `dist_prefix_len` are mutually exclusive. Beside the
    /// packing, so every layer holding a `TableProps` shares it, and
    /// `Result<_, String>` like [`validate_dist_prefix`], so each layer decorates
    /// the sentence rather than re-wording the rule.
    pub fn validate(&self) -> Result<(), String> {
        if self.replicated && self.dist_prefix_len != 0 {
            return Err(format!(
                "REPLICATED and CLUSTER BY are mutually exclusive: a replicated table keeps \
                 a full copy on every worker, so a hash-distribution prefix (k={}) is meaningless",
                self.dist_prefix_len
            ));
        }
        Ok(())
    }

    /// The second rule the flags packing cannot make unrepresentable:
    /// `dist_prefix_len` is a *leading PK prefix* length, so it cannot exceed
    /// `pk_len`. `TABLE_FLAG_DIST_MASK` is `0xFF`, so a forged flags word can
    /// carry 255. `0` is the persisted "default distribution" sentinel.
    pub fn validate_against_pk(&self, pk_len: usize) -> Result<(), String> {
        if self.dist_prefix_len > pk_len {
            return Err(format!(
                "distribution prefix length {} exceeds PK column count {pk_len}",
                self.dist_prefix_len
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/catalog.rs"]
mod tests;
