pub mod error;
pub mod connection;
pub mod ops;

pub use error::ClientError;
pub use connection::Connection;
pub use ops::{
    alloc_table_id, alloc_schema_id, push, scan,
    SCHEMA_TAB, TABLE_TAB, VIEW_TAB, COL_TAB, DEP_TAB, SEQ_TAB,
    FIRST_USER_TABLE_ID, FIRST_USER_SCHEMA_ID,
};
