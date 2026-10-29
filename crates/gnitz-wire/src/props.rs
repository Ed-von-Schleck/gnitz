//! The `TABLE_TAB.flags` word and a view's `WITH` options.

use std::num::NonZeroU64;

// ---------------------------------------------------------------------------
// TABLE_TAB.flags layout. Every bit outside the constants below is refused on
// decode. `replicated` and a non-default prefix length are mutually exclusive (a
// CLUSTER BY prefix is meaningless when every worker holds the full copy):
// [`TableDistribution`] cannot represent both, and
// [`TableDistribution::cluster_by`] refuses the pair for `TableProps::from_flags`
// and the SQL planner alike.
// ---------------------------------------------------------------------------

/// Bit 0: the table is **replicated** — every worker holds an identical full
/// copy (writes broadcast, reads single-source).
const TABLE_FLAG_REPLICATED: u64 = 1;
/// Bit 1: the table is a **stream** — a storeless, append-only ingestion point.
/// Independent of the distribution: a stream may be replicated or CLUSTER BY'd.
const TABLE_FLAG_STREAM: u64 = 1 << 1;
/// Bit 2: the table's single PK column is **SERIAL** — INSERT draws it from the
/// table's sequence.
const TABLE_FLAG_SERIAL: u64 = 1 << 2;
/// Bit 3: the table's store **compresses** its heaps — `WITH (compress = true)`.
const TABLE_FLAG_COMPRESS: u64 = 1 << 3;
/// Bit position of the distribution-prefix-length byte in `TABLE_TAB.flags`;
/// `0` there is the default, the full PK.
const TABLE_FLAG_DIST_SHIFT: u32 = 8;
/// Mask for the distribution-prefix-length byte (one byte: 0..=255). An
/// explicit prefix is `1..=PK_LIST_MAX_COLS`, well within the byte.
const TABLE_FLAG_DIST_MASK: u64 = 0xFF;
/// Every bit a `TABLE_TAB.flags` word may carry.
const TABLE_FLAGS_DEFINED: u64 = TABLE_FLAG_REPLICATED
    | TABLE_FLAG_STREAM
    | TABLE_FLAG_SERIAL
    | TABLE_FLAG_COMPRESS
    | (TABLE_FLAG_DIST_MASK << TABLE_FLAG_DIST_SHIFT);

/// How a table's rows are spread over the workers.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum TableDistribution {
    /// Hash-distributed by the first `prefix_len` PK columns; `0` = the full PK.
    Keyed { prefix_len: u8 },
    /// A full copy on every worker.
    Replicated,
}

impl TableDistribution {
    /// This distribution under a `CLUSTER BY` prefix of `prefix_len` PK columns,
    /// `0` for none: a keyed one takes the prefix, a replicated one refuses any.
    pub fn cluster_by(self, prefix_len: u8) -> Result<Self, String> {
        match (self, prefix_len) {
            (TableDistribution::Keyed { .. }, prefix_len) => Ok(TableDistribution::Keyed { prefix_len }),
            (TableDistribution::Replicated, 0) => Ok(TableDistribution::Replicated),
            (TableDistribution::Replicated, k) => Err(format!(
                "REPLICATED and CLUSTER BY are mutually exclusive: a replicated table keeps \
                 a full copy on every worker, so a hash-distribution prefix (k={k}) is meaningless"
            )),
        }
    }
}

/// Hash-distributed by the full PK.
impl Default for TableDistribution {
    fn default() -> Self {
        TableDistribution::Keyed { prefix_len: 0 }
    }
}

/// The logical content of `TABLE_TAB.flags`: the non-column properties of a
/// `CREATE TABLE`.
#[derive(Copy, Clone, Default, PartialEq, Eq, Debug)]
pub struct TableProps {
    /// A storeless, append-only ingestion point rather than a table: it holds no
    /// rows and nothing it ingests survives a restart.
    pub stream: bool,
    /// INSERT draws the table's single PK column from a sequence.
    pub serial: bool,
    /// Where the rows live. A `Keyed` prefix is `CLUSTER BY` the PK's leading
    /// prefix; the SQL planner validates it against the PK before packing.
    pub distribution: TableDistribution,
    /// The table's store codes its shards' heaps under a symbol table where
    /// that pays.
    pub compress: bool,
}

impl TableProps {
    /// Pack the persisted `TABLE_TAB.flags` u64. Inverse of `from_flags`.
    #[inline]
    pub fn pack(self) -> u64 {
        let dist = match self.distribution {
            TableDistribution::Keyed { prefix_len } => (prefix_len as u64) << TABLE_FLAG_DIST_SHIFT,
            TableDistribution::Replicated => TABLE_FLAG_REPLICATED,
        };
        let bit = |set: bool, flag: u64| if set { flag } else { 0 };
        dist | bit(self.stream, TABLE_FLAG_STREAM)
            | bit(self.serial, TABLE_FLAG_SERIAL)
            | bit(self.compress, TABLE_FLAG_COMPRESS)
    }

    /// Decode a persisted `TABLE_TAB.flags` u64, refusing a bit outside the
    /// defined set and a replicated table that also carries a prefix.
    pub(crate) fn from_flags(flags: u64) -> Result<TableProps, String> {
        if flags & !TABLE_FLAGS_DEFINED != 0 {
            return Err(format!("table flags {flags:#x} carry unknown bits"));
        }
        let prefix_len = ((flags >> TABLE_FLAG_DIST_SHIFT) & TABLE_FLAG_DIST_MASK) as u8;
        let distribution = if flags & TABLE_FLAG_REPLICATED == 0 {
            TableDistribution::default()
        } else {
            TableDistribution::Replicated
        }
        .cluster_by(prefix_len)?;
        Ok(TableProps {
            stream: flags & TABLE_FLAG_STREAM != 0,
            serial: flags & TABLE_FLAG_SERIAL != 0,
            distribution,
            compress: flags & TABLE_FLAG_COMPRESS != 0,
        })
    }

    /// The rules against a PK of `pk_len` columns that the flags word cannot
    /// encode.
    pub fn validate(&self, pk_len: usize) -> Result<(), String> {
        if let TableDistribution::Keyed { prefix_len } = self.distribution {
            if prefix_len as usize > pk_len {
                return Err(format!(
                    "distribution prefix length {prefix_len} exceeds PK column count {pk_len}"
                ));
            }
        }
        if self.compress && self.stream {
            return Err("a stream cannot be compressed: it holds no rows".to_string());
        }
        if self.serial && self.stream {
            return Err("a stream cannot be SERIAL: it holds no rows to seed the generator from".to_string());
        }
        if self.serial && pk_len != 1 {
            return Err("a SERIAL table's primary key is its one SERIAL column".to_string());
        }
        Ok(())
    }
}

/// The `WITH (…)` options of one user-named view.
#[derive(Copy, Clone, Default, PartialEq, Eq, Debug)]
pub struct ViewProps {
    pub budget: ViewBudget,
    /// `WITH (compress = …)`: the view's output store codes its heaps.
    pub compress: bool,
}

/// The byte budget a view's `WITH (…)` names, if any.
#[derive(Copy, Clone, Default, PartialEq, Eq, Debug)]
pub enum ViewBudget {
    #[default]
    Plain,
    /// `WITH (capacity = …)`: the view's output store is bounded, dehydrating past it.
    Bounded { capacity_bytes: NonZeroU64 },
    /// `WITH (delta = …)`: the view keeps its recent deltas in a store of its own.
    Fed { delta_bytes: NonZeroU64 },
}

impl ViewBudget {
    /// The props two optional budgets name.
    pub fn from_budgets(
        capacity_bytes: Option<NonZeroU64>,
        delta_bytes: Option<NonZeroU64>,
    ) -> Result<ViewBudget, String> {
        match (capacity_bytes, delta_bytes) {
            (None, None) => Ok(ViewBudget::Plain),
            (Some(capacity_bytes), None) => Ok(ViewBudget::Bounded { capacity_bytes }),
            (None, Some(delta_bytes)) => Ok(ViewBudget::Fed { delta_bytes }),
            (Some(_), Some(_)) => Err("a capacity-bounded view cannot carry a delta feed: its bootstrap read \
                                       hydrates from the source relation's live store, which no tick round governs"
                .to_string()),
        }
    }

    /// Decode the `(capacity, delta)` `VIEW_TAB` words, where `0` is absent.
    pub(crate) fn from_row(capacity: u64, delta: u64) -> Result<ViewBudget, String> {
        Self::from_budgets(NonZeroU64::new(capacity), NonZeroU64::new(delta))
    }

    /// The `(capacity, delta)` `VIEW_TAB` words.
    pub fn row_words(self) -> (u64, u64) {
        (self.capacity_bytes().unwrap_or(0), self.delta_bytes().unwrap_or(0))
    }

    pub fn capacity_bytes(self) -> Option<u64> {
        match self {
            ViewBudget::Bounded { capacity_bytes } => Some(capacity_bytes.get()),
            _ => None,
        }
    }

    pub fn delta_bytes(self) -> Option<u64> {
        match self {
            ViewBudget::Fed { delta_bytes } => Some(delta_bytes.get()),
            _ => None,
        }
    }
}

#[cfg(test)]
#[path = "tests/props.rs"]
mod tests;
