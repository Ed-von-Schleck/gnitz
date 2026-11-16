//! A table's non-column properties and a view's `WITH` options.

use std::num::NonZeroU64;

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

/// The non-column properties of a `CREATE TABLE`, as its `TABLE_TAB` row states
/// them.
#[derive(Copy, Clone, Default, PartialEq, Eq, Debug)]
pub struct TableProps {
    /// A storeless, append-only ingestion point rather than a table: it holds no
    /// rows and nothing it ingests survives a restart.
    pub stream: bool,
    /// INSERT draws the table's single PK column from a sequence.
    pub serial: bool,
    /// Where the rows live. A `Keyed` prefix is `CLUSTER BY` the PK's leading
    /// prefix; the SQL planner validates it against the PK.
    pub distribution: TableDistribution,
    /// The table's store codes its shards' heaps under a symbol table where
    /// that pays.
    pub compress: bool,
}

impl TableProps {
    /// The rules these properties must meet against a PK of `pk_len` columns,
    /// and between each other.
    pub fn validate(&self, pk_len: usize) -> Result<(), String> {
        if let TableDistribution::Keyed { prefix_len } = self.distribution
            && prefix_len as usize > pk_len
        {
            return Err(format!(
                "distribution prefix length {prefix_len} exceeds PK column count {pk_len}"
            ));
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
