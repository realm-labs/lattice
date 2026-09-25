/// An opaque resume position produced by a paged list.
///
/// The bytes are backend-defined, so a cursor may only be handed back to the store that produced
/// it. Every backend scans its records in a stable key order and resumes strictly after the record
/// the cursor names, which is what an offset cannot do: a record inserted or removed at a position
/// the sweep already passed shifts every later offset and therefore duplicates or skips a record
/// that never changed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageCursor(Vec<u8>);

impl PageCursor {
    pub fn new(position: Vec<u8>) -> Self {
        Self(position)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// One bounded page of durable records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorePage<T> {
    pub records: Vec<T>,
    /// Where the next page resumes, or `None` once the sweep reached the end of the range.
    pub next_cursor: Option<PageCursor>,
    /// Records the backend still has to visit after this page. A range cannot filter on a decoded
    /// value, so a request that carries a record filter reports an upper bound instead.
    pub remaining: usize,
}
