use super::{StorageError, StorePage};

pub(super) fn bounded_page<T: Clone>(
    records: &[T],
    offset: usize,
    limit: usize,
) -> Result<StorePage<T>, StorageError> {
    if limit == 0 || offset > records.len() {
        return Err(StorageError::BackendArgument);
    }
    let end = offset.saturating_add(limit).min(records.len());
    Ok(StorePage {
        records: records[offset..end].to_vec(),
        next_offset: (end < records.len()).then_some(end),
        total: records.len(),
    })
}
