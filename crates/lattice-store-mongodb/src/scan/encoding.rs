use std::collections::BTreeMap;

use mongodb::bson::{Bson, Document};
use serde::Serialize;

use super::{ScanError, ScanFieldPolicy};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct StableHash(u128);

pub(super) fn encode_value<T>(value: &T) -> Result<Bson, ScanError>
where
    T: Serialize,
{
    mongodb::bson::to_bson(value).map_err(|error| ScanError::Encoding(error.to_string()))
}

fn estimated_document_size(document: &Document) -> usize {
    document.iter().fold(5_usize, |size, (key, value)| {
        size.saturating_add(1)
            .saturating_add(key.len())
            .saturating_add(1)
            .saturating_add(estimated_bson_value_size(value))
    })
}

pub(super) fn estimated_bson_value_size(value: &Bson) -> usize {
    match value {
        Bson::Double(_) | Bson::Int64(_) | Bson::Timestamp(_) | Bson::DateTime(_) => 8,
        Bson::String(value) | Bson::JavaScriptCode(value) | Bson::Symbol(value) => {
            4_usize.saturating_add(value.len()).saturating_add(1)
        }
        Bson::Array(values) => values
            .iter()
            .enumerate()
            .fold(5_usize, |size, (index, value)| {
                size.saturating_add(1)
                    .saturating_add(index.to_string().len())
                    .saturating_add(1)
                    .saturating_add(estimated_bson_value_size(value))
            }),
        Bson::Document(document) => estimated_document_size(document),
        Bson::Boolean(_) => 1,
        Bson::Null | Bson::Undefined | Bson::MinKey | Bson::MaxKey => 0,
        Bson::Int32(_) => 4,
        Bson::Binary(value) => 5_usize.saturating_add(value.bytes.len()),
        Bson::ObjectId(_) => 12,
        Bson::Decimal128(_) => 16,
        Bson::RegularExpression(value) => value
            .pattern
            .len()
            .saturating_add(1)
            .saturating_add(value.options.len())
            .saturating_add(1),
        Bson::JavaScriptCodeWithScope(value) => 8_usize
            .saturating_add(value.code.len())
            .saturating_add(1)
            .saturating_add(estimated_document_size(&value.scope)),
        // DBPointer and any future scalar variants are rare in business
        // documents. A bounded estimate keeps metrics allocation-free.
        _ => 32,
    }
}

pub(super) fn field_policy<'a>(
    field: &str,
    policies: &'a [ScanFieldPolicy],
) -> Option<&'a ScanFieldPolicy> {
    policies.iter().find(|policy| policy.path == field)
}

pub(super) fn stable_hash(value: &Bson) -> Result<StableHash, ScanError> {
    let mut hasher = StableHasher::new();
    hash_bson(value, &mut hasher)?;
    Ok(StableHash(hasher.finish()))
}

pub(super) fn hash_document_entries(
    value: &Document,
) -> Result<BTreeMap<String, StableHash>, ScanError> {
    value
        .iter()
        .map(|(key, value)| {
            validate_map_key(key)?;
            Ok((key.clone(), stable_hash(value)?))
        })
        .collect()
}

pub(super) fn validate_map_key(key: &str) -> Result<(), ScanError> {
    if key.is_empty() || key.starts_with('$') || key.contains('.') || key.contains('\0') {
        Err(ScanError::InvalidMapKey(key.to_owned()))
    } else {
        Ok(())
    }
}

pub(super) fn validate_field_path(path: &str) -> Result<(), ScanError> {
    if path.is_empty() || path.starts_with('$') || path.contains('.') {
        Err(ScanError::InvalidFieldPath(path.to_owned()))
    } else {
        Ok(())
    }
}

fn hash_bson(value: &Bson, hasher: &mut StableHasher) -> Result<(), ScanError> {
    match value {
        Bson::Double(value) => tagged(hasher, 1, &value.to_bits().to_le_bytes()),
        Bson::String(value) => tagged(hasher, 2, value.as_bytes()),
        Bson::Array(values) => {
            tagged(hasher, 3, &(values.len() as u64).to_le_bytes());
            for value in values {
                hash_bson(value, hasher)?;
            }
        }
        Bson::Document(value) => {
            tagged(hasher, 4, &(value.len() as u64).to_le_bytes());
            let mut entries = value.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            for (key, value) in entries {
                tagged(hasher, 5, key.as_bytes());
                hash_bson(value, hasher)?;
            }
        }
        Bson::Boolean(value) => tagged(hasher, 8, &[*value as u8]),
        Bson::Null => tagged(hasher, 10, &[]),
        Bson::Int32(value) => tagged(hasher, 16, &value.to_le_bytes()),
        Bson::Int64(value) => tagged(hasher, 18, &value.to_le_bytes()),
        other => {
            // The driver's canonical raw BSON encoding covers less common
            // scalar variants while preserving their BSON type identity.
            let wrapper = Document::from_iter([("v".to_owned(), other.clone())]);
            let bytes = mongodb::bson::to_vec(&wrapper)
                .map_err(|error| ScanError::Encoding(error.to_string()))?;
            tagged(hasher, 255, &bytes);
        }
    }
    Ok(())
}

fn tagged(hasher: &mut StableHasher, tag: u8, bytes: &[u8]) {
    hasher.write(&[tag]);
    hasher.write(&(bytes.len() as u64).to_le_bytes());
    hasher.write(bytes);
}

struct StableHasher(u128);

impl StableHasher {
    fn new() -> Self {
        Self(0x6c62_272e_07bb_0142_62b8_2175_6295_c58d)
    }

    fn write(&mut self, bytes: &[u8]) {
        const PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;
        for byte in bytes {
            self.0 ^= u128::from(*byte);
            self.0 = self.0.wrapping_mul(PRIME);
        }
    }

    fn finish(self) -> u128 {
        self.0
    }
}
