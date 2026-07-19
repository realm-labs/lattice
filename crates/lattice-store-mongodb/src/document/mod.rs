//! Typed MongoDB document envelopes.

pub mod bson_serde;
pub mod set;
pub mod tracked;

use crate::error::MongoStoreError;
use mongodb::bson::{Bson, Document};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::scan::{MongoScan, ScanSnapshot};

pub(crate) const WRITE_ID_FIELD: &str = "_lattice_write_id";

/// An identified business entity stored in one MongoDB collection.
///
/// Persisted state must only be mutated through exclusive (`&mut`) access so a
/// surrounding [`tracked::Tracked`] value can conservatively advance its
/// mutation epoch. Requesting mutable access may produce a false-positive scan;
/// mutating serialized state through a lock or atomic behind `&self` violates
/// this contract.
pub trait MongoDocument: Serialize + DeserializeOwned + Send + Sync + Sized + 'static {
    type Id: Clone + Ord + std::fmt::Debug + Serialize + DeserializeOwned + Send + Sync + 'static;

    const COLLECTION: &'static str;
    const ID_FIELD: &'static str;
    /// Determines whether an optimistic-lock conflict in this document blocks
    /// the whole actor-local coordinator or only quarantines this document.
    const CONFLICT_POLICY: crate::persistence::coordinator::ConflictPolicy =
        crate::persistence::coordinator::ConflictPolicy::BlockCoordinator;

    fn id(&self) -> &Self::Id;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedDocumentMeta {
    pub version: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedDocument<D>
where
    D: MongoDocument,
{
    pub version: i64,
    pub updated_at_ms: i64,
    pub value: D,
}

/// Internal load envelope that carries a baseline captured from the original
/// MongoDB BSON without serializing the decoded Rust value again.
#[doc(hidden)]
pub struct LoadedScannedDocument<D>
where
    D: MongoScan,
{
    loaded: LoadedDocument<D>,
    baseline: ScanSnapshot,
}

impl<D> LoadedScannedDocument<D>
where
    D: MongoScan,
{
    pub fn value(&self) -> &D {
        &self.loaded.value
    }

    pub fn id(&self) -> &D::Id {
        self.loaded.id()
    }

    pub(crate) fn into_parts(self) -> (LoadedDocument<D>, ScanSnapshot) {
        (self.loaded, self.baseline)
    }
}

impl<D> LoadedDocument<D>
where
    D: MongoDocument,
{
    pub fn id(&self) -> &D::Id {
        self.value.id()
    }

    pub fn split(self) -> (D, LoadedDocumentMeta) {
        (
            self.value,
            LoadedDocumentMeta {
                version: self.version,
                updated_at_ms: self.updated_at_ms,
            },
        )
    }

    pub fn into_flat_document(self) -> Result<Document, MongoStoreError> {
        encode_flat_document(&self.value, self.version, self.updated_at_ms)
    }
}

impl<D> std::ops::Deref for LoadedDocument<D>
where
    D: MongoDocument,
{
    type Target = D;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<D> std::ops::DerefMut for LoadedDocument<D>
where
    D: MongoDocument,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

pub fn encode_flat_document<D>(
    value: &D,
    version: i64,
    updated_at_ms: i64,
) -> Result<Document, MongoStoreError>
where
    D: MongoDocument,
{
    let mut document = encode_business_document(value)?;
    document.insert("_id", encode_document_id::<D>(value.id())?);
    document.insert("version", version);
    document.insert("updated_at_ms", updated_at_ms);
    Ok(document)
}

pub fn encode_business_document<D>(value: &D) -> Result<Document, MongoStoreError>
where
    D: MongoDocument,
{
    let mut document = mongodb::bson::to_document(value)
        .map_err(|error| MongoStoreError::encode("encode Mongo business document", error))?;
    let encoded_field = document.remove(D::ID_FIELD).ok_or_else(|| {
        MongoStoreError::new(format!(
            "serialized Mongo document is missing identity field `{}`",
            D::ID_FIELD
        ))
    })?;
    let encoded_id = encode_document_id::<D>(value.id())?;
    if encoded_field != encoded_id {
        return Err(MongoStoreError::new(format!(
            "serialized Mongo identity field `{}` does not match MongoDocument::id()",
            D::ID_FIELD
        )));
    }
    reject_reserved_fields(&document)?;
    Ok(document)
}

pub fn encode_document_id<D>(id: &D::Id) -> Result<Bson, MongoStoreError>
where
    D: MongoDocument,
{
    mongodb::bson::to_bson(id)
        .map_err(|error| MongoStoreError::encode("encode Mongo document id", error))
}

pub fn decode_flat_document<D>(mut document: Document) -> Result<LoadedDocument<D>, MongoStoreError>
where
    D: MongoDocument,
{
    let id = document
        .remove("_id")
        .ok_or_else(|| MongoStoreError::new("Mongo document missing `_id`"))?;
    let version = take_i64(&mut document, "version")?;
    let updated_at_ms = take_i64(&mut document, "updated_at_ms")?;
    document.remove(WRITE_ID_FIELD);
    if document.insert(D::ID_FIELD, id).is_some() {
        return Err(MongoStoreError::new(format!(
            "Mongo document body shadows identity field `{}`",
            D::ID_FIELD
        )));
    }
    let value = mongodb::bson::from_document(document)
        .map_err(|error| MongoStoreError::decode("decode Mongo business document", error))?;
    Ok(LoadedDocument {
        version,
        updated_at_ms,
        value,
    })
}

pub(crate) fn decode_flat_scanned_document<D>(
    mut document: Document,
) -> Result<LoadedScannedDocument<D>, MongoStoreError>
where
    D: MongoScan,
{
    let id = document
        .remove("_id")
        .ok_or_else(|| MongoStoreError::new("Mongo document missing `_id`"))?;
    let version = take_i64(&mut document, "version")?;
    let updated_at_ms = take_i64(&mut document, "updated_at_ms")?;
    document.remove(WRITE_ID_FIELD);
    let baseline = D::capture_bson(&document)
        .map_err(|error| MongoStoreError::decode("capture Mongo scan baseline", error))?;
    if document.insert(D::ID_FIELD, id).is_some() {
        return Err(MongoStoreError::new(format!(
            "Mongo document body shadows identity field `{}`",
            D::ID_FIELD
        )));
    }
    let value = mongodb::bson::from_document(document)
        .map_err(|error| MongoStoreError::decode("decode Mongo business document", error))?;
    Ok(LoadedScannedDocument {
        loaded: LoadedDocument {
            version,
            updated_at_ms,
            value,
        },
        baseline,
    })
}

fn reject_reserved_fields(document: &Document) -> Result<(), MongoStoreError> {
    for field in ["_id", "version", "updated_at_ms", WRITE_ID_FIELD] {
        if document.contains_key(field) {
            return Err(MongoStoreError::new(format!(
                "business document must not contain reserved storage field `{field}`"
            )));
        }
    }
    Ok(())
}

fn take_i64(document: &mut Document, field: &str) -> Result<i64, MongoStoreError> {
    match document.remove(field) {
        Some(Bson::Int64(value)) => Ok(value),
        Some(Bson::Int32(value)) => Ok(i64::from(value)),
        Some(other) => Err(MongoStoreError::new(format!(
            "Mongo `{field}` must be an integer, got {other:?}"
        ))),
        None => Err(MongoStoreError::new(format!(
            "Mongo document missing `{field}`"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use mongodb::bson::doc;
    use serde::{Deserialize, Serialize, Serializer};

    use super::{
        LoadedDocument, MongoDocument, decode_flat_document, decode_flat_scanned_document,
        encode_flat_document,
    };
    use crate::scan::MongoScan as _;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Profile {
        id: u64,
        name: String,
        level: i32,
    }

    impl MongoDocument for Profile {
        type Id = u64;
        const COLLECTION: &'static str = "profiles";
        const ID_FIELD: &'static str = "id";

        fn id(&self) -> &Self::Id {
            &self.id
        }
    }

    static SERIALIZE_CALLS: AtomicUsize = AtomicUsize::new(0);

    fn count_string_serialization<S>(value: &str, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        SERIALIZE_CALLS.fetch_add(1, Ordering::Relaxed);
        serializer.serialize_str(value)
    }

    #[derive(Debug, Serialize, Deserialize, crate::MongoDocument, crate::MongoScan)]
    #[mongo(collection = "raw_baseline_tests")]
    struct RawBaselineDocument {
        #[mongo(id)]
        id: u64,
        #[serde(serialize_with = "count_string_serialization")]
        value: String,
    }

    #[test]
    fn flat_envelope_round_trips_without_exposing_metadata_to_business_value() {
        let stored = LoadedDocument {
            version: 7,
            updated_at_ms: 123,
            value: Profile {
                id: 42,
                name: "Ada".to_owned(),
                level: 9,
            },
        };
        let document = stored
            .clone()
            .into_flat_document()
            .expect("profile envelope should encode");

        assert_eq!(
            document,
            doc! {
                "name": "Ada",
                "level": 9,
                "_id": 42_i64,
                "version": 7_i64,
                "updated_at_ms": 123_i64,
            }
        );
        assert_eq!(
            decode_flat_document::<Profile>(document).expect("profile envelope should decode"),
            stored
        );
    }

    #[test]
    fn scanned_decode_builds_baseline_without_reserializing_the_rust_value() {
        SERIALIZE_CALLS.store(0, Ordering::Relaxed);
        let loaded = decode_flat_scanned_document::<RawBaselineDocument>(doc! {
            "_id": 7_i64,
            "version": 3_i64,
            "updated_at_ms": 11_i64,
            "value": "already encoded",
        })
        .expect("raw BSON should decode with a captured baseline");
        assert_eq!(SERIALIZE_CALLS.load(Ordering::Relaxed), 0);

        loaded
            .value()
            .capture()
            .expect("ordinary capture should serialize the Rust value");
        assert_eq!(SERIALIZE_CALLS.load(Ordering::Relaxed), 1);
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct InvalidBusinessValue {
        id: u64,
        version: i64,
    }

    impl MongoDocument for InvalidBusinessValue {
        type Id = u64;
        const COLLECTION: &'static str = "invalid";
        const ID_FIELD: &'static str = "id";

        fn id(&self) -> &Self::Id {
            &self.id
        }
    }

    #[test]
    fn business_values_cannot_shadow_storage_metadata() {
        let error = encode_flat_document::<InvalidBusinessValue>(
            &InvalidBusinessValue { id: 1, version: 99 },
            0,
            0,
        )
        .expect_err("reserved business metadata should be rejected");
        assert!(error.message().contains("reserved storage field `version`"));
    }
}
