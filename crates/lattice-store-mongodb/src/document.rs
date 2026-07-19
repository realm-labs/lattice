//! Typed MongoDB document envelopes.

use crate::error::MongoStoreError;
use mongodb::bson::{Bson, Document};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// An identified business entity stored in one MongoDB collection.
pub trait MongoDocument: Serialize + DeserializeOwned + Send + Sized + 'static {
    type Id: Clone + Ord + std::fmt::Debug + Serialize + DeserializeOwned + Send + 'static;

    const COLLECTION: &'static str;
    const ID_FIELD: &'static str;

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

fn reject_reserved_fields(document: &Document) -> Result<(), MongoStoreError> {
    for field in ["_id", "version", "updated_at_ms"] {
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
    use std::cell::Cell;

    use mongodb::bson::doc;
    use serde::{Deserialize, Serialize};

    use super::{LoadedDocument, MongoDocument, decode_flat_document, encode_flat_document};

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

    #[derive(Debug, Serialize, Deserialize)]
    struct ActorLocalDocument {
        id: u64,
        mutation_count: Cell<u64>,
    }

    impl MongoDocument for ActorLocalDocument {
        type Id = u64;
        const COLLECTION: &'static str = "actor_local_documents";
        const ID_FIELD: &'static str = "id";

        fn id(&self) -> &Self::Id {
            &self.id
        }
    }

    #[test]
    fn document_may_be_send_without_being_sync() {
        fn assert_document<D: MongoDocument>() {}

        assert_document::<ActorLocalDocument>();
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
