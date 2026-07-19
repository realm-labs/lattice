use mongodb::bson::Bson;

use crate::document::MongoDocument;
use crate::error::MongoStoreError;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MongoFieldPath(pub String);

impl MongoFieldPath {
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    pub fn child(&self, child: impl AsRef<str>) -> Self {
        Self(format!("{}.{}", self.0, child.as_ref()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MongoDocumentKey {
    pub collection: &'static str,
    pub id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MongoIndexSpec {
    pub collection: &'static str,
    pub name: &'static str,
    pub fields: &'static [&'static str],
    pub unique: bool,
}

impl MongoDocumentKey {
    pub fn new(collection: &'static str, id: impl Into<String>) -> Self {
        Self {
            collection,
            id: id.into(),
        }
    }

    /// Builds the actor-local key from the typed BSON identity. Canonical
    /// Extended JSON preserves BSON type distinctions and lets structured IDs
    /// work without a business `Display` implementation.
    pub fn for_document<D>(id: &D::Id) -> Result<Self, MongoStoreError>
    where
        D: MongoDocument,
    {
        let id = mongodb::bson::to_bson(id)
            .map_err(|error| MongoStoreError::encode("encode coordinator document ID", error))?;
        Ok(Self::new(D::COLLECTION, stable_id_string(id)))
    }
}

fn stable_id_string(id: Bson) -> String {
    id.into_canonical_extjson().to_string()
}

#[cfg(test)]
mod tests {
    use mongodb::bson::{Bson, doc};

    use super::stable_id_string;

    #[test]
    fn coordinator_identity_preserves_bson_types_and_structured_values() {
        assert_ne!(
            stable_id_string(Bson::Int64(42)),
            stable_id_string(Bson::String("42".to_owned())),
        );
        assert_eq!(
            stable_id_string(Bson::Document(
                doc! { "world_id": 7_i64, "member_id": 9_i64 }
            )),
            stable_id_string(Bson::Document(
                doc! { "world_id": 7_i64, "member_id": 9_i64 }
            )),
        );
    }
}
