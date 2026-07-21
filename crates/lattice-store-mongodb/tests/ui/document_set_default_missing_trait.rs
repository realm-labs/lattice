use lattice_store_mongodb::document::tracked::Tracked;
use lattice_store_mongodb::{MongoDocument, MongoDocumentSet, MongoScan};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "missing_default_trait")]
struct Document {
    #[mongo(id)]
    id: u64,
}

#[derive(MongoDocumentSet)]
#[mongo(id = u64)]
struct InvalidDocuments {
    #[mongo(default)]
    value: Tracked<Document>,
}

fn main() {}
