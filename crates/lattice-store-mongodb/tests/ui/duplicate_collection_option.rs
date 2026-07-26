use lattice_store_mongodb::MongoDocument;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, MongoDocument)]
#[mongo(collection = "first", collection = "second")]
struct DuplicateCollection {
    #[mongo(id)]
    id: u64,
}

#[derive(Serialize, Deserialize, MongoDocument)]
#[mongo(collection = "conflicting")]
#[mongo(conflict = "block")]
#[mongo(conflict = "quarantine")]
struct DuplicateConflictPolicy {
    #[mongo(id)]
    id: u64,
}

fn main() {}
