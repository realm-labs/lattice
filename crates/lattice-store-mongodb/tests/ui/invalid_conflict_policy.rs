use lattice_store_mongodb::MongoDocument;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, MongoDocument)]
#[mongo(collection = "invalid_conflict", conflict = "continue")]
struct InvalidConflictPolicy {
    #[mongo(id)]
    id: u64,
}

fn main() {}
