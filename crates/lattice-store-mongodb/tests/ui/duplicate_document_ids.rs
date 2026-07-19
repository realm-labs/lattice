use lattice_store_mongodb::MongoDocument;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, MongoDocument)]
#[mongo(collection = "duplicate_ids")]
struct DuplicateIds {
    #[mongo(id)]
    first: u64,
    #[mongo(id)]
    second: u64,
}

fn main() {}
