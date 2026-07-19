use lattice_store_mongodb::MongoDocument;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, MongoDocument)]
#[mongo(collection = "skipped_id")]
struct SkippedId {
    #[mongo(id)]
    #[serde(skip)]
    id: u64,
}

fn main() {}
