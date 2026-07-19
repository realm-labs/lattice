use serde::{Deserialize, Serialize};
use lattice_store_mongodb::MongoDocument;

#[derive(Serialize, Deserialize, MongoDocument)]
#[mongo(collection = "missing_id")]
struct MissingId {
    value: i32,
}

fn main() {}
