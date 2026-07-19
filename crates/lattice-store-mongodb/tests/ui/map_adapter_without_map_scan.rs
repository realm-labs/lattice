use lattice_store_mongodb::{MongoDocument, MongoScan};
use serde::{Deserialize, Serialize};

struct Adapter;

#[derive(Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "invalid_adapter")]
struct InvalidAdapter {
    #[mongo(id)]
    id: u64,
    #[mongo(adapter = Adapter)]
    values: std::collections::BTreeMap<String, i32>,
}

fn main() {}
