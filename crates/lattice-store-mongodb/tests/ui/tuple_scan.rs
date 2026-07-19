use serde::{Deserialize, Serialize};
use lattice_store_mongodb::{MongoDocument, MongoScan};

#[derive(Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "tuple")]
struct Tuple(#[mongo(id)] u64, i32);

fn main() {}
