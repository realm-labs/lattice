use lattice_store_mongodb::{MongoDocument, MongoScan};
use serde::{Deserialize, Serialize};

/// A business module that shares the adapter's name but encodes keys
/// differently, so map update paths would not match the captured keys.
mod path_key_map {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &BTreeMap<String, i32>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value
            .iter()
            .map(|(key, value)| (key.to_uppercase(), *value))
            .collect::<BTreeMap<_, _>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<BTreeMap<String, i32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        BTreeMap::<String, i32>::deserialize(deserializer)
    }
}

#[derive(Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "foreign_path_key_map")]
struct ForeignPathKeyMap {
    #[mongo(id)]
    id: u64,
    #[serde(with = "path_key_map")]
    #[mongo(scan = "map")]
    values: std::collections::BTreeMap<String, i32>,
}

fn main() {}
