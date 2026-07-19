pub mod string_key_map {
    use std::collections::BTreeMap;
    use std::fmt::Display;
    use std::str::FromStr;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S, K, V>(value: &BTreeMap<K, V>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        K: Display + Ord,
        V: Serialize,
    {
        value
            .iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect::<BTreeMap<_, _>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D, K, V>(deserializer: D) -> Result<BTreeMap<K, V>, D::Error>
    where
        D: Deserializer<'de>,
        K: FromStr + Ord,
        K::Err: Display,
        V: Deserialize<'de>,
    {
        BTreeMap::<String, V>::deserialize(deserializer)?
            .into_iter()
            .map(|(key, value)| {
                key.parse::<K>()
                    .map(|key| (key, value))
                    .map_err(serde::de::Error::custom)
            })
            .collect()
    }
}

/// Encodes one logical map key into a MongoDB update-path-safe BSON field name.
///
/// The encoding is injective: `%` is escaped before MongoDB's special path
/// characters, and the empty key has its own representation. Full-document
/// serialization and incremental update paths must use the same encoding.
pub fn encode_path_key(raw: &str) -> String {
    if raw.is_empty() {
        return "%EMPTY".to_owned();
    }

    let mut encoded = String::with_capacity(raw.len());
    for character in raw.chars() {
        match character {
            '%' => encoded.push_str("%25"),
            '.' => encoded.push_str("%2E"),
            '$' => encoded.push_str("%24"),
            '\0' => encoded.push_str("%00"),
            character => encoded.push(character),
        }
    }
    encoded
}

/// Decodes a map key produced by [`encode_path_key`].
pub fn decode_path_key(encoded: &str) -> String {
    if encoded == "%EMPTY" {
        return String::new();
    }

    let bytes = encoded.as_bytes();
    let mut decoded = String::with_capacity(encoded.len());
    let mut plain_start = 0;
    let mut index = 0;
    while index < bytes.len() {
        let replacement = if bytes[index] == b'%' && index + 2 < bytes.len() {
            match (
                bytes[index + 1].to_ascii_uppercase(),
                bytes[index + 2].to_ascii_uppercase(),
            ) {
                (b'2', b'5') => Some('%'),
                (b'2', b'E') => Some('.'),
                (b'2', b'4') => Some('$'),
                (b'0', b'0') => Some('\0'),
                _ => None,
            }
        } else {
            None
        };
        if let Some(replacement) = replacement {
            decoded.push_str(&encoded[plain_start..index]);
            decoded.push(replacement);
            index += 3;
            plain_start = index;
        } else {
            index += 1;
        }
    }
    decoded.push_str(&encoded[plain_start..]);
    decoded
}

/// Serde adapter for maps whose serialized BSON field names may contain
/// MongoDB update-path characters.
///
/// Pair this with `#[mongo(scan = "map")]` when entry-level diffs are desired:
///
/// ```ignore
/// #[serde(with = "lattice_store_mongodb::document::bson_serde::path_key_map")]
/// #[mongo(scan = "map")]
/// values: HashMap<String, Value>,
/// ```
pub mod path_key_map {
    use std::collections::BTreeMap;
    use std::fmt::Display;
    use std::str::FromStr;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::{decode_path_key, encode_path_key};

    pub fn serialize<S, C, K, V>(value: &C, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        for<'a> &'a C: IntoIterator<Item = (&'a K, &'a V)>,
        K: Display,
        V: Serialize,
    {
        let mut encoded = BTreeMap::new();
        for (key, value) in value {
            let key = encode_path_key(&key.to_string());
            if encoded.insert(key.clone(), value).is_some() {
                return Err(serde::ser::Error::custom(format!(
                    "multiple map keys serialize to the MongoDB key `{key}`"
                )));
            }
        }
        encoded.serialize(serializer)
    }

    pub fn deserialize<'de, D, C, K, V>(deserializer: D) -> Result<C, D::Error>
    where
        D: Deserializer<'de>,
        C: FromIterator<(K, V)>,
        K: FromStr,
        K::Err: Display,
        V: Deserialize<'de>,
    {
        BTreeMap::<String, V>::deserialize(deserializer)?
            .into_iter()
            .map(|(key, value)| {
                decode_path_key(&key)
                    .parse::<K>()
                    .map(|key| (key, value))
                    .map_err(serde::de::Error::custom)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct NumericMap {
        #[serde(with = "super::string_key_map")]
        values: BTreeMap<i32, i64>,
    }

    #[test]
    fn numeric_keys_round_trip_as_bson_document_keys() {
        let value = NumericMap {
            values: BTreeMap::from([(1001, 3), (1002, 7)]),
        };
        let document = mongodb::bson::to_document(&value).expect("numeric map should encode");
        let values = document
            .get_document("values")
            .expect("map should encode as a BSON document");
        assert_eq!(values.get_i64("1001"), Ok(3));
        assert_eq!(
            mongodb::bson::from_document::<NumericMap>(document)
                .expect("numeric map should decode"),
            value,
        );
    }

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct PathKeyMaps {
        #[serde(with = "super::path_key_map")]
        ordered: BTreeMap<String, i64>,
        #[serde(with = "super::path_key_map")]
        hashed: HashMap<String, i64>,
    }

    #[test]
    fn path_key_encoding_is_reversible_and_collision_free() {
        let keys = [
            "plain", "a.b", "a$b", "a%b", "a%2Eb", "a%24b", "a%25b", "", "%EMPTY", "a\0b",
        ];
        let encoded = keys
            .iter()
            .map(|key| super::encode_path_key(key))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(encoded.len(), keys.len());
        assert_eq!(
            keys,
            keys.map(|key| super::decode_path_key(&super::encode_path_key(key)))
        );
    }

    #[test]
    fn path_key_map_round_trips_btree_and_hash_maps() {
        let value = PathKeyMaps {
            ordered: BTreeMap::from([
                ("a.b".to_owned(), 1),
                ("a%2Eb".to_owned(), 2),
                (String::new(), 3),
            ]),
            hashed: HashMap::from([("$state".to_owned(), 4), ("a\0b".to_owned(), 5)]),
        };
        let document = mongodb::bson::to_document(&value).expect("path-key maps should encode");
        let ordered = document
            .get_document("ordered")
            .expect("ordered map should be a document");
        assert_eq!(ordered.get_i64("a%2Eb"), Ok(1));
        assert_eq!(ordered.get_i64("a%252Eb"), Ok(2));
        assert_eq!(ordered.get_i64("%EMPTY"), Ok(3));
        assert_eq!(
            mongodb::bson::from_document::<PathKeyMaps>(document)
                .expect("path-key maps should decode"),
            value,
        );
    }
}
