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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

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
}
