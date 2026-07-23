use std::fmt;

use serde::{Deserializer, Serializer, de::Visitor};

pub fn serialize<S>(value: &u128, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<u128, D::Error>
where
    D: Deserializer<'de>,
{
    struct U128Visitor;

    impl Visitor<'_> for U128Visitor {
        type Value = u128;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a decimal u128 string or an unsigned integer")
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
            Ok(u128::from(value))
        }

        fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E> {
            Ok(value)
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            value.parse().map_err(E::custom)
        }
    }

    deserializer.deserialize_any(U128Visitor)
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Artifact {
        #[serde(with = "super")]
        incarnation: u128,
    }

    #[test]
    fn full_width_value_round_trips_as_a_json_string() {
        let encoded = serde_json::to_string(&Artifact {
            incarnation: u128::MAX,
        })
        .unwrap();
        assert_eq!(
            encoded,
            r#"{"incarnation":"340282366920938463463374607431768211455"}"#
        );
        assert_eq!(
            serde_json::from_str::<Artifact>(&encoded).unwrap(),
            Artifact {
                incarnation: u128::MAX
            }
        );
    }

    #[test]
    fn legacy_numeric_value_remains_readable() {
        assert_eq!(
            serde_json::from_str::<Artifact>(r#"{"incarnation":42}"#).unwrap(),
            Artifact { incarnation: 42 }
        );
    }
}
