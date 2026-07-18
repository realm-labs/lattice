use std::str::FromStr;

use http::Uri;
use serde::{Deserialize, Deserializer, Serializer, de::Error};

pub fn serialize<S>(value: &Uri, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Uri, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    Uri::from_str(&value).map_err(Error::custom)
}
