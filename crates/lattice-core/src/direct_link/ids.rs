use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

static NEXT_LINK_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LinkId(String);

impl LinkId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn next_local() -> Self {
        Self(format!(
            "local-{}",
            NEXT_LINK_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LinkId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DirectLinkMessageId(pub u64);

impl DirectLinkMessageId {
    pub fn for_proto(stream_name: &str, proto_full_name: &str) -> Self {
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        for byte in stream_name
            .as_bytes()
            .iter()
            .copied()
            .chain([0])
            .chain(proto_full_name.as_bytes().iter().copied())
        {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        Self(hash)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LinkSequence(pub u64);
