use std::collections::BTreeMap;

use lattice_core::actor_ref::ProtocolId;
use prost::Message;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::wire::{Frame, FrameKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProtocolFingerprint([u8; 32]);

impl ProtocolFingerprint {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn digest(canonical_descriptor: &[u8]) -> Self {
        Self(*blake3::hash(canonical_descriptor).as_bytes())
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolDescriptor {
    pub protocol_id: ProtocolId,
    pub fingerprint: ProtocolFingerprint,
}

pub fn catalogue_frame(descriptors: &[ProtocolDescriptor]) -> Frame {
    Frame::encode_message(
        FrameKind::ProtocolCatalogue,
        &ProtocolCatalogueWire {
            protocols: descriptors
                .iter()
                .map(|descriptor| ProtocolDescriptorWire {
                    protocol_id: descriptor.protocol_id.get(),
                    fingerprint: descriptor.fingerprint.as_bytes().to_vec(),
                })
                .collect(),
        },
    )
}

pub fn decode_catalogue_frame(
    frame: &Frame,
    maximum: usize,
) -> Result<Vec<ProtocolDescriptor>, CatalogueError> {
    if maximum == 0 {
        return Err(CatalogueError::ZeroLimit);
    }
    if frame.kind != FrameKind::ProtocolCatalogue {
        return Err(CatalogueError::WrongFrameKind);
    }
    let wire = frame
        .decode_message::<ProtocolCatalogueWire>()
        .map_err(|_| CatalogueError::InvalidWire)?;
    if wire.protocols.len() > maximum {
        return Err(CatalogueError::TooMany {
            actual: wire.protocols.len(),
            maximum,
        });
    }
    let mut protocols = BTreeMap::new();
    for descriptor in wire.protocols {
        let protocol_id =
            ProtocolId::new(descriptor.protocol_id).map_err(|_| CatalogueError::InvalidWire)?;
        let fingerprint: [u8; 32] = descriptor
            .fingerprint
            .try_into()
            .map_err(|_| CatalogueError::InvalidWire)?;
        if protocols
            .insert(protocol_id.get(), ProtocolFingerprint::new(fingerprint))
            .is_some()
        {
            return Err(CatalogueError::DuplicateProtocol(protocol_id.get()));
        }
    }
    Ok(protocols
        .into_iter()
        .map(|(id, fingerprint)| ProtocolDescriptor {
            protocol_id: ProtocolId::new(id).expect("validated nonzero protocol ID"),
            fingerprint,
        })
        .collect())
}

#[derive(Clone, PartialEq, Message)]
struct ProtocolCatalogueWire {
    #[prost(message, repeated, tag = "1")]
    protocols: Vec<ProtocolDescriptorWire>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtocolDescriptorWire {
    #[prost(uint64, tag = "1")]
    protocol_id: u64,
    #[prost(bytes = "vec", tag = "2")]
    fingerprint: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolCatalogue {
    maximum: usize,
    protocols: BTreeMap<u64, ProtocolFingerprint>,
}

impl ProtocolCatalogue {
    pub fn new(maximum: usize) -> Result<Self, CatalogueError> {
        if maximum == 0 {
            return Err(CatalogueError::ZeroLimit);
        }
        Ok(Self {
            maximum,
            protocols: BTreeMap::new(),
        })
    }

    pub fn install<I>(&mut self, descriptors: I) -> Result<(), CatalogueError>
    where
        I: IntoIterator<Item = ProtocolDescriptor>,
    {
        let mut next = BTreeMap::new();
        for descriptor in descriptors {
            if next
                .insert(descriptor.protocol_id.get(), descriptor.fingerprint)
                .is_some()
            {
                return Err(CatalogueError::DuplicateProtocol(
                    descriptor.protocol_id.get(),
                ));
            }
            if next.len() > self.maximum {
                return Err(CatalogueError::TooMany {
                    actual: next.len(),
                    maximum: self.maximum,
                });
            }
        }
        self.protocols = next;
        Ok(())
    }

    pub fn compare(
        &self,
        protocol_id: ProtocolId,
        expected: ProtocolFingerprint,
    ) -> CatalogueDecision {
        match self.protocols.get(&protocol_id.get()) {
            Some(actual) if *actual == expected => CatalogueDecision::Enabled,
            Some(actual) => CatalogueDecision::FingerprintMismatch { actual: *actual },
            None => CatalogueDecision::Unsupported,
        }
    }

    pub fn len(&self) -> usize {
        self.protocols.len()
    }

    pub fn is_empty(&self) -> bool {
        self.protocols.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogueDecision {
    Enabled,
    Unsupported,
    FingerprintMismatch { actual: ProtocolFingerprint },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CatalogueError {
    #[error("protocol catalogue limit must be nonzero")]
    ZeroLimit,
    #[error("duplicate protocol ID {0}")]
    DuplicateProtocol(u64),
    #[error("protocol catalogue contains {actual} entries, maximum is {maximum}")]
    TooMany { actual: usize, maximum: usize },
    #[error("protocol catalogue used the wrong frame kind")]
    WrongFrameKind,
    #[error("protocol catalogue frame is invalid")]
    InvalidWire,
    #[error("protocol catalogue changed after association negotiation")]
    ChangedAfterInstall,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mismatch_is_scoped_to_one_protocol() {
        let first = ProtocolId::new(1).unwrap();
        let second = ProtocolId::new(2).unwrap();
        let a = ProtocolFingerprint::digest(b"a");
        let b = ProtocolFingerprint::digest(b"b");
        let mut catalogue = ProtocolCatalogue::new(4).unwrap();
        catalogue
            .install([
                ProtocolDescriptor {
                    protocol_id: first,
                    fingerprint: a,
                },
                ProtocolDescriptor {
                    protocol_id: second,
                    fingerprint: b,
                },
            ])
            .unwrap();
        assert!(matches!(
            catalogue.compare(first, b),
            CatalogueDecision::FingerprintMismatch { .. }
        ));
        assert_eq!(catalogue.compare(second, b), CatalogueDecision::Enabled);
    }
}
