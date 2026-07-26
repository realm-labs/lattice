use std::collections::BTreeMap;
use std::time::Duration;

use mongodb::bson::Bson;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{
    ConflictPolicy, DocumentPresence, MongoPersistenceCoordinator, PersistenceConflictKind,
    PersistenceError, RetryPolicy,
};
use crate::document::{LoadedDocument, LoadedDocumentMeta};
use crate::error::MongoStoreError;
use crate::persistence::actor::{CompletionStatus, MongoFlushCompleted};
use crate::persistence::request::{
    CreateMode, DocumentOperation, DocumentWriteOutcome, FlushOutcome,
};
use crate::persistence::types::MongoDocumentKey;
use crate::scan::ScanBudget;
use crate::{MongoDocument as MongoDocumentDerive, MongoScan};

mod absent;
mod completion;
mod conflict;
mod registration;
mod rejection;
mod retry;
mod scanning;

#[derive(Debug, Clone, Serialize, Deserialize, MongoDocumentDerive, MongoScan)]
#[mongo(collection = "coordinator_test")]
struct TestDocument {
    #[mongo(id)]
    id: u64,
    name: String,
    #[mongo(scan = "map")]
    items: BTreeMap<String, i32>,
}

#[derive(Debug, Clone)]
struct RejectingString {
    value: String,
    reject: bool,
}

impl Serialize for RejectingString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.reject {
            return Err(serde::ser::Error::custom(
                "intentional test encoding failure",
            ));
        }
        self.value.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RejectingString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self {
            value: String::deserialize(deserializer)?,
            reject: false,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, MongoDocumentDerive, MongoScan)]
#[mongo(collection = "coordinator_rejecting_test", conflict = "quarantine")]
struct RejectingDocument {
    #[mongo(id)]
    id: u64,
    payload: RejectingString,
}

fn document(name: &str) -> TestDocument {
    TestDocument {
        id: 42,
        name: name.to_owned(),
        items: BTreeMap::from([("one".to_owned(), 1)]),
    }
}

fn loaded(value: &TestDocument, mutation_epoch: Option<u64>) -> MongoPersistenceCoordinator {
    let mut coordinator = MongoPersistenceCoordinator::new(7);
    let meta = LoadedDocumentMeta {
        version: 3,
        updated_at_ms: 10,
    };
    if let Some(epoch) = mutation_epoch {
        coordinator
            .attach_loaded_tracked(value, epoch, meta)
            .expect("tracked document should attach");
    } else {
        coordinator
            .attach_loaded(value, meta)
            .expect("document should attach");
    }
    coordinator
}
