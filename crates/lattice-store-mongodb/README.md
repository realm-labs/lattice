# lattice-store-mongodb

MongoDB document persistence for Lattice applications. The crate provides typed
documents, optimistic versions, budgeted field-level change scans, direct
document writes, a reusable persistence coordinator, and actor integration.

```rust
use lattice_store_mongodb::{MongoDocument, MongoScan};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "players")]
struct PlayerDocument {
    #[mongo(id)]
    id: u64,
    name: String,
}
```

The annotated identity remains part of the Rust entity. The store maps that
field to MongoDB `_id` without duplicating it in the stored business body.
Direct inserts and replacements, coordinator registration, and scans read the
identity from `MongoDocument::id()` rather than requiring a separate ID
argument.

Actor-owned persistence state can derive `MongoDocumentSet`. Plain
`Tracked<T>` fields are required singleton documents. A `#[mongo(many)]`
field implements `MongoDocumentCollection` and retains full control over its
map/vector representation and derived business indexes.

```rust
# use lattice_store_mongodb::document::LoadedDocument;
# use lattice_store_mongodb::scan::ScanBudget;
use lattice_store_mongodb::tracked::Tracked;
use lattice_store_mongodb::{MongoDocumentCollection, MongoDocumentSet};
# use lattice_store_mongodb::coordinator::MongoPersistenceCoordinator;
# use lattice_store_mongodb::coordinator::PersistenceError;
# use lattice_store_mongodb::MongoStoreError;
# use mongodb::bson::{doc, to_bson};
# use std::collections::HashMap;
# type PlayerId = u64;
# type MemberId = u64;
# use lattice_store_mongodb::{MongoDocument, MongoScan};
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Serialize, Deserialize, MongoDocument, MongoScan)]
# #[mongo(collection = "player_core")]
# struct PlayerCore { #[mongo(id)] id: PlayerId, level: i32 }
# #[derive(Debug, Serialize, Deserialize, MongoDocument, MongoScan)]
# #[mongo(collection = "player_profile")]
# struct PlayerProfile { #[mongo(id)] id: PlayerId, name: String }
# #[derive(Debug, Serialize, Deserialize, MongoDocument, MongoScan)]
# #[mongo(collection = "player_members")]
# struct PlayerMember { #[mongo(id)] id: MemberId, owner_id: PlayerId }

struct PlayerMembers {
    by_id: HashMap<MemberId, Tracked<PlayerMember>>,
}

impl MongoDocumentCollection<PlayerId> for PlayerMembers {
    type Document = PlayerMember;

    fn load_filter(owner_id: &PlayerId) -> Result<mongodb::bson::Document, MongoStoreError> {
        Ok(doc! { "owner_id": to_bson(owner_id)
            .map_err(|error| MongoStoreError::encode("encode player ID", error))? })
    }

    fn owner_id(document: &Self::Document) -> &PlayerId {
        &document.owner_id
    }

    fn from_documents(
        documents: Vec<Tracked<Self::Document>>,
    ) -> Result<Self, PersistenceError> {
        Ok(Self {
            by_id: documents
                .into_iter()
                .map(|document| (document.id, document))
                .collect(),
        })
    }

    fn documents(&self) -> impl Iterator<Item = &Tracked<Self::Document>> {
        self.by_id.values()
    }
}

#[derive(MongoDocumentSet)]
#[mongo(id = PlayerId)]
struct PlayerDocuments {
    core: Tracked<PlayerCore>,
    profile: Tracked<PlayerProfile>,
    #[mongo(many)]
    members: PlayerMembers,
    #[mongo(skip)]
    transient_counter: usize,
}

# let core = LoadedDocument { version: 1, updated_at_ms: 0,
#     value: PlayerCore { id: 42, level: 1 } };
# let profile = LoadedDocument { version: 1, updated_at_ms: 0,
#     value: PlayerProfile { id: 42, name: "Ada".into() } };
# let members = Vec::new();
# let player_id = 42;
# let budget = ScanBudget::generous();
let mut coordinator = MongoPersistenceCoordinator::new(1);
let documents = coordinator.attach_loaded_set::<PlayerDocuments>(
    &player_id,
    LoadedPlayerDocuments { core, profile, members },
)?;
let prepared = coordinator.prepare_set(budget, &documents)?;
# let _ = prepared;
# Ok::<(), Box<dyn std::error::Error>>(())
```

The derive also generates `PlayerDocuments::load(&store, &player_id,
&mut coordinator)`, which performs singleton `find_one` and collection
`find_many` queries before registering anything. Skipped fields are initialized
with `Default::default()`. The collection adapter builds custom indexes after
the coordinator has converted the loaded batch into tracked documents.

Business documents must not define the reserved top-level fields `_id`,
`version`, or `updated_at_ms`. Prepared multi-document flushes report an exact
result per document but are not cross-document transactions.

## Coordinated persistence

`MongoPersistenceCoordinator` owns acknowledged scan baselines and optimistic
versions for all documents belonging to one actor activation. Business code
registers loaded or new documents, then enumerates current values in a bounded
preparation pass:

```rust
# use lattice_store_mongodb::coordinator::MongoPersistenceCoordinator;
# use lattice_store_mongodb::document::LoadedDocumentMeta;
# use lattice_store_mongodb::scan::ScanBudget;
# use lattice_store_mongodb::{MongoDocument, MongoScan};
# use serde::{Deserialize, Serialize};
# #[derive(Debug, Serialize, Deserialize, MongoDocument, MongoScan)]
# #[mongo(collection = "players")]
# struct PlayerDocument { #[mongo(id)] id: u64, name: String }
# let player = PlayerDocument { id: 42, name: "Ada".into() };
let mut persistence = MongoPersistenceCoordinator::new(1);
persistence.attach_loaded(
    &player,
    LoadedDocumentMeta { version: 1, updated_at_ms: 0 },
)?;

let prepared = persistence.prepare(ScanBudget::generous(), |batch| {
    batch.scan(&player)
})?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

The actor adapter sends prepared writes through `ActorContext::pipe_to_self`.
An actor handles `MongoFlushCompleted` in a later turn and calls
`MongoPersistenceCoordinator::apply_completion`; baselines advance only after
an acknowledged write. Set `LATTICE_MONGODB_TEST_URI` to enable the live
MongoDB integration test, with optional `LATTICE_MONGODB_TEST_DATABASE`.
