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

Loading policy is part of the field type, so eager business code never gains
an unnecessary async API:

| Model | Field type | Access after actor startup |
| --- | --- | --- |
| Eager singleton | `Tracked<D>` | synchronous |
| Eager complete collection | `C` with `#[mongo(many)]` | synchronous, including iteration |
| Resident lazy singleton | `MongoLazyDocument<D>` with `#[mongo(lazy)]` | first access is async, then resident |
| Resident lazy complete collection | `MongoLazyCollection<Owner, C>` with `#[mongo(lazy)]` | first access is async, then ordinary `C` APIs |
| Idle-unloadable singleton/collection | `MongoUnloadableDocument` / `MongoUnloadableCollection` | async acquisition; clean idle state can unload |
| Row-lazy table | `MongoLazyTable<Owner, Spec>` | async point/page load; synchronous access to resident rows |
| Idle-unloadable row table | `MongoUnloadableTable<Owner, Spec>` | row-level bounded idle eviction |

```rust
# use lattice_store_mongodb::document::LoadedDocument;
# use lattice_store_mongodb::scan::ScanBudget;
use lattice_store_mongodb::document::set::MongoDocumentCollection;
use lattice_store_mongodb::document::tracked::Tracked;
use lattice_store_mongodb::MongoDocumentSet;
# use lattice_store_mongodb::persistence::coordinator::{MongoPersistenceCoordinator, PersistenceError};
# use lattice_store_mongodb::error::MongoStoreError;
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
`find_many` queries for eager fields before registering anything. Lazy fields
do not occur in the generated `LoadedPlayerDocuments` type and perform no
startup query. Skipped fields are initialized with `Default::default()`. The
collection adapter builds custom indexes after the coordinator has converted
the loaded batch into tracked documents.

## Lazy and unloadable state

The derive only wires loading policy and persistence scanning. The business
document still owns its ID, and the application still chooses owner filters,
map layout, and secondary indexes.

```rust
# use lattice_store_mongodb::document::tracked::Tracked;
# use lattice_store_mongodb::error::MongoStoreError;
# use lattice_store_mongodb::loading::document::MongoLazyDocument;
# use lattice_store_mongodb::loading::table::{MongoTableSpec, MongoUnloadableTable};
# use lattice_store_mongodb::persistence::coordinator::{MongoPersistenceCoordinator, PersistenceError};
# use lattice_store_mongodb::store::MongoStore;
# use lattice_store_mongodb::{MongoDocument, MongoDocumentSet, MongoScan};
# use mongodb::bson::{Document, doc, to_bson};
# use serde::{Deserialize, Serialize};
# type WorldId = u64;
# type PlayerId = u64;
# #[derive(Debug, Serialize, Deserialize, MongoDocument, MongoScan)]
# #[mongo(collection = "world_settings")]
# struct WorldSettings { #[mongo(id)] id: WorldId, tick: u64 }
# #[derive(Debug, Serialize, Deserialize, MongoDocument, MongoScan)]
# #[mongo(collection = "world_mail")]
# struct WorldMail { #[mongo(id)] id: WorldId, unread: u32 }
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct WorldPlayerKey {
    world_id: WorldId,
    player_id: PlayerId,
}

#[derive(Debug, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "world_players")]
struct WorldPlayer {
    #[mongo(id)]
    id: WorldPlayerKey,
    level: u32,
}

struct WorldPlayers;

impl MongoTableSpec<WorldId> for WorldPlayers {
    type Key = PlayerId;
    type Document = WorldPlayer;

    const PAGE_KEY_FIELD: &'static str = "_id.player_id";

    fn document_id(world_id: &WorldId, player_id: &PlayerId) -> WorldPlayerKey {
        WorldPlayerKey { world_id: *world_id, player_id: *player_id }
    }

    fn owner_id(document: &WorldPlayer) -> &WorldId {
        &document.id.world_id
    }

    fn key(document: &WorldPlayer) -> &PlayerId {
        &document.id.player_id
    }

    fn owner_filter(world_id: &WorldId) -> Result<Document, MongoStoreError> {
        Ok(doc! { "_id.world_id": to_bson(world_id)
            .map_err(|error| MongoStoreError::encode("encode world ID", error))? })
    }
}

#[derive(MongoDocumentSet)]
#[mongo(id = WorldId)]
struct WorldDocuments {
    // Loaded during actor startup. Every later access is synchronous.
    settings: Tracked<WorldSettings>,

    // Loaded on first access and retained for the actor lifetime.
    #[mongo(lazy)]
    mail: MongoLazyDocument<WorldMail>,

    // Each player row loads independently and clean rows idle for ten minutes
    // can be evicted with a bounded maintenance pass.
    #[mongo(lazy_unload = "10m")]
    players: MongoUnloadableTable<WorldId, WorldPlayers>,
}

async fn use_documents(
    documents: &mut WorldDocuments,
    store: &MongoStore,
    persistence: &mut MongoPersistenceCoordinator,
    player_id: PlayerId,
) -> Result<(), PersistenceError> {
    // Eager: no await and no load/get wrapper.
    documents.settings.write().tick += 1;

    // Lazy singleton: the returned mutable reference is borrowed directly
    // from the document set and cannot escape it.
    documents.mail.get_mut(store, persistence).await?.unread += 1;

    // Row-lazy: only this player document is queried.
    if let Some(player) = documents.players.get_mut(store, persistence, &player_id).await? {
        player.level += 1;
    }

    // Fetching a keyset page is async; iterating the fetched page is sync.
    let page = documents
        .players
        .load_page(store, persistence, doc! {}, None, 128)
        .await?;
    for player in page.iter() {
        let _ = player.level;
    }
    Ok(())
}
```

`MongoLazyCollection` loads the complete owner collection once and returns
`&C`/`&mut C`, so its maps, arrays, and custom indexes remain normal synchronous
Rust APIs after the initial `await`. `MongoLazyTable` is the separate model for
collections too large to keep completely resident. Its `MongoTableSpec`
defines the owner query, row cache key, document identity, and stable pagination
field; the framework does not prescribe business ID layout.

Call `unload_idle` from an actor timer or maintenance message. Singleton and
complete-collection unload returns `IdleUnloadStatus`; row tables additionally
accept `TableEvictionBudget` and report examined, unloaded, and dirty rows.
Dirty, newly created, scanning, in-flight, or conflicted documents stay
resident until their persistence state is safe to detach.

For efficient row pages, `PAGE_KEY_FIELD` must be unique within one owner and
must use the same ascending ordering as `Spec::Key`. Create a matching compound
MongoDB index, for example `{ "_id.world_id": 1, "_id.player_id": 1 }`; the
default index on the complete `_id` value does not replace this owner-prefix
query index.

Business documents must not define the reserved top-level fields `_id`,
`version`, or `updated_at_ms`. Prepared multi-document flushes report an exact
result per document but are not cross-document transactions.

## Coordinated persistence

`MongoPersistenceCoordinator` owns acknowledged scan baselines and optimistic
versions for all documents belonging to one actor activation. Business code
registers loaded or new documents, then enumerates current values in a bounded
preparation pass:

```rust
# use lattice_store_mongodb::persistence::coordinator::MongoPersistenceCoordinator;
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
