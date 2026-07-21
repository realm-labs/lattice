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
`Tracked<T>` fields are required singleton documents: activation fails with
`RequiredDocumentMissing` when MongoDB has no matching row. An explicitly
annotated `#[mongo(default)] Tracked<T>` field is an eager singleton whose
absence activates an owner-aware in-memory default. A `#[mongo(many)]` field
implements `MongoDocumentCollection` and retains full control over its
map/vector representation and derived business indexes.

Loading policy is part of the field type, so eager business code never gains
an unnecessary async API:

| Model | Field type | Access after actor startup |
| --- | --- | --- |
| Required eager singleton | `Tracked<D>` | synchronous; missing is an error |
| Default-on-missing eager singleton | `#[mongo(default)] Tracked<D>` | synchronous; absent default stays in memory until changed |
| Eager complete collection | `C` with `#[mongo(many)]` | synchronous, including iteration |
| Resident lazy singleton | `MongoLazyDocument<D>` with `#[mongo(lazy)]` | first access is async, then resident |
| Resident lazy complete collection | `MongoLazyCollection<Owner, C>` with `#[mongo(lazy)]` | first access is async, then ordinary `C` APIs |
| Idle-unloadable singleton/collection | `MongoUnloadableDocument` / `MongoUnloadableCollection` | async acquisition; clean idle state can unload |
| Row-lazy table | `MongoLazyTable<Owner, Spec>` | async point/page load; synchronous access to resident rows |
| Idle-unloadable row table | `MongoUnloadableTable<Owner, Spec>` | row-level bounded idle eviction |

The default factory receives the aggregate owner ID because ordinary
`Default` cannot reliably construct the document identity:

```rust
use std::collections::BTreeMap;

use lattice_store_mongodb::document::tracked::Tracked;
use lattice_store_mongodb::{
    MongoDefaultDocument, MongoDocument, MongoDocumentSet, MongoScan,
};
use serde::{Deserialize, Serialize};

type PlayerId = u64;

#[derive(Debug, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "player_core")]
struct PlayerCore {
    #[mongo(id)]
    id: PlayerId,
    level: u32,
}

#[derive(Debug, Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "player_items")]
struct PlayerItems {
    #[mongo(id)]
    id: PlayerId,
    items: BTreeMap<String, u32>,
}

impl MongoDefaultDocument<PlayerId> for PlayerItems {
    fn default_for(player_id: &PlayerId) -> Self {
        Self {
            id: *player_id,
            items: BTreeMap::new(),
        }
    }
}

#[derive(MongoDocumentSet)]
#[mongo(id = PlayerId)]
struct PlayerDocuments {
    // Missing is an activation error.
    core: Tracked<PlayerCore>,
    // Missing constructs PlayerItems::default_for(player_id) in memory.
    #[mongo(default)]
    items: Tracked<PlayerItems>,
}
```

`LoadedPlayerDocuments` gives `core` the required
`LoadedDocument<PlayerCore>` type and gives `items` the optional
`Option<LoadedDocument<PlayerItems>>` type. Passing `None` follows exactly the
same default-and-absent registration path as a direct MongoDB load.

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
the loaded batch into tracked documents. Store-driven eager, lazy, and table
loads build their scan baselines directly from the MongoDB business BSON, so
loaded Rust values are not serialized a second time just to capture a
baseline.

A missing `#[mongo(default)]` field is not a pending create. The coordinator
captures its factory value as an absent baseline, and untouched mutable access
or a value changed and restored to that baseline remains write-free. A bounded
scan may retain a cursor across passes without creating anything. Once a scan
finds the first real BSON difference, the coordinator prepares one complete
`Create` using `CreateMode::InsertOnly`. A concurrent document inserted after
the missing read therefore becomes a conflict instead of being overwritten.
After Create acknowledgement, the complete written value becomes the normal
acknowledged baseline and later changes use incremental `Update` operations.

Map entry scanning is explicit. An ordinary map is treated as one whole BSON
field; opt in with `#[mongo(scan = "map")]` only when per-key `$set`/`$unset`
updates are useful. A Map scan consumes one field from `ScanBudget`, walks the
Map once, and serializes each value independently. Unchanged values are dropped
immediately; changed values are reused directly in `$set`, and missing keys
produce `$unset`. The full Map is never materialized as BSON during a diff.
There is deliberately no Map-entry cursor or separate Map-entry budget.

Dynamic keys containing `.`, a leading `$`, NUL, or the empty string cannot be
used directly as MongoDB update-path segments. Encode the stored BSON keys and
update paths consistently with the Serde adapter:

```rust
# use std::collections::HashMap;
# use lattice_store_mongodb::{MongoDocument, MongoScan};
# use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, MongoDocument, MongoScan)]
#[mongo(collection = "player_bag")]
struct PlayerBag {
    #[mongo(id)]
    id: u64,
    #[serde(with = "lattice_store_mongodb::document::bson_serde::path_key_map")]
    #[mongo(scan = "map")]
    items: HashMap<String, i32>,
}
```

The adapter uses reversible percent encoding compatible with Asteria's
`MongoPath`: `%`, `.`, `$`, NUL, and empty keys remain collision-free and are
decoded back into their logical Rust keys on load. Use
`bson_serde::encode_path_key` when building a MongoDB query against one encoded
map entry.

Custom Map serializers must declare how one entry is encoded so the scan does
not have to invoke the container serializer:

```rust
use lattice_store_mongodb::scan::{MongoMapScanAdapter, ScanError};
use mongodb::bson::Bson;

struct StringValueMapAdapter;

impl MongoMapScanAdapter<String, i32> for StringValueMapAdapter {
    fn encode_key(key: &String) -> Result<String, ScanError> {
        Ok(format!("key_{key}"))
    }

    fn encode_value(value: &i32) -> Result<Bson, ScanError> {
        Ok(Bson::String(value.to_string()))
    }
}

#[serde(with = "custom_map_serde")]
#[mongo(scan = "map", adapter = StringValueMapAdapter)]
items: HashMap<String, i32>,
```

The adapter's key and value BSON must exactly match `custom_map_serde`'s stored
representation. Invalid update-path keys and duplicate encoded keys are still
rejected by the framework.

`ScanBudget` limits documents and complete business fields. A field is the
smallest scan unit: with a two-field budget, one preparation scans fields A and
B and the next resumes at C. Large Map fields can therefore exceed the duration
target for one field, but cannot repeatedly starve later fields. State that
requires bounded entry-level loading should be modeled as a `MongoLazyTable`.

`MongoPersistenceCoordinator::scan_metrics()` exposes cumulative encoded
values, encoding time, estimated encoded bytes, hashed map entries, and
false-positive scans. The byte count is estimated from BSON values already
produced for comparison and does not trigger another serialization pass. To
measure scan regressions locally, run
`cargo bench -p lattice-store-mongodb --bench scan`; the benchmark covers
1 KiB, 10 KiB, and 1 MiB documents plus 1,000- and 10,000-entry maps.

`cargo bench -p lattice-store-mongodb --bench persistence` measures the
acknowledgement-based `prepare -> flush -> complete` pipeline and shutdown
draining for 1, 100, and 1,000 dirty resident documents. It uses an in-memory
acknowledging store so the result isolates framework scanning, request
construction, per-document outcome handling, and baseline advancement from
MongoDB server and network variance.

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
resident until their persistence state is safe to detach. Documents rejected
by storage also remain resident until current actor state is changed and
successfully acknowledged.

For efficient row pages, `PAGE_KEY_FIELD` must be unique within one owner and
must use the same ascending ordering as `Spec::Key`. Create a matching compound
MongoDB index, for example `{ "_id.world_id": 1, "_id.player_id": 1 }`; the
default index on the complete `_id` value does not replace this owner-prefix
query index.

Business documents must not define the reserved top-level fields `_id`,
`version`, `updated_at_ms`, or `_lattice_write_id`. The last field is an
internal idempotency marker used to reconcile writes whose server result became
ambiguous after a timeout. Prepared multi-document flushes report an exact
result per document but are not cross-document transactions.

Ambiguous failures such as timeouts retain the exact prepared write and
operation ID for retry. Definitive storage rejections, including MongoDB's
maximum-document-size error, do not retry the rejected BSON. The coordinator
records the document's mutation epoch and returns an incomplete preparation
while that epoch is unchanged. After business code removes or shrinks data,
the new epoch causes a fresh diff against the unmodified durable baseline.
`document_rejection` exposes the retained diagnostic until the current state is
successfully acknowledged.

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

There are two intentionally different creation states:

- `track_new(value, mode)` means the business has created a new durable
  document. It is a pending create and the next progressed scan emits Create
  even when the value has not changed since registration.
- `track_absent(value)` means a load proved storage absence and `value` is only
  an in-memory baseline. It is clean and detachable while unchanged, and emits
  no operation until a real BSON difference is found. `#[mongo(default)]`
  selects this path automatically with `InsertOnly`.

The actor adapter sends prepared writes through `ActorContext::pipe_to_self`.
An actor handles `MongoFlushCompleted` in a later turn and calls
`MongoPersistenceCoordinator::apply_completion`; baselines advance only after
an acknowledged write. Set `LATTICE_MONGODB_TEST_URI` to enable the live
MongoDB integration test, with optional `LATTICE_MONGODB_TEST_DATABASE`.
