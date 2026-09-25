# Cluster Shutdown and etcd State Lifecycle: Design Draft

> Status: work package 1 is complete (see section 9.6 for evidence). Cluster shutdown, run-scoped storage, authority consolidation, and dynamic candidacy remain planned work packages 2/3.
> This document is the shared entry point for ongoing design edits and pre-implementation review, incorporating the earlier memo on minimal etcd state and Coordinator candidacy.
> Revise and approve this document before implementing it; no protocol or storage migration has been completed by this draft.
> Current implementation references: [placement architecture](architecture/03-placement.md) and [discovery](cluster-discovery.md).

## 1. Goals and boundaries

The primary goal is an explicit lifecycle for cluster runtime data: after a successful whole-cluster shutdown, the next startup must not restore membership, shard assignments, or scheduling operations left over from the previous run.

- Retain etcd, but progressively limit its role to necessary safety arbitration and recovery information rather than persisting all runtime state by default.
- During graceful whole-cluster shutdown, coordinate application shutdown, ownership release, and runtime-record cleanup.
- After an abnormal full stop, allow residual state to be removed through an explicit, inspectable, retryable administrative reset.
- Bound data growth by current members, current shards, and bounded in-flight operations, not the cumulative history of startups and migrations.
- Provide node-level and cluster-level shutdown capabilities; applications decide when to invoke them and how to bind signals.
- Require one exact Lattice version per cluster. Framework upgrades use a full stop and runtime-state reset. `AppVersion` and application rolling releases are deferred to a separate design, not included in these implementation packages.

An empty membership list must not automatically imply that the entire cluster has stopped. Automatic cleanup is not guaranteed after a forced kill or power loss, and deleting etcd records does not prove that old processes have stopped or that external writes are fenced. This work does not replace etcd with a custom Raft/Gossip system or immediately remove existing persisted fields.

## 2. Existing capabilities and gaps

- [Service](../crates/lattice-service/src/builder/service.rs) implements `shutdown()` through `leave(deadline)`, waiting for domain drain and membership removal confirmation before stopping components.
- In the same file, `terminal_shutdown()` supports whole-deployment termination without requiring local placement slots to migrate to another node. It fences local cluster authority before stopping local actors and runtime components.
- [Deployment](../crates/lattice-service/src/deployment.rs) shutdown coordinates logic and Coordinator components within the current deployment; it is not a global cluster shutdown protocol.
- [Member removal](../crates/lattice-coordination/src/runtime/membership_domain_ops.rs) handles claims according to the removal reason and may initiate shard/singleton recovery for the previous owner.

These capabilities do not implement the proposed `cluster.shutdown()`: cross-node and cross-domain coordination, completion evidence, and unified runtime-state cleanup are missing. The existing `terminal_shutdown()` execution order cannot simply become the new protocol; control sessions that must survive until confirmation and cleanup need to be identified.

Removing members or revoking leases does not currently delete all persisted assignments, revisions, and counters. Membership and each placement domain also have independent leadership scopes; they cannot be assumed to share one leader.

## 3. Public interfaces and application responsibilities

The following names express target semantics. Final Rust types, signatures, deadlines, and error structures remain undecided; these are not complete APIs already available today.

| Capability | Semantics |
|---|---|
| `node.shutdown()` | Gracefully leave the current node, transferring ownership when necessary while other nodes continue running |
| `cluster.shutdown()` | Initiate and await shutdown of the entire target cluster, coordinating stopping, ownership release, and runtime-state cleanup |

Applications bind Ctrl+C, SIGTERM, administrative endpoints, or deployment tools themselves. The library does not install global signal handlers, define an implicit development mode, or call `process::exit()`.

A local process managing a complete cluster could use this illustrative call sequence:

```rust,ignore
tokio::signal::ctrl_c().await?;
cluster.shutdown().await?;
```

A node joining an existing cluster can instead bind the signal to `node.shutdown()`. A single process does not prove exclusive ownership of a cluster; operations must explicitly identify the target cluster and authorization scope. Local development should use an isolated namespace.

Ordinary nodes can request cluster shutdown through the control plane from an authorized Coordinator without obtaining direct permission to delete etcd data. Cluster shutdown is not an iteration over in-process components: it must cover remote members and all relevant Coordinator scopes.

### 3.1 Target terminology

Use application-facing names that describe what users configure, while retaining precise internal terms for allocation, ownership, and rebalance.

| Current term | Target term | Meaning |
|---|---|---|
| Placement domain | Actor group | A group of actor types managed within one allocation and rebalance boundary |
| `PlacementDomainId` | `ActorGroupId` | Stable group identity, such as `gameplay` or `social` |
| Placement-domain Coordinator | Group Coordinator | Coordinates allocation and ownership transfers within one actor group |
| Membership Coordinator | Cluster Coordinator | Coordinates cluster membership and, under the proposed shutdown protocol, the cluster-wide lifecycle |
| Allocation strategy | Allocation strategy | Chooses where a shard should run |
| Rebalance strategy | Rebalance strategy | Chooses when and how existing assignments should change |

An actor group contains actor types, not an enumeration of active actor instances, and is not a broadcast group. It is also not a node role: a role selects eligible nodes, whereas a group defines a coordination boundary. A node can host types from one or several groups, or participate only as a proxy. Hosting eligibility and Coordinator candidacy remain separate concerns.

Keep `CoordinatorScope` as an internal election abstraction, with the target conceptual variants `Cluster` and `Group(ActorGroupId)`. Each scope has independent leadership; a dedicated candidate may campaign for multiple groups without being guaranteed to lead all of them. Applications should primarily configure clusters and actor groups rather than manipulate election scopes.

In existing-source descriptions and the earlier analysis, `domain` remains an alias for the current placement-domain concept, mapped to actor group by this table. Public Rust APIs now use the new names without forwarding aliases; historical descriptions use the mapping above. General phrases such as authorization scope or shutdown scope mean an operation's range, not a `CoordinatorScope` value. Renaming a Coordinator does not itself add cluster-wide shutdown behavior to the existing membership implementation.

### 3.2 Crate and migration naming

Review package names together with their actual responsibilities during implementation. The selected name for `lattice-placement` is `lattice-coordination`: the crate already contains membership, leadership, ownership, allocation, handoff, and singleton coordination, so `lattice-allocation` or `lattice-sharding` would describe only part of it. The crate rename is implemented in the section 9.6 checkpoint; its responsibilities remain together.

Do not split the crate solely to match terminology. Prefer cohesive internal modules such as membership, groups, allocation, ownership, and rebalance. `lattice-service` remains the assembly and application lifecycle layer; `lattice-remoting` remains transport and messaging. Neither needs a rename merely because placement terminology changes. Actor runtime scheduling must remain distinguishable from distributed allocation.

The eventual migration must consistently update package/directory names, workspace dependencies and lockfile, Rust imports and public types, examples, tests, feature references, scripts, and documentation links. Public naming changes and persisted/wire identifiers are different migrations: review key paths, serialized fields, protocol identities, and compatibility gates explicitly rather than applying a blind textual replacement. Physical `groups/` paths shown below are target paths, not the current stored schema.

### 3.3 Framework version and application releases

Implement only the framework-version boundary in this work. Framework upgrades are expected to be infrequent; cross-framework-version compatibility is deliberately not a goal, and must not drive parallel protocol implementations, schema readers, or compatibility matrices.

| Concept | Source | Purpose |
|---|---|---|
| `LatticeVersion` | Supplied automatically by the framework build, not application configuration | Exact framework identity required across all members and Coordinator candidates |
| `AppVersion` (deferred) | To be designed separately with the application release model | Future application release identity; no new API, field, or rollout policy is introduced by this work |

| Joining node versus the running cluster | Result |
|---|---|
| Same Lattice version | Passes the framework gate only; configuration/protocol checks still apply; application-version compatibility is not inferred |
| Different Lattice version | Reject, regardless of application version or semantic-version compatibility |

**Application rolling releases are deferred.** Remove the old release manifest, N/N+1 admission, and release-biased placement mechanism without introducing a replacement. Deploy one application version at a time until the separate rolling-release design is implemented. Their design must address old-release shard draining and migration, target-release eligibility, coexistence, failure/rollback, and message/business-state compatibility together. Adding a version field and an admission predicate alone would not settle those behaviors. Do not add `AppVersion`, prescribe its format/ordering, or implement new application-version coexistence rules in the current packages. Matching Lattice versions does not imply that arbitrary application binaries are compatible.

**One framework gate, not multiple compatibility negotiations.** Check the exact framework identity at connection/bootstrap admission before interpreting version-sensitive payloads, and before a candidate participates in coordination. Keep a minimal framework identity marker in etcd so a new binary cannot silently load old runtime records. The identity envelope and marker must be readable sufficiently early to reject mismatches clearly; this does not require supporting the old protocol after rejection.

Replace independently configured framework transport, control, storage, and watch compatibility generations with this framework gate. Retain independent configuration/protocol checks. The old application release mechanism is explicitly removed, including its self-reported ABI/state fingerprints; no replacement application compatibility guarantee is provided. Internal encoding discriminants may remain where parsing needs them, but must not become another public compatibility policy. Both published packages and development/custom builds use the exact Cargo package version, with no source digest or additional user-maintained version field. Developers are responsible for deploying consistent same-version builds and bumping the package version for incompatible protocol or storage changes. Protocol-changing feature combinations must not silently alter the same version's protocol contract. The gate deliberately does not detect different sources sharing a version; ordinary development edits and restarts do not create a new framework identity. Runtime cleanup remains a separate cluster-lifecycle responsibility.

**Framework upgrades require a full stop.** Complete cluster shutdown and approved runtime cleanup under the old deployment, or use the explicit abnormal-stop reset procedure after confirming old processes have stopped. Only then establish the new framework marker and a new run through a guarded administrative/startup transition. An upgrade must refuse leftover runtime state rather than deserialize it optimistically. No mixed-framework rolling upgrade or automatic reinterpretation of old runtime records is promised. A rollback to a different Lattice version follows the same full-stop/reset rule.

This is runtime cleanup, not deletion of all etcd data. Preserve required monotonic authority history, worker-ID safety history, and persistent configuration; inspect or explicitly migrate retained records if their representation changes. Never overwrite the framework marker as a shortcut around an active run or incomplete cleanup. The minimal retained metadata format and upgrade procedure need a deliberate contract, without requiring general compatibility with old runtime schemas.

**Safety counters are not release versions.** Keep internal epochs, terms, revisions, assignment generations, grant sequences, and incarnations wherever they distinguish stale ownership, sessions, or updates. Hide these from normal user setup; external fencing integrations should consume framework-issued authority tokens rather than assemble counters themselves. A full restart does not justify resetting counters with external safety meaning.

Automatic consistency checks also remain necessary: equal framework versions do not imply equal shard counts, mapper behavior, or registered message protocols. Retain required configuration/protocol identities or derived fingerprints, without exposing them as additional release-version knobs. Mapping changes still require the migration described in section 7.3.

### 3.4 Discovery and ordinary-node etcd access

Ordinary cluster clients obtain membership, routing, and serving authority through Coordinator control sessions, not by reading shard, claim, or membership storage records themselves. Discovery remains a replaceable provider and may directly use etcd. An etcd-backed provider is an acceptable initial deployment choice; a separate discovery service is not required.

| Component | etcd responsibility |
|---|---|
| Ordinary node's etcd discovery provider | Narrow, read-only lookup/watch of Coordinator connection information |
| Ordinary node's cluster client | No direct coordination-store access; communicates through Coordinator sessions |
| Coordinator candidate component | Election and leases; authoritative state access when holding leadership |

Candidates need etcd connectivity before election, including while on standby; connecting only after becoming leader would be too late. Ordinary nodes using static or DNS discovery need no etcd connection for cluster coordination. Separate features such as application configuration or distributed ID allocation may independently use etcd. Discovery is a provider capability, not a node role, and ordinary nodes are not required to carry dormant etcd configuration. A node enabling candidacy may receive connection configuration and credentials at that time or use pre-provisioned configuration; discovery-only read permissions do not grant election/write permissions.

An ordinary process using etcd discovery still opens an etcd connection and needs appropriate credentials and network access. This design reduces the scope of access and subscriptions, not necessarily the number of etcd clients. Static seeds, DNS, or a discovery proxy can replace the provider later without changing the cluster client's coordination contract.

The current discovery implementations supply entry-point lists: `StaticDiscovery` uses configured endpoints, DNS uses host/SRV resolution, and `ConfigStoreDiscovery` reads/watches an endpoint document (using etcd when backed by `EtcdConfigStore`). `JoinController` then probes those entries and connects to the selected Coordinator. `BootstrapView` answers from its in-memory leader view; it does not query etcd for every probe. An entry must support the remoting bootstrap protocol, not merely expose an arbitrary discovery HTTP endpoint. The leader-first resolution below is a target change, not a description of the existing etcd-backed provider.

Prefer resolving the current leader for the requested scope directly when the provider has that information. Return online candidate addresses as a fallback when no leader is known. Providers that only supply seeds still use bootstrap probing. Discovery results are connection hints, not proof of current authority; a leader can change between lookup and connection.

```text
resolve Cluster or Group scope
→ try current leader hint, otherwise candidate/bootstrap fallback
→ validate identity, framework version, and current leadership
→ establish a long-lived control session
→ receive membership, routing, and authority updates
→ on disconnect or NotLeader, resolve and reconnect with backoff
```

Share the provider client and cache within a process, coalesce lookups for the same scope, and watch only the Cluster scope and relevant actor groups. Prefer cache/watch updates and existing control sessions over periodically opening short-lived probe connections while a healthy session exists. Watch recovery must reconcile snapshots and revisions, including history compaction; retries need backoff and jitter. A changed leader hint may trigger session reconciliation, but neither a lookup nor a watch notification replaces authority checks.

A discovery outage alone does not require tearing down a healthy control session, and cached endpoints may still be tried. It also never extends serving authority or proves a cached leader remains valid. Business messages continue to travel directly to their owners rather than through the Coordinator. This design does not require eager Associations to all cluster members.

**Cold-start deployment requirements.** Prefer static seeds or DNS records pointing to Coordinator candidates. The Cluster scope and every actor group in use need at least one running, eligible candidate capable of reaching etcd; one candidate node may cover multiple scopes, which elect independently. A seed entry does not itself enable candidacy. Ordinary nodes with previously learned leader hints can answer bootstrap requests, but a group of uninformed ordinary nodes pointing at one another is not a valid cold-start foundation.

Candidates campaign independently of incoming bootstrap probes. Before election/recovery and leader-view propagation finish, all reachable entries may temporarily report that no leader is known. Retry with backoff and report join timeout/readiness blockers when appropriate. A reachable port is not evidence of a ready Coordinator. Missing candidates for one group block that group's coordination; independently ready groups need not fail, while a node requiring multiple groups must apply an explicit readiness policy. One candidate permits startup but provides no candidate-failure redundancy.

### 3.5 Runtime addition and removal of Coordinator candidates

Support runtime changes to candidate eligibility independently for the Cluster scope and each actor group. Candidacy is separate from hosting business Actors. Enabling it requires a node capable of running the Coordinator component, the appropriate etcd access, and the framework-version checks in section 3.3.

Separate managed eligibility configuration from online discovery registration:

- Eligibility identifies which authenticated node identity may campaign for a scope. Keep it in controlled persistent configuration, not in an address list that any node can rewrite.
- Online registration identifies the current incarnation and endpoint and has a lease-backed lifetime. Its presence alone does not grant eligibility; its removal alone does not revoke it.

Add a candidate by authorizing its scope, enabling its Coordinator component, validating connectivity/version, and registering it online before it starts campaigning. Becoming eligible does not make it leader. After election, it must complete the recovery gates in section 5.1 before serving the corresponding operations.

Remove a candidate by revoking eligibility and preventing further campaigns. A standby exits election participation. A current leader stops initiating new coordination work and safely relinquishes leadership; another eligible candidate wins an ordinary election and recovers state. Withdraw the removed scope's online registration as part of completion. The node may continue hosting Actors or campaigning for other scopes. Prefer adding a ready replacement before removing an existing candidate; the first implementation need not support choosing an exact successor.

Preserve discoverability during replacement as well as election eligibility. Update distributed static seed configuration, or the relevant DNS/discovery records, before retiring all old entry points, and verify that cold-start clients can reach the replacement through their configured provider. Changing candidate eligibility does not rewrite clients' static lists. A healthy new leader may otherwise be invisible to fresh or reconnecting nodes. Endpoint overlap, configuration rollout, and cache refresh must be included in the replacement procedure.

Eligibility revocation must participate in election and authoritative-write guards, not merely arrive through an asynchronous configuration notification. A paused or partitioned old candidate must not regain leadership or commit new authority using stale eligibility. Define guard semantics for removal and re-addition of the same identity so an old attempt cannot become valid again. Removing discovery records is not an authorization mechanism, and removing eligibility is not proof that the process or already-started external work has stopped.

During normal operation, reject removal of the last eligible candidate for a required scope by default. Serialize that check with configuration updates so concurrent removals cannot bypass it. Configured eligibility alone does not prove another candidate is online or ready. Whole-cluster shutdown has a separate ordering that preserves takeover capability until cleanup can finish. These are Lattice candidate changes, not changes to etcd's Raft membership; no additional consensus implementation is introduced.

## 4. State model and shutdown flow

```text
Cluster: Running → Closing → Closed
Node:    Ready   → Draining → Stopping → Terminated
```

This is the target state model. `Closing` must survive Coordinator failover. Node drain must distinguish leaving with migration from stopping with the cluster without migration. `Terminated` means the library runtime instance has ended; it does not require the host process to exit.

### 4.1 Commit shutdown intent

The initiating node submits a request to the control plane. After authorization, the coordinator records a shutdown operation for the current cluster run generation, using a stable operation ID to handle repeated calls, concurrent requests, and retries.

After entering `Closing`, reject new application-member admission and new ownership assignments, suspend ordinary rebalance and recovery allocation, and retain the control-plane and takeover capabilities needed to advance shutdown.

The Cluster Coordinator owns global shutdown orchestration, while Group Coordinators execute and report group-level progress. Persist enough progress for replacement leaders to continue the same operation. The exact cross-group barrier, and how joining, migrating, or campaigning participants enter the shutdown scope, remain protocol details to finalize. Notifying one leader does not mean every group has stopped allocating.

### 4.2 Nodes enter Draining

Other nodes receive shutdown control commands rather than operating-system signals. Nodes withdraw readiness and close library-level admission for new application work; readiness changes alone do not stop messages arriving over existing connections.

Wait for already-started processing to finish without migrating shards to other nodes. Drain policies and timeouts must explicitly cover queued messages, ask/reply, deferred work, child actors, background tasks, and lifecycle callbacks. There is no implicit guarantee that every message will complete.

Application-owned ingress such as HTTP requires an integration contract or notification mechanism; the library cannot automatically close unknown ingress. State transitions for nodes still starting, joining, or already draining must be specified later.

### 4.3 Stop actors and report completion

Nodes execute local stopping, confirm that application execution has ended, complete ownership release, and report the result. Receiving a command is not evidence of completed shutdown. Reports identify the shutdown operation and node incarnation.

Required control connections remain alive until stop reports are processed; endpoints must not be destroyed before reporting. Stop failures and timeouts expose specific blockers rather than automatically escalating to forced termination.

Lease renewal during drain, lease revocation timing, and the ordering of local fencing and stop callbacks must be decided together with the ownership protocol. Stop callbacks may have external side effects; the shutdown protocol does not replace fencing validation at external storage.

### 4.4 Cleanup and final termination

After confirming that relevant application participants have stopped and ownership is invalidated, the coordinator deletes runtime records according to an approved inventory. Information still needed to prove shutdown safety must not be deleted early.

Retain enough progress to support paginated cleanup and retries; do not rely on an unguarded prefix-wide deletion. Commit the shutdown result after cleanup, then terminate remaining control components. Candidates must respect shutdown state rather than campaigning during final termination and restoring ordinary scheduling.

The caller may itself be within the shutdown scope, so result queries, notifications, and destruction of the caller's runtime environment need an explicit order. Bounded completion records and minimal safety metadata may remain; the namespace need not become instantly empty.

## 5. Failure and completion semantics

- Losing the initiator does not revoke committed shutdown intent. A replacement Coordinator continues the same operation without restoring ordinary scheduling.
- Control-plane takeover needed to advance shutdown remains possible until completion; do not disable all candidate takeover prematurely.
- An unreachable node is not evidence of successful graceful stopping. Lease expiry invalidates the relevant authority but does not prove that application callbacks or external writes have ended.
- Partial stop failures, etcd unavailability, or incomplete cleanup must report an incomplete operation and its blockers, not success.
- A caller timeout does not implicitly roll back a committed operation. Callers must be able to query or await it again; cancellation semantics remain open.
- Waiting timeout, execution failure, and temporarily unknown results should be distinguishable; error structures remain open.

Success requires at least that relevant application instances have stopped, ownership release or invalidation has been verified, approved runtime records have been cleaned up, and the shutdown result is confirmable. The precise synchronization boundary between final control-task termination and API return must be settled before implementation.

### 5.1 Coordinator recovery and degraded operation

A newly elected Coordinator first recovers the minimal authoritative records for its scope, including relevant membership, assignments, claims, unfinished transfers, and lifecycle/shutdown progress. Nodes re-establish control sessions and re-report complete descriptions. Restore outstanding transfer budget usage before admitting new work. Do not treat election success as proof that reconstruction is complete.

Gate operations by their actual dependencies: an actor group or slot missing required descriptions or safety evidence cannot allocate, while independently recovered scopes need not wait for every cluster participant. Shared membership or lifecycle prerequisites still apply. Recovery must preserve `Closing` and continue shutdown instead of restoring ordinary allocation.

When etcd authority cannot be obtained or committed, stop new allocations and other operations requiring that authority. Existing owners may serve only while their previously established grants and all other authority conditions remain valid; neither a live socket nor a cached discovery record renews a grant. Close admission at the conservative local authority deadline and follow the defined retirement contract for in-flight work. Exact lease/session timing and recovery synchronization remain implementation obligations.

Unreachable participants and stop failures remain explicit blockers to successful graceful shutdown. Do not silently force completion or reset; an abnormal-stop administrative reset is a separate operation with the safeguards in section 6.3.

### 5.2 Owner behavior when connectivity or authority is lost

An assignment such as `shard-7 -> A` records the ownership decision; it is not perpetual permission for A to execute. Serving requires a valid grant for the exact owner incarnation and authority generation. The assignment can remain stored after the grant expires, preserving recovery information without keeping execution admission open.

The owner need not determine whether etcd failed or its own network is partitioned. It must determine whether its existing authorization remains valid:

| Failure | Owner behavior |
|---|---|
| Owner's etcd discovery is unavailable, but its Coordinator session and valid grant renewal continue | Continue serving under the normal authority checks; discovery failure alone does not revoke the grant |
| Owner cannot obtain confirmed renewal through its Coordinator | Retry, but do not extend the existing safe deadline; serve only while all existing authority conditions remain valid |
| Coordinator cannot establish renewed authority through etcd | It cannot manufacture an extension from a live connection or cached leadership; affected owners eventually close admission if no valid renewal arrives |

Illustrative timeline, not a chosen TTL or deadline algorithm:

```text
t=0s    A holds a validated grant with a conservative local deadline at t=10s
t=3s    renewal cannot be confirmed; retry without moving that deadline
t<10s   existing authorization may still permit work unless otherwise revoked
t=10s   no valid renewal: close execution admission and begin retirement
later   connectivity returns: reconcile authority, not merely socket health
```

At expiration, block new activation/publication and prevent queued business messages from beginning execution under the expired authority. Do not wait for an etcd deletion/watch event or a periodic timer to notice the loss; use-boundary checks must also cover a process resuming after a pause. A timed-out renewal may have succeeded remotely, but the owner cannot assume that it did.

Specify a conservative deadline protocol before implementation. It must account for request/response delay, Coordinator-to-owner delivery, clock-rate assumptions, and process or machine suspension. Receiving a delayed response must not start a fresh full TTL at receipt. A local grant must not outlive the backing authority, and early revocation/reassignment must respect the outstanding-grant and handoff rules. A safety margin without explicit timing assumptions is not a complete proof.

A healthy partition may authorize a replacement only after validating current leadership, loss or safe revocation of old authority, and required handoff conditions, then committing the replacement assignment/claim. An unreachable owner alone is not sufficient evidence. If etcd cannot provide the required authoritative commits, there is no local fallback that invents a new owner; affected shards become unavailable as their authorization expires.

Admission closure does not complete already-running handlers or retract external requests. Cancellation and stop callbacks follow an explicit retirement contract, and neither proves that external writes have ended. Where exclusive external writes are required, the destination must enforce the appropriate fencing boundary before replacement-owner writes; see section 7.11.

On reconnection, reconcile the current epoch, owner incarnation, assignment generation, and grant. A late renewal for a revoked generation must not reopen admission. **Proposed lifecycle rule, pending final confirmation:** once authority loss has put an instance into retirement, do not revive that instance; obtain fresh authorization and activate a new instance through the normal safety checks. A transient connection failure repaired by valid renewal before expiration need not retire the Actor.

## 6. etcd lifecycle and reset after abnormal termination

### 6.1 Distinguish recovery from a new cluster run

| Scenario | Target semantics |
|---|---|
| Single-node leave, application rolling update under one Lattice version, Coordinator failover | The cluster remains running; retain information needed for safe takeover |
| Lattice framework upgrade | Fully stop the old deployment and clean/reset runtime records before starting a new run; no mixed-framework operation |
| Explicit graceful whole-cluster shutdown | Coordinate stopping and runtime-state cleanup; the next startup begins a new run |
| Abnormal whole-cluster termination | Leased records expire while etcd is available; durable leftovers require an explicit recovery or administrative reset |

A temporary absence of members or leaders is not sufficient reason to erase data. The cluster may be starting, partitioned, or temporarily without etcd quorum. Explicitly starting a new run after `Closed` requires a separate definition and must not automatically override an unfinished `Closing` operation.

### 6.2 Data classification principles

| Category | Target direction; physical keys and migration remain undecided |
|---|---|
| Members, participants, assignments, and in-flight scheduling state | Define run generations and reclamation conditions; do not restore these as active state in the next run after graceful shutdown |
| Authority information such as leaders, terms, and required fencing generations | Retain only what safety requires and prove cleanup cannot make old requests valid again |
| Schema, persistent configuration, and similar metadata | Separate from runtime cleanup; do not automatically delete on shutdown |
| Completed operations and diagnostic history | Bounded retention and expiry rather than unlimited accumulation |

Clearing runtime state does not mean emptying the entire namespace. Counters must not be deleted and restarted if their fencing tokens still carry external meaning. New epochs also require comparison and validation rules; a fresh random ID is not itself a monotonic fencing token.

### 6.3 Administrative inspection and reset

Provide the following capabilities; these commands are illustrative and do not exist yet:

```text
lattice-admin runtime inspect --cluster demo
lattice-admin runtime reset --cluster demo --dry-run
lattice-admin runtime reset --cluster demo --apply
```

Show targets, record categories, and counts; scope operations to the cluster and run generation; protect application data and persistent configuration; prevent concurrent admission and allocation during reset; support retries and progress queries.

By default, refuse reset when active members or leaders are found. Even without active records, operators must confirm that the old deployment has stopped; absent records do not prove that partitioned processes are gone. Any force path needs a separate definition of permissions and risks.

This entry point supports recovery after abnormal full termination or incomplete shutdown. Normal local development should not require manual cleanup every time.

## 7. Target logical data model

The target is to persist minimal authority and recovery facts, not a complete snapshot of every runtime object. The model below is the agreed design direction, not an implemented schema or a complete handoff protocol. Record sketches describe logical fields rather than compilable Rust definitions.

### 7.1 Namespace and retention boundaries

```text
/clusters/<cluster>/
├── meta/
│   ├── framework                         [exact framework identity]
│   ├── lifecycle
│   └── terms/<scope>
├── definitions/
│   └── <group>/<entity-type>
├── coordination-config/
│   └── candidates/<scope>/<node-id>          [managed eligibility]
└── runs/<epoch>/
    ├── leaders/<scope>                       [lease-backed]
    ├── candidates/<scope>/<node-id>          [lease-backed online registration]
    ├── members/<node-id>                     [lease-backed]
    ├── groups/<group>/members/<node-id>      [session-leased target]
    ├── groups/<group>/slots/<slot-key>
    ├── groups/<group>/claims/<slot-key>      [lease-backed]
    ├── groups/<group>/transfers/<transfer-id>/meta
    ├── groups/<group>/barriers/<barrier-id>/meta
    ├── groups/<group>/barriers/<barrier-id>/participants/<participant-id>
    └── shutdown/<operation-id>/
        ├── meta
        ├── participants/<node-incarnation>
        └── cleanup/<scope>
```

Paths are illustrative. `scope` distinguishes the cluster-level Coordinator from each group-level Coordinator; `group` is an `ActorGroupId`. `slot-key` distinguishes a shard `(entity-type, shard-id)` from a singleton kind; singleton definitions require equivalent protocol/configuration metadata without shard-mapping fields.

Metadata, definitions, and managed candidate eligibility survive runtime cleanup. Records under `runs/<epoch>/` belong to one cluster run; slots and shutdown progress are durable within that run, while the marked records are lease-backed. Lease expiry alone does not remove the entire run.

Candidate configuration and online registrations are bounded per-scope/per-node records, not one growing candidate-list value. They distinguish authorization from discoverability as described in section 3.5. Bind registrations to exact incarnations and eligibility changes; expired registrations do not remove persistent eligibility. Discovery needs a narrow view of leader/candidate endpoints, not access to all coordination records. The physical representation of that view, its read permissions, and how startup discovers a new run without trusting a stale epoch remain to be specified; any projection is a hint, not another authority source.

`meta/framework` replaces a separately configured schema-compatibility generation in the target model. Its identity gate and upgrade transition follow section 3.3; retained metadata and definitions must be reviewed explicitly during a framework upgrade.

Cleanup must preserve the active cleanup cursor, exact leadership guard, and completion evidence until their last required use. The final result must remain confirmable through retained lifecycle/completion metadata before remaining per-run progress is reclaimed. The tree does not authorize an unguarded recursive deletion of the run prefix.

### 7.2 Cluster lifecycle and leadership

```text
ClusterLifecycle {
    epoch,                 // monotonically increasing cluster run generation
    phase,                 // Running | Closing | Closed
    shutdown_operation,    // optional current shutdown operation
}

LeaderRecord {
    node,                  // exact node identity, including incarnation
    term,
}
```

The leader's scope and epoch are supplied by its key and must be validated in the control protocol. `meta/terms/<scope>` retains the scope's election counter across runtime cleanup. A leader record is lease-backed; its durable term counter is not.

After a completed shutdown, an authorized, concurrency-safe startup operation advances the lifecycle epoch and establishes a new run. Empty membership does not authorize this transition, and an unfinished `Closing` operation cannot be silently replaced.

Epoch must participate in request, session, and authority validation. Merely changing the key prefix does not fence old messages or external writes. The comparison rules for epoch, term, and assignment generation, including their representation at external fencing boundaries, remain to be specified. Cleanup must not reset counters that still carry external authority.

### 7.3 Entity definitions and deterministic mapping

```text
EntityDefinition {
    protocol_id,
    shard_count,
    mapper_id,
    config_fingerprint,
}
```

Keep one authoritative mapping definition per domain/entity type rather than embedding complete copies in every participant record. Other required persistent configuration must be accounted for in the migration inventory; this sketch does not authorize dropping it.

The routing relationship is:

```text
Entity ID → deterministic mapper → Shard ID → persisted owner → local ActorRegistry
```

The current default mapper uses `xxh3_with_fixed_seed(entity_id) % shard_count`; the current implementation gives custom mappers an explicit identity and version. See [mapping.rs](../crates/lattice-coordination/src/mapping.rs). The target model avoids a separate mapper release-version knob: mapping identity and configuration must unambiguously identify the algorithm and its parameters, including custom behavior. A framework version alone cannot establish that agreement. Every host and proxy must agree on the mapping configuration. Changing shard count or mapper behavior requires a mapping migration, not an ordinary configuration refresh.

Do not add a default per-entity placement table to etcd:

- If only the Coordinator fails, actors on surviving owners remain local runtime instances. The new leader recovers shard authority and reconciles owner sessions; it does not need an entity enumeration to route messages.
- If an owner fails, safely reassign its shard after validating loss of old authority. Entities can activate on demand when messages arrive, using the application loader to recover any persisted business state.
- An entity ID list does not recover actor memory or business state. Those are separate persistence concerns.
- Recreating all previously active entities without waiting for new messages would require an explicit remembered-entities capability and storage contract. It is not part of the baseline placement model.

### 7.4 Minimal membership and domain eligibility

```text
Member {
    incarnation,
    endpoint,
    phase,                       // Joining | Up | Leaving
    descriptor_fingerprint,      // validated session description, not a release version
}

GroupMember {
    incarnation,
    phase,
    eligibility_fingerprint,
}
```

Node identity and domain are also supplied by the keys. Keep the authority needed to establish exact membership and placement eligibility; move full protocol catalogues, deployment descriptions, capacity reports, and scheduling attributes to versioned Coordinator memory, populated through control sessions.

The cluster-wide framework identity is checked at admission against `meta/framework`; it need not be duplicated as a framework compatibility matrix in each member. No new `app_version` field is added in this work. The removed application release manifest and rollout-participant flag are not part of this record.

A new leader obtains missing descriptions from re-established sessions and must not make allocations that depend on unconfirmed information. Fingerprints must be bound to the validated session data; a fingerprint alone is not proof of eligibility.

Domain participation should have an explicit session lifetime rather than indefinite persistence. The lease binding, renewal owner, and dependency on global membership are open protocol details, not permission to attach an arbitrary lease to the current record. Existing ownership transactions compare global and domain member records; replacing their payloads or lifetime requires preserving that safety check.

### 7.5 Slot assignment and in-progress transfer

```text
SlotRecord {
    generation,
    config_fingerprint,
    state,           // conceptual: Unassigned | Activating | Running | Transferring
    owner,           // optional exact node identity
    transfer_id,     // optional reference, not an embedded transfer or participant set
}

TransferRecord {
    operation_id,
    source,
    target,
    phase,
    barrier_id,     // reference to separately stored handoff safety conditions
}
```

The slot identity comes from its key. Preserve assignment generations and transfer facts across leader changes, including when no claim remains. The state names above are a simplification, not a finalized replacement for existing fenced/stop-failed states; failed or blocked handoffs must remain representable.

Persist facts about transfers that have started, not the entire rebalance decision process. Candidate moves, scores, and plans not yet executed can be recomputed. Evidence that an old owner has stopped or that a new owner has been authorized cannot simply be discarded.

Transfer metadata is stored separately from the slot. Barrier metadata and participant acknowledgements are separate bounded records as specified in section 7.9. The exact safety conditions and recovery rules still require review of the existing handoff protocol. Current barrier participant sets cannot be removed merely because they look like temporary runtime collections.

### 7.6 Lease-backed serving claims

```text
Claim {
    owner,
    leader_term,
    assignment_generation,
    grant_sequence,
    ttl,
}
```

The run, domain, and slot are supplied by the key. The etcd lease attachment supplies expiry; the TTL field alone does not make a record expire.

`SlotRecord` records assignment and transfer facts; `Claim` represents current serving authority under the ownership protocol. Do not merge both into a single lease-deleted record: losing a claim must not erase generations or unfinished handoff evidence.

A replacement leader checks the slot, claim, exact owner incarnation, and membership eligibility before adopting or replacing authority. A stored `owner = A` does not prove A can still serve. Claim absence also does not, by itself, prove that old application work or external writes have stopped.

### 7.7 Shutdown progress

```text
ShutdownOperationMeta {
    operation_id,
    phase,
    participant_set_version,
    participant_set_state,    // Building | Sealed
    participant_count,
}

ShutdownParticipantProgress {
    phase,
    result,                  // bounded status, not arbitrary logs or error chains
}

ShutdownCleanupProgress {
    phase,
    cursor,                  // bounded cursor for one cleanup scope
}
```

These records allow a replacement Coordinator to continue the same shutdown rather than resume ordinary scheduling. Progress must cover exact participants and relevant leadership scopes. Participant acknowledgements and per-scope cleanup cursors must be stored separately, not embedded as growing maps in the operation metadata. Keys and validation bind each progress record to its operation, run, participant incarnation, and participant-set version. If per-node progress needs a growing set of domain acknowledgements, split those records by domain as well.

The final completion evidence and its retention must be coordinated with `ClusterLifecycle` before deleting per-run shutdown records, as described in section 7.1.

### 7.8 Data outside the default etcd runtime model

| Owner | Data |
|---|---|
| Coordinator memory | Full member descriptions and protocol catalogues, load samples, scheduling scores, derived routing views, and unstarted rebalance plans |
| Actor owner memory | Entity ID to local actor-instance registry, active entity sets, mailboxes, and local execution state |
| Application storage | Persisted actor business state and its recovery contract |
| Observability systems | Diagnostic history that is not required as authority or deduplication evidence |

Small bounded administrative deduplication results and required durable operational settings may still need persistence. Their physical location and retention belong in the inventory; they are not silently removed by omission from the core tree. Existing revision and capacity-counter records also require an explicit disposition rather than automatic deletion.

ConfigStore data and worker-ID allocation history are separate storage concerns and are excluded from cluster runtime reset. Retaining data needed for ID reuse safety is not the same as retaining disposable runtime history.

In summary, the core retained model is cluster lifecycle, minimal membership eligibility, leadership, stable mapping definitions, and slot authority plus safe transfer facts. Entity-to-shard mapping is computed; shard-to-owner placement is a decision that must survive leader loss.

### 7.9 Bounded values, participant sets, and storage operations

Avoid large values by construction, rather than relying on an expectation that clusters will stay small. In particular, transfer barriers and shutdown progress must not embed all participants in one value.

```text
BarrierMeta {
    participant_set_version,
    phase,                   // Building | Sealed | Completed
    participant_count,
}

BarrierParticipant {
    incarnation,
    status,                  // pending or confirmed under the handoff protocol
}
```

Barrier identity, run, and domain come from the key. Acknowledgements must identify the exact barrier, participant incarnation, and participant-set version; an old acknowledgement must not satisfy a later transfer. Count and phase fields are summaries, not sufficient evidence of safety on their own.

Required properties:

- Build participant records in bounded batches and seal the set before evaluating completion. Recovery from a partial build must not treat missing records as acknowledgements.
- Tie set construction and sealing to guarded revisions or equivalent validated manifests. Do not infer completion from a transient empty page or an inconsistent multi-page scan.
- Preserve the meaning of existing stop/fencing proofs. Participant disappearance or lease expiry is not automatically a positive acknowledgement.
- Create, reconcile, and reclaim large sets through bounded pages. Delete transfer/barrier records only when no live authority or recovery path still depends on their evidence.
- Do not share a barrier between transfers merely because their participant sets match; sharing also requires proof that their acknowledgement semantics and generations are compatible.
- Keep identifiers, cursors, status details, and error summaries bounded. Do not move an unbounded participant or scope list into another nominally small metadata field.

Ordinary records should target roughly 1 KiB or less. Use 4 KiB as the initial proposed serialized-value ceiling, to be finalized with encoding tests and worst-case identifier lengths. This is a design budget, not an existing enforced limit. Collections must use bounded records or explicitly bounded chunks; they must not bypass the budget through ad hoc exceptions. Separately bound key length, transaction operation count, total encoded request bytes, and scan page bytes.

Splitting values increases key count and does not eliminate total storage growth. If each of `M` active transfers needs `P` participants, independent barriers can require `O(M × P)` records. Review whether all participants are genuinely required, cap set cardinality, bound active transfers through admission budgets, and reclaim completed operations. A small value does not justify unbounded fan-out or a giant transaction.

When transfer metadata and its slot reference are separate, their publication and phase transitions must remain guarded and recoverable. Incomplete record creation cannot authorize an owner or bypass a barrier. The exact commit protocol is an implementation prerequisite.

### 7.10 Application-selected rebalance policy and runtime admission

Applications select a provided rebalance policy or supply their own. Policy controls which shards should move, where they should move, and how quickly to start work; the runtime enforces the selected budget and non-negotiable authority constraints.

Build on the existing [allocation policies and RebalanceLimits](../crates/lattice-coordination/src/allocation.rs), rather than introducing an unrelated scheduling mechanism. The following describes the target contract, not a finalized Rust API:

```text
RebalanceDecision {
    candidates,    // bounded list of shard/source/target proposals
    budget,        // round-start and in-flight limits, with explicit scope
}
```

Policy inputs should include the current placement view, load and eligibility information, trigger reason, and already active or reserved transfers. Policy may select thresholds, cooldowns, absolute or relative round limits, and budgets per domain, entity type, source, and target. Automatic balance, node drain, and failure recovery may have different priorities or pacing, but none may bypass ownership safety. Failure recovery is not permission to wait for a failed source forever; it follows the separately defined loss-of-authority protocol.

Round-start limits, in-flight concurrency limits, and rate limits are distinct. A round limit bounds newly started transfers per evaluation; a concurrency limit bounds unfinished/reserved transfers across rounds; a rate limit or cooldown bounds starts over time. For example, a concurrency budget of four with three transfers already active admits at most one additional transfer, even if the policy proposes twenty candidates.

Runtime obligations:

- Revalidate each candidate immediately before admission: current owner and generation, target eligibility, lifecycle state, and remaining budgets may have changed since planning.
- Account for active and reserved work across automatic, manual, drain, and recovery triggers; independent proposals cannot each consume the same available capacity.
- Prevent duplicate concurrent transfers of a slot. Reserve capacity and publish recoverable transfer intent before executing external handoff effects; keep reservations bounded and recoverable.
- On leader takeover, reconcile unfinished transfers and restore their budget usage before admitting new work. Do not reset concurrency accounting merely because the policy instance restarted.
- If policy lowers a budget below current usage, pause new admission rather than interrupt an in-progress safety-critical handoff. Release capacity only when the transfer reaches a safely resolved state.
- Reject or defer unsafe candidates even when a custom policy selects them. In `Closing`, disable ordinary rebalance regardless of policy output.

The initial budget scope is an actor group (currently a placement domain), with per-node incoming/outgoing limits within that group. A domain-wide limit must not be advertised as a whole-cluster limit: independent domain leaders each enforcing four transfers can collectively execute more than four, including at a shared node. Defer cross-group global quotas until a concrete requirement justifies a shared admission mechanism; they are not implied by an existing field named `concurrent_cluster`.

Only transfers admitted to start create durable transfer intent and barrier state. Unstarted candidates remain bounded in memory and may be recomputed after failover. This does not remove separately required administrative request deduplication. Execution capacity and storage cardinality limits must both be satisfied before admitting a transfer.

Together, these mechanisms bound different costs: record splitting bounds individual values; participant limits and concurrency budgets bound in-flight record count; round/rate budgets bound bursts; completion cleanup bounds retained history. Business pacing remains configurable, while storage limits and the ownership protocol remain enforced by the library.

### 7.11 Consolidating authority checks and revocation

Simplify fencing by centralizing rules and revocation orchestration, not by indiscriminately removing checks. Similar-looking checks can protect different objects or different moments in an asynchronous operation.

| Boundary | Protection that must remain |
|---|---|
| Authoritative etcd transaction | Reject writes from a Coordinator that no longer holds the exact leadership authority |
| Slot ownership and serving grant | Reject stale owner identities, generations, or expired local grant deadlines |
| Registry activation and publication | Prevent a load started under old authority from publishing or returning a usable stale instance |
| Actor admission and retirement | Prevent cached handles and queued work from bypassing revoked admission; stop or retain old instances safely |
| Membership snapshot application | Prevent delayed snapshots from reintroducing a retired node incarnation |
| External side-effect boundary | Enforce the external resource's fencing contract; local admission closure alone cannot reject an already-issued write |

Use the existing [PlacementAuthority](../crates/lattice-coordination/src/authority.rs) and [Registry authority validation](../crates/lattice-actor-distributed/src/registry.rs) as the starting points. Do not introduce a third independently maintained authority model alongside them. Exact target type names remain subject to the terminology migration.

**Centralize the ownership decision.** Entity and singleton routes, direct Registry activation paths, and publication must use one authoritative interpretation of owner incarnation, assignment generation, slot state, grant validity, and admission. Callers should not each reconstruct these rules with their own combinations of conditions. Derived admission gates may exist at execution boundaries, but their revocation and generation binding must remain consistent with that authority source.

A validated token describes a successful check, not permanent permission. Preserve revalidation across relevant asynchronous gaps:

```text
validate authority
→ await actor loading
→ revalidate and publish under synchronization with revocation
```

An owner can lose authority while loading. Merely wrapping the first check in a proof object or moving duplicate predicates into a boolean helper does not close that race. Define the synchronization or guarded commit boundary between validation, publication, and revocation. Do not hold a synchronous authority lock across arbitrary asynchronous application work.

Similarly, checking before enqueue does not establish permission when a queued message later begins execution. All supported ingress paths, including cached/exact references, must obey the execution-admission contract. Keep grant-deadline checks at the relevant use boundary: a periodic tick alone cannot protect a process that resumes after being paused beyond its lease deadline.

**Provide one revocation orchestration entry point.** Route authority loss through an idempotent operation bound to the exact slot and authority generation. Its responsibilities are:

1. Invalidate the authority and promptly close admission for affected instances.
2. Prevent in-flight activation under the revoked generation from publishing a usable instance.
3. Remove or invalidate old addressability and Registry entries without affecting replacement instances.
4. Drive the appropriate stop or quarantine path and expose progress and failures.

These are ordering requirements, not permission to implement four unsynchronized calls. Admission closure must not wait for asynchronous cleanup or a full scan of actors. A delayed duplicate revocation must not revoke a newer generation. Cleanup retries must be safe, and stop failure must never reopen an old instance's admission.

**Separate authority, admission, and retirement responsibilities.**

- Authority determines whether the distributed owner may serve.
- Admission controls whether new application work may enter execution. Closing it is not proof that all in-flight work has ended.
- Retirement/quarantine manages stopping, retained failures, diagnostics, and explicit recovery of old instances.

Keep the local Actor kernel concerned with admission and lifecycle rather than etcd terms, actor groups, shards, or leases. Group-wide authority loss is translated into kernel admission/retirement actions by the distributed integration layer. Graceful drain and unexpected authority loss may share primitives but need not have identical handling of queued or in-flight work.

Some mechanisms cannot be collapsed into this local gate. Leader/term comparisons must remain inside authoritative etcd transactions; a preflight check cannot replace them. Membership-incarnation filtering is stale-view protection, not Actor admission. External stores must validate their own fencing tokens where required: neither local cancellation nor a closed mailbox proves that an outstanding request or stop callback cannot perform an external side effect.

Before removing any existing check, identify the invariant it protects, every path that reaches it, and the synchronization boundary that will replace it. Prioritize shared entity/singleton and Registry activation rules, then consolidate the authority-loss-to-quarantine path. Treat check removal as a correctness change requiring focused race tests, not a textual deduplication.

## 8. Current etcd inventory and candidates for relocation

Use the target model in section 7 to review actual stored data and its read/write dependencies before finalizing each retention, reduction, or relocation. The following categories remain a migration checklist, not an approved complete key inventory or deletion plan:

- Schema, storage limits, and compatibility metadata.
- Leaders, terms, revisions, and counters for each scope.
- Discovery records and candidate configuration/registration, including the migration from existing discovery configuration to separately guarded eligibility.
- Global membership and domain participants.
- Entity-type and shard/singleton configuration and assignments.
- Claims, leases, ownership generations, and handoff records.
- Rebalance plans, administrative operations, deduplication, and diagnostic history.

The first source inventory is recorded in [P0 source inventory and protocol evidence](cluster-control-plane-preparation.md), inspected on 2026-09-25. It covers current coordination keys, administrative migration/repair keys, config-backed discovery, and worker-ID allocations/history, with writers/readers, growth patterns, lease/deletion behavior, and proposed treatment. It also records transaction invariants and a focused test map. It is not an approved deletion manifest or a measured size/performance report.

Key findings:

- Global members are leased; current group member records are not. Assignments, plans, state/capacity counters, settings, and operation results can remain after every process stops.
- Full member descriptions and per-move/slot participant sets are major structural growth sources. Stored type definitions duplicate parts of group-member descriptions.
- Leader/term guards and exact global/group member revisions are compared inside authority transactions. Payload minimization must retain those safety boundaries.
- Config-store data and worker-ID history use separate configured prefixes and are not generic runtime cleanup targets.
- The current grant path installs a receipt-time TTL deadline and avoids reliable-outbox replay. The end-to-end delay/suspension contract and retired-instance behavior still need explicit decisions before W2.3/W2.4.

For each entry, identify the safety invariants requiring etcd storage, whether it must survive a whole-cluster restart, whether it can become a versioned in-memory leader view, how a new leader would reconstruct it and verify previous ownership loss, which operations must pause during recovery, and whether reclamation would remove deduplication evidence needed for old requests or retries.

There are currently no per-entity placement keys; concrete actor activation identity lives in local/remoting runtime state. Shard count and application entity count must not be treated as the same source of data growth.

Shutdown cleanup and reducing persistence during operation are related but separate tasks. Adding cleanup alone does not complete etcd minimization, and recovery information must not be removed before designing the replacement takeover protocol.

## 9. Pre-implementation decisions and acceptance criteria

The [W2/W3 implementation contracts](cluster-control-plane-contracts.md) propose concrete transaction boundaries, renewal freshness, candidate revocation, sealed transfer sets, and shutdown/reset semantics. They refine the questions below without marking them implemented. Explicit behavior-approval and validation gates remain open; use section 9 here as the sole completion checklist.

This section is the single active list of remaining decisions and validation requirements. The historical analysis in section 10 is not a second backlog. Sections 3.4, 3.5, and 5.1 establish discovery through replaceable providers (including etcd), runtime candidate changes, and recovery/degraded-operation boundaries. Section 4 assigns global shutdown orchestration to the Cluster Coordinator and group execution to Group Coordinators.

Remaining shutdown details include cross-group barriers, authorization and operation deduplication, drain policies, handling of unreachable nodes and in-flight migrations without false success, shutdown-progress recovery, new-run startup and safety generations, per-key cleanup scope and exclusion, and the relationship between existing terminal/force APIs and the public interface boundary.

For discovery, finalize provider results for leader hints versus candidate fallback, narrow etcd permissions and endpoint representation, run transitions, shared caching/watch recovery, and reconnect pacing. Ordinary cluster clients do not gain direct access to shard/claim storage. An etcd-backed discovery provider's client/watch load must be measured rather than described as eliminated.

Specify per-group startup/readiness reporting and candidate-replacement procedures that preserve cold-start discovery, including static seed distribution and DNS/cache refresh. Candidate eligibility and entry-point reachability are separate deployment requirements.

For dynamic candidacy, finalize authenticated identity binding, configuration administration, per-scope eligibility guards, removal/re-addition races, safe leader retirement, and progress reporting. Define concurrent last-candidate checks and shutdown exceptions. Configuration updates must remain possible during leader replacement without relying solely on the leader being removed; exact administrative access and completion semantics remain to be specified.

For failover and partitions, specify dependency-based recovery readiness, conservative grant deadlines, and the boundary between stopping new authority operations and retiring existing owners. Missing information blocks dependent operations; discovery cache freshness is not a serving-authority guarantee.

For section 5.2, prove the deadline/renewal rules under the chosen delay, clock, and suspension assumptions, including early revocation and replacement. Confirm the proposed rule against reviving instances already in retirement and define fresh activation when old work or quarantine has not yet completed.

For the target data model, also finalize session-lease binding for domain members, the minimal transfer barrier and blocked states, epoch-aware fencing, persistent configuration ownership, and the replacement of existing transaction predicates. The logical record sketches do not settle those protocol obligations.

For bounded storage and rebalance admission, finalize participant-set sealing and completion proofs, multi-record publication/recovery, encoded-size limits, policy API and budget update semantics, and recoverable capacity accounting. Cross-group global quotas are deferred, not a prerequisite for the initial implementation.

For authority consolidation, specify the shared authority source, publication/admission synchronization with revocation, generation-bound idempotent retirement, and the treatment of queued versus in-flight work. Audit existing checks against these invariants before removing or relocating them.

For the version policy in section 3.3, use the exact Cargo package version across crates without a source digest. Finalize the early rejection envelope and retained metadata upgrade/reset contract. Inventory existing compatibility generations and fingerprints before removing them; distinguish framework compatibility machinery from runtime safety counters and configuration consistency checks; the old application release mechanism is removed separately. `AppVersion` and rolling-release semantics are deferred and do not block package 1.

Apply the terminology mapping in section 3.1 throughout the eventual API and documentation migration. Confirm the proposed crate name in section 3.2 and separately decide the persisted/wire compatibility boundary before renaming stored identifiers.

Execution follows the three work packages in sections 9.2-9.4, after the preparation gate in section 9.1. Their dependency order and validation policy are defined in sections 9.5-9.6. The open decisions above remain authoritative; the implementation plan does not silently resolve them.

Acceptance coverage must include at least:

- A local single-process application binds Ctrl+C to cluster shutdown, and the next startup does not restore old runtime state.
- Single-node leave does not trigger whole-cluster cleanup; multi-node, multi-domain shutdown does not migrate shards among stopping nodes.
- Long-running handlers, queued messages, background work, and stop failures follow explicit drain contracts.
- Initiator loss, Coordinator failover, repeated requests, and interrupted cleanup remain queryable and retryable without restoring ordinary scheduling.
- Network partitions, lease expiry, and etcd unavailability are not misreported as successful graceful shutdown.
- Inspect/dry-run/reset operations do not affect other clusters, application data, or persistent configuration.
- When shutdown/reset races with node startup, barriers and run generations prevent old state from entering the new run.
- Coordinator failover recovers shard authority without a per-entity placement table; missing member descriptions block dependent allocations until reconfirmed.
- Leader-first discovery and candidate-only providers both establish validated control sessions. Stale hints, NotLeader responses, watch interruption/compaction, and run changes recover without granting authority from discovery data.
- Ordinary etcd discovery uses only its allowed read surface and shares clients/cache within a process. Healthy control sessions survive a discovery-only outage without extending serving grants; reconnect storms have bounded, jittered retries.
- Static/DNS clients cold-start through candidate entry points without an etcd client of their own. Missing candidates and election/recovery delays produce scope-specific readiness or timeout diagnostics, not false readiness from a listening port.
- Candidate replacement preserves discovery for fresh and reconnecting clients; tests include stale static seed lists and delayed DNS/cache updates, not just already-connected clients.
- Candidates can be added or removed independently per scope while business Actors remain hosted. Removal of a standby and of the current leader both complete through observable steps and preserve recovery safety.
- Revoked candidates cannot win elections or commit new authority despite delayed notifications; removal/re-addition and delayed registration cleanup cannot revive stale attempts or erase a replacement incarnation.
- Concurrent removals cannot eliminate the last eligible candidate during normal operation. Shutdown retains the takeover capability needed for completion; candidate administration does not require etcd Raft membership changes.
- Owner failure supports safe shard reassignment and on-demand entity activation, with business-state recovery tested separately.
- Claim expiry preserves assignment generations and unfinished transfer evidence; takeover cannot bypass a pending safety barrier.
- Mapping mismatches are rejected, and changes to shard count or mapper behavior require an explicit migration.
- A different Lattice version is rejected during bootstrap/connection admission and Coordinator participation, before version-sensitive state is consumed.
- Framework upgrade and rollback refuse uncleared runtime records or an active old deployment. A guarded full-stop/reset transition preserves required safety history and establishes the new framework identity and run.
- Protocol/configuration mismatch detection remains after old application release guards are removed. No `AppVersion` or new application-version coexistence behavior is introduced.
- Worst-case encoded records obey the agreed value/key limits; participant growth creates bounded records rather than enlarging slot or shutdown metadata. Transactions and scan pages obey byte and operation-count limits.
- Interrupted participant-set construction, stale acknowledgements, and concurrent updates during paginated reads cannot falsely complete a barrier or shutdown operation.
- Repeated and concurrent rebalance evaluations cannot oversubscribe domain/entity/source/target budgets or start duplicate transfers; in-flight work restored after failover consumes capacity before new admission.
- Lowered budgets pause new work without abandoning unsafe handoffs; custom policies cannot bypass eligibility, authority checks, or `Closing` restrictions.
- Multi-domain tests distinguish per-domain limits from true cluster-wide quotas. High-participant barriers and repeated transfers remain within cardinality limits and are reclaimed safely.
- Authority revoked while an Actor loader awaits prevents stale activation publication; logical, direct Registry, and cached/exact-reference paths obey the same authority contract.
- Queued work cannot begin under revoked admission, and an expired grant is rejected on use even before the next periodic fencing tick. Already-running work follows an explicit retirement contract rather than being assumed stopped.
- Renewal timeouts and delayed replies cannot extend an unconfirmed grant; process suspension and reconnect cannot revive an expired/revoked generation. Test discovery-only failure separately from loss of the control session or Coordinator-to-etcd authority.
- Repeated or delayed revocation cannot affect a replacement generation; stop/quarantine failure keeps old admission closed and retains actionable diagnostics.
- A Coordinator losing leadership between preflight and commit cannot write authoritative state; delayed membership snapshots cannot resurrect a retired incarnation.
- Entity and singleton paths exercise the shared authority rules. Tests distinguish local admission closure from external-store fencing and do not claim the former prevents all external side effects.
- Historical records remain bounded across repeated startups, shutdowns, and migrations; write, watch, snapshot, and reconnect load is measured at representative scale.

### 9.1 Preparation gate and implementation rules

Status: work package 1 is complete; the remaining P0 authority/storage preparation applies to packages 2/3. The [preparation evidence](cluster-control-plane-preparation.md) records the first key-family audit, protocol findings, and test map. P0.1 still needs finalized field-removal/cleanup contracts; P0.4 has no executed baseline or scale measurements. Only work items with completed deliverables and recorded validation are checked. This plan groups responsibilities, not independent crate rewrites or separate production rollouts. Keep one final target protocol/schema; do not build old/new dual-read, dual-write, or mixed-framework compatibility paths merely to stage development.

- [ ] **P0.1 Inventory:** complete section 8 with actual keys, readers, writers, leases, transaction predicates, and retention rules. Include configuration and worker-ID safety history so runtime cleanup cannot erase unrelated persistent state. Record each removed field's replacement or why it is no longer needed.
- [ ] **P0.2 Contract decisions:** resolve the relevant section 9 questions before implementing each affected boundary. First settle candidate eligibility/revocation guards, grant deadline and renewal assumptions, transfer/barrier completion evidence, and cluster shutdown success/cleanup conditions. Confirm the proposed no-revival lifecycle rule rather than implementing it as an unstated assumption.
- [ ] **P0.3 Public contract:** the crate rename and Cargo package version as the sole framework identity are confirmed; finalize public administration/lifecycle entry points. API examples must distinguish configuration, candidate eligibility, discovery hints, and serving authority. Application rolling-release design is deferred, not a prerequisite for this work.
- [ ] **P0.4 Evidence baseline:** map existing focused tests and failpoints to the acceptance requirements above. Record known failures and representative workload sizes; preserve unrelated worktree changes. Do not run the full suite solely to begin the inventory.

Preparation delivers the storage/invariant inventory, the necessary state-transition and transaction contracts, and a test map. Complete the rename/version-specific decisions before package 1; authority and storage decisions may be refined alongside that work, but must be settled before the corresponding package 2/3 slice starts. If a decision changes agreed product behavior or safety guarantees, bring it back for review.

### 9.2 Work package 1: public concepts and version model

**Preparatory cleanup:** the old application release model, N/N+1 admission,
release-biased placement, release diagnostics, and release-upgrade simulation
fixtures have been removed. The subsequent W1 implementation replaces framework
compatibility generations with exact package-version admission. Independent
configuration/protocol checks remain; no `AppVersion` or rolling-release policy is added.

**Outcome:** application-facing terminology is consistent, and one automatic framework identity replaces the public framework compatibility matrix with the old application release mechanism removed and no replacement rolling-release policy. `AppVersion` and rolling updates are outside this package and the current implementation effort.

Primary areas: `lattice-model`, `lattice-coordination` (renamed from `lattice-placement`), `lattice-remoting`, `lattice-service`, distributed Actor integration, and workspace consumers. These are inspection targets, not permission to reorganize unrelated crates.

- [x] **W1.1 Terminology:** apply the approved Actor group / Cluster Coordinator / Group Coordinator naming to public types, configuration, diagnostics, examples, and tests. Rename the coordination crate if confirmed, including workspace dependencies, lockfile, imports, scripts, and documentation links. Keep module boundaries cohesive and distinguish Actor scheduling from distributed allocation.
- [x] **W1.2 Version inventory and identity:** classify existing generation/version/fingerprint fields as release compatibility, configuration/protocol consistency, or runtime safety. Define and automatically supply the exact Lattice identity across participating crates; retain necessary terms, epochs, generations, and incarnations.
- [x] **W1.3 Framework admission gate:** apply the exact identity check to bootstrap/connection setup, candidate startup, and the minimal etcd framework marker before version-sensitive payload/state consumption. Specify the small retained marker format jointly with W3.1. Mismatches fail clearly; they do not select an older codec or bypass storage guards.
- **W1.4 Deferred application release redesign:** not an implementation task or completion prerequisite. Design `AppVersion` together with old-release shard migration and rolling-update/rollback semantics in a separate effort. The old application release model, admission guards, and release-biased placement are removed first; their removal does not provide a replacement rolling-update guarantee.
- [x] **W1.5 Remove superseded surfaces:** migrate consumers and remove obsolete framework compatibility knobs and paths after their replacements are wired. Do not reintroduce the removed application release model or its metadata. Update examples and upgrade guidance; do not retain forwarding aliases solely to hide an incomplete rename.

**Exit evidence:** affected consumers compile with the new public surface; tests reject mixed Lattice versions at the early gates, retain mapping/protocol mismatch detection, and keep runtime authority safeguards intact. Application rolling-release guarantees are explicitly absent. Searches find no unintended old names or redundant framework compatibility negotiations. No `AppVersion` API or new rollout policy is required. This package does not claim that runtime reset or cluster shutdown is implemented; those operational guarantees remain gated on package 3.

### 9.3 Work package 2: cluster control and runtime authority

**Outcome:** clients find the right Coordinator, candidates can change at runtime, and one coherent authority model controls activation, execution admission, retirement, and rebalance.

Primary areas: discovery providers, service deployment/join/control orchestration, coordination host/election/authority modules, distributed Registry/entity/singleton integration, and narrowly scoped local Actor admission hooks.

- [ ] **W2.1 Discovery and readiness:** implement leader hints with candidate/bootstrap fallback, process-shared provider/cache behavior, bounded reconnect pacing, and watch recovery. Support static/DNS cold starts without ordinary-node etcd access. Expose required-scope readiness and test endpoint changes during candidate replacement.
- [ ] **W2.2 Dynamic candidacy:** add observable per-scope enable/remove operations, identity/permission checks, safe leader retirement, and serialized last-candidate protection. Implement together with W3.2's eligibility records and election/write guards; discovery-list updates alone are not eligibility revocation.
- [ ] **W2.3 Recovery and grants:** define dependency-based readiness after election, recover outstanding authority/transfer facts, and implement the agreed end-to-end grant renewal/deadline protocol. Distinguish discovery failure, control-session failure, and Coordinator-to-etcd failure. Reconnection must revalidate authority rather than merely reopen admission.
- [ ] **W2.4 Authority consolidation:** use the existing authority and validated Registry mechanisms as the starting point. Share entity/singleton predicates and exact-generation revocation; synchronize activation publication with authority loss and cover queued execution. Keep etcd/group details outside the local Actor kernel. Remove an old check only after its invariant has a verified replacement.
- [ ] **W2.5 Rebalance admission:** implement application-selected policy inputs/results and group-scoped round/in-flight budgets with per-node incoming/outgoing limits. Account across triggers, prevent duplicate slot transfers, restore capacity usage after failover, and suspend ordinary allocation under `Closing`. Persist only admitted transfer intent through W3.3; global cross-group quotas remain deferred.

**Exit evidence:** focused tests cover candidate addition/removal and delayed revocation, stale discovery hints, leader loss during activation/commit, expired grants after pauses, delayed renewals, cached/exact-reference ingress, singleton/entity parity, and recovered rebalance budgets. State and errors distinguish discovery availability from serving authority. No local cancellation or admission test is presented as proof of external-write fencing.

### 9.4 Work package 3: etcd data model and cluster lifecycle

**Outcome:** etcd retains bounded authority/recovery facts; a cluster run can end with verifiable runtime cleanup, and abnormal leftovers can be inspected and reset without erasing persistent safety information.

Primary areas: coordination storage traits and in-memory/etcd implementations, membership and transfer records, service lifecycle orchestration, administrative tooling, simulation fixtures, and operations documentation. Reuse existing tooling where appropriate rather than introducing an unrelated administration stack.

- [ ] **W3.1 Namespace and retention foundation:** implement framework/lifecycle metadata, run epochs, persistent configuration boundaries, and bounded key/value codecs from the approved inventory. Define guarded new-run initialization and preservation of externally meaningful counters. Supply W1.3's marker contract without adding general old-runtime schema compatibility.
- [ ] **W3.2 Minimal authority records:** implement bounded candidate eligibility/online registration, minimal membership/group participation, assignments, and lease-backed claims. Preserve exact identity and transaction guards; move full descriptions to revalidated control-session memory. Keep in-memory and etcd backends behaviorally aligned. Integrate with W2.2-W2.4, not as an isolated storage rewrite.
- [ ] **W3.3 Transfers and bounded storage:** split transfer metadata and participant acknowledgements, implement participant-set sealing/completion and interrupted-publication recovery, and bound encoded values, transactions, pages, and retained history. Reclaim completed records only when no safety or deduplication obligation remains. Integrate with W2.5's reservations and failover accounting.
- [ ] **W3.4 Coordinated shutdown:** expose distinct node and cluster shutdown contracts. Persist `Closing` and operation identity; let the Cluster Coordinator coordinate Group Coordinators and participant progress. Stop ordinary admission/allocation without migrating work among stopping nodes. Keep reporting and takeover paths alive until completion, and make retries and initiator/leader failure recoverable.
- [ ] **W3.5 Cleanup and abnormal reset:** implement scoped inspect/dry-run/apply behavior, active-run exclusion, guarded paginated cleanup, resumable progress, and confirmable final results. Preserve persistent configuration and safety history. A blocked or partially completed stop is not success; reset must not infer that partitioned old processes are gone.
- [ ] **W3.6 Cutover and operations:** document old-deployment shutdown, approved cleanup/reset, retained-record treatment, new-framework startup, and rollback via the same full-stop rule. Update deployment examples, actual architecture references, and test fixtures. Do not execute cleanup against a user's live etcd as part of development or tests.

**Exit evidence:** single-node leave preserves a running cluster; whole-cluster shutdown does not rebalance among closing nodes; interrupted shutdown/cleanup resumes after leader replacement; a new run does not load old runtime state. Reset tests preserve other clusters, application configuration, worker-ID history, and authority counters. Worst-case record and repeated-run tests demonstrate bounded storage. External side effects and business-state migration remain separate contracts.

### 9.5 Dependency order and coupled delivery slices

Start with P0 and package 1's approved public surface. Packages 2 and 3 then proceed as coupled end-to-end slices, not “finish all runtime code, then rewrite storage.” Internal commits may be staged and independently reviewed; no partially migrated production deployment is implied.

| Order | Delivery slice | Required coupling |
|---|---|---|
| 1 | Naming and version boundary | W1 plus the minimal retained-marker contract from W3.1 |
| 2 | Run/record foundation and Coordinator discovery | W3.1, necessary W3.2 records, and W2.1; only open serving paths with complete authority checks |
| 3 | Dynamic candidacy and authority lifecycle | W2.2-W2.4 with W3.2; eligibility, grants, guarded commits, and activation/revocation move together |
| 4 | Transfer/rebalance lifecycle | W2.5 with W3.3; no split-record handoff without completion and recovery semantics |
| 5 | Whole-cluster stop and reset | W3.4-W3.5 with the control/admission restrictions from package 2 |
| 6 | Final cutover and evidence | W3.6 and the cross-package acceptance suite |

Within each slice, migrate every producer and consumer of a changed contract and add its focused tests before removing the old path. Do not delete recovery data first and defer the replacement protocol. The old/new framework boundary remains a full-stop hard switch even when implementation uses multiple commits.

### 9.6 Verification and completion tracking

#### Work package 1 completed (2026-09-25)

- Renamed `lattice-placement` to `lattice-coordination` and migrated consumers,
  examples, simulation fixtures, Docker configuration, imports, and documentation.
  Public APIs use `ActorGroupId`, Cluster/Group Coordinators and session handles,
  `group` fields and `groups` collections. Coordinator host configuration exposes
  `cluster` and `group`. Storage request/record types live in `storage::records`.
  No old-name forwarding aliases remain.
- `CoordinatorScope::Cluster/Group` and serialized group fields now use the new
  terminology. Existing physical `membership/` and `domains/` etcd key families
  remain for W3; this does not implement the target run-scoped layout.
- `LatticeVersion` equals the automatic Cargo package version for all builds.
  There is no source digest. Same-version source differences are the developer's
  responsibility. Participating actor/remoting/coordination/service crates check
  their linked release version against the model crate at compile time.
- Bootstrap requests/responses and handshake requests/ACKs check the exact version
  in a small envelope before decoding their inner payload. The fixed `LTCE`
  framing magic is only a discriminator, not a negotiated version. Business
  frames carry no additional per-frame framework identity.
- Removed transport major/minor fields, mandatory feature bits, Coordinator
  protocol/control generations, and watch generations. Removed the old default
  mapper serialization fallback: mapper ID/version are explicit configuration
  fields, with no silent legacy `shard_hash_version` translation.
- `ensure_framework` initializes `meta/framework` (exact version, UTF-8),
  `schema/limits`, and required counters/revision in one empty-prefix transaction.
  Matching concurrent initializers revalidate the winner. Missing/foreign markers
  or legacy `schema_generation` keys fail without writes. Inconsistent limits or
  incomplete initialization metadata fail separately. No schema generation is
  written, so pre-identity binaries cannot accept the new namespace.
- Coordinator startup checks the marker before granting a lease, campaigning,
  or decoding recovery records. In-memory startup follows the same identity rule.
  The old migration API/CLI, its migration failpoints/tests, and attached offline
  counter inspection/repair commands are removed. Normal runtime capacity counters
  and reconciliation are retained. This is not a new cleanup/reset tool.
- The [version inventory](cluster-control-plane-preparation.md#w1-version-inventory-and-disposition)
  distinguishes removed compatibility machinery from retained protocol/configuration
  fingerprints, mapper/policy versions, terms, revisions, assignment generations,
  grant sequences, incarnations, activation IDs, and transport sequence identities.
- Validation: model/remoting/coordination/service library tests passed (14/105/114/73).
  All 17 Bootstrap integration tests passed, including foreign Bootstrap and
  Handshake rejection before Association registration. An isolated Docker etcd
  passed the framework-identity test and all 7 etcd acceptance tests (with
  `test-failpoints`), including concurrent initialization, candidate rejection
  before malformed state recovery, lease expiry, guarded commits, capacity and paging.
  Workspace all-target/all-feature compilation passed. Doc-test commands completed
  successfully (these four crates currently expose no runnable doctests). The three\n  actor-group simulation unit tests, Docker Compose validation, and repository\n  structure check also passed; all 18 ops/telemetry unit tests passed. The temporary\n  isolated etcd container was stopped and automatically removed after validation.
- No live cluster data was changed. Full workspace tests, multi-process chaos,
  and load tests were not run; those remain part of the coupled final W2/W3
  acceptance. Cluster shutdown, run epochs, reset/cleanup, dynamic candidacy,
  new grant timing, and application rolling releases are not claimed here.

- Use focused unit, integration, UI/API, and doctests for the slice being changed, plus compilation of affected reverse dependencies. Consolidate expensive workspace runs rather than repeating the full suite after every naming or documentation edit.
- Run formatting checks and `bash scripts/check-structure.sh` after structural changes. Preserve `foo.rs` plus `foo/`, explicit imports, and the effective-LOC limit without shrinking useful documentation.
- Use deterministic time/failpoints where possible for authority races and interrupted operations. Verify storage transaction/lease behavior with isolated etcd integration fixtures; in-memory tests alone cannot establish those guarantees.
- Before the final cutover, run the agreed workspace regression suite and multi-process failure scenarios once the coupled paths are complete. Validate representative startup, discovery/watch, renewal, transfer, shutdown, and reconnect load against budgets fixed during preparation. Record failures or untested environments explicitly rather than treating skipped coverage as success.
- Check off a work item only with implementation and relevant evidence. Record changed surfaces, exact validation performed, remaining blockers, and any approved decision changes here. Mark each package complete only when its exit evidence and cross-package dependencies are satisfied.

Implementation status is recorded above. No live etcd cleanup or storage migration has been executed.

## 10. Earlier control-plane analysis: minimal etcd state and candidate topology

The earlier memo's analysis and alternatives are retained below as historical background, not competing implementation instructions. Section 7 defines the target logical data model; sections 3.4 and 3.5 settle the discovery boundary and runtime candidate-management direction. In particular, ordinary nodes may use etcd-backed discovery without taking on coordination-store responsibilities. Section 9 is the sole active list of remaining decisions and acceptance requirements.

### Motivation

The current design persists substantial membership and placement runtime state in
etcd, including member records, domain members, assignments, claims, handoffs,
plans, and administrative operations. Some of those records outlive the processes
that produced them. A full cluster shutdown followed by restart can therefore
expose old state that must be recognized and reconciled before the cluster can
operate safely. We want to revisit which records actually need durable,
linearizable arbitration, rather than treating etcd as the default home for every
piece of cluster state.

There is a separate topology question: should every ordinary node connect to
etcd, or should only a smaller, explicitly configured set of Coordinator
candidates do so? These decisions interact but are not the same decision.

### Current behavior to preserve or reconsider

- Membership leadership and each placement-domain leadership are independent
  scopes. A `CoordinatorHost` may campaign for multiple scopes.
- Discovery supplies Coordinator **candidates**, not authoritative members or
  business-routing targets. Bootstrap probes identify a current leader; an
  Association to that leader carries membership/domain admission and control
  traffic. Ordinary peer Associations need not be opened at startup.
- A node becomes globally `Up` only after the membership session's
  `MemberHello -> Joining -> JoinReady -> Up` sequence. Hosting a placement
  domain additionally requires its domain session.
- The current design gives Coordinator hosts the election/storage capability;
  ordinary nodes use discovery and control sessions. A ConfigStore-backed
  discovery provider may itself use etcd, so "no direct etcd dependency" is
  true only when the chosen discovery path does not open a per-node etcd client.
- Serving authority is separate from finding a leader or being a member. Claims
  and local fencing must continue to prevent two owners from safely writing at
  once, including during partitions and leader changes.

See [discovery providers](cluster-discovery.md) and the
[control-plane boundary](architecture/03-placement.md#1-control-plane-boundary)
for the currently implemented contracts.

### Direction under consideration: minimal etcd authority

Use etcd for the smallest set of facts that must survive or arbitrate a leader
change safely: for example, scope leadership/term, schema or cluster epoch, and
ownership claims or fencing generations where external authority requires them.
This is a design criterion, **not yet an approved key list**. Review each existing
key family against these questions:

1. What safety invariant requires this record to be in etcd? Is it needed after
   all cluster processes have stopped, or only while a process is alive?
2. Could it instead be a leader-maintained, versioned runtime view distributed
   through control sessions? If so, how does a new leader reconstruct that view?
3. If it remains in etcd, what lease, incarnation, epoch, expiry, and cleanup
   rules prevent a full restart from interpreting old data as live authority?
4. Can recovery prove previous ownership has ended before granting replacement
   authority? Simplifying persistence must not weaken fencing or failover safety.

In particular, decide explicitly whether membership and domain participation
are reconstructed by rejoining after leader election, represented by minimal
lease-backed records, or use another bounded recovery mechanism. Removing
durable runtime records without specifying leader recovery would merely move the
problem. Conversely, attaching a lease to a record does not automatically make
every durable assignment or plan safe to reuse after a full-cluster restart.

### Direction under consideration: restricted Coordinator candidates

Configure a small, failure-domain-diverse candidate set for each required
leadership scope. Only candidates run the election component and hold the etcd
permissions needed for that scope. An ordinary node would:

1. obtain candidate addresses through static seeds, DNS, Kubernetes discovery,
   or a discovery service that does not give every node its own etcd connection;
2. probe/reconcile the current leader for membership and each needed domain;
3. establish control Association(s), complete admission, and install the
   versioned runtime views before becoming ready;
4. open Associations to other business peers only when traffic requires them;
5. on leader loss, stop actions that require fresh authority, rediscover, and
   resynchronize before resuming them.

An ordinary node would not automatically become a leader: it would need an
explicit role/permission change and the Coordinator host capability. Restricting
candidacy does **not** mean only candidates may host actors. Candidate eligibility
also need not be identical for the membership scope and every placement domain.

The alternative remains viable: ordinary nodes could have narrow, read-only
etcd access to fetch/watch leader records directly, while only candidates can
campaign or mutate authority. That simplifies leader discovery but increases
client/watch count, credential distribution, and direct failure coupling to
etcd. Direct leader lookup still does not replace admission or the control
session. Neither approach requires eager Associations to every member.

### Expected load and trade-offs

Restricting etcd access to candidates can reduce connection, watch,
authentication, and reconnect load from ordinary nodes. It does **not** by
itself reduce writes performed by leaders for the current persisted membership,
placement, or claim model. The state-scope redesign is needed to address those
writes and the full-restart stale-state problem.

Thousands of etcd clients are not inherently disqualifying. Capacity depends on
connections and watch streams, update fan-out, write/lease rate, snapshot size,
etcd hardware, and synchronized startup or reconnection. A decision should use
the expected cluster/domain sizes and a measured failure-recovery workload, not
client count alone. The restricted-candidate design also concentrates join and
snapshot work on Coordinators, so their admission capacity and failover behavior
must be measured.

### Historical decision list (superseded)

The original open questions have been consolidated into section 9. The target data model is in section 7; discovery and dynamic candidate management are in sections 3.4 and 3.5; recovery and degraded-operation boundaries are in section 5.1; full-cluster cleanup/reset is in section 6. Representative-scale validation must cover steady-state discovery/watch/write load, simultaneous startup, candidate replacement, leader failover, and etcd reconnection. Do not maintain an independent decision list here.

This historical analysis does not define additional implementation work; see section 9.6 for the current checkpoint.
