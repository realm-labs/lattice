# Code-only rolling upgrade

Lattice supports one deliberately narrow mixed-release mode for Logic nodes:
`CodeOnlyRollingUpgrade`. It is intended for handler bug fixes, internal
algorithm changes, private helper changes, performance work, and logging or
telemetry changes. It is not a general schema-evolution mechanism.

The cluster admits at most two application releases, ordered by `ReleaseId`.
The first member of release N+1 is admitted only when its
`ReleaseCompatibility` exactly equals release N. During coexistence, existing
authority on N remains valid, but new shard and singleton allocations target
only N+1. Normal handoff drains actor activations on N and recreates them on
N+1; in-memory actor state is not copied. Durable actor state must already be
recoverable through the application's normal save/load path.

Only nodes that actually host at least one entity or singleton participate in
this application rollout state. Proxy-only gateways do not consume an N/N+1
slot, and their release does not influence placement target selection.

## Release manifest

Every `NodeConfig` must carry a `ReleaseManifest`. `ReleaseId` is a nonzero,
monotonically increasing deployment number. The compatibility contract contains:

- remoting transport generation;
- Coordinator control generation;
- placement storage generation;
- complete actor-protocol catalogue fingerprint;
- actor durable-state schema fingerprint;
- placement and singleton configuration fingerprint;
- application service ABI fingerprint.

The linked framework generations are validated locally before startup. The
four application fingerprints must be generated reproducibly by the build or
release pipeline; do not derive them from a Git commit or image tag alone.
Changing any compatibility field requires a full deployment stop. Reusing one
`ReleaseId` with a different manifest is rejected.

`ReleaseManifest::development(...)` exists for tests and examples only. It
uses fixed placeholder application fingerprints and is not a production
manifest. Zero fingerprints are rejected.

## Admission rules

- An empty cluster admits one release and becomes stable.
- A stable cluster admits the same manifest or a higher, exactly compatible
  release and becomes N/N+1 rolling.
- A rolling cluster admits only either of its two existing manifests.
- A third release, an older release after rollout completion, or the same
  `ReleaseId` with different data is rejected.
- Protocol, actor-state, placement, transport, control, storage, or service-ABI
  changes are rejected with `FullRestartRequired`.

Release state is derived from live lease-backed member records, so leader
failover does not erase the guard and expired members do not keep a rollout
open. `LatticeService::cluster_release_state()` exposes the same derived state;
authenticated admin snapshots can publish it through `AdminSnapshot::release`.

## Kubernetes sequence

Use `maxUnavailable: 0` and enough surge capacity to host N+1 before moving
authority. For each old Logic Pod:

1. start an N+1 Pod and wait for `/readyz`;
2. call `LatticeService::cordon()` on the selected N Pod, which immediately
   closes readiness and external actor admission;
3. keep the probe HTTP service running and call `leave(deadline)` (or the
   application-level `shutdown()`), allowing placement handoffs to finish;
4. stop the process only after drain completes; `/livez` remains successful
   until termination;
5. wait for placement movement and capacity budgets before continuing.

Do not use liveness failure as a drain signal. `preStop` must have enough time
for the same cordon/drain path, and `terminationGracePeriodSeconds` must exceed
the configured leave deadline.

Automatic and initial shard allocation, manual relocation targets, recovery
targets, and singleton selection all exclude N while N+1 exists. A rollout can
therefore pause with `NoEligibleNode` rather than silently place new work back
on N. Fix N+1 capacity or compatibility instead of bypassing the guard.

## Full-stop boundary

Stop the complete application deployment for any of the following:

- actor message IDs, codecs, request/reply schemas, or protocol fingerprints;
- persisted actor-state schemas or migration semantics;
- entity/singleton declarations, shard mapping, placement policy/configuration;
- remoting, Coordinator control, or storage generation;
- application service ABI or cross-service contract.

Drain all Logic nodes, wait for membership and placement leases to expire,
perform any explicit offline migration, deploy one release only, and then
reopen admission. Gateway and CoordinatorHost upgrades remain separate
operational rollouts and must follow their own generation compatibility rules.
