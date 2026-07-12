# 08. Distributed Testing

> Deterministic state-machine verification, Docker-based multi-process testing, fault injection, and release evidence.
> Back to: [architecture index](README.md)

---

## 1. Testing Objective

No finite test suite proves every distributed execution. lattice therefore combines bounded state-space exploration, seeded simulation, real protocol/process tests, and long-running chaos. The goal is to make every safety claim continuously executable and every failure reproducible.

The correctness methodology is simulation-first: explicit reducers, virtual time, controlled protocol events, invariant checks, and bounded state exploration provide deterministic evidence. Docker is the reproducible execution, integration, and acceptance environment: it packages the toolchain and runs real processes, TCP/TLS, etcd, resource limits, and external faults. Docker does not replace the simulator or serve as a deterministic scheduler.

A developer or CI worker running canonical acceptance needs Docker Engine with Compose support; it does not need a host Rust toolchain, etcd, TLS utilities, network-fault tools, or lattice test binaries. Those dependencies live in pinned images built or pulled by the test composition. Direct native reducer/simulator tests remain an optional fast path when a developer already has the toolchain.

Four evidence layers are required:

| Layer | Runtime | Primary evidence |
|---|---|---|
| State-machine tests | Pure reducer/effect code in the test runner image | Legal/illegal transitions, idempotency, effect generation |
| Deterministic cluster simulation | Virtual clock/network/store/processes | Reorder, duplicate, loss, partition, pause, crash, exhaustive small-state exploration |
| Real multi-process integration | Real lattice processes, TCP/TLS and disposable etcd containers | Wire framing, sockets, process lifecycle, leases, TLS identity, shutdown |
| Chaos and soak | Real containers plus controlled failure orchestration | Rare races, recovery under combinations, resource leaks, latency/throughput degradation |

Safety invariants must hold after every simulated or observed transition. Liveness is checked only after the harness establishes a bounded stable period in which the required nodes, network, and etcd are healthy.

## 2. Architecture for Testability

Distributed control components expose explicit state transitions rather than hiding authoritative state inside detached Tokio tasks:

```rust
pub trait StateMachine {
    type State;
    type Event;
    type Effect;
    type Error;

    fn step(
        state: &mut Self::State,
        event: Self::Event,
    ) -> Result<Vec<Self::Effect>, Self::Error>;
}
```

This shape applies to at least:

```text
Association and reliable control delivery
Coordinator session and revisioned snapshot installation
PlacementSlot claim/assignment lifecycle
ShardRegion route/buffer state
Handoff barrier and Shard lifecycle
RebalancePlan and move lifecycle
Singleton ownership lifecycle
DeathWatch registration and terminal delivery
Logic service join/degraded/drain/stop lifecycle
```

Production executors interpret effects through real TCP, etcd, clocks, task supervisors, and actor registries. The simulator interprets the same effects through deterministic substitutes. Production-only behavior may wrap the reducer, but it must not duplicate a second transition algorithm for tests.

External effects carry stable operation IDs and the domain fencing fields needed for replay-safe application:

```text
Coordinator term and state revision
NodeIncarnation and Association epoch
Control sequence and command ID
Placement assignment generation and grant sequence
Rebalance plan/move ID
Actor ActivationId and WatchId
Ask correlation ID
```

## 3. Deterministic Cluster Simulator

The workspace provides the dedicated `lattice-sim` simulation/test-support crate, containing:

```text
SimClock             monotonic virtual time and scheduled deadlines
SimScheduler         deterministic event choice and task/effect completion
SimNetwork           framed links, partitions, delay, loss, duplication and reorder
SimEtcd              transactional records, revisions, watches, leases and compaction gaps
SimProcess           boot, pause, resume, crash and new-incarnation restart
FaultInjector        named failpoints and seeded fault policy
TraceJournal         complete structured causal event log
InvariantChecker     safety/resource checks after every transition
LivenessChecker      bounded eventual checks after a declared stable period
StateExplorer        DFS/BFS exploration of bounded small-cluster states
```

The simulator must be deterministic for the same build, scenario, configuration, and seed. Every failure reports:

```text
test/scenario name
seed and exploration strategy
minimal or original event sequence
initial configuration
last valid state snapshot
violated invariant
structured trace artifact path
one-command replay instruction
```

Property/random tests use a recorded seed and shrink event traces where practical. Unseeded randomness, wall-clock sleeps, host timing, and unordered map iteration cannot determine test outcomes.

### 3.1 Controllable Events

```text
deliver, delay, drop, duplicate or reorder one frame/control command
partition or heal selected node pairs/directions
close one lane or the entire Association
advance virtual time to a selected timer/deadline
pause/resume a process beyond heartbeat, ask or claim deadlines
crash before/after a named persistence or send boundary
restart the same address with a new NodeIncarnation
fail/timeout/commit the next etcd transaction
expire a lease, compact a watch revision, elect a new Coordinator
fill or release a bounded queue/buffer/pending map
join, drain or remove a node during snapshot, handoff or rebalance
```

The scheduler can run one explicitly selected event, a seeded fair event sequence, or bounded exhaustive exploration. Exhaustive exploration starts with small topologies such as 2–3 nodes, 1–2 entity types, 1–2 shards, and bounded message counts; large topology coverage comes from seeded simulation and real stress tests.

## 4. Trace and Test Oracle

Every runtime component emits a test-observable structured `TraceEvent`. Production may sample/export a subset, but tests retain the full bounded journal:

```text
event_index and causal_parent_ids
virtual/monotonic time
node ID and NodeIncarnation
AssociationId/epoch, lane and control sequence
Coordinator term and state revision
PlacementSlot, assignment generation and grant sequence
Rebalance plan/move ID and move status
actor path and ActivationId
WatchId or ask correlation ID
event kind, previous state, next state and effect outcome
```

Ordering assertions use causal/domain sequence fields rather than log timestamps. Tests assert only documented order:

- tell order for one sender/recipient/stable bulk stripe/Association epoch;
- reliable control sequence and cumulative acknowledgement order;
- atomic Coordinator revision installation and gap rejection;
- handoff order from invalidation barrier through old-authority invalidation to new Active;
- generation-conditional RebalanceMove progress;
- at-most-one completion of one ask correlation or WatchId.

Tests must not invent ordering across tell/ask lanes, different senders, reconnect, reroute, or shard generations.

## 5. Invariant Catalogue

### 5.1 Ownership and Placement Safety

```text
At most one valid claim holder exists for one PlacementSlot generation.
An Active Shard/Singleton has a matching term, generation, owner incarnation and unexpired local grant.
A stale NodeIncarnation, assignment generation, grant sequence or Coordinator term cannot regain admission.
The new owner is never Active before ShardDrained or independently proven old-authority invalidation.
One shard has at most one active RebalanceMove and PlacementSlot.active_move link.
Pending moves reserve bounded target capacity and concurrent plans cannot overcommit it.
A move that entered Handoff never rolls back to ambiguous dual ownership; recovery proceeds forward.
Strategy/load/admin inputs never grant authority without Coordinator validation and persisted transition.
```

### 5.2 Messaging, Revision, and Watch Safety

```text
Concrete ActorRef delivery matches node incarnation, path and ActivationId.
Each ask correlation completes at most once with an allowed result, timeout or UnknownResult.
Business tell/ask frames are never replayed by reliable control recovery.
Reliable control duplicates do not duplicate domain state transitions.
Coordinator revisions install atomically and monotonically; a gap never becomes live routing state.
Each WatchId emits at most one terminal notification and never follows a replacement activation.
Logical watch_current never activates an Entity or Singleton.
```

### 5.3 Resource and Lifecycle Safety

```text
Every queue, outbox, buffer, pending map, watch registry, plan set and history stays within limits.
Every long-lived task is supervised and joined/cancelled within shutdown policy.
Draining stops external admission before migration and final mailbox shutdown.
Terminated service state retains no live actor path, claim, association, lease or detached task.
```

### 5.4 Conditional Liveness

After a scenario declares a stable period and advances beyond the relevant bounded deadlines:

```text
A compatible joining node becomes Ready or returns an explicit terminal join failure.
An allocated shard becomes Active or explicitly Unavailable/Failed.
A handoff/rebalance move reaches Completed/Cancelled/Failed with inspectable reason.
A valid pending watch reaches WatchAck or Terminated.
A Coordinator failover reconciles claims/plans before new automatic placement work.
Bounded buffers drain or fail their entries; no request waits forever.
```

## 6. Named Failpoints and Fault Matrix

Every persistence/send boundary that can leave an uncertain distributed state has a stable test-only failpoint name. At minimum:

```text
association_after_handshake_before_catalogue
control_after_outbox_before_socket_write
control_after_remote_apply_before_ack
coordinator_after_etcd_commit_before_delta
snapshot_after_stage_before_install
rebalance_after_plan_persist
rebalance_after_reservation_before_handoff
handoff_after_begin_persist
handoff_after_partial_barrier
handoff_after_drain_send
handoff_after_shard_drained_before_claim_revoke
handoff_after_new_claim_before_grant_send
handoff_after_grant_before_shard_ready
handoff_after_active_persist_before_delta
watch_after_install_before_ack
watch_after_terminated_before_ack
shutdown_after_fence_before_task_join
```

Failpoint behavior is test-only and cannot be enabled by unauthenticated production traffic. Each critical boundary is exercised against the relevant fault set:

The stable names live in `lattice-core::failpoint::Failpoint` behind the `test-failpoints` feature.
Production remoting, placement, watch, and shutdown code calls this shared catalogue at all 17
boundaries; `lattice-sim` consumes the enum and machine-checks both source presence and the required
failpoint/fault-target matrix.

```text
Coordinator crash/re-election
source or target crash
process pause beyond TTL
one-way/two-way partition
control connection/lane reset
duplicate/delayed old command
etcd transaction failure, lease expiry or watch compaction
queue/buffer exhaustion
```

The required fault matrix is tracked as data, not only separate hand-written tests, so CI can report missing boundary/fault combinations.

## 7. Docker-Based Integration and Acceptance Environment

### 7.1 Responsibility Boundary

Docker is authoritative evidence for production adapters and environmental behavior:

```text
real process boot/pause/kill/restart
real TCP/TLS framing, identity and reconnect
real single-member/HA etcd transactions, watches and leases
independent production Coordinator process election, lease loss and higher-term takeover
container network partition/delay/loss
CPU, memory and file-descriptor pressure
drain, shutdown, cleanup and long-running resource behavior
```

The simulator is authoritative for semantic schedules that Docker cannot make precise or reproducible:

```text
specific Tokio/effect completion order
control-message duplicate/reorder/drop at an exact sequence
virtual advancement to heartbeat/ask/claim deadlines
crash immediately before/after one persistence/send boundary
partial protocol application before acknowledgement
bounded exhaustive exploration and trace shrinking
```

Docker containers share the host kernel and clock and introduce nondeterministic scheduling. Container chaos may discover an ordering bug, but the bug is accepted as reproducible evidence only after the artifact identifies a seed/failpoint/trace or is converted into a deterministic regression scenario. Protocol-level faults belong in SimNetwork/failpoints; packet/process-level faults belong in Docker.

### 7.2 Repository Shape

Repository layout:

```text
tests/distributed/
  compose.yaml
  Dockerfile.runner
  images.lock
  k8s/
scripts/
  test-docker.sh
target/test-artifacts/<run-id>/
```

`Dockerfile.runner` pins the Rust toolchain and installs all test-only utilities. External images such as etcd and network-fault helpers are pinned by immutable digest in `images.lock`; `latest` tags are forbidden for acceptance evidence. TLS certificates are generated inside the isolated test environment or loaded from non-secret test fixtures, never by host tooling.

The thin host entrypoint checks Docker/Compose availability, assigns a unique Compose project/run ID, invokes the requested profile, collects the exit code, and always runs `down --volumes --remove-orphans --rmi local`. It must not require host cargo, rustc, etcdctl, openssl, curl, Python packages, or a pre-existing lattice binary.

### 7.3 Compose Profiles

```text
quality:
  pinned runner only; fmt, workspace clippy and workspace tests

sim:
  test runner only; reducer tests, state exploration and seeded simulation

model:
  runner only; bounded exhaustive small-cluster exploration and invariant catalogue

e2e:
  runner, disposable etcd, concrete-actor server, claimed entity owner and Gateway client
  real child ActorRef ask/watch/stop, Gateway-to-EntityRef routing, TCP/TLS and normal lifecycle

chaos:
  e2e topology plus dedicated fault orchestration/network proxy
  pause/resume/kill/restart, partitions, delay/loss and failpoint scenarios

soak:
  long deterministic seed sequence, rolling replay trace and FD/RSS/thread growth sampling

k8s:
  disposable local Kubernetes cluster for probes, preStop, rollout, eviction and Service/DNS only
```

An additional HA-etcd profile runs a disposable three-member etcd cluster for lease/store failover scenarios. Ordinary PR tests may use one disposable etcd container; final storage/leadership evidence includes the HA profile.

`e2e-ha-etcd` identifies the elected member through structured `etcdctl` JSON, stops that exact
leader, waits for a different surviving leader, reruns schema/lease/slot/claim acceptance on the
quorum, restores health, and exercises persisted Coordinator handoff and Singleton forward recovery.

Containers share only an isolated per-run network and named test volumes. Fixed host ports are unnecessary by default. Services use health/readiness protocols, not fixed sleeps. Every container has CPU/memory/file-descriptor limits representative enough to expose unbounded growth, while performance benchmarks use a separate documented profile without chaos throttling.

### 7.4 Topology and Scenario Orchestration

Docker Compose declares stable topology and infrastructure: images, networks, volumes, health checks, resource limits, service dependencies and default environment. It must not encode a complex scenario as a long chain of sleeps and shell commands.

The programmatic `testctl` orchestrator owns scenario behavior:

```text
start/wait/pause/resume/kill/restart node
partition/heal selected links
enable/release a named failpoint
submit business/admin operations
wait for structured lifecycle/revision/generation/plan predicates
capture artifacts and assert invariants
```

Chaos orchestration may require access to the Docker control API. If the runner mounts the Docker socket, that profile is restricted to trusted repository code on a dedicated CI worker because the socket is host-equivalent authority. The orchestrator may manipulate only containers carrying the current project/run labels and must verify the label before every destructive action. Non-chaos profiles should avoid Docker-socket access when ordinary service/admin protocols are sufficient.

### 7.5 Canonical Commands

The repository supplies one stable wrapper whose implementation uses Docker Compose:

```text
./scripts/test-docker.sh quality
./scripts/test-docker.sh sim
./scripts/test-docker.sh model
./scripts/test-docker.sh e2e
./scripts/test-docker.sh e2e-ha-etcd
./scripts/test-docker.sh chaos
./scripts/test-docker.sh k8s
./scripts/test-docker.sh soak --duration 4h --seed <seed>
./scripts/test-docker.sh replay --artifact <trace.json>
```

Direct `cargo test` remains a useful developer fast path for reducers/simulation, but it is not a substitute for final adapter/integration Docker evidence. The wrapper records image digests, source commit, platform, configuration, seed, scenario list, start/end time and exit status.

### 7.6 Artifacts and Cleanup

Every failed scenario, and a configured sample of successful scenarios, writes:

```text
manifest.json
scenario/seed/configuration
structured trace and minimized replay trace
container stdout/stderr
Coordinator/placement/admin snapshots
etcd revision/key dump with secrets redacted
resource time series
network/fault schedule
test result/JUnit summary
```

The chaos schedule includes pause/unpause, `tc netem` delay and packet loss, network detach/reattach,
kill/start, and restart at the same address with new incarnations. Soak writes one rolling trace plus
the final replay trace rather than an unbounded trace file per successful seed, and samples process
resources every second with explicit growth ceilings.

Artifacts are copied to the host-mounted run directory before teardown. Secrets, business payloads, private keys and bearer tokens are redacted. Cleanup is idempotent and removes containers, networks, disposable volumes and project-local runner/probe images for the run even after timeout or interruption; failed cleanup is itself a test failure.

The HA profile runs two independent production `CoordinatorLeader` processes against the
three-member etcd cluster. Its structured oracle stops the elected Coordinator, requires a peer to
publish a higher leadership term with its exact boot incarnation, restarts the old process, and
checks that the restarted candidate cannot displace the current leader.

### 7.7 Kubernetes Boundary

Kubernetes is not the primary correctness harness. Adding Pod scheduling, controllers, DNS and CNI behavior to every protocol test makes failures slower and harder to reproduce without improving reducer/claim/handoff coverage.

After Docker integration is stable, the separate `k8s` deployment profile uses a disposable local Kubernetes cluster to verify only Kubernetes-specific contracts:

```text
readiness/liveness/startup probes
preStop drain and termination grace period
rolling replacement and Pod eviction
Service/DNS discovery
resource requests/limits and disruption policy
```

Kubernetes evidence complements but never replaces simulation invariants or Docker TCP/TLS/etcd acceptance.
The profile builds the workspace-owned `k8s-probe` binary in a pinned image, loads it into the
disposable kind cluster, and drives its HTTP startup/readiness/liveness endpoints. The preStop hook
publishes a drain request and waits for the process to acknowledge it by closing readiness before
Kubernetes sends termination; the same image is exercised through rollout, PDB-governed eviction,
and Service/DNS discovery.

## 8. Real Multi-Process Scenario Matrix

At minimum, real Docker scenarios cover:

| Area | Required scenarios |
|---|---|
| Node lifecycle | boot, incompatible join, Ready, Coordinator loss/restore, drain, forced kill, same-address new incarnation |
| Association | simultaneous dial, lane failure, control reconnect/replay, TLS identity failure, malformed/oversized frame |
| Coordinator | leader crash before/after etcd commit, stale leader command, revision gap/resnapshot, lease expiry |
| Shard | first allocation, lazy activation, passivation, buffer overflow, owner crash, claim expiry |
| Handoff | join/leave during barrier, source crash at each boundary, target crash before/after grant/ready |
| Rebalancing | deterministic plan, stale load, hysteresis/cooldown, capacity reservation, preemption, leader recovery |
| DeathWatch | lost/replayed WatchAck/Terminated, reconnect, activation/path reuse, node-down confirmation |
| Ask/tell | write-boundary UnknownResult, no automatic replay, stripe ordering, deadline at every admission point |
| Resource/shutdown | queue/map limits, task panic, cancellation, repeated reconnect, bounded drain and zero orphan resources |

Assertions query structured admin/trace state. Tests must not parse human log text as the primary oracle or use `sleep` as proof that a state transition completed.

## 9. CI and Release Gates

```text
Pull request:
  Docker quality profile (fmt, clippy, workspace tests)
  reducer/unit/property tests
  bounded small-state exploration
  fixed regression seeds
  Docker e2e smoke with real TCP and disposable etcd

Main branch:
  broader seed corpus
  TLS and HA-etcd profiles
  Coordinator/handoff/rebalance failpoint matrix
  multi-process lifecycle and shutdown tests

Nightly:
  thousands of generated schedules
  chaos profile with partitions/pause/kill/store faults
  1–4 hour soak and resource-growth checks

Release:
  full required fault matrix
  reproducible Docker image digest manifest
  migration/full-stop cutover scenario
  disposable Kubernetes deployment-lifecycle profile
  performance and capacity profile
  zero unresolved invariant violation or non-replayable failure
```

Flaky tests cannot be silently retried to green. A retry must preserve the first failure artifact, report both results, and open/attach a tracked issue. Stable known regression seeds remain in the PR suite until the underlying class is proven covered by a stronger invariant or exhaustive test.

## 10. Completion Criteria

- All documented safety invariants are executable in `InvariantChecker` or mapped to an explicit real-process assertion.
- Every critical control/persistence boundary has named failpoint coverage and appears in the fault matrix.
- Every randomized failure is seed-replayable; shrinking/minimization is available for simulator traces.
- Small-cluster state exploration terminates under documented bounds with no invariant violation.
- Docker profiles build from pinned dependencies and require no host test toolchain beyond Docker/Compose.
- Real TCP/TLS, disposable single/HA etcd, process pause/kill/restart and network partition scenarios pass.
- The disposable Kubernetes profile verifies probes, preStop drain, rollout/eviction and discovery without serving as the protocol correctness oracle.
- CI stores enough artifacts to reproduce any failure locally with one Docker command.
- Test processes, containers, networks, volumes, tasks and credentials are cleaned up or reported as failures.
