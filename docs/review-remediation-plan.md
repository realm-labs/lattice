# Code Review Remediation Plan

Status: complete. Implemented and validated 2026-09-08 against `23bd4e83126b6aeb5f273e94ca35d4f3de69cfd7`. No deployment was performed.

Execution source: [2026-09-08 code review](code-review-2026-09-08.md), especially Section 7. This plan tracks the newly discovered issues; completed historical plans are invariant references, not evidence that these fixes are complete.

## Current Execution Pointer

All prioritized repairs are complete. Format, structure, workspace Clippy/tests, real-NATS acceptance, Docker e2e, deterministic simulation, and bounded model exploration passed. The original review's 329 passing unit tests and six defect reproductions describe the baseline; the validation record below covers the repairs.

| Order | Scope | State | Acceptance evidence |
|---:|---|---|---|
| 1 | R1/R2: loading activation ownership, cancellation, drain/fence | Complete | Registry 19/19; local authority fence 3/3; service loader-fencing scenarios included in 73 passing lib tests |
| 2 | R3/R4: queued budgets and retired reconnect tasks | Complete | Final remoting lib 100/100; focused no-default-feature retirement 5/5 |
| 3 | R5/R6: absolute leave deadline and authoritative commit confirmation | Complete | Drain commit 6/6; service lib 73/73; endpoint shutdown cancellation/retry 3/3; real cluster lifecycle and failover passed |
| 4 | R7: inbound establishment deadline | Complete | Plaintext/catalogue retirement tests and real TLS stall/capacity recovery regression passed |
| 5 | R8 and observer panic: truthful task failure cleanup | Complete | Host 16/16, including panic withdrawal/re-election; observer isolation matrix 1/1 |
| 6 | R10: EventBus drain contract | Complete | EventBus lib 30/30, shutdown integration 12/12, and real NATS Core/durable timeout/cancellation matrix passed |
| 7 | ActivationDirectory capacity and duplicate host registration | Complete | Concurrent capacity and original-host preservation 2/2 |
| 8 | R9: membership snapshot clock | Complete | Clock regression passed within final placement lib 119/119 |
| 9 | Necessary internal ownership/API/documentation cleanup | Complete | Redundant APIs removed/narrowed; architecture and runbooks aligned; stale model test filters fixed and guarded against empty discovery |

## Invariants

- A failed Actor stopping hook retains the exact instance and state as StopFailed. Graceful shutdown must not silently force-discard it.
- External authority loss closes old business admission and prevents old-generation publication, without extending the old claim while local cleanup runs.
- Loading placeholders, live cells, retained failures, and quarantine remain owned and observable through their entire lifetimes.
- Every queued-byte reservation and connection permit is reclaimed exactly once on success, cancellation, error, or retirement.
- Leave uses one absolute deadline and succeeds only after authoritative commit, never solely on a transport ACK or local directory mutation.
- Domain and Actor health, directories, and DeathWatch agree with actual task/resource lifetime.

## Validation and Integration

Focused regressions assert correct postconditions, including fault timing and resource reclamation. Original-defect characterizations were run for activation cancellation/loading, transport retirement/budgets/setup, drain confirmation/deadlines, observer panic, directory/host registration, EventBus drain, and cleanup cancellation before their fixes were accepted.

Cargo commands sharing the workspace target directory were coordinated to avoid overlapping builds. Integration covered crate tests, structure/format checks, workspace Clippy/tests, and simulation and cluster checks. Only executed checks are recorded as acceptance evidence; environmental failures and unexecuted profiles are distinguished below.

Keep changes reviewable and preserve established public semantics unless correctness requires a deliberate hard switch. Record any protocol-generation change and its deployment implications. No compatibility mode, silent force-stop fallback, or extra transport framework is part of this repair.

## Implementation and Compatibility

Activation loaders remain caller-owned. A cancellation guard removes only its own placeholder and resolves its waiters. Publication and generation validation share the placement authority lock. Drain/fence covers loading entries, and a persistent Actor admission gate prevents queued or prefetched messages from starting after fencing. Already-admitted callbacks finish normally; stopping still provides a persistence opportunity and retains StopFailed instances for intervention or retry.

Queued remoting byte reservations now follow Frame ownership through queues and write batches. Dropping the final admitted frame releases its reservation. Association retirement has a persistent wakeable state checked during backoff, dialing, idle waits, and active lane execution. One inbound deadline starts at TCP accept and includes TLS and protocol setup.

CoordinatorHost retains task scope outside each future. Panic or cancellation clears the corresponding ownership markers and stale advertised state, allowing recovery without disrupting healthy scopes. Observer callbacks are isolated from lifecycle cleanup and the shared observer handle is disabled after its first panic.

Control generation advances from **9 to 10**; storage generation stays **5**. `DrainComplete` and `MembershipDrainComplete` now carry node identity, and completion requires a scoped `DrainCommitted` response bound to the operation, incarnation, and Coordinator term. Retry after a lost response rechecks durable removal and remaining authority rather than depending on a volatile receipt cache. See the [full-stop upgrade boundary](operations/code-only-rolling-upgrade.md#full-stop-boundary); mixed generation 9/10 deployment is unsupported.

Source API changes are deliberate: `ActorActivationError::Cancelled` distinguishes abandoned loading; the hidden fencing-token resolver now invokes a callback while holding the authority lock; hidden Actor admission-fence methods support distributed ownership. Manual `Association::release_queued_bytes` is removed because ownership now releases bytes automatically, including in the benchmark example. The unused Ops `DrainController`/`GracefulShutdown` API and its private-use error variant are removed. The serial `serve_inbound_connection` loop and its error type now exist only in messaging test support. Production remoting continues through `BidirectionalLane`. `EventSubscriptionHandle::detach` is removed; backend ownership persists when a caller drops its handle. New `EventBusError::Closed` and `ServiceError::ShuttingDown` variants report permanently closed admission.

Architecture documentation now distinguishes serializable addresses from bound runtime references and describes current local-child and activation ownership APIs. The existing Actor lifecycle atomic/watch representation is preserved; this repair does not introduce a general lifecycle framework. Throughput has not been benchmarked.

## Validation Record

- `target/review-integrated-core.log`: Actor and distributed Actor libraries and integration suites passed, including 19 Registry regressions, 3 authority-fence tests, observer isolation, capacity/duplicate registration, and compile-fail API tests. The first placement run exposed a test fixture assumption about in-memory lease expiry; the test now models that expiry explicitly after observing advertisement withdrawal.
- `target/review-host.log`: all 16 host tests passed after that fixture correction.
- `target/review-integrated-network.log`: placement lib 119/119, drain commit 5/5, remoting lib 97/97, and bootstrap/TLS integration 16/16 passed. The real-etcd acceptance binary returned early without `LATTICE_ETCD_ENDPOINTS`; its reported success is **not** evidence of real-etcd validation.
- The added stalled-TLS regression, endpoint shutdown 3/3, final service 73/73 and EventBus 30/30 + 12/12 checks passed. Format and structure checks passed. Full workspace Clippy with `--all-targets --all-features -- -D warnings` passed on Rust 1.97.0; native MSVC emits informational linker-output warnings that this lint does not promote to errors.
- `target/review-workspace-tests.log`: `cargo +1.97.0 test --workspace --all-features --locked --offline` passed, reporting **802 passed, 0 failed, 3 ignored** across 96 test-suite summaries, including compile-fail API checks and doctests. The real-NATS endpoint was set for this run. Other environment-dependent acceptance fixtures can return early when their endpoints are absent; this count does not imply every external backend was exercised.
- Real NATS **2.11.17**, image digest `sha256:e4bf19f15fd3218814a4e3c9e0064e1334bd8aa20d5984b9f1a0afd084f8cc00`, passed Core and JetStream durable delivery with both deadline expiry and cancelled shutdown waiters. All four cases verified closed admission, retained handlers, and successful retry after completion. The owned broker was removed; logs remain at `target/review-nats-broker.log`.
- Docker e2e passed **9/9**, including real etcd acceptance **10/10**, discovery lifecycle, multi-domain/membership failover, TCP/TLS, and claimed entity routing. Artifacts: `target/test-artifacts/review-20260908-r1-r10-api144/`. The first run's 8/9 result was an environment failure: its old Docker client could not invoke fault injection against Docker Desktop's minimum API. A temporary runner-only `DOCKER_API_VERSION=1.44` override allowed the complete rerun; it was removed afterward and the Compose file has no remaining diff.
- Docker simulation, seed **1**, passed **1/1**: `target/test-artifacts/review-20260908-sim-seed1/`.
- Docker model exploration, seed **1**, passed **2/2**, with each expected production-reducer test actually executing: `target/test-artifacts/review-20260908-model-exact-seed1/`. The original model profile used stale filters and incorrectly passed with zero tests. `testctl.rs` now uses the existing exact-test discovery guard and current names. The initial empty run is explicitly invalidated in `target/test-artifacts/review-20260908-model-seed1/validation-note.txt`. Final workspace Clippy and formatting were rerun after this harness correction; the corrected model profile validates its behavior.
- Docker runs used CI's pinned Rust **1.97.0** and separate Cargo volumes. All owned test containers, networks, and ephemeral volumes were cleaned up. Earlier focused native checks used Rust 1.98.0; final workspace gates used 1.97.0.

No throughput benchmark, Kubernetes/kind, extended chaos/soak/scale profile, or real MongoDB acceptance was run for this repair. These are not claimed as passing. Test logs and container artifacts are ignored local files under `target/`; the permanent regression sources and this record remain in the project.
