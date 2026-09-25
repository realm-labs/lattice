# Cluster Control-Plane Implementation Contracts

> Status: proposed implementation contracts for work packages 2/3; no runtime changes are implemented by this document.
> Parent plan and completion tracking: [control-plane memo](cluster-control-plane-memo.md), sections 9.1-9.6.
> Evidence: [preparation inventory](cluster-control-plane-preparation.md). W1 is complete; these contracts do not mark P0 or W2/W3 complete.

This document makes the next delivery slices reviewable in terms of state transitions, transaction predicates, and failure tests. It supplements the memo rather than introducing another implementation backlog. Items explicitly marked **behavior approval** or **validation gate** must be resolved before the affected serving path is enabled. Examples are conceptual contracts, not finalized Rust signatures.

## 1. Shared transaction and run boundary

Every authoritative mutation carries an exact cluster/run identity and operation-specific expected state. The store assembles the required comparisons; a caller's preflight check is not a substitute.

| Operation | Required predicates, in addition to record revisions |
|---|---|
| Join, new assignment, transfer admission | Exact current epoch, `Running`, current leader/term and candidate authorization, exact eligible member incarnations |
| Grant issuance | Same authority predicates, exact claim/lease/slot generation and session; a drain-only renewal additionally requires the matching close/transfer operation |
| Election | Current epoch, permitted lifecycle phase, current candidate authorization and online incarnation, absent leader and previous term revision |
| Transfer progress | Current leader authorization, exact slot/transfer/set version, permitted lifecycle phase and progress transition |
| Shutdown progress | Current epoch, `Closing`, exact close operation and current authorized leader |
| Cleanup batch | Exact epoch/operation, cleanup ownership, frozen write phase and explicit allowed key family |
| New run | `Closed`, confirmed cleanup completion, no active cleanup/reset, expected framework/configuration contract and lifecycle revision |

Advance the run epoch atomically with new-run initialization. Retain monotonic epoch and scope-term history outside the run subtree. Neither an empty member list nor a missing leader initializes a new run. An uncertain transaction result is resolved by reading the exact operation/epoch, not blindly creating another operation.

Freeze the persistent definition/configuration revision used by a running group; incompatible mapping changes are not an incidental reload. Store an explicit disposition for every old counter, revision, settings record, and deduplication record before replacing its writer. Configuration-store data and worker-ID allocation/history remain outside runtime cleanup.

Epoch, term, assignment generation, session incarnation, and activation identity have distinct roles. Do not order unrelated slots' tokens or claim that a framework-local tuple automatically fences an external database. A future external adapter must specify its comparison and installation contract.

## 2. Renewal freshness and authority retirement

### 2.1 Proposed request-correlated renewal

Replace receipt-time TTL installation with owner-initiated, request-correlated renewal. Initial activation uses the same freshness exchange; unsolicited messages cannot open admission.

1. The owner records local monotonic request start `s`, a fresh request ID, and its exact run/session/slot authority. Bound outstanding requests and their lifetimes.
2. The Coordinator confirms backing-lease validity and then performs an authoritative guarded grant commit. The commit binds the request, exact owner/session, assignment generation, leader authorization and grant sequence. A successful keepalive alone is not permission to issue a grant.
3. The reply carries the correlated identity and a conservative usable duration `d`, capped by a configured maximum grant interval `G`. It must not assert more backing validity than the lease observation supports after elapsed time and clock uncertainty are deducted.
4. The owner computes `deadline = s + d`, never `receive_time + d`. Reject an already elapsed result, unknown/consumed request, old session, wrong generation, stale sequence, or result for a retired instance. Processing and network delays consume the request's existing interval.
5. A timeout, ambiguous keepalive/commit, duplicate reply, or reconnect does not extend the installed deadline. Retry with a new bounded request; the Coordinator must revalidate authority. No reliable-outbox replay may manufacture fresh validity.

Replies must follow the successful guard commit. On takeover, recover issued-grant/claim facts before renewing; do not mint authority from a cached member description. Where batching is needed, retain per-slot predicates and bounded transaction/request sizes rather than assuming one successful slot validates the whole batch.

**Validation gate:** define the lease adapter's observation point, elapsed-time deductions, timestamp resolution and maximum clock-rate error before choosing `d`. A TTL reported at a remote observation point is not automatically the TTL remaining when its reply arrives. This is a proposed protocol, not a proof that the current lease adapter supplies these guarantees.

### 2.2 Expiry, early revocation, and replacement

Deleting a claim does not retract a previously delivered grant. Candidate revocation likewise stops new guarded issuance but does not instantly stop old owner execution.

- Graceful replacement may use an exact-generation acknowledgement that admission has closed and the required old processing has stopped, together with the existing transfer barrier proof.
- Without that evidence, establish a store-guarded boundary after which the old authority cannot issue further valid grants. A replacement Coordinator then waits a conservative full maximum outstanding grant interval, including clock uncertainty, from its local observation of that boundary. A restart of this wait starts the full interval again; never restore a process-local timestamp as portable time.
- A late response committed before the boundary remains bounded by the original owner's request start and `G`; it cannot restart a full interval at receipt.
- Claim absence, lease expiry, or a disconnected session alone must not bypass this replacement holdoff or the transfer protocol. New grants under a replacement generation must not overlap old permitted execution admission.
- Waiting out grants proves loss of permission for new work under the timing contract, not termination of in-flight external requests. Preserve the separate failure-recovery and external-fencing limitations.

The holdoff is deliberately conservative. Reducing it requires validated durable evidence, not an estimated watch delay. Persist the replacement phase and its reason; no wall-clock timestamp alone is completion evidence.

**Validation gate:** supported platforms need a time source that accounts for suspension, or an independently reliable resume-invalidates-authority mechanism before business execution resumes. Plain use of a monotonic API is not a cross-platform suspension proof. Specify clock drift assumptions for both grant deadlines and replacement waiting. If the deployment cannot satisfy them, do not advertise timer-based exclusive ownership; fail closed or require a separately designed external fencing contract. Arbitrary pauses inside already-running application work remain outside admission control.

### 2.3 Shared admission and retirement boundary

Use one distributed authority cell per exact slot generation, shared by entity and singleton integration. Loading occurs outside its synchronization boundary; final publication checks and authority revocation serialize against that same cell. Revocation closes the shared gate before enumerating Actors for asynchronous cleanup. Cached references and queued messages recheck the gate at execution admission.

**Behavior approval — recommended:** once an instance enters retirement after expiry or revocation, it never reopens. Reconnection may authorize fresh activation, but does not revive the retired instance. Transient loss repaired before expiry need not retire it. A new local activation waits for old-instance retirement or remains explicitly blocked/quarantined; stop failure is not permission to overlap local instances.

Do not promise preemption of an already-running handler. On unexpected authority loss, close new execution immediately and drive the defined cancellation/retirement path; retain incomplete stop diagnostics. Stop callbacks may themselves issue external writes and must not be presented as safe solely because the mailbox is closed.

Required focused tests:

- Delayed/reordered/duplicate replies, ambiguous renewal, and Coordinator pause between lease confirmation, commit, and reply.
- Leadership/eligibility loss before the guard commit; reply delivery after replacement begins; maximum-interval holdoff across takeover.
- Owner suspension beyond the deadline, use before a periodic tick, and reconnect with an expired or retired generation.
- Loader paused across revocation, cached-reference execution, singleton/entity parity, and stop failure without reopened admission.

## 3. Candidate authorization and removal

### 3.1 Identity and storage

Bind a stable candidate principal to the authenticated control/transport identity. A supplied node name or seed address is not authorization. Map deployment credentials to that principal through an explicit administrative policy; insecure local-test identity must not silently become production authorization.

Use separate bounded records:

- Persistent per-scope candidate authorization with an enable/disable state and monotonic authorization generation.
- A small per-scope membership revision and eligible count, updated with authorization changes in the same transaction.
- Leased online registration containing the exact process incarnation, endpoint, and authorization generation.
- Leader records and write guards carrying the same authorization generation and incarnation.

Keep the authorization generation across removal/re-addition. An old registration, election attempt, or delayed cleanup cannot become valid because the same principal is enabled again. Registration deletion compares the exact incarnation/generation.

### 3.2 Transitions and administration

```text
Authorized -> Registered -> Campaigning -> Recovering -> Serving
RevocationCommitted -> AuthorityWritesDisabled -> LeadershipReleased
                    -> RegistrationWithdrawn
```

An administrative operation may be submitted through an authenticated Coordinator, or executed by an explicitly privileged store-backed admin client when that leader is unavailable. Ordinary nodes do not acquire etcd write credentials. Both entry points use the same guarded store operations and bounded operation IDs/fingerprints.

Enable first commits authorization, then registers the process; it does not promise readiness or election. Removal atomically disables authorization, advances the scope revision and count, and records its operation result. Serialize the last-eligible-candidate check with that mutation: concurrent removals cannot each observe the other as the remaining candidate. Never equate eligible count with online redundancy.

Every election and authoritative write compares the current enabled authorization generation. A delayed process therefore cannot commit new authority after revocation, even before receiving a notification. An unavailable leader may remain registered/leased temporarily; report these as separate progress states rather than claiming its process exited. Re-addition starts a fresh generation and requires fresh registration/election.

During `Closing`, keep candidates needed for takeover. Reject ordinary last-candidate removal; final control-plane shutdown follows the separate `Closed` transition, not a blanket bypass of candidate checks.

Required focused tests: concurrent last-candidate removal, absent leader administration, revoke/re-add races, stale registration deletion, revoked leader commit, independent scopes, and replacement discovery through stale seeds. Permissions and identity mapping require integration tests, not only in-memory store tests.

## 4. Transfer publication, barriers, and rebalance capacity

### 4.1 Publication and admission

Admit a transfer in one bounded guarded transaction: compare lifecycle, leader authorization, slot generation, target eligibility, absent active transfer, and capacity revisions; write the slot reference, transfer metadata and durable reservation together. Unstarted policy proposals stay in memory. Use group and per-node budgets; cross-group global quotas remain deferred.

```text
Reserved/Building -> Sealed -> Invalidating -> Draining
                  -> ReplacingAuthority -> Starting -> Completed
```

Blocked/stop-failed outcomes remain explicit. These phases refine the existing handoff machine rather than replacing all failure states with a generic owner swap. Recovery reconstructs capacity from durable reservations before new admissions. Lowered limits stop new transfers without abandoning existing ones.

### 4.2 Participant-set construction

Capture a fixed source revision and record it with the transfer/set version. Read all source pages at that revision. Persist participant identities in bounded batches together with an idempotent build cursor, count, and canonical rolling manifest digest. A new leader may resume only the same revision/build version.

Seal only after the source scan finishes and the persisted manifest has been verified. No invalidation/drain effect that depends on the set may start while it is `Building`. If the source revision is no longer readable before sealing, discard/rebuild the unsealed set under a new set version without admitting handoff effects; do not silently switch remaining pages to a newer snapshot.

Snapshot construction also needs an admission boundary: sessions joining after the snapshot must not gain old-generation routes/authority and escape the barrier. Bind snapshot installation and session catch-up to the transfer fence revision. Define this guard for every relevant session path before enabling split barriers.

Once sealed, the participant identity set is immutable. Do not lease-delete evidence records. Acknowledgements compare exact run, transfer, set version and participant incarnation; duplicate transitions are idempotent. An explicit session-fencing proof is a distinct terminal outcome, not a synthesized stop acknowledgement. A missing participant is corruption/incomplete evidence, never success.

Completion uses a verified sealed manifest and guarded per-participant terminal transitions. A remaining-count summary may be maintained atomically, but cannot replace manifest construction or recover missing records. Only the matching completed barrier and source-stop/authority-loss evidence permit advancing the slot. The grant holdoff from section 2 still applies when no graceful source-stop proof exists.

### 4.3 Bounds and reclamation

Start with the memo's proposed 4 KiB encoded-value ceiling. Finalize key size, transaction operations/bytes, page bytes, participant cardinality, and retained-operation count from worst-case codec tests before deployment. Large administrative diagnostics belong outside authority records.

Transfer completion and reservation release are one guarded transition. Reclaim evidence only after live slot/authority references no longer depend on it and deduplication obligations are satisfied. Keep bounded terminal retry evidence with an explicit retention horizon; old requests outside it return an expired/unknown outcome, not silent re-execution.

Required focused tests: crash after each batch, compaction during build, leader loss before/after sealing, joining sessions during construction, missing/stale acknowledgements, double admission, budget lowering, failover accounting, byte limits, and repeated transfer reclamation. Fixed-revision reads and resume behavior must be verified against real etcd as well as the memory backend.

## 5. Cluster shutdown, cleanup, and reset

### 5.1 Request and close boundary

Separate initiating/querying a durable operation from waiting for it. Conceptual operations are `request_shutdown(operation_id)`, `shutdown_status(handle)`, and `wait_shutdown(handle, deadline)`; a convenience `cluster.shutdown()` may combine them. Final Rust names remain an API-design task.

Atomically publish `Running -> Closing` and the shutdown operation. Authoritative admission/allocation transactions compare that same lifecycle record, so a stale Group Coordinator cannot allocate after the boundary. Existing work may continue until participants receive the command; the etcd commit is not simultaneous execution preemption across nodes.

The same operation ID and parameters are idempotent; reuse with different parameters is rejected. A competing request for an already-closing run returns the canonical active operation. Caller cancellation/timeouts stop waiting only; there is no return from `Closing` to `Running`. Results distinguish waiting timeout, blocked progress and an unknown transaction outcome.

### 5.2 Participant and local-stop semantics

Construct and seal shutdown manifests using the fixed-revision/build protocol in section 4, covering groups, admitted exact node incarnations, and unresolved ownership/transfer obligations. Live membership alone is insufficient: a lease-expired owner can still leave an unresolved stop obligation. Stop reports for previously departed nodes must remain usable evidence until their safety obligation ends.

Reject late business joins and new assignments. Allow authenticated recovery/control participants that can advance the same close operation. An already-leaving node switches to cluster-stop handling rather than starting more migrations. Classify each in-flight transfer: close any already-authorized target as a participant; abandon an unstarted target only through a guarded transition; never promote a new owner merely to finish a rebalance during shutdown.

**Behavior approval — recommended initial drain contract:**

- Close library execution admission and activation publication; withdraw readiness and notify application ingress integration.
- Do not begin queued business messages or deferred business continuations. Pending requests complete with a shutdown/closed error where their reply channel remains available; tell acceptance does not imply processing.
- Allow the currently executing handler to finish within the configured drain deadline. Track framework-managed child/background work and lifecycle callbacks in local stop completion; detached application tasks remain the application's responsibility through an explicit shutdown hook.
- Do not silently force-stop on deadline or stop-hook failure. Record a blocker and keep business admission closed. Necessary reply/control/report paths remain available.
- If bounded authority renewal is still required for admitted draining work, permit only exact existing-owner drain renewal tied to the close operation. It must not reopen business admission or authorize replacement. Loss of that authority switches to unexpected-authority-loss retirement, not successful graceful completion.

Local completion reports mean the defined execution/stop contract completed and the exact ownership was released or invalidated. Unreachable nodes and expired leases are not positive graceful-stop reports. External writes remain subject to their separate fencing contract.

### 5.3 Cleanup and completion

After sealed participant evidence and group obligations are complete, enter a durable cleanup subphase. Freeze ordinary progress writers to the key families being reclaimed; only takeover, cleanup bookkeeping and retained completion may continue. Each bounded deletion transaction compares lifecycle/operation and cleanup leadership, deletes explicit inventoried run keys, and advances its cursor atomically. Unknown key families stop cleanup for inspection.

Retain leadership guards, cleanup cursors and completion evidence until their last use. A final bounded transaction commits `Closed` plus a bounded retained result after verifying that all cleanup scopes completed. Any remaining control-only leased records are non-serving and must be explicitly inventoried for final deletion or lease expiry; success cannot conceal residual assignment/transfer data. No candidate may campaign for that closed epoch or reconstruct ordinary scheduling.

A retained result outside the reclaimed run supports query after initiator loss. Remote control tasks may observe `Closed` and tear down asynchronously; cluster shutdown success does not prove every host process has exited. The initiating convenience API waits for its own managed teardown before returning. A combined single-process deployment must perform this wait outside the runtime it is destroying.

**Behavior approval:** graceful success requires the specified application stop evidence, invalid ownership, completed runtime cleanup and confirmable result. It does not require operating-system process termination or remote control-task acknowledgements after `Closed`. A missing graceful stop report remains a blocker, even if failure recovery could otherwise reassign that shard.

### 5.4 Abnormal reset and restart exclusion

Reset is separately authorized and never disguised as successful graceful shutdown. Require exact cluster/run selection, inspect/dry-run output, and explicit operator confirmation that the old deployment is stopped. A reset lock alone is insufficient: persist a reset/maintenance operation fence that every election, authority write, close continuation and new-run transaction checks. Losing the lock does not reopen the run; an authorized administrator resumes the same operation.

Deleting keys cannot prove partitioned application code stopped. Abort on conflicting active authority unless the separately approved reset procedure explicitly resolves it; do not treat an expired lease as operator confirmation. Apply paginated deletion only to the reviewed runtime families. Preserve definitions, authorization/configuration, epoch/term history and external worker-ID safety data.

Finalize a retained `ResetCompleted` result and a closed epoch. Only a separate guarded startup may increment the epoch and establish a new run/framework marker after retained-format/configuration validation. Existing `Running` may recover the same run; `Closing` or reset-in-progress cannot be bypassed by normal startup. No automatic reset on Ctrl+C, empty membership, or framework mismatch.

Required focused tests: request retry/conflict, caller loss, Group Coordinator allocation racing `Closing`, multiple groups, join/leave/transfer races, long handler and queued ask, background/stop failure, lost reports, leader failure per cleanup batch, final result retrieval, reset/startup races, unrelated-prefix preservation, and repeated stop/restart without old-run resurrection.

## 6. Next implementation slice and remaining gates

Start with W3.1 plus W2.1: lifecycle/epoch and operation guard types, namespace codecs, bounded discovery hints, shared provider/cache and required-scope readiness. Static/DNS providers keep bootstrap fallback; etcd discovery reads only lifecycle/framework hints and leader/candidate endpoints. A healthy session does not require periodic new probe connections. No discovery result is serving authority.

Before migrating each writer, extend the inventory with its exact predicates and retained/deleted fields. Keep all dependent producers and consumers aligned; do not claim a deployable intermediate protocol. Then implement candidate/grant/retirement, transfer/rebalance, and shutdown/reset in the memo's coupled order.

The following remain gates, not silently completed decisions:

1. Approval of the three business-behavior choices above: no revival, queued/in-flight drain treatment, and graceful completion semantics.
2. Timing/lease proof and platform suspension support; candidate credential-to-principal policy; concrete public API/error types.
3. Final inventory/byte budgets, fixed-revision and guarded-transaction acceptance tests, and representative grant/transfer workload measurements.

Do not rerun the full workspace test suite merely to begin this work. Use focused deterministic race tests, isolated real-etcd acceptance, touched-crate checks, and structure checks as applicable. The new protocols are not validated by W1's passing tests.
