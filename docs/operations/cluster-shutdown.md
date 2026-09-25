# Cluster shutdown

Cluster shutdown is a durable operation, not a broadcast operating-system signal.
`LatticeService::shutdown()` still leaves and stops only the local node. Use
`request_cluster_shutdown(operation)` to initiate a whole-run stop, or
`shutdown_cluster(operation, deadline)` to request it, wait for the retained
result, and complete the initiating service's managed teardown.

The request future waits for a correlated coordinator acceptance reply, including
the canonical durable Closing operation. Authorization denial, stale-run requests
and unavailable coordinators are typed rejections, not completion timeouts. If
transport interruption or acceptance timeout makes the result uncertain, retry
with the same operation ID; cancelling the wait does not undo a committed stop.
Each membership session permits at most 32 pending administrative shutdown
requests, and cancellation/session teardown releases those local waiters. The
convenience `shutdown_cluster` deadline covers acceptance, completion waiting and
local managed teardown; expiry never implicitly forces a blocked Actor.

Remote initiation requires mTLS and an explicit `AdministratorAllowlist` grant
with `shutdown_cluster: true`. Candidate eligibility and discovery access do not
grant administrative permission. A privileged store-backed administrator can
initiate the same operation without a live initiating service.

## Application integration

Install `LatticeServiceBuilder::cluster_stop_hook(Arc<dyn ClusterStopHook>)` for
application-owned ingress and detached work. The hook must be idempotent and must
not report success until that work is quiescent. Framework-managed Actors and
their stop lifecycle are drained separately. The library does not install Ctrl+C
handlers; applications choose whether a signal means local leave or whole-run
shutdown.

On the Closing command, the node withdraws readiness and irreversibly closes
every hosted registry's business-admission gate before awaiting the hook or
Actor drain. Queued business turns cannot begin. A currently executing handler
may finish; exceeding the configured shutdown timeout or failing a hook records
a blocker, never an implicit force-stop. Control and reply paths remain alive
for progress reports. Repeated commands retry blocked work but do not rerun an
already successful local stop hook for the same operation.

For a single-process development deployment, the application can map its signal
handler to the cluster operation instead of local leave:

```rust,ignore
use lattice_model::run::{ControlOperationId, RunCompletion, RunPhase};
use tokio::time::{Duration, Instant};

tokio::signal::ctrl_c().await?;
let outcome = service.shutdown_cluster(
    ControlOperationId::new("developer-stop")?,
    Instant::now() + Duration::from_secs(60),
).await?;
match outcome.phase {
    RunPhase::Closed { completion: RunCompletion::Graceful, .. } => {}
    _ => return Err("cluster did not complete a graceful stop".into()),
}
```

Configure the mTLS administrator grant first; local signal handling does not bypass
authorization. Keep the same operation ID on retry. An offline Reset completion is
a different outcome and must not be reported as successful graceful shutdown.
The next startup requires the explicit guarded new-run action, not merely creating
another service against the Closed epoch.

## Evidence and cleanup

Every admitted exact node incarnation creates an unleased shutdown obligation
in the membership-admission transaction. A normal node leave discharges it only
after positive post-drain confirmation. Lease expiry does not erase it. The
number of unresolved obligations is bounded by the configured membership limit;
repeated unconfirmed failures require positive stop proof or explicit abnormal
reset rather than an unbounded historical journal.

Running-to-Closing freezes new membership and assignment writes. The Cluster
Coordinator builds a bounded fixed-revision manifest of the frozen obligations
and seals its identity digest before issuing stop commands. Reports identify the
exact epoch, operation and node incarnation. Missing reports, different
incarnations and failed stops cannot satisfy the manifest. Successful reports
cannot be downgraded by a late failure response. Evidence verification itself is
paged, with a durable verified-prefix cursor and digest, so coordinator takeover
resumes verification instead of scanning the whole cluster in one renewal tick.

After every sealed participant has positive stop evidence, Cleaning freezes
progress writers and deletes only inventoried runtime key families in bounded
transactions. Exact leadership/candidate guards remain until the final atomic
Closed and retained-result commit. Elections remain possible during Cleaning
so a replacement can resume unfinished cleanup. Definitions, candidate
authorization, monotonic terms and unrelated namespaces remain untouched.

## Waiting and result recovery

`cluster_shutdown_status()` is the local observed state, not a new authoritative
read. `wait_cluster_shutdown(epoch, deadline)` waits for the exact run. A timeout
ends that wait only: it does not undo Closing or silently reset the namespace.
Competing shutdown requests converge on the canonical operation already stored
for that run.

The completion record remains outside the cleaned run subtree. If the final
leader dies before delivering Closed, clients can recover the exact-epoch result:

- Etcd-backed discovery reads the bounded lifecycle/completion metadata directly,
  without reading membership, assignments or creating leases.
- Static or DNS discovery probes the configured bootstrap endpoints. A candidate
  can start as a read-only Closed-run observer and answer with the retained
  outcome without becoming leader or recreating runtime records.

Keep at least one discovery entry reachable while callers need confirmation, or
restart a read-only observer. If every process is physically stopped and the
client cannot read etcd, no network protocol can return a result until an entry
is restored. `lattice-admin inspect` also exposes the durable outcome. A `Closed { completion: Graceful }`
result confirms the stop/evidence/cleanup contract, not remote OS process exit.
An explicit abnormal reset produces a distinct `RunCompletion::Reset` receipt;
it must not be interpreted as proof of graceful application shutdown.

Starting another run is a separate guarded administrative action. An unfinished
or blocked graceful stop must not be bypassed by normal startup. Use the
[abnormal reset procedure](cluster-reset.md) only with explicit stopped-deployment
confirmation and independent administrative credentials.
