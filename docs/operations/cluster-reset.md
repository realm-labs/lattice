# Offline Cluster Run Maintenance

`lattice-admin` uses explicitly privileged etcd credentials. Its reset and start-run
commands are offline maintenance operations; candidate-policy changes can be
performed while the run is active. The tool does not start a
Coordinator, signal processes, or prove that old application work has stopped.
Remote node administration and end-to-end graceful cluster shutdown are separate
protocols. Never run reset as automatic recovery for an empty membership list.

## Credentials and targeting

Use separate etcd administrative credentials, not ordinary node credentials.
Supply a client certificate with `--ca`, `--cert`, and `--key`, or provide both
`LATTICE_ADMIN_ETCD_USER` and `LATTICE_ADMIN_ETCD_PASSWORD` through your secret
manager. Passwords are not command-line arguments. Configure the etcd role to
limit access to the intended deployment namespace.

HTTPS is required except for explicitly permitted loopback development endpoints
with `--allow-insecure-local`. Each operation requires an exact `--namespace` and
nonzero `--epoch`; a stale epoch is rejected. The framework marker must match the
tool's Lattice version. This tool does not migrate legacy unscoped schemas or
upgrade a foreign framework marker.

## Inspect and preview

```sh
cargo run -p lattice-ops --bin lattice-admin -- \
  --endpoints https://etcd.example:2379 --namespace /lattice/dev --epoch 1 inspect

cargo run -p lattice-ops --bin lattice-admin -- \
  --endpoints https://etcd.example:2379 --namespace /lattice/dev --epoch 1 \
  reset --operation reset-dev-1
```

`inspect` and reset without `--execute` are read-only. They print the current
lifecycle and runtime-key count; the count is an observation, not a deletion
plan guaranteed to remain unchanged. For PowerShell, put commands on one line
or replace shell continuation backslashes with PowerShell continuation syntax.

## Execute and resume reset

1. Stop **all** processes belonging to the target deployment, including
   partitioned nodes and candidates. Do not restore saved process identities
   from old VM snapshots. Key deletion cannot stop an in-flight external write.
2. Inspect the exact namespace and epoch, then use the same operation ID for
   every retry, including retries after an uncertain network result.
3. Add `--execute --confirm-stopped /lattice/dev@1` to the reset command.

Reset first commits a durable maintenance fence. Ordinary coordinator writes
and elections cannot pass that fence. Cleanup uses at most 32 keys per transaction
and rejects live leased records or unrecognized key families. The fence remains
in place on errors: allow stopped processes' leases to expire, inspect the
reported blocker, and resume the same operation. Do not delete the fence manually.

`--batch-keys` accepts 1 through 32. `--maximum-batches` bounds one invocation
(default 256); reaching it leaves a resumable operation rather than pretending
cleanup completed. A restart scans from the first remaining key. No local cursor
file is needed. An unknown family requires explicit operator investigation;
there is no broad-prefix force-delete option.

Only `runs/<epoch>/` and its reviewed runtime families are eligible. Preserve
definitions, framework metadata, monotonic terms, worker-ID history and unrelated
application data. Completion requires an atomic empty-range check, then records
`Closed` with `completion: Reset`, not `Graceful`. One last-completion record is
retained outside the run subtree. Longer audit retention belongs in external
operator logs; old completion queries are not an unbounded history API.

## Start a new run

After confirmed cleanup, preview `start-run` using the same namespace and old
epoch; add `--execute` to commit the next epoch. Startup does not infer this
transition from absent members or leaders. Existing store handles stay bound to
the old run and must be recreated. Scope terms and persistent type definitions
survive the transition; runtime counters and membership start fresh.

This command accepts either a completed graceful stop or a completed abnormal
reset. It does not replace the [multi-node graceful stop protocol](cluster-shutdown.md).
## Candidate provisioning and live changes

Every election scope must have explicitly provisioned eligible candidates.
For a brand-new empty namespace, deployment bootstrap first calls
`CoordinatorLeaseStore::ensure_framework()` with the intended storage limits;
a capable host started without authorization can also initialize and remain in
standby. The CLI deliberately refuses an uninitialized or foreign namespace.
Starting a capable Coordinator host does not grant it candidate permission.
Use `candidates` to inspect the current scope revision and online registrations:

```sh
lattice-admin --endpoints https://etcd.example:2379 --namespace /lattice/dev --epoch 1 candidates
lattice-admin --endpoints https://etcd.example:2379 --namespace /lattice/dev --epoch 1 candidate --node-id coordinator-a --expected-revision 0 --operation enable-a
```

The second command is a preview. Add `--execute` to enable the candidate.
Add `--group gameplay` to target a Group Coordinator instead of the Cluster
Coordinator. Add `--disable` to revoke eligibility. Read the latest scope revision
before each new change; concurrent modifications reject stale expected revisions.
Reuse the exact request and operation ID to retry an uncertain result. Only the
latest scope mutation receipt is retained; after intervening changes, inspect
the current policy before deciding on a new request.

Revocation removes the authorization and online registration atomically. Elections
and all authoritative writes check that authorization, even if the old process
has not yet noticed revocation. Re-addition creates a newer authorization
generation; delayed old deregistration cannot delete its replacement. The
scope-level monotonic revision survives removal, without accumulating one
permanently disabled record for every historical candidate.

Removal of the last eligible candidate is rejected transactionally. Eligibility
does not imply that a candidate is reachable or ready: provision and verify
replacement capacity before withdrawing a serving node. Hosts must already be
configured as capable of the requested scope, with etcd connectivity/credentials.
Changing eligibility does not remotely install configuration on arbitrary workers.

There are at most 64 eligible candidates per scope. Authorization survives run
cleanup; online registration is leased and belongs only to the current run.
Remote management over remoting uses `ChangeCandidate` and `CandidateChanged`
with certificate-verified identity and an explicit administrator allowlist;
the CLI instead uses separate privileged etcd credentials.
