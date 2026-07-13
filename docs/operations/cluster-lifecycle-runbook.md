# Cluster discovery and member lifecycle runbook

Discovery records are reachability hints only. Never repair membership by editing DNS,
EndpointSlices, or the ConfigStore discovery document. The Coordinator member revision, exact node
incarnation, lease, and placement claims are authoritative.

## Signals and first checks

The dashboard at `docs/operations/dashboards/cluster-lifecycle.json` groups the operational signals
for discovery health, bootstrap attempts, join duration, lifecycle state, Coordinator reconciliation,
drain, removal reasons, and stale-incarnation rejection. All queries use bounded labels such as
cluster, provider, lifecycle state, and rejection/removal reason. Node IDs, addresses, operation IDs,
and certificate identities belong in traces and bounded inspection output, not metric labels.

For every incident, capture the exact cluster ID, node ID and incarnation, Coordinator term and
revision, discovery provider generations, selected bootstrap endpoint, and the current member
snapshot before taking a mutating action.

## Stuck Joining

1. Confirm the node is listening and remains `Joining`; external readiness must be closed.
2. Inspect each provider's last generation, target count, error, and stale age. An empty or stale
   candidate set explains reachability only; it does not imply missing membership.
3. Probe a candidate with the configured TLS hostname. Check cluster mismatch, expected-node
   mismatch, feature/version rejection, certificate URI identity, redirect loops, and RetryAfter.
4. Inspect the Coordinator for a `Joining` record with the node's exact incarnation and snapshot
   revision. A different live incarnation under the same node ID must be drained or force-removed;
   never rewrite discovery to make the new process inherit it.
5. If the join is retryable, repair discovery/network/TLS and let the supervised backoff continue.
   Protocol/catalogue rejection is terminal and requires a compatible full-stop deployment.

## Prolonged Degraded

1. Verify admission closed when the Coordinator session was lost.
2. Compare the node's last term/revision with the current leader. Same-term conflicting leaders are
   a safety incident; stop new changes and preserve artifacts.
3. Restore candidate reachability or the authenticated redirect path. Do not use discovery records
   as a member directory and do not extend placement claims from discovery.
4. Require a fresh atomic snapshot after a revision gap or leader rollover. Ready may reopen only
   after exact `MemberUp` acknowledgement for the installed revision.
5. Check claim deadlines. Expired shard/singleton claims stay fenced even if the data connection is
   healthy.

## Drain timeout

1. Inspect the drain operation ID, exact incarnation, remaining shard/singleton owners, handoff
   barriers, and actor stop failures.
2. Confirm the member is `Leaving` and new external/activation admission is closed.
3. Resume persisted handoffs after leader failover; do not bypass the old-claim invalidation proof.
4. If the shutdown deadline expires, use local force shutdown. Record that the Coordinator will
   remove the exact incarnation after lease expiry and that recovery remains fenced until then.

## Force removal

Use the authenticated Coordinator command with a unique operation ID, node ID, and the observed
expected incarnation. Repeat the same operation ID only with identical arguments. An incarnation
mismatch is protective: refresh the member snapshot and never force-remove a newer replacement.
After success, verify the revisioned `Removed(ForceRemoved)` event, revoked member lease, fenced
claims, closed exact Association, and DeathWatch termination.

## Evidence and replay

Docker and kind runs write manifests, structured traces, snapshots, logs, JUnit, and resource samples
under `target/test-artifacts/<run-id>`. Preserve the first failing artifact. Replay deterministic
failures with the command in `manifest.json`. Every Docker/kind path must finish through
`scripts/docker-image-lifecycle.sh`; only images labeled
`org.realm-labs.lattice.test=true` are eligible for retention cleanup.
